"""
GCSArchiveDagBundle: stage an Airflow Dag bundle from one ``.tar.gz`` object.

This is the vendored stand-in for
``airflow.providers.google.cloud.bundles.gcs.GCSArchiveDagBundle`` until that
class is installed with the Google provider (apache/airflow#75566, split the
same way as ``S3ArchiveDagBundle`` in apache/airflow#73015). ``GCSDagBundle``
stays the prefix path. ``archive_object`` is not a flag on that class.

Staging is one metadata read, then one download pinned with
``if_generation_match`` to the generation just read. The ETag marker lives
beside the bundle directory (``.name/etag``). An unchanged ETag skips the
download only when that directory still contains files, so wiping the bundle
directory (a fresh worker, or this demo's benchmark) restages. Leftover
``archive-staging-*`` directories are removed from the same state directory.
Unpacking uses ``tarfile`` ``filter="data"`` and requires Python 3.11.4 or
later.
"""

from __future__ import annotations

import os
import shutil
import tarfile
import tempfile
from pathlib import Path

import structlog
from google.api_core.exceptions import NotFound

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


def _require_tar_data_filter() -> None:
    if hasattr(tarfile, "data_filter"):
        return
    raise RuntimeError("Archive Dag bundles require Python 3.11.4 or later (tarfile filter='data').")


def _extract_archive(archive_path: Path, unpack_dir: Path) -> None:
    """Unpack a bundle archive without writing outside ``unpack_dir``."""
    _require_tar_data_filter()
    with tarfile.open(archive_path, "r:*") as tar:
        tar.extractall(unpack_dir, filter="data")


def _archive_state_dir(bundle_dir: Path, bundle_name: str) -> Path:
    return bundle_dir.parent / f".{bundle_name}"


def _archive_etag_path(bundle_dir: Path, bundle_name: str) -> Path:
    return _archive_state_dir(bundle_dir, bundle_name) / "etag"


class GCSArchiveDagBundle(BaseDagBundle):
    """
    GCS Dag bundle staged from a single ``.tar.gz`` object.

    The archive is the only source of the Dags. Members are extracted relative
    to the bundle root, for example ``tar -C <dags_dir> -czf dags.tar.gz .``.
    Requires Python 3.11.4 or later.

    :param gcp_conn_id: Airflow connection ID for GCS. Defaults to
        ``GoogleBaseHook.default_conn_name``.
    :param bucket_name: The name of the GCS bucket containing the archive.
    :param archive_object: Object name of the ``.tar.gz`` archive holding the Dag files.
    """

    supports_versioning = False

    def __init__(
        self,
        *,
        gcp_conn_id: str = GoogleBaseHook.default_conn_name,
        bucket_name: str,
        archive_object: str,
        **kwargs,
    ) -> None:
        _require_tar_data_filter()
        self.archive_object = archive_object
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.bucket_name = bucket_name
        self.gcs_dags_dir: Path = self.base_dir

        log = structlog.get_logger(__name__)
        self._log = log.bind(
            bundle_name=self.name,
            version=self.version,
            bucket_name=self.bucket_name,
            archive_object=self.archive_object,
            gcp_conn_id=self.gcp_conn_id,
        )
        self._gcs_hook: GCSHook | None = None

    def _initialize(self):
        with self.lock():
            if not self.gcs_dags_dir.exists():
                self._log.info("Creating local Dags directory: %s", self.gcs_dags_dir)
                os.makedirs(self.gcs_dags_dir)

            if not self.gcs_dags_dir.is_dir():
                raise NotADirectoryError(f"Local Dags path: {self.gcs_dags_dir} is not a directory.")

            try:
                self.gcs_hook.get_bucket(bucket_name=self.bucket_name)
            except NotFound:
                raise ValueError(f"GCS bucket '{self.bucket_name}' does not exist.")
            self.refresh()

    def initialize(self) -> None:
        self._initialize()
        super().initialize()

    @property
    def gcs_hook(self):
        if self._gcs_hook is None:
            try:
                self._gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
            except AirflowException as e:
                self._log.warning("Could not create GCSHook for connection %s: %s", self.gcp_conn_id, e)
        return self._gcs_hook

    def __repr__(self):
        return (
            f"<GCSArchiveDagBundle("
            f"name={self.name!r}, "
            f"bucket_name={self.bucket_name!r}, "
            f"archive_object={self.archive_object!r}, "
            f"version={self.version!r}"
            f")>"
        )

    def get_current_version(self) -> str | None:
        """Return the current version of the Dag bundle. Currently not supported."""
        return None

    @property
    def path(self) -> Path:
        """Return the local path to the Dag files."""
        return self.gcs_dags_dir

    def view_url(self, version: str | None = None) -> str | None:
        """
        Return a URL for viewing the archive object in GCS.

        Versioning is not supported. This method is deprecated and will be
        removed when the minimum supported Airflow version is 3.1. Use
        ``view_url_template`` instead.
        """
        return self.view_url_template()

    def view_url_template(self) -> str | None:
        """Return a URL for viewing the archive object in GCS. Versioning is not supported."""
        if self.version:
            raise ValueError("GCS url with version is not supported")
        if hasattr(self, "_view_url_template") and self._view_url_template:
            return self._view_url_template
        domain = GoogleBaseHook.get_high_value_cookie_domain()
        # https://console.cloud.google.com/storage/browser/_details/<bucket>/<object>
        return (
            f"https://console.cloud.{domain}/storage/browser/_details/"
            f"{self.bucket_name}/{self.archive_object}"
        )

    def _archive_state_dir(self) -> Path:
        return _archive_state_dir(self.gcs_dags_dir, self.name)

    def _archive_etag_path(self) -> Path:
        return _archive_etag_path(self.gcs_dags_dir, self.name)

    def _bundle_dir_has_files(self) -> bool:
        return self.gcs_dags_dir.is_dir() and any(path.is_file() for path in self.gcs_dags_dir.rglob("*"))

    def _remove_stale_archive_staging_dirs(self) -> None:
        state_dir = self._archive_state_dir()
        if not state_dir.is_dir():
            return
        for leftover in state_dir.iterdir():
            if leftover.is_dir() and leftover.name.startswith("archive-staging-"):
                shutil.rmtree(leftover, ignore_errors=True)

    def refresh(self) -> None:
        """Refresh the Dag bundle by downloading and unpacking the archive object."""
        if self.version:
            raise ValueError("Refreshing a specific version is not supported")

        with self.lock():
            self._remove_stale_archive_staging_dirs()
            blob = self.gcs_hook.get_conn().bucket(self.bucket_name).blob(self.archive_object)
            try:
                blob.reload()
            except NotFound:
                raise ValueError(
                    f"GCS archive 'gs://{self.bucket_name}/{self.archive_object}' does not exist."
                )
            etag = blob.etag or ""

            marker = self._archive_etag_path()
            if etag and marker.is_file() and marker.read_text() == etag and self._bundle_dir_has_files():
                self._log.debug(
                    "Dag bundle archive 'gs://%s/%s' is unchanged (ETag %s), skipping staging",
                    self.bucket_name,
                    self.archive_object,
                    etag,
                )
                return

            state_dir = self._archive_state_dir()
            state_dir.mkdir(parents=True, exist_ok=True)
            staging_dir = Path(tempfile.mkdtemp(dir=state_dir, prefix="archive-staging-"))
            try:
                archive_path = staging_dir / "_bundle_archive"
                download_kwargs: dict[str, int] = {}
                if blob.generation is not None:
                    download_kwargs["if_generation_match"] = blob.generation
                blob.download_to_filename(os.fspath(archive_path), **download_kwargs)

                unpack_dir = staging_dir / "unpacked"
                unpack_dir.mkdir()
                _extract_archive(archive_path, unpack_dir)
                archive_path.unlink()

                # The unpacked tree replaces the bundle directory only after
                # extract succeeds. Between the two renames the directory is
                # missing. A failed second rename puts the previous tree back.
                # Readers do not take this lock, so that gap is visible.
                backup_dir = staging_dir / "previous"
                if self.gcs_dags_dir.exists():
                    self.gcs_dags_dir.rename(backup_dir)
                try:
                    unpack_dir.rename(self.gcs_dags_dir)
                except Exception:
                    if backup_dir.exists():
                        backup_dir.rename(self.gcs_dags_dir)
                    raise

                if etag:
                    self._archive_etag_path().write_text(etag)

                self._log.info(
                    "Staged Dag bundle from archive 'gs://%s/%s' (%s bytes) to %s",
                    self.bucket_name,
                    self.archive_object,
                    blob.size,
                    self.gcs_dags_dir,
                )
            finally:
                shutil.rmtree(staging_dir, ignore_errors=True)
