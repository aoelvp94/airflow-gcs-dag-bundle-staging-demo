"""
GCSArchiveDagBundle: stage an Airflow DAG bundle from ONE compressed archive
object instead of downloading the bucket prefix one object at a time.

The stock ``GCSDagBundle`` (apache-airflow-providers-google) refreshes with
``GCSHook.sync_to_local_dir()``, a serial per-object loop. For a bundle of N
small files that costs one media download per file on every fresh worker —
which under KubernetesExecutor means on every task start. When the local file
already exists, the same loop also issues a metadata GET per object
(``blob.reload()``) before deciding the bytes have not changed.

This subclass changes only the staging strategy:

    GET  <archive_object> metadata   (one object, skipped when the md5 matches)
    GET  <archive_object> bytes
    verify + unpack to a temp dir (path-traversal safe)
    atomic rename swap into place

``archive_object`` is mutually exclusive with the parent's ``prefix``: the
archive is the only source of the DAGs, so there is no second copy to keep in
sync and no fallback to the per-object sync. That matches the shape of the S3
archive bundle after review of apache/airflow#73015: a fallback turns one
failed GET into ~2N requests against the same prefix, from every component
staging at that moment.
"""

from __future__ import annotations

import shutil
import tarfile
import tempfile
from pathlib import Path

from google.api_core.exceptions import NotFound

from airflow.providers.google.cloud.bundles.gcs import GCSDagBundle

VERSION_MARKER = ".staged-archive-md5"


class GCSArchiveDagBundle(GCSDagBundle):
    """
    DAG bundle that stages from a single ``.tar.gz`` object in GCS.

    :param archive_object: Object name of the archive
        (e.g. ``archive/dag-bundle.tar.gz``). Members are extracted relative
        to the bundle root, so pack the contents of the DAGs directory rather
        than the directory itself
        (``tar -C dags -czf dag-bundle.tar.gz .``).
    :param prefix: rejected. The parent uses it as the per-object staging
        source, which this bundle replaces rather than supplements.
    """

    def __init__(self, *, archive_object: str, **kwargs) -> None:
        if kwargs.get("prefix"):
            raise ValueError(
                "GCSArchiveDagBundle takes either 'prefix' or 'archive_object', not both: "
                "the archive is the only staging source."
            )
        super().__init__(**kwargs)
        self.archive_object = archive_object
        self._log = self._log.bind(archive_object=self.archive_object)

    def _initialize(self):
        # The parent validates that `prefix` exists before refreshing. With no
        # prefix and no fallback, the archive is what has to be there.
        with self.lock():
            if not self.gcs_dags_dir.exists():
                self.gcs_dags_dir.mkdir(parents=True, exist_ok=True)
            if not self.gcs_dags_dir.is_dir():
                raise NotADirectoryError(f"Local Dags path: {self.gcs_dags_dir} is not a directory.")
            try:
                self.gcs_hook.get_bucket(bucket_name=self.bucket_name)
            except NotFound:
                raise ValueError(f"GCS bucket '{self.bucket_name}' does not exist.")
            blob = self.gcs_hook.get_conn().bucket(self.bucket_name).blob(self.archive_object)
            if not blob.exists():
                raise ValueError(
                    f"GCS archive 'gs://{self.bucket_name}/{self.archive_object}' does not exist."
                )
            self.refresh()

    def refresh(self) -> None:
        if self.version:
            raise ValueError("Refreshing a specific version is not supported")
        with self.lock():
            self._stage_from_archive()

    def _stage_from_archive(self) -> None:
        blob = self.gcs_hook.get_conn().bucket(self.bucket_name).blob(self.archive_object)
        blob.reload()
        token = blob.md5_hash or blob.crc32c or str(blob.generation or "")

        marker = self.gcs_dags_dir / VERSION_MARKER
        if token and marker.is_file() and marker.read_text() == token:
            self._log.debug("Archive unchanged (md5 match), skipping staging")
            return

        parent = self.gcs_dags_dir.parent
        staging_dir = Path(tempfile.mkdtemp(dir=parent, prefix=".archive-staging-"))
        try:
            archive_path = staging_dir / "_bundle_archive"
            blob.download_to_filename(str(archive_path))

            unpack_dir = staging_dir / "unpacked"
            unpack_dir.mkdir()
            with tarfile.open(archive_path, "r:*") as tar:
                tar.extractall(unpack_dir, filter="data")
            archive_path.unlink()

            if token:
                (unpack_dir / VERSION_MARKER).write_text(token)

            # Atomic-ish swap: rename current out of the way, new one in. The
            # previous tree is parked inside the staging dir, so a failed swap
            # can put it back and the cleanup below always removes it.
            backup_dir = staging_dir / "previous"
            if self.gcs_dags_dir.exists():
                self.gcs_dags_dir.rename(backup_dir)
            try:
                unpack_dir.rename(self.gcs_dags_dir)
            except Exception:
                if backup_dir.exists() and not self.gcs_dags_dir.exists():
                    backup_dir.rename(self.gcs_dags_dir)
                raise

            self._log.info(
                "Staged DAG bundle from archive (1 GET instead of per-object sync)",
                archive_bytes=blob.size,
            )
        finally:
            shutil.rmtree(staging_dir, ignore_errors=True)
