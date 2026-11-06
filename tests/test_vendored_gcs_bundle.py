"""Host-side checks for vendor/gcs.py (aoelvp94/airflow acec0cf).

Airflow itself is stubbed. The test locks the behavior this demo relies on:
the ETag marker is ``.archive_etag`` inside the staged tree, an empty tree
or a tree that holds only that marker is restaged, and ``GCSDagBundle``
does not unlink a sibling ``.{name}/etag``. ``delete_stale`` on the prefix
sync removes files inside the bundle directory, including an in-tree marker,
and does not reach a sibling path.
"""

from __future__ import annotations

import hashlib
import importlib.util
import inspect
import io
import shutil
import sys
import tarfile
import tempfile
import types
import unittest
from contextlib import contextmanager
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
VENDORED = REPO / "vendor" / "gcs.py"
UPSTREAM = REPO / "vendor" / "UPSTREAM.txt"
TIP = "acec0cfed761a04d3dabd7b5851662198c792882"
ARCHIVE_OBJECT = "archive/dag-bundle.tar.gz"


class NotFound(Exception):
    pass


class _Log:
    def bind(self, **_kwargs):
        return self

    def debug(self, *_args, **_kwargs):
        return None

    def info(self, *_args, **_kwargs):
        return None

    def warning(self, *_args, **_kwargs):
        return None


class _GCSHook:
    store: dict[str, tuple[bytes, str, int]] = {}
    sync_calls: list[dict] = []
    downloads: list[dict] = []

    def __init__(self, gcp_conn_id: str | None = None) -> None:
        self.gcp_conn_id = gcp_conn_id

    def get_bucket(self, bucket_name: str):
        return object()

    def list(self, bucket_name: str, prefix: str = ""):
        return [f"{prefix}demo_dag.py"]

    def get_conn(self):
        return _Client()

    def sync_to_local_dir(self, **kwargs) -> None:
        type(self).sync_calls.append(kwargs)
        local_dir = Path(kwargs["local_dir"])
        local_dir.mkdir(parents=True, exist_ok=True)
        # Stale cleanup stays inside the bundle directory. A sibling
        # ".{name}/etag" is outside local_dir and is not unlinked.
        if kwargs.get("delete_stale"):
            for child in list(local_dir.iterdir()):
                if child.is_dir():
                    shutil.rmtree(child)
                else:
                    child.unlink()
        (local_dir / "from_prefix.py").write_text("prefix\n")


class _Client:
    def bucket(self, _name: str) -> _Bucket:
        return _Bucket()


class _Bucket:
    def blob(self, name: str) -> _Blob:
        return _Blob(name)


class _Blob:
    def __init__(self, name: str) -> None:
        self.name = name
        self.etag: str | None = None
        self.generation: int | None = None
        self.size: int | None = None

    def reload(self) -> None:
        if self.name not in _GCSHook.store:
            raise NotFound(self.name)
        data, etag, generation = _GCSHook.store[self.name]
        self.etag = etag
        self.generation = generation
        self.size = len(data)

    def download_to_filename(self, filename: str, if_generation_match: int | None = None) -> None:
        data, _etag, generation = _GCSHook.store[self.name]
        _GCSHook.downloads.append(
            {"name": self.name, "if_generation_match": if_generation_match, "generation": generation}
        )
        if if_generation_match is not None and if_generation_match != generation:
            raise RuntimeError("generation mismatch")
        Path(filename).write_bytes(data)


def _tiny_archive() -> bytes:
    buf = io.BytesIO()
    payload = b"print('staged')\n"
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        info = tarfile.TarInfo(name="demo_dag.py")
        info.size = len(payload)
        tar.addfile(info, io.BytesIO(payload))
    return buf.getvalue()


def _install_stubs(root: Path) -> None:
    airflow = types.ModuleType("airflow")
    dag_processing = types.ModuleType("airflow.dag_processing")
    bundles = types.ModuleType("airflow.dag_processing.bundles")
    base = types.ModuleType("airflow.dag_processing.bundles.base")
    providers = types.ModuleType("airflow.providers")
    google = types.ModuleType("airflow.providers.google")
    cloud = types.ModuleType("airflow.providers.google.cloud")
    hooks = types.ModuleType("airflow.providers.google.cloud.hooks")
    gcs_hook_mod = types.ModuleType("airflow.providers.google.cloud.hooks.gcs")
    common_google = types.ModuleType("airflow.providers.google.common")
    google_hooks = types.ModuleType("airflow.providers.google.common.hooks")
    base_google = types.ModuleType("airflow.providers.google.common.hooks.base_google")
    common = types.ModuleType("airflow.providers.common")
    compat = types.ModuleType("airflow.providers.common.compat")
    sdk = types.ModuleType("airflow.providers.common.compat.sdk")
    structlog = types.ModuleType("structlog")
    google_pkg = types.ModuleType("google")
    api_core = types.ModuleType("google.api_core")
    exceptions = types.ModuleType("google.api_core.exceptions")

    class BaseDagBundle:
        def __init__(
            self,
            *,
            name: str,
            refresh_interval: int = 0,
            version: str | None = None,
            view_url_template: str | None = None,
        ) -> None:
            self.name = name
            self.version = version
            self.refresh_interval = refresh_interval
            self._view_url_template = view_url_template
            self.is_initialized = False
            self.base_dir = root / name
            self.base_dir.mkdir(parents=True, exist_ok=True)

        def initialize(self) -> None:
            self.is_initialized = True

        @contextmanager
        def lock(self):
            yield

    class GoogleBaseHook:
        default_conn_name = "google_cloud_default"

        @staticmethod
        def get_high_value_cookie_domain() -> str:
            return "google.com"

    class AirflowException(Exception):
        pass

    base.BaseDagBundle = BaseDagBundle
    base_google.GoogleBaseHook = GoogleBaseHook
    gcs_hook_mod.GCSHook = _GCSHook
    sdk.AirflowException = AirflowException
    exceptions.NotFound = NotFound
    structlog.get_logger = lambda *_a, **_k: _Log()

    for name, module in {
        "airflow": airflow,
        "airflow.dag_processing": dag_processing,
        "airflow.dag_processing.bundles": bundles,
        "airflow.dag_processing.bundles.base": base,
        "airflow.providers": providers,
        "airflow.providers.google": google,
        "airflow.providers.google.cloud": cloud,
        "airflow.providers.google.cloud.hooks": hooks,
        "airflow.providers.google.cloud.hooks.gcs": gcs_hook_mod,
        "airflow.providers.google.common": common_google,
        "airflow.providers.google.common.hooks": google_hooks,
        "airflow.providers.google.common.hooks.base_google": base_google,
        "airflow.providers.common": common,
        "airflow.providers.common.compat": compat,
        "airflow.providers.common.compat.sdk": sdk,
        "structlog": structlog,
        "google": google_pkg,
        "google.api_core": api_core,
        "google.api_core.exceptions": exceptions,
    }.items():
        sys.modules[name] = module


def _load_bundle_module():
    spec = importlib.util.spec_from_file_location("vendored_gcs_bundle_acec0cf", VENDORED)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {VENDORED}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class VendoredGCSBundleTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self._tmpdir.name)
        _GCSHook.store = {ARCHIVE_OBJECT: (_tiny_archive(), "etag-1", 11)}
        _GCSHook.sync_calls = []
        _GCSHook.downloads = []
        _install_stubs(self.root)
        self.bundle_mod = _load_bundle_module()

    def tearDown(self) -> None:
        self._tmpdir.cleanup()
        sys.modules.pop("vendored_gcs_bundle_acec0cf", None)

    def test_vendored_bytes_match_upstream_tip(self) -> None:
        text = UPSTREAM.read_text()
        self.assertIn(TIP, text)
        recorded = next(
            line.split(":", 1)[1].strip() for line in text.splitlines() if line.startswith("sha256:")
        )
        digest = hashlib.sha256(VENDORED.read_bytes()).hexdigest()
        self.assertEqual(digest, recorded)
        source = VENDORED.read_text()
        self.assertIn('return self.gcs_dags_dir / ".archive_etag"', source)
        self.assertNotIn("_require_tar_data_filter()", inspect.getsource(self.bundle_mod._extract_archive))

    def test_archive_marker_is_inside_the_staged_tree(self) -> None:
        bundle = self.bundle_mod.GCSArchiveDagBundle(
            name="bench-archive",
            bucket_name="dag-bundles",
            archive_object=ARCHIVE_OBJECT,
            gcp_conn_id="google_cloud_default",
        )
        stale = bundle._archive_state_dir() / "archive-staging-leftover"
        stale.mkdir(parents=True)
        (stale / "junk").write_text("x")

        bundle.initialize()

        marker = bundle.path / ".archive_etag"
        sibling = bundle.base_dir.parent / f".{bundle.name}" / "etag"
        self.assertEqual(marker, bundle._archive_etag_path())
        self.assertEqual(marker.read_text(), "etag-1")
        self.assertFalse(sibling.exists())
        self.assertEqual((bundle.path / "demo_dag.py").read_text(), "print('staged')\n")
        self.assertFalse(stale.exists())
        self.assertEqual(len(_GCSHook.downloads), 1)
        self.assertEqual(_GCSHook.downloads[0]["if_generation_match"], 11)
        self.assertTrue(bundle.is_initialized)
        self.assertEqual(_GCSHook.sync_calls, [])

        bundle.refresh()
        self.assertEqual(len(_GCSHook.downloads), 1)

        (bundle.path / "demo_dag.py").unlink()
        self.assertTrue(marker.is_file())
        self.assertFalse(bundle._bundle_dir_has_files())
        bundle.refresh()
        self.assertEqual(len(_GCSHook.downloads), 2)
        self.assertTrue((bundle.path / "demo_dag.py").is_file())
        self.assertFalse(sibling.exists())

        shutil.rmtree(bundle.base_dir)
        self.assertFalse(sibling.exists())
        bundle.initialize()
        self.assertEqual(len(_GCSHook.downloads), 3)
        self.assertEqual((bundle.path / ".archive_etag").read_text(), "etag-1")
        self.assertFalse(sibling.exists())

    def test_empty_etag_does_not_write_a_marker(self) -> None:
        _GCSHook.store = {ARCHIVE_OBJECT: (_tiny_archive(), "", 11)}
        bundle = self.bundle_mod.GCSArchiveDagBundle(
            name="bench-archive",
            bucket_name="dag-bundles",
            archive_object=ARCHIVE_OBJECT,
            gcp_conn_id="google_cloud_default",
        )
        bundle.initialize()
        self.assertFalse((bundle.path / ".archive_etag").exists())
        self.assertFalse((bundle.base_dir.parent / f".{bundle.name}" / "etag").exists())
        bundle.refresh()
        self.assertEqual(len(_GCSHook.downloads), 2)

    def test_prefix_refresh_does_not_unlink_sibling_marker(self) -> None:
        refresh_source = inspect.getsource(self.bundle_mod.GCSDagBundle._refresh_locked)
        self.assertNotIn("unlink", refresh_source)
        self.assertNotIn("archive_etag", refresh_source)
        self.assertNotIn(".etag", refresh_source)

        bundle = self.bundle_mod.GCSDagBundle(
            name="bench-per-object",
            bucket_name="dag-bundles",
            prefix="dags/",
            gcp_conn_id="google_cloud_default",
        )
        sibling = bundle.base_dir.parent / f".{bundle.name}" / "etag"
        sibling.parent.mkdir(parents=True)
        sibling.write_text("old-sibling")
        (bundle.path / ".archive_etag").write_text("in-tree")
        (bundle.path / "archive_only.py").write_text("archive\n")

        bundle.initialize()

        self.assertTrue(sibling.is_file())
        self.assertEqual(sibling.read_text(), "old-sibling")
        self.assertFalse((bundle.path / ".archive_etag").exists())
        self.assertFalse((bundle.path / "archive_only.py").exists())
        self.assertEqual((bundle.path / "from_prefix.py").read_text(), "prefix\n")
        self.assertEqual(len(_GCSHook.sync_calls), 1)
        call = _GCSHook.sync_calls[0]
        self.assertEqual(call["prefix"], "dags/")
        self.assertTrue(call["delete_stale"])
        self.assertEqual(call["bucket_name"], "dag-bundles")
        self.assertEqual(Path(call["local_dir"]), bundle.gcs_dags_dir)

    def test_constructors_take_one_source_each(self) -> None:
        with self.assertRaises(TypeError):
            self.bundle_mod.GCSDagBundle(
                name="prefix-only",
                bucket_name="dag-bundles",
                archive_object=ARCHIVE_OBJECT,
            )
        with self.assertRaises(TypeError):
            self.bundle_mod.GCSArchiveDagBundle(
                name="archive-only",
                bucket_name="dag-bundles",
                prefix="dags/",
                archive_object=ARCHIVE_OBJECT,
            )


if __name__ == "__main__":
    unittest.main()
