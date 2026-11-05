"""Resolve ``GCSArchiveDagBundle`` from the provider when that class exists."""

from __future__ import annotations

import importlib

PROVIDER_CLASSPATH = "airflow.providers.google.cloud.bundles.gcs.GCSArchiveDagBundle"
VENDORED_CLASSPATH = "gcs_archive_bundle.GCSArchiveDagBundle"


def resolve_gcs_archive_dag_bundle():
    """
    Return ``(class, classpath)``.

    The provider classpath is preferred once
    ``airflow.providers.google.cloud.bundles.gcs.GCSArchiveDagBundle`` is
    installed. Otherwise the vendored class in this package is used. That
    class mirrors the provider shape: ``archive_object``, an ETag marker
    beside the bundle directory, restage when the directory has no files,
    and a generation-pinned download.
    """
    module_name, _, class_name = PROVIDER_CLASSPATH.rpartition(".")
    try:
        module = importlib.import_module(module_name)
        cls = getattr(module, class_name)
    except (ImportError, AttributeError):
        from gcs_archive_bundle.bundle import GCSArchiveDagBundle

        return GCSArchiveDagBundle, VENDORED_CLASSPATH
    return cls, PROVIDER_CLASSPATH


GCSArchiveDagBundle, ARCHIVE_CLASSPATH = resolve_gcs_archive_dag_bundle()

__all__ = [
    "ARCHIVE_CLASSPATH",
    "GCSArchiveDagBundle",
    "PROVIDER_CLASSPATH",
    "VENDORED_CLASSPATH",
    "resolve_gcs_archive_dag_bundle",
]
