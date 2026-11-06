FROM astrocrpublic.azurecr.io/runtime:3.3-7

# The runtime image does not ship the Google provider. Install the real
# provider wheel (so GCSHook is the upstream class) plus only the import
# closure the bundle module needs. A full provider install also pulls Ray,
# pandas, and every Google product client, which this reproduction never calls.
#
# Provider 22.3.0 has no GCSArchiveDagBundle. vendor/gcs.py is a verbatim
# copy of aoelvp94/airflow@acec0cf (apache/airflow#75566) and replaces
# airflow.providers.google.cloud.bundles.gcs after install. The ETag marker
# in that file is {gcs_dags_dir}/.archive_etag. GCSDagBundle does not unlink
# a sibling .{name}/etag.
USER root
COPY --chown=astro:0 vendor/gcs.py /tmp/gcs_bundle_acec0cf.py
RUN pip freeze > /tmp/existing.txt \
    && pip install --no-deps "apache-airflow-providers-google==22.3.0" \
    && pip install --constraint /tmp/existing.txt \
        "google-cloud-storage==3.13.1" \
        "gcloud-aio-storage>=9" \
        "gcloud-aio-auth>=5" \
        "google-auth-httplib2" \
        "google-api-python-client>=2" \
        "google-cloud-secret-manager" \
    && python - <<'PY'
from pathlib import Path

import airflow.providers.google.cloud.bundles.gcs as installed

dest = Path(installed.__file__)
payload = Path("/tmp/gcs_bundle_acec0cf.py").read_bytes()
dest.write_bytes(payload)
cache = dest.parent / "__pycache__"
if cache.is_dir():
    for compiled in cache.glob("gcs*.pyc"):
        compiled.unlink()
print(f"overlaid {dest} ({len(payload)} bytes)")
PY
    && python -c "import inspect; from airflow.providers.google.cloud.bundles.gcs import GCSArchiveDagBundle, GCSDagBundle; assert '.archive_etag' in inspect.getsource(GCSArchiveDagBundle._archive_etag_path); assert 'unlink' not in inspect.getsource(GCSDagBundle._refresh_locked)" \
    && rm -f /tmp/existing.txt /tmp/gcs_bundle_acec0cf.py
USER astro
