FROM astrocrpublic.azurecr.io/runtime:3.3-7

# The runtime image does not ship the Google provider. Install the real
# provider wheel (so GCSDagBundle / GCSHook are the upstream classes) plus
# only the import closure those modules need. A full provider install also
# pulls Ray, pandas, and every Google product client, which this reproduction
# never calls.
USER root
RUN pip freeze > /tmp/existing.txt \
    && pip install --no-deps "apache-airflow-providers-google==22.3.0" \
    && pip install --constraint /tmp/existing.txt \
        "google-cloud-storage==3.13.1" \
        "gcloud-aio-storage>=9" \
        "gcloud-aio-auth>=5" \
        "google-auth-httplib2" \
        "google-api-python-client>=2" \
        "google-cloud-secret-manager" \
    && rm -f /tmp/existing.txt

COPY --chown=astro:0 bundle_pkg /tmp/bundle_pkg
RUN pip install --no-cache-dir --no-deps /tmp/bundle_pkg \
    && rm -rf /tmp/bundle_pkg
USER astro
