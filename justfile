# End-to-end reproduction of the per-object GCS DAG-bundle staging cost.
# Requires: astro CLI, Docker, python3. Run `just e2e` for the whole thing.

set shell := ["bash", "-cu"]

n_files := "400"
avg_kb := "8"

# List recipes
default:
    @just --list

# Generate the synthetic bundle (N small files + one tar.gz of the same files)
generate:
    python3 scripts/generate_bundle.py {{n_files}} {{avg_kb}}

# Build the image and start Airflow + fake-gcs
up:
    astro dev start --no-browser --no-proxy --wait 5m --verbosity info

# Create the bucket and load BOTH layouts: per-object files and the archive
seed:
    python3 scripts/seed_gcs.py

# Time cold staging of both strategies and count real GCS requests
bench rounds="3":
    #!/usr/bin/env bash
    set -euo pipefail
    scheduler=$(docker ps --filter name=airflow-gcs-dag-bundle-staging-demo | awk 'NR>1 && $NF ~ /scheduler/ {print $NF; exit}')
    [ -n "$scheduler" ] || { echo "Scheduler is not running — run 'just up' first"; exit 1; }
    processor=$(docker ps --filter name=airflow-gcs-dag-bundle-staging-demo | awk 'NR>1 && $NF ~ /dag-processor/ {print $NF; exit}')
    if [ -n "$processor" ]; then
        docker stop "$processor" >/dev/null
    fi
    set +e
    docker exec "$scheduler" python /usr/local/airflow/include/benchmark.py {{rounds}}
    status=$?
    set -e
    if [ -n "$processor" ]; then
        docker start "$processor" >/dev/null
    fi
    exit "$status"

# Show how the DAG processor staged the two live bundles
logs:
    #!/usr/bin/env bash
    set -euo pipefail
    dp=$(docker ps -a --filter name=airflow-gcs-dag-bundle-staging-demo | awk 'NR>1 && $NF ~ /dag-processor/ {print $NF; exit}')
    [ -n "$dp" ] || dp=$(docker ps --filter name=airflow-gcs-dag-bundle-staging-demo | awk 'NR>1 && $NF ~ /scheduler/ {print $NF; exit}')
    docker logs "$dp" 2>&1 | grep -iE "staged|Downloading Dags|does not exist|archive" | tail -40

# Watch every request the counting proxy forwards (Ctrl+C to stop)
trace:
    docker logs -f gcs-bundle-proxy

# The whole reproduction: generate -> start -> seed -> benchmark
e2e: generate up seed bench

# Stop everything
down:
    astro dev stop

# Stop and delete containers, generated files, and local state
clean: down
    astro dev kill || true
    rm -rf bundle_build
