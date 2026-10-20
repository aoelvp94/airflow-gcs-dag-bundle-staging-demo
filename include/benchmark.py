#!/usr/bin/env python3
"""
Benchmark cold DAG-bundle staging: stock per-object sync vs single archive.

Runs INSIDE an Airflow container (see `just bench`). Each round wipes the
bundle's local directory first, simulating what every fresh worker pod
experiences under KubernetesExecutor: an empty filesystem, so the full
staging cost sits on the critical path of the task start.

GCS request counts are read from the counting proxy in front of fake-gcs,
so the numbers are what the storage server actually served.
"""

from __future__ import annotations

import json
import shutil
import statistics
import sys
import time
import urllib.request

from airflow.providers.google.cloud.bundles.gcs import GCSDagBundle

from gcs_archive_bundle import GCSArchiveDagBundle

METRICS_URL = "http://gcs-proxy:8888/__metrics"
BUCKET = "dag-bundles"
PREFIX = "dags/"
ARCHIVE_OBJECT = "archive/dag-bundle.tar.gz"
ROUNDS = int(sys.argv[1]) if len(sys.argv) > 1 else 3
CLASSES = ("download", "list", "metadata", "bucket", "other")


def gcs_request_counts() -> dict[str, int]:
    with urllib.request.urlopen(METRICS_URL, timeout=5) as resp:
        payload = json.load(resp)
    return {key: int(payload.get(key, 0)) for key in ("total", *CLASSES)}


def diff(before: dict[str, int], after: dict[str, int]) -> dict[str, int]:
    return {
        key: after.get(key, 0) - before.get(key, 0)
        for key in set(after) | set(before)
        if after.get(key, 0) != before.get(key, 0)
    }


def run_cold(make_bundle) -> tuple[float, dict[str, int], int]:
    bundle = make_bundle()
    shutil.rmtree(bundle.base_dir, ignore_errors=True)
    before = gcs_request_counts()
    t0 = time.perf_counter()
    bundle.initialize()
    elapsed = time.perf_counter() - t0
    deltas: dict[str, int] = {}
    for _ in range(15):
        deltas = diff(before, gcs_request_counts())
        if deltas.get("total"):
            break
        time.sleep(0.2)
    staged = sum(1 for path in bundle.path.rglob("*.py") if path.is_file())
    return elapsed, deltas, staged


def scenario(label: str, make_bundle) -> dict:
    timings, last_reqs, staged = [], {}, 0
    for i in range(ROUNDS):
        elapsed, reqs, staged = run_cold(make_bundle)
        timings.append(elapsed)
        last_reqs = reqs
        pretty = ", ".join(f"{key}={last_reqs.get(key, 0)}" for key in CLASSES if last_reqs.get(key))
        pretty = pretty or "no requests"
        print(f"  round {i + 1}: {elapsed:6.2f}s   ({pretty})")
    return {"label": label, "median": statistics.median(timings), "reqs": last_reqs, "staged": staged}


def main() -> None:
    print(f"Cold-staging benchmark, {ROUNDS} rounds each (local dir wiped before every round)\n")

    print("[1/2] stock GCSDagBundle — serial per-object sync")
    stock = scenario(
        "per-object sync (stock GCSDagBundle)",
        lambda: GCSDagBundle(
            name="bench-per-object",
            bucket_name=BUCKET,
            prefix=PREFIX,
            gcp_conn_id="google_cloud_default",
        ),
    )

    print("\n[2/2] GCSArchiveDagBundle — single tar.gz object")
    archive = scenario(
        "single archive (GCSArchiveDagBundle)",
        lambda: GCSArchiveDagBundle(
            name="bench-archive",
            bucket_name=BUCKET,
            archive_object=ARCHIVE_OBJECT,
            gcp_conn_id="google_cloud_default",
        ),
    )

    print(f"\n{'=' * 74}")
    print(f"{'scenario':<42}{'median':>9}{'GCS reqs':>10}{'files':>8}")
    print(f"{'-' * 74}")
    for row in (stock, archive):
        reqs = row["reqs"].get("total", "n/a")
        print(f"{row['label']:<42}{row['median']:>8.2f}s{reqs!s:>10}{row['staged']:>8}")
    print(f"{'=' * 74}")
    if archive["median"] > 0:
        print(
            f"\nspeedup: {stock['median'] / archive['median']:.1f}x   "
            f"(every KubernetesExecutor task pod pays the cold-staging cost)"
        )
    if stock["staged"] != archive["staged"] or stock["staged"] == 0:
        raise SystemExit(
            f"staged file counts differ or are empty: stock={stock['staged']} archive={archive['staged']}"
        )


if __name__ == "__main__":
    main()
