#!/usr/bin/env python3
"""Create the bucket and upload both layouts to local fake-gcs. Stdlib only."""

from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BUILD = ROOT / "bundle_build"
BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:4443"
BUCKET = "dag-bundles"
PROJECT = "demo-local"


def request(method: str, url: str, data: bytes | None = None, headers: dict | None = None):
    req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read()


def wait_until_up() -> None:
    deadline = time.time() + 60
    url = f"{BASE}/storage/v1/b?project={PROJECT}"
    while time.time() < deadline:
        try:
            status, _ = request("GET", url)
        except Exception:
            status = 0
        if status == 200:
            return
        time.sleep(0.5)
    raise SystemExit(f"fake-gcs did not answer at {BASE}")


def create_bucket() -> None:
    status, body = request(
        "POST",
        f"{BASE}/storage/v1/b?project={PROJECT}",
        data=json.dumps({"name": BUCKET}).encode(),
        headers={"Content-Type": "application/json"},
    )
    if status not in (200, 409):
        raise SystemExit(f"create bucket failed: {status} {body[:300]!r}")


def upload(object_name: str, payload: bytes) -> None:
    query = urllib.parse.urlencode({"uploadType": "media", "name": object_name})
    url = f"{BASE}/upload/storage/v1/b/{BUCKET}/o?{query}"
    status, body = request(
        "POST",
        url,
        data=payload,
        headers={"Content-Type": "application/octet-stream"},
    )
    if status != 200:
        raise SystemExit(f"upload {object_name} failed: {status} {body[:300]!r}")


def main() -> None:
    dags = BUILD / "dags"
    archive = BUILD / "dag-bundle.tar.gz"
    if not dags.is_dir() or not archive.is_file():
        raise SystemExit("bundle_build/ is missing — run 'just generate' first")

    wait_until_up()
    create_bucket()

    files = sorted(path for path in dags.rglob("*") if path.is_file())
    for path in files:
        upload(str(path.relative_to(dags.parent)).replace("\\", "/"), path.read_bytes())
    upload("archive/dag-bundle.tar.gz", archive.read_bytes())
    print(f"uploaded {len(files)} objects under gs://{BUCKET}/dags/ and 1 archive")


if __name__ == "__main__":
    main()
