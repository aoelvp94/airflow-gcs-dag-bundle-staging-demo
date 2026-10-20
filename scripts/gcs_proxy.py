#!/usr/bin/env python3
"""Count GCS JSON API calls, then forward them to fake-gcs-server.

The benchmark reads GET /__metrics. Those requests are not counted.
Seed uploads go straight to fake-gcs and never pass through here.
"""

from __future__ import annotations

import json
import os
import threading
import urllib.parse
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

UPSTREAM_HOST = os.environ.get("UPSTREAM_HOST", "fake-gcs")
UPSTREAM_PORT = int(os.environ.get("UPSTREAM_PORT", "4443"))
LISTEN_PORT = int(os.environ.get("LISTEN_PORT", "8888"))

LOCK = threading.Lock()
COUNTS = {"download": 0, "list": 0, "metadata": 0, "bucket": 0, "other": 0, "total": 0}
EXAMPLES: dict[str, str] = {}

HOP_BY_HOP = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailers", "transfer-encoding", "upgrade"}


def classify(path: str) -> str:
    parsed = urllib.parse.urlsplit(path)
    p = parsed.path
    q = parsed.query
    if p.startswith("/__"):
        return "ignore"
    if "/download/" in p or "alt=media" in q:
        return "download"
    if "/o/" in p:
        return "metadata"
    if p.rstrip("/").endswith("/o"):
        return "list"
    if "/b/" in p or p.rstrip("/").endswith("/b"):
        return "bucket"
    return "other"


def record(path: str) -> None:
    kind = classify(path)
    if kind == "ignore":
        return
    with LOCK:
        COUNTS[kind] += 1
        COUNTS["total"] += 1
        EXAMPLES.setdefault(kind, path.split("?", 1)[0])


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args) -> None:
        if args and str(args[1]).startswith(("4", "5")):
            super().log_message(fmt, *args)

    def _metrics(self) -> None:
        with LOCK:
            body = json.dumps({**COUNTS, "examples": EXAMPLES}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _proxy(self) -> None:
        if self.path == "/__metrics" or self.path.startswith("/__metrics?"):
            self._metrics()
            return

        length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(length) if length else None
        headers = {
            key: value
            for key, value in self.headers.items()
            if key.lower() not in HOP_BY_HOP and key.lower() != "host"
        }
        headers["Host"] = f"{UPSTREAM_HOST}:{UPSTREAM_PORT}"

        try:
            conn = HTTPConnection(UPSTREAM_HOST, UPSTREAM_PORT, timeout=120)
            conn.request(self.command, self.path, body=body, headers=headers)
            resp = conn.getresponse()
            payload = resp.read()
            status = resp.status
            resp_headers = resp.getheaders()
            conn.close()
        except Exception as exc:
            message = f"proxy error: {exc}\n".encode()
            self.send_response(502)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(message)))
            self.end_headers()
            self.wfile.write(message)
            return

        self.send_response(status)
        for key, value in resp_headers:
            if key.lower() in HOP_BY_HOP or key.lower() == "content-length":
                continue
            self.send_header(key, value)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(payload)
        record(self.path)

    def do_GET(self) -> None:
        self._proxy()

    def do_HEAD(self) -> None:
        self._proxy()

    def do_POST(self) -> None:
        self._proxy()

    def do_PUT(self) -> None:
        self._proxy()

    def do_PATCH(self) -> None:
        self._proxy()

    def do_DELETE(self) -> None:
        self._proxy()


def main() -> None:
    server = ThreadingHTTPServer(("0.0.0.0", LISTEN_PORT), Handler)
    print(f"counting proxy on :{LISTEN_PORT} -> {UPSTREAM_HOST}:{UPSTREAM_PORT}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
