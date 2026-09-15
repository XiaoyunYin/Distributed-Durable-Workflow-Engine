"""Minimal worker process used to validate the DUR-001 runtime topology."""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from workers import __version__


class HealthHandler(BaseHTTPRequestHandler):
    """Serve process health until the Kafka activity runner is implemented."""

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        if self.path not in {"/healthz", "/readyz"}:
            self.send_error(404)
            return
        body = json.dumps(
            {
                "status": "ok",
                "worker_id": os.getenv("WORKER_ID", "worker"),
                "version": __version__,
            }
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        return


def run() -> None:
    host, port_text = os.getenv("WORKER_ADDR", "0.0.0.0:8081").rsplit(":", 1)
    server = ThreadingHTTPServer((host, int(port_text)), HealthHandler)

    def stop(_signum: int, _frame: object) -> None:
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    print(
        json.dumps(
            {
                "event": "worker_foundation_started",
                "listen": f"{host}:{port_text}",
                "worker_id": os.getenv("WORKER_ID", "worker"),
            }
        ),
        flush=True,
    )
    server.serve_forever()
    server.server_close()


def healthcheck() -> None:
    url = os.getenv("WORKER_HEALTH_URL", "http://127.0.0.1:8081/healthz")
    with urllib.request.urlopen(url, timeout=2) as response:  # noqa: S310 - fixed local URL
        if response.status != 200:
            raise RuntimeError(f"unexpected health status {response.status}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command", choices=("run", "healthcheck", "version"), nargs="?", default="run"
    )
    args = parser.parse_args()

    if args.command == "run":
        run()
    elif args.command == "healthcheck":
        healthcheck()
    else:
        print(__version__)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"worker command failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
