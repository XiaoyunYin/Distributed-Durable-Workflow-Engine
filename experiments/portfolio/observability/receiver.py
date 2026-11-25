"""Private test webhook and observer metrics for the J3 overlay."""

import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path("/evidence")
LOG = ROOT / "notifications.jsonl"
LOCK = threading.Lock()
OBSERVATIONS = ("ready_slots", "lease_takeovers_total", "stale_rejections_total")


def record(row):
    with LOCK, LOG.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps({"received_ns": time.time_ns(), **row}) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):  # noqa: N802
        if self.server.server_port != 8080 or self.path != "/notify":
            self.send_error(404)
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= 64 * 1024:
                raise ValueError("invalid body size")
            batch = json.loads(self.rfile.read(length))
            if batch["status"] not in {"firing", "resolved"} or not batch["alerts"]:
                raise ValueError("invalid notification")
            alerts = [
                {
                    key: alert[key]
                    for key in ("status", "fingerprint", "labels", "startsAt", "endsAt")
                }
                for alert in batch["alerts"]
            ]
        except (KeyError, TypeError, ValueError):
            self.send_error(400)
            return
        record({"status": batch["status"], "alerts": alerts})
        self.send_response(204)
        self.end_headers()

    def do_GET(self):  # noqa: N802
        if self.server.server_port != 8082 or self.path != "/metrics":
            self.send_error(404)
            return
        try:
            observed = json.loads((ROOT / "metrics.json").read_bytes())
        except FileNotFoundError:
            observed = {}
        body = "".join(
            f"j3_{name} {observed[name]}\n"
            for name in OBSERVATIONS
            if type(observed.get(name)) is int and observed[name] >= 0
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    record({"heartbeat": "start"})
    metrics = ThreadingHTTPServer(("", 8082), Handler)
    threading.Thread(target=metrics.serve_forever, daemon=True).start()
    ThreadingHTTPServer(("", 8080), Handler).serve_forever()
