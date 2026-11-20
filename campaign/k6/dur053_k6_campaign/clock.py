"""Campaign-only in-process wall clock responder."""

import json
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

PORT = 8083
CLOCK_PATH = "/internal/k6/clock"


def server(role: str) -> ThreadingHTTPServer:
    pid = os.getpid()
    fields = Path("/proc/self/stat").read_text().rsplit(")", 1)[1].split()
    identity = {
        "pid": pid,
        "process_start_id": f"{pid}:{fields[19]}",
        "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
        "pod_uid": os.environ["POD_UID"],
        "role": role,
    }

    class ClockHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path != CLOCK_PATH:
                self.send_error(404)
                return
            body = json.dumps({**identity, "wall_ns": time.time_ns()}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            pass

    return ThreadingHTTPServer(("0.0.0.0", PORT), ClockHandler)


if __name__ == "__main__":
    with server("clock-control") as responder:
        responder.serve_forever()
