"""Loopback-only submission proxy for the hosted capped-sender control."""

import base64
import hashlib
import json
import sys
import time
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, HTTPServer

PORT = 18080
HOLD_NS = 3_000_000_000
PATH = "/v1/workflows"
REQUEST_HEADERS = ("Authorization", "Content-Type", "X-Dur053-K4-Request")
HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}


class Proxy(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response_only(204 if self.path == "/ready" else 404)
        self.end_headers()

    def do_POST(self):
        if self.path != PATH:
            self.send_error(404)
            return
        headers = {key: self.headers[key] for key in REQUEST_HEADERS}
        size = int(self.headers["Content-Length"])
        if not 0 < size <= 1 << 20 or not all(headers.values()):
            self.send_error(400)
            return
        payload = self.rfile.read(size)
        key = json.loads(payload)["workflow_id"]
        if key != headers["X-Dur053-K4-Request"] or key in self.server.seen:
            self.send_error(409)
            return
        first = not self.server.seen
        self.server.seen.add(key)
        entered = time.monotonic_ns()
        entered_unix = time.time_ns()
        upstream = HTTPConnection("api", 8080, timeout=10)
        try:
            upstream.request("POST", PATH, payload, headers)
            response = upstream.getresponse()
            body = response.read((1 << 20) + 1)
            upstream_end = time.monotonic_ns()
            upstream_unix = time.time_ns()
            if len(body) > 1 << 20:
                raise ValueError("upstream response exceeds bound")
            if first:
                while (remaining := upstream_end + HOLD_NS - time.monotonic_ns()) > 0:
                    time.sleep(remaining / 1_000_000_000)
            release_unix = time.time_ns()
            release = time.monotonic_ns()
            self.send_response_only(response.status, response.reason)
            for name, value in response.getheaders():
                if name.lower() not in HOP_HEADERS:
                    self.send_header(name, value)
            self.end_headers()
            self.wfile.write(body)
            self.wfile.flush()
            released = time.monotonic_ns()
            print(
                json.dumps(
                    {
                        "key": key,
                        "request_sha256": hashlib.sha256(payload).hexdigest(),
                        "upstream_status": response.status,
                        "downstream_status": response.status,
                        "upstream_body_base64": base64.b64encode(body).decode(),
                        "downstream_body_sha256": hashlib.sha256(body).hexdigest(),
                        "entered_mono_ns": entered,
                        "entered_unix_ns": entered_unix,
                        "upstream_end_mono_ns": upstream_end,
                        "upstream_end_unix_ns": upstream_unix,
                        "release_mono_ns": release,
                        "release_unix_ns": release_unix,
                        "released_mono_ns": released,
                    }
                ),
                flush=True,
            )
        finally:
            upstream.close()


def main():
    with HTTPServer(("127.0.0.1", PORT), Proxy) as server:
        server.seen = set()
        server.serve_forever()


if __name__ == "__main__":
    sys.exit(main())
