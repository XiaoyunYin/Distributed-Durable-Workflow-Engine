"""Minimal worker process used to validate the DUR-001 runtime topology."""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from workers import __version__


class HealthHandler(BaseHTTPRequestHandler):
    """Process health; this is not a claim that dependencies are available."""

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
    from faults.client import FailpointClient

    from workers.kafka_worker import start_pool

    host, port_text = os.getenv("WORKER_ADDR", "0.0.0.0:8081").rsplit(":", 1)
    server = ThreadingHTTPServer((host, int(port_text)), HealthHandler)
    logging.basicConfig(level=logging.INFO)
    stopping = threading.Event()
    threads = []
    if brokers := os.getenv("KAFKA_BOOTSTRAP_SERVERS"):
        threads = start_pool(
            brokers,
            os.getenv("CONTROL_API_URL", "http://127.0.0.1:8080"),
            os.getenv("WORKER_ID", "worker"),
            stopping,
            int(os.getenv("WORKER_SLOTS", "4")),
        )
    # A worker process can participate in deterministic M5 campaigns without
    # making fault control a production dependency. With no endpoint this is a
    # no-op; with one, startup is acknowledged at a named boundary.
    failpoint = FailpointClient.from_environment()
    if failpoint is not None:
        fields: dict[str, object] = {"worker_id": os.getenv("WORKER_ID", "worker")}
        try:
            failpoint.hit("worker_ready", fields)
            failpoint.emit("released", "worker_ready", fields)
        finally:
            failpoint.close()

    def stop(_signum: int, _frame: object) -> None:
        stopping.set()
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
    stopping.set()
    for thread in threads:
        thread.join(timeout=2)
    server.server_close()


def healthcheck() -> None:
    url = os.getenv("WORKER_HEALTH_URL", "http://127.0.0.1:8081/healthz")
    with urllib.request.urlopen(url, timeout=2) as response:  # noqa: S310 - fixed local URL
        if response.status != 200:
            raise RuntimeError(f"unexpected health status {response.status}")


def run_task(arguments: list[str]) -> None:
    from workers.registry import default_registry
    from workers.runner import ActivityTask, runner_for_url

    parser = argparse.ArgumentParser(prog="python -m workers run-task")
    parser.add_argument(
        "--control-url", default=os.getenv("CONTROL_API_URL", "http://127.0.0.1:8080")
    )
    parser.add_argument("--workflow-id", required=True)
    parser.add_argument("--node-id", required=True)
    parser.add_argument("--iteration", type=int, default=0)
    parser.add_argument("--worker-id", default=os.getenv("WORKER_ID", "worker"))
    parser.add_argument("--request-id", required=True)
    parser.add_argument("--activity", default="pure.echo")
    parser.add_argument("--version", default="v1")
    parser.add_argument("--input", default="null", dest="activity_input")
    parser.add_argument("--max-workers", type=int, default=4)
    args = parser.parse_args(arguments)
    task = ActivityTask(
        workflow_id=args.workflow_id,
        node_id=args.node_id,
        iteration=args.iteration,
        worker_id=args.worker_id,
        request_id=args.request_id,
        activity_name=args.activity,
        activity_version=args.version,
        input=json.loads(args.activity_input),
    )
    result = runner_for_url(args.control_url, default_registry(), args.max_workers).run_task(task)
    print(json.dumps(result, separators=(",", ":")))


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "run-task":
        run_task(sys.argv[2:])
        return
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
