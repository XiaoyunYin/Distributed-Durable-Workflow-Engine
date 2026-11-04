"""Minimal worker process used to validate the DUR-001 runtime topology."""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from workers import __version__
from workers.lifecycle import WorkerLifecycle, worker_identity


class HealthHandler(BaseHTTPRequestHandler):
    lifecycle: WorkerLifecycle | None = None
    """Process health; this is not a claim that dependencies are available."""

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        if self.path == "/metrics" and self.lifecycle is not None:
            body = self.lifecycle.render().encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if self.path not in {"/healthz", "/readyz"}:
            self.send_error(404)
            return
        body = json.dumps(
            {
                "status": "ok",
                "worker_id": worker_identity(),
                "version": __version__,
            }
        ).encode()
        self.send_response(
            503
            if self.path == "/readyz"
            and self.lifecycle is not None
            and self.lifecycle.drain.is_set()
            else 200
        )
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
    mode = os.getenv("WORKER_DRAIN_MODE", "drain")
    if mode not in {"drain", "legacy"}:
        raise ValueError("WORKER_DRAIN_MODE must be drain or legacy")
    lifecycle = WorkerLifecycle(float(os.getenv("WORKER_DRAIN_SECONDS", "30")), mode == "legacy")
    HealthHandler.lifecycle = lifecycle
    process_done = threading.Event()
    watchdog = start_abort_watchdog(lifecycle, process_done)
    stopping = lifecycle.drain

    def stop(_signum: int, _frame: object) -> None:
        lifecycle.begin_drain()
        if lifecycle.legacy:
            threading.Thread(target=server.shutdown, daemon=True).start()

    if not lifecycle.legacy:
        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)

    threads = []
    if brokers := os.getenv("KAFKA_BOOTSTRAP_SERVERS"):
        threads = start_pool(
            brokers,
            os.getenv("CONTROL_API_URL", "http://127.0.0.1:8080"),
            worker_identity(),
            stopping,
            int(os.getenv("WORKER_SLOTS", "4")),
            lifecycle,
        )
    # A worker process can participate in deterministic M5 campaigns without
    # making fault control a production dependency. With no endpoint this is a
    # no-op; with one, startup is acknowledged at a named boundary.
    failpoint = FailpointClient.from_environment()
    if failpoint is not None:
        fields: dict[str, object] = {"worker_id": worker_identity()}
        try:
            failpoint.hit("worker_ready", fields)
            failpoint.emit("released", "worker_ready", fields)
        finally:
            failpoint.close()

    if lifecycle.legacy:
        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
    print(
        json.dumps(
            {
                "event": "worker_foundation_started",
                "listen": f"{host}:{port_text}",
                "worker_id": worker_identity(),
            }
        ),
        flush=True,
    )
    if lifecycle.legacy:
        server.serve_forever()
    else:
        health_thread = threading.Thread(target=server.serve_forever, daemon=True)
        health_thread.start()
        lifecycle.drain.wait()
    lifecycle.begin_drain()
    if lifecycle.legacy:
        for thread in threads:
            thread.join(timeout=2)
    else:
        finish_drain(lifecycle, threads)
        server.shutdown()
        health_thread.join(timeout=1)
    server.server_close()
    process_done.set()
    if watchdog is not None:
        watchdog.join(timeout=1)


def start_abort_watchdog(
    lifecycle: WorkerLifecycle, process_done: threading.Event
) -> threading.Thread | None:
    if lifecycle.legacy:
        return None

    def enforce() -> None:
        lifecycle.drain.wait()
        if not process_done.wait(lifecycle.remaining()):
            lifecycle.abort.set()
            os._exit(0)

    watchdog = threading.Thread(target=enforce, name="worker-abort-deadline", daemon=True)
    watchdog.start()
    return watchdog


def finish_drain(lifecycle: WorkerLifecycle, threads: list[threading.Thread]) -> None:
    while True:
        with lifecycle.lock:
            pending = lifecycle.in_flight
        if not pending and not any(thread.is_alive() for thread in threads):
            return
        if lifecycle.aborted():
            # Daemon executor threads may be uncooperative. Never manufacture a
            # failure/result or wait for Python's executor atexit hooks (I13).
            os._exit(0)
        time.sleep(min(0.05, lifecycle.remaining()))


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
    parser.add_argument("--worker-id", default=worker_identity())
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
