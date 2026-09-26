#!/usr/bin/env python3
"""Run many real single-request loadgen invocations under two-CPU affinity."""

from __future__ import annotations

import argparse
import hashlib
import http.server
import json
import os
import platform
import subprocess
import tempfile
import threading
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
        size = int(self.headers.get("Content-Length", "0"))
        request = json.loads(self.rfile.read(size))
        body = json.dumps(
            {"created": True, "workflow": {"workflow_id": request["workflow_id"]}}
        ).encode()
        self.send_response(201)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format: str, *_args: object) -> None:
        return


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--record-json", type=Path)
    args = parser.parse_args()
    if not hasattr(os, "sched_getaffinity"):
        raise SystemExit("two-CPU affinity verification requires Linux")
    allowed = sorted(os.sched_getaffinity(0))
    if len(allowed) < 2:
        raise SystemExit(f"runner exposes fewer than two CPUs: {allowed}")
    selected = set(allowed[:2])
    os.sched_setaffinity(0, selected)
    if os.sched_getaffinity(0) != selected:
        raise SystemExit("could not constrain the test process to two CPUs")

    binary = ROOT / "bin/dur050-loadgen"
    if not binary.is_file():
        raise SystemExit("CI-built bin/dur050-loadgen is missing")
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    failures = 0
    count = 1000
    started = datetime.now(UTC)
    try:
        with tempfile.TemporaryDirectory(prefix="dur050-cpu-loop-") as directory:
            root = Path(directory)
            config = root / "config.json"
            config.write_text(
                json.dumps(
                    {
                        "api_url": f"http://127.0.0.1:{server.server_port}",
                        "namespace": "dur050-cpuloop",
                        "run_id": "cpuloop",
                        "seed": 50050,
                        "families": [
                            {
                                "name": "seq-8",
                                "definition_id": "seq-v1",
                                "definition_version": 1,
                                "initial_node_id": "activity-0",
                                "payload": {"profile": "seq-8"},
                                "initial_input": {},
                            },
                            {
                                "name": "fanout-8",
                                "definition_id": "fanout-v1",
                                "definition_version": 1,
                                "initial_node_id": "root",
                                "payload": {"profile": "fanout-8"},
                                "initial_input": {},
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )
            env = os.environ.copy()
            env.pop("HOME", None)
            for index in range(1, count + 1):
                run_id = f"loop-{index:07d}"
                output = root / f"{run_id}.csv"
                result = subprocess.run(
                    [
                        str(binary),
                        "-config",
                        str(config),
                        "-single-family",
                        "seq-8",
                        "-run-id",
                        run_id,
                        "-rate",
                        "1",
                        "-count",
                        "1",
                        "-request-timeout",
                        "2s",
                        "-output",
                        str(output),
                    ],
                    env=env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    timeout=10,
                    check=False,
                )
                summary_path = Path(str(output) + ".summary.json")
                if result.returncode != 0 or not summary_path.is_file():
                    failures += 1
                    if failures <= 5:
                        print(
                            f"FAIL {run_id}: exit={result.returncode} "
                            f"output={result.stdout.strip()}"
                        )
                    continue
                summary = json.loads(summary_path.read_text(encoding="utf-8"))
                if (
                    summary.get("status") != "PASS"
                    or summary.get("generator_cpu", {}).get("status") != "PASS"
                ):
                    failures += 1
                    if failures <= 5:
                        print(
                            f"FAIL {run_id}: status={summary.get('status')} "
                            f"cpu={summary.get('generator_cpu')}"
                        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
    completed = datetime.now(UTC)
    source_commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()
    record = {
        "schema": "dur050-loadgen-cpu-window-campaign.v1",
        "classification": "VALIDATION ONLY - NOT CAPACITY RESULTS",
        "status": "PASS" if failures == 0 else "FAIL",
        "runs": count,
        "failed_runs": failures,
        "cpu_affinity": sorted(selected),
        "host_platform": platform.platform(),
        "source_commit": source_commit,
        "loadgen_binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "started_at_utc": started.isoformat().replace("+00:00", "Z"),
        "completed_at_utc": completed.isoformat().replace("+00:00", "Z"),
        "api": "local fake POST /v1/workflows returning 201; no AWS, SSM, PostgreSQL, or Kafka",
    }
    if args.record_json:
        args.record_json.parent.mkdir(parents=True, exist_ok=True)
        args.record_json.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    if failures:
        raise SystemExit(
            f"single-request loop: {failures}/{count} failed on CPUs {sorted(selected)}"
        )
    print(json.dumps(record, sort_keys=True))
    print(f"PASS: {count}/{count} single-request runs; failures=0; CPU affinity={sorted(selected)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
