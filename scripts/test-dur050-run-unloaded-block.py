#!/usr/bin/env python3
"""Exercise the checked-in unloaded runner without AWS or PostgreSQL."""

from __future__ import annotations

import csv
import http.server
import json
import os
import shutil
import subprocess
import tempfile
import threading
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class WorkflowAPI(http.server.ThreadingHTTPServer):
    def __init__(
        self,
        address: tuple[str, int],
        handler: type[http.server.BaseHTTPRequestHandler],
        submissions: Path,
    ):
        super().__init__(address, handler)
        self.submissions = submissions


class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
        if self.path != "/v1/workflows":
            self.send_error(404)
            return
        length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(length))
        workflow_id = payload["workflow_id"]
        with self.server.submissions.open("a", encoding="utf-8") as stream:  # type: ignore[attr-defined]
            stream.write(workflow_id + "\n")
        body = json.dumps({"created": True, "workflow": {"workflow_id": workflow_id}}).encode()
        self.send_response(201)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format: str, *_args: object) -> None:
        return


def write_observer_stub(path: Path, columns: list[str]) -> None:
    source = r"""#!/usr/bin/env python3
import csv, os, sys, time
from datetime import datetime, timezone
args = sys.argv[1:]
workflow_id = args[args.index("-workflow-id") + 1]
output = args[args.index("-output") + 1]
columns = os.environ["DUR050_TEST_OBSERVER_COLUMNS"].split(",")
submissions = os.environ["DUR050_TEST_SUBMISSIONS"]
def row(record_type, **values):
    record = {name: "" for name in columns}
    record.update(record_type=record_type, workflow_id=workflow_id,
        sequence="1", scheduled_at_utc=datetime.now(timezone.utc).isoformat(),
        observed_at_utc=datetime.now(timezone.utc).isoformat(), state="SUCCEEDED",
        valid="true", reason="queries=1", observer_qps="1",
        scheduled_at_monotonic_ns=str(time.monotonic_ns()),
        observed_at_monotonic_ns=str(time.monotonic_ns()), completion_marker_seen="false")
    record.update(values)
    return record
with open(output, "x", newline="", encoding="utf-8") as stream:
    writer = csv.DictWriter(stream, fieldnames=columns, extrasaction="ignore")
    writer.writeheader()
    writer.writerow(row("poll")); stream.flush()
    deadline = time.monotonic() + (0.3 if os.getenv("DUR050_TEST_OBSERVER_SHORT") else 10)
    while time.monotonic() < deadline:
        try:
            if workflow_id in open(submissions, encoding="utf-8").read().splitlines():
                writer.writerow(row("first_terminal_observation"));
                writer.writerow(row("summary")); stream.flush(); sys.exit(0)
        except FileNotFoundError:
            pass
        time.sleep(0.005)
sys.exit(0 if os.getenv("DUR050_TEST_OBSERVER_SHORT") else 1)
"""
    path.write_text(source, encoding="utf-8")
    path.chmod(0o755)


def run_case(
    fail_loadgen: bool, header: list[str]
) -> tuple[subprocess.CompletedProcess[str], Path]:
    root = Path(tempfile.mkdtemp(prefix="dur050-unloaded-runner-"))
    scripts = root / "scripts"
    binary = root / "bin"
    output = root / ("partial" if fail_loadgen else "complete")
    scripts.mkdir()
    binary.mkdir()
    shutil.copy2(ROOT / "scripts/dur050-run-unloaded-block.sh", scripts)
    shutil.copy2(ROOT / "scripts/dur050-append-observer-rows.py", scripts)
    loadgen = ROOT / "bin/dur050-loadgen"
    if not loadgen.is_file():
        raise RuntimeError("CI-built bin/dur050-loadgen is missing")
    if fail_loadgen:
        (binary / "dur050-loadgen").write_text("#!/usr/bin/env bash\nexit 1\n", encoding="utf-8")
        (binary / "dur050-loadgen").chmod(0o755)
    else:
        shutil.copy2(loadgen, binary / "dur050-loadgen")
    write_observer_stub(binary / "dur050-observer", header)
    aws = binary / "aws"
    aws.write_text("#!/usr/bin/env bash\nprintf '%s\\n' observer-password\n", encoding="utf-8")
    aws.chmod(0o755)

    submissions = root / "submissions.txt"
    config_path = root / "config.json"
    api = None

    class BoundHandler(Handler):
        pass

    with tempfile.TemporaryDirectory(prefix="dur050-api-"):
        api = WorkflowAPI(("127.0.0.1", 0), BoundHandler, submissions)
        handler = threading.Thread(target=api.serve_forever, daemon=True)
        handler.start()
        config_path.write_text(
            json.dumps(
                {
                    "api_url": f"http://127.0.0.1:{api.server_port}",
                    "namespace": "dur050-ci",
                    "run_id": "runner-ci",
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
        env["PATH"] = f"{binary}{os.pathsep}{env['PATH']}"
        env.update(
            {
                "DUR050_OBSERVER_SECRET_PARAMETER": "/ci/observer-password",
                "DUR050_DATABASE_PRIVATE_IP": "127.0.0.1",
                "DUR050_DATABASE_NAME": "dur050_ci",
                "DUR050_TEST_OBSERVER_COLUMNS": ",".join(header),
                "DUR050_TEST_SUBMISSIONS": str(submissions),
            }
        )
        if fail_loadgen:
            env["DUR050_TEST_OBSERVER_SHORT"] = "1"
        try:
            result = subprocess.run(
                [
                    "bash",
                    str(scripts / "dur050-run-unloaded-block.sh"),
                    str(config_path),
                    "ci-block",
                    "seq-8",
                    str(output),
                ],
                cwd=ROOT,
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=180,
                check=False,
            )
        finally:
            api.shutdown()
            api.server_close()
            handler.join(timeout=2)
    return result, output


def main() -> int:
    if "HOME" in os.environ:
        raise SystemExit("test must execute with HOME unset")
    observer = ROOT / "bin/dur050-observer"
    result = subprocess.run(
        [str(observer), "-print-csv-header"],
        text=True,
        capture_output=True,
        check=True,
    )
    header = next(csv.reader([result.stdout.strip()]))
    if len(header) != 19 or "completion_marker_seen" not in header:
        raise SystemExit(f"observer binary emitted unexpected source-derived header: {header}")

    complete, complete_dir = run_case(False, header)
    if complete.returncode != 0:
        raise SystemExit(f"100-sample runner failed ({complete.returncode}):\n{complete.stdout}")
    with (complete_dir / "unloaded-latency.csv").open(newline="", encoding="utf-8") as stream:
        latency_lines = sum(1 for _ in stream)
    if latency_lines != 101:
        raise SystemExit(f"unloaded-latency.csv has {latency_lines} lines, want 101")
    summary = (complete_dir / "block-summary.txt").read_text(encoding="utf-8")
    if "samples=100" not in summary:
        raise SystemExit(f"block summary did not report 100 samples:\n{summary}")
    summaries = list(complete_dir.glob("runs/*/submission.csv.summary.json"))
    pass_count = sum(
        json.loads(item.read_text(encoding="utf-8")).get("status") == "PASS" for item in summaries
    )
    if len(summaries) != 100 or pass_count != 100:
        raise SystemExit(f"per-run PASS summary count is {pass_count} of {len(summaries)}")
    shutil.rmtree(complete_dir.parent)
    print("PASS: unloaded runner completed 100 samples, 101 latency lines, and 100 PASS summaries.")

    failed, partial_dir = run_case(True, header)
    if failed.returncode == 0:
        raise SystemExit("runner accepted a load generator that exited 1")
    if not (partial_dir / "runs").is_dir() or not list(partial_dir.glob("runs/*/loadgen.log")):
        raise SystemExit("failed load-generator control did not retain partial evidence")
    print("PASS: load-generator exit 1 makes runner fail and preserves partial evidence.")
    shutil.rmtree(partial_dir.parent)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
