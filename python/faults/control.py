"""Event-driven controller for named-boundary process fixtures.

The target reports a named boundary and then blocks on an explicit command.
The controller waits for that report, so release and kill operations are tied
to an observed protocol event rather than an arbitrary sleep duration.
"""

from __future__ import annotations

import argparse
import json
import os
import queue
import subprocess
import sys
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import cast

TRACE_SCHEMA_VERSION = "fault-trace.v1"


@dataclass(frozen=True)
class BoundaryReached:
    """A boundary reported by the target process."""

    name: str
    seed: int
    fingerprint: str


def _object_from_json(line: str) -> dict[str, object] | None:
    value = json.loads(line)
    if not isinstance(value, dict):
        return None
    return cast(dict[str, object], value)


class FaultController:
    """Start and control one deterministic target process."""

    def __init__(self, seed: int, boundary: str, trace_path: Path | None = None) -> None:
        self.seed = seed
        self.boundary = boundary
        self.trace_path = trace_path
        self._events: queue.Queue[BoundaryReached] = queue.Queue()
        self._trace: list[dict[str, object]] = []
        self._trace_lock = threading.Lock()
        self._process: subprocess.Popen[str] | None = None

    def _record(self, event: str, **fields: object) -> None:
        with self._trace_lock:
            record: dict[str, object] = {
                "schema_version": TRACE_SCHEMA_VERSION,
                "sequence": len(self._trace) + 1,
                "event": event,
                "seed": self.seed,
            }
            record.update(fields)
            self._trace.append(record)
            if self.trace_path is not None:
                self.trace_path.parent.mkdir(parents=True, exist_ok=True)
                lines = "".join(f"{json.dumps(item, sort_keys=True)}\n" for item in self._trace)
                self.trace_path.write_text(lines, encoding="utf-8")

    def start(self, skip_boundary: bool = False) -> None:
        if self._process is not None:
            raise RuntimeError("controller already started")
        python_root = str(Path(__file__).resolve().parents[1])
        environment = os.environ.copy()
        existing_python_path = environment.get("PYTHONPATH")
        environment["PYTHONPATH"] = os.pathsep.join(
            path for path in (python_root, existing_python_path) if path
        )
        command = [
            sys.executable,
            "-m",
            "faults.target",
            "--seed",
            str(self.seed),
            "--boundary",
            self.boundary,
        ]
        if skip_boundary:
            command.append("--skip-boundary")
        self._process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=environment,
        )
        self._record("process_started", boundary=self.boundary)
        reader = threading.Thread(target=self._read_output, daemon=True)
        reader.start()

    def _read_output(self) -> None:
        process = self._process
        if process is None or process.stdout is None:
            return
        for line in process.stdout:
            event = _object_from_json(line)
            if event is None:
                continue
            if event.get("event") == "boundary_reached":
                name = event.get("boundary")
                seed = event.get("seed")
                fingerprint = event.get("work_fingerprint")
                if isinstance(name, str) and isinstance(seed, int) and isinstance(fingerprint, str):
                    self._record("boundary_reached", boundary=name, fingerprint=fingerprint)
                    self._events.put(BoundaryReached(name, seed, fingerprint))

    def wait_for_boundary(self, timeout_seconds: float) -> BoundaryReached | None:
        try:
            reached = self._events.get(timeout=timeout_seconds)
        except queue.Empty:
            self._record(
                "boundary_timeout",
                boundary=self.boundary,
                timeout_seconds=timeout_seconds,
            )
            return None
        if reached.name != self.boundary or reached.seed != self.seed:
            self._record(
                "protocol_error",
                boundary=reached.name,
                expected_boundary=self.boundary,
                reported_seed=reached.seed,
            )
            raise RuntimeError("target reported an unexpected boundary")
        return reached

    def release(self) -> None:
        process = self._require_process()
        if process.stdin is None:
            raise RuntimeError("target stdin is unavailable")
        process.stdin.write(json.dumps({"command": "release", "boundary": self.boundary}) + "\n")
        process.stdin.flush()
        self._record("command", command="release", boundary=self.boundary)

    def pause(self) -> None:
        """Record that the target remains held at its reported barrier."""
        self._require_process()
        self._record("command", command="pause", boundary=self.boundary)

    def kill(self) -> None:
        process = self._require_process()
        if process.poll() is None:
            process.kill()
        self._record("command", command="kill", boundary=self.boundary)

    def finish(self) -> int:
        process = self._require_process()
        try:
            return_code = process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.kill()
            return_code = process.wait(timeout=5)
        self._record("process_exited", return_code=return_code)
        return return_code

    def trace(self) -> list[dict[str, object]]:
        with self._trace_lock:
            return list(self._trace)

    def _require_process(self) -> subprocess.Popen[str]:
        if self._process is None:
            raise RuntimeError("controller has not been started")
        return self._process


def _run_command(args: argparse.Namespace) -> int:
    trace_path = Path(args.trace) if args.trace else None
    controller = FaultController(args.seed, args.boundary, trace_path)
    controller.start(skip_boundary=args.skip_boundary)
    reached = controller.wait_for_boundary(args.timeout_seconds)
    if reached is None:
        controller.kill()
        controller.finish()
        print(json.dumps({"status": "timeout", "boundary": args.boundary}))
        return 2
    if args.action == "release":
        controller.release()
    else:
        controller.kill()
    controller.finish()
    print(json.dumps({"status": "completed", "action": args.action, "boundary": args.boundary}))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--boundary", default="after-effect")
    parser.add_argument("--action", choices=("release", "kill"), default="release")
    parser.add_argument("--timeout-seconds", type=float, default=5.0)
    parser.add_argument("--trace")
    parser.add_argument("--skip-boundary", action="store_true")
    args = parser.parse_args()
    return _run_command(args)


if __name__ == "__main__":
    raise SystemExit(main())
