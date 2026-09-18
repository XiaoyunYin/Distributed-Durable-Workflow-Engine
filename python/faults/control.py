"""Event-driven controller for reusable named-boundary fault fixtures.

The controller gives an arbitrary child command a loopback protocol endpoint
through environment variables. A target reports a named boundary over that
dedicated channel and blocks until an explicit release command. stdout/stderr
remain ordinary logs and are drained independently.
"""

from __future__ import annotations

import argparse
import contextlib
import ctypes
import json
import os
import queue
import secrets
import signal
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import IO, cast

TRACE_SCHEMA_VERSION = "fault-trace.v1"


@dataclass(frozen=True)
class BoundaryReached:
    """A boundary reported by a target failpoint client."""

    name: str
    seed: int
    fields: dict[str, object]


@dataclass(frozen=True)
class ProtocolEvent:
    """A non-boundary event reported by a target failpoint client."""

    name: str
    payload: dict[str, object]


@dataclass(frozen=True)
class ProcessExited:
    """The child process exited, whether or not it reached a boundary."""

    return_code: int


@dataclass(frozen=True)
class FaultOutcome:
    """Observed result of a requested fault action.

    A requested command is not considered successful until the trace contains
    the corresponding observation.  Campaign runners use this object rather
    than inferring success from a timeout or a process return code alone.
    """

    boundary_reached: bool
    requested_action: str | None
    observed_action: str | None
    process_return_code: int | None
    cleaned_up: bool


ControllerEvent = BoundaryReached | ProtocolEvent | ProcessExited


def _object_from_json(line: bytes) -> dict[str, object] | None:
    value = json.loads(line.decode("utf-8"))
    if not isinstance(value, dict):
        return None
    return cast(dict[str, object], value)


class FaultController:
    """Start and control one arbitrary deterministic target process."""

    def __init__(self, seed: int, boundary: str, trace_path: Path | None = None) -> None:
        self.seed = seed
        self.boundary = boundary
        self.trace_path = trace_path
        self._events: queue.Queue[ControllerEvent] = queue.Queue()
        self._trace: list[dict[str, object]] = []
        self._trace_lock = threading.Lock()
        self._connection_lock = threading.Lock()
        self._exit_lock = threading.Lock()
        self._exit_recorded = False
        self._process: subprocess.Popen[str] | None = None
        self._server: socket.socket | None = None
        self._connection: socket.socket | None = None
        self._token = secrets.token_hex(16)
        self._run_id = secrets.token_hex(16)
        self._boundary_reached = False
        self._requested_action: str | None = None
        self._observed_action: str | None = None
        self._process_return_code: int | None = None
        self._cleaned_up = False
        self._cleanup_attempted = False
        self._cleanup_bounded = False

    def _prepare_trace(self) -> None:
        if self.trace_path is None:
            return
        self.trace_path.parent.mkdir(parents=True, exist_ok=True)
        # A trace path names one run. Refuse reuse so a campaign cannot
        # silently concatenate two runs with independent sequence numbers.
        with self.trace_path.open("x", encoding="utf-8", newline="\n") as handle:
            handle.flush()
            os.fsync(handle.fileno())

    def _record(self, event: str, **fields: object) -> None:
        with self._trace_lock:
            record: dict[str, object] = {
                "schema_version": TRACE_SCHEMA_VERSION,
                "run_id": self._run_id,
                "sequence": len(self._trace) + 1,
                "event": event,
                "seed": self.seed,
            }
            record.update(fields)
            self._trace.append(record)
            if self.trace_path is not None:
                self.trace_path.parent.mkdir(parents=True, exist_ok=True)
                with self.trace_path.open("a", encoding="utf-8", newline="\n") as handle:
                    handle.write(json.dumps(record, sort_keys=True) + "\n")
                    handle.flush()
                    os.fsync(handle.fileno())

    def start(
        self,
        command: Sequence[str] | None = None,
        *,
        skip_boundary: bool = False,
        cwd: Path | None = None,
    ) -> None:
        if self._process is not None:
            raise RuntimeError("controller already started")
        self._prepare_trace()

        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        server.settimeout(0.2)
        self._server = server

        environment = os.environ.copy()
        environment["DURABLE_FAULT_ENDPOINT"] = f"127.0.0.1:{server.getsockname()[1]}"
        environment["DURABLE_FAULT_TOKEN"] = self._token
        environment["DURABLE_FAULT_SEED"] = str(self.seed)
        python_root = str(Path(__file__).resolve().parents[1])
        existing_python_path = environment.get("PYTHONPATH")
        environment["PYTHONPATH"] = os.pathsep.join(
            path for path in (python_root, existing_python_path) if path
        )

        child_command = (
            list(command)
            if command is not None
            else [
                sys.executable,
                "-m",
                "faults.target",
                "--seed",
                str(self.seed),
                "--boundary",
                self.boundary,
            ]
        )
        if skip_boundary and command is None:
            child_command.append("--skip-boundary")
        self._process = subprocess.Popen(
            child_command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=environment,
            cwd=str(cwd) if cwd is not None else None,
        )
        self._record("process_started", boundary=self.boundary, command=child_command)
        threading.Thread(target=self._read_protocol, daemon=True).start()
        threading.Thread(
            target=self._watch_process,
            daemon=True,
        ).start()
        threading.Thread(
            target=self._drain_output,
            args=(self._process.stdout, "target_stdout"),
            daemon=True,
        ).start()
        threading.Thread(
            target=self._drain_output,
            args=(self._process.stderr, "target_stderr"),
            daemon=True,
        ).start()

    def _read_protocol(self) -> None:
        server = self._server
        if server is None:
            return
        connection: socket.socket | None = None
        while connection is None:
            try:
                connection, _ = server.accept()
            except TimeoutError:
                process = self._process
                if process is not None and process.poll() is not None:
                    return
            except OSError:
                return
        with self._connection_lock:
            self._connection = connection
        buffer = b""
        try:
            while True:
                chunk = connection.recv(4096)
                if not chunk:
                    return
                buffer += chunk
                while b"\n" in buffer:
                    raw_line, buffer = buffer.split(b"\n", 1)
                    self._handle_protocol_line(raw_line)
        except OSError as error:
            self._record("protocol_error", detail=f"protocol channel: {error}")

    def _handle_protocol_line(self, raw_line: bytes) -> None:
        try:
            payload = _object_from_json(raw_line)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            self._record("protocol_error", detail=str(error))
            return
        if payload is None:
            self._record("protocol_error", detail="protocol message was not an object")
            return
        if payload.get("token") != self._token:
            self._record("protocol_error", detail="protocol token mismatch")
            return
        event = payload.get("event")
        boundary = payload.get("boundary")
        seed = payload.get("seed")
        fields_value = payload.get("fields", {})
        fields = cast(dict[str, object], fields_value) if isinstance(fields_value, dict) else {}
        if not isinstance(event, str) or not isinstance(boundary, str) or not isinstance(seed, int):
            self._record("protocol_error", detail="protocol event has invalid fields")
            return
        if event == "boundary_reached":
            self._record("boundary_reached", boundary=boundary, fields=fields)
            if boundary == self.boundary and seed == self.seed:
                self._boundary_reached = True
            self._events.put(BoundaryReached(boundary, seed, fields))
        else:
            self._record(event, boundary=boundary, fields=fields)
            self._events.put(ProtocolEvent(event, payload))

    def _watch_process(self) -> None:
        process = self._require_process()
        return_code = process.wait()
        self._record_process_exit(return_code)

    def _record_process_exit(self, return_code: int) -> None:
        with self._exit_lock:
            if self._exit_recorded:
                return
            self._exit_recorded = True
            self._process_return_code = return_code
            self._record("process_exited", return_code=return_code)
            self._events.put(ProcessExited(return_code))
            server = self._server
            if server is not None:
                with contextlib.suppress(OSError):
                    server.close()

    def _drain_output(self, stream: IO[str] | None, event: str) -> None:
        if stream is None:
            return
        for line in stream:
            self._record(event, line=line.rstrip("\r\n"))

    def wait_for_boundary(self, timeout_seconds: float) -> BoundaryReached | None:
        deadline = time.monotonic() + timeout_seconds
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self._record(
                    "boundary_timeout",
                    boundary=self.boundary,
                    timeout_seconds=timeout_seconds,
                )
                self._observed_action = "boundary_not_reached"
                return None
            try:
                event = self._events.get(timeout=remaining)
            except queue.Empty:
                self._record(
                    "boundary_timeout",
                    boundary=self.boundary,
                    timeout_seconds=timeout_seconds,
                )
                self._observed_action = "boundary_not_reached"
                return None
            if isinstance(event, ProcessExited):
                self._record(
                    "process_exited_before_boundary",
                    boundary=self.boundary,
                    return_code=event.return_code,
                )
                return None
            if isinstance(event, BoundaryReached):
                if event.name != self.boundary or event.seed != self.seed:
                    self._record(
                        "protocol_error",
                        detail="target reported an unexpected boundary or seed",
                        boundary=event.name,
                        reported_seed=event.seed,
                    )
                    raise RuntimeError("target reported an unexpected boundary")
                self._boundary_reached = True
                return event

    def wait_for_event(self, name: str, timeout_seconds: float) -> bool:
        deadline = time.monotonic() + timeout_seconds
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            try:
                event = self._events.get(timeout=remaining)
            except queue.Empty:
                return False
            if isinstance(event, ProtocolEvent) and event.name == name:
                return True
            if isinstance(event, ProcessExited):
                # The child can exit immediately after sending its final
                # protocol event; let the reader drain that event first.
                continue

    def pause(self) -> None:
        """Record that the target remains blocked at its reported barrier.

        This is the portable cooperative pause.  ``pause_process`` below is
        the process-level variant used by POSIX campaign hosts.
        """
        self._require_process()
        self._requested_action = "pause"
        self._record("command", command="pause", boundary=self.boundary)

    def pause_process(self) -> bool:
        """Pause the child when the host exposes SIGSTOP; otherwise report a
        portable cooperative pause without pretending an OS pause occurred.
        """
        process = self._require_process()
        self._requested_action = "pause_process"
        self._record("command", command="pause_process", boundary=self.boundary)
        if os.name == "nt":
            if self._windows_process_control("NtSuspendProcess"):
                self._observed_action = "process_paused"
                self._record("fault_observed", action="process_paused", pid=process.pid)
                return True
            self._observed_action = "pause_at_boundary"
            self._record("fault_observed", action="pause_at_boundary", supported=False)
            return False
        sigstop = getattr(signal, "SIGSTOP", None)
        if not isinstance(sigstop, int):
            self._observed_action = "pause_at_boundary"
            self._record("fault_observed", action="pause_at_boundary", supported=False)
            return False
        if process.poll() is not None:
            self._observed_action = "process_already_exited"
            self._record("fault_observed", action=self._observed_action)
            return False
        os.kill(process.pid, sigstop)
        self._observed_action = "process_paused"
        self._record("fault_observed", action="process_paused", pid=process.pid)
        return True

    def resume_process(self) -> bool:
        """Resume a process paused by :meth:`pause_process` on POSIX."""
        process = self._require_process()
        if os.name == "nt":
            resumed = self._windows_process_control("NtResumeProcess")
            if resumed:
                self._record("fault_observed", action="process_resumed", pid=process.pid)
                self._observed_action = "process_resumed"
            else:
                self._record("fault_observed", action="resume_unsupported", supported=False)
            return resumed
        sigcont = getattr(signal, "SIGCONT", None)
        if not isinstance(sigcont, int):
            self._record("fault_observed", action="resume_unsupported", supported=False)
            return False
        os.kill(process.pid, sigcont)
        self._record("fault_observed", action="process_resumed", pid=process.pid)
        self._observed_action = "process_resumed"
        return True

    def _windows_process_control(self, operation: str) -> bool:
        process = self._require_process()
        loader = getattr(ctypes, "WinDLL", None)
        if not callable(loader) or process.poll() is not None:
            return False
        try:
            ntdll = loader("ntdll")
            kernel32 = loader("kernel32", use_last_error=True)
            open_process = kernel32.OpenProcess
            open_process.argtypes = [ctypes.c_uint32, ctypes.c_int, ctypes.c_uint32]
            open_process.restype = ctypes.c_void_p
            close_handle = kernel32.CloseHandle
            close_handle.argtypes = [ctypes.c_void_p]
            close_handle.restype = ctypes.c_int
            handle = open_process(0x0800 | 0x0400, 0, process.pid)
            if not handle:
                return False
            try:
                function = getattr(ntdll, operation)
                function.argtypes = [ctypes.c_void_p]
                function.restype = ctypes.c_long
                return bool(function(handle) == 0)
            finally:
                close_handle(handle)
        except (AttributeError, OSError):
            return False

    def release(self) -> None:
        self._require_process()
        with self._connection_lock:
            connection = self._connection
        if connection is None:
            raise RuntimeError("target has not opened the fault channel")
        self._requested_action = "release"
        payload = {
            "command": "release",
            "boundary": self.boundary,
            "token": self._token,
            "seed": self.seed,
        }
        connection.sendall((json.dumps(payload, sort_keys=True) + "\n").encode("utf-8"))
        self._record("command", command="release", boundary=self.boundary)

    def inject_message(self, payload: dict[str, object]) -> None:
        """Send a deliberately manipulated protocol command to the target.

        The target is expected to reject or record the command.  The method
        never calls that rejection success; the resulting trace is the
        evidence consumed by a campaign.
        """
        self._require_process()
        with self._connection_lock:
            connection = self._connection
        if connection is None:
            raise RuntimeError("target has not opened the fault channel")
        message = dict(payload)
        message.setdefault("boundary", self.boundary)
        message.setdefault("token", self._token)
        message.setdefault("seed", self.seed)
        connection.sendall((json.dumps(message, sort_keys=True) + "\n").encode("utf-8"))
        self._requested_action = "message_injection"
        self._record("command", command="message_injection", payload=message)

    def cut_network(self) -> None:
        """Cut the controller channel and record the observed channel fault."""
        self._require_process()
        self._requested_action = "network_cut"
        with self._connection_lock:
            connection = self._connection
        if connection is not None:
            with contextlib.suppress(OSError):
                connection.shutdown(socket.SHUT_RDWR)
            with contextlib.suppress(OSError):
                connection.close()
        self._observed_action = "network_cut"
        self._record("command", command="network_cut", boundary=self.boundary)
        self._record("fault_observed", action="network_cut", channel_closed=True)

    def kill(self) -> bool:
        process = self._require_process()
        self._requested_action = "kill"
        self._record("command", command="kill", boundary=self.boundary)
        if process.poll() is not None:
            self._observed_action = "process_already_exited"
            self._record("fault_observed", action=self._observed_action, pid=process.pid)
            return False
        with contextlib.suppress(OSError):
            process.kill()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self._observed_action = "process_kill_unconfirmed"
            self._record("fault_observed", action=self._observed_action, pid=process.pid)
            return False
        self._observed_action = "process_killed"
        self._record("fault_observed", action="process_killed", pid=process.pid)
        return True

    def finish(self) -> int:
        process = self._require_process()
        try:
            return_code = process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            if not self.kill():
                raise RuntimeError("target process did not exit after kill") from None
            return_code = process.wait(timeout=5)
        self._record_process_exit(return_code)
        return return_code

    def cleanup(self, timeout_seconds: float = 5.0) -> int | None:
        """Bounded, idempotent cleanup used by campaign runners."""
        if self._cleanup_attempted:
            return self._process_return_code
        self._cleanup_attempted = True
        process = self._process
        if process is None:
            self._cleaned_up = True
            self._cleanup_bounded = True
            self._record("cleanup_completed", bounded=True, process_present=False)
            return None
        if process.poll() is None:
            with contextlib.suppress(OSError):
                process.kill()
        try:
            return_code = process.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            self._record("cleanup_timeout", timeout_seconds=timeout_seconds)
            self._record("cleanup_unbounded", pid=process.pid, process_alive=process.poll() is None)
            self._cleanup_bounded = False
            self._cleaned_up = False
            return_code = None
        else:
            assert return_code is not None
            self._record_process_exit(return_code)
            self._cleanup_bounded = True
            self._cleaned_up = True
        with self._connection_lock:
            connection = self._connection
        if connection is not None:
            with contextlib.suppress(OSError):
                connection.close()
        if self._server is not None:
            with contextlib.suppress(OSError):
                self._server.close()
        if self._cleanup_bounded:
            self._record("cleanup_completed", bounded=True)
        return return_code

    def outcome(self) -> FaultOutcome:
        return FaultOutcome(
            self._boundary_reached,
            self._requested_action,
            self._observed_action,
            self._process_return_code,
            self._cleaned_up,
        )

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
    controller.start(command=args.command, skip_boundary=args.skip_boundary)
    reached = controller.wait_for_boundary(args.timeout_seconds)
    if reached is None:
        controller.kill()
        controller.finish()
        controller.cleanup()
        print(json.dumps({"status": "boundary_not_reached", "boundary": args.boundary}))
        return 2
    if args.action == "release":
        controller.release()
        if not controller.wait_for_event("released", 5):
            controller.kill()
            controller.finish()
            controller.cleanup()
            print(json.dumps({"status": "release_ack_missing", "boundary": args.boundary}))
            return 3
    elif args.action == "kill":
        controller.kill()
    elif args.action == "pause":
        controller.pause_process()
        time.sleep(min(args.pause_seconds, 1.0))
        controller.resume_process()
        controller.release()
        controller.wait_for_event("released", 5)
    elif args.action == "cut-network":
        controller.cut_network()
    elif args.action == "message":
        controller.inject_message(
            {"command": "release", "boundary": args.boundary, "fields": {"injected": True}}
        )
    controller.finish()
    controller.cleanup()
    print(json.dumps({"status": "completed", "action": args.action, "boundary": args.boundary}))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--boundary", default="after-effect")
    parser.add_argument(
        "--action",
        choices=("release", "kill", "pause", "cut-network", "message"),
        default="release",
    )
    parser.add_argument("--pause-seconds", type=float, default=0.1)
    parser.add_argument("--timeout-seconds", type=float, default=5.0)
    parser.add_argument("--trace")
    parser.add_argument("--skip-boundary", action="store_true")
    parser.add_argument("--command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    return _run_command(args)


if __name__ == "__main__":
    raise SystemExit(main())
