"""Out-of-process J4 stub with its own durable receipt journal."""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from dataclasses import asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from workers.recorded_wire import (
    EXPECTED_HASH,
    IDENTITY,
    MAX_BODY,
    MISMATCH,
    STUB_PATH,
    STUB_PORT,
    CallIdentity,
    canonical_request,
    request_hash,
    strict_json,
)

SCRIPTS: dict[str, tuple[int | str, ...]] = {
    "C0": ("success",),
    "C1": ("withhold", "success"),
    "C2": ("success",),
    "C3": (),
    "C4": ("success",),
    "B0": (1, "withhold", 12, "success"),
    "B1": ("unused", 1, "success"),
    "B2": ("withhold", 1, "success"),
    "B3": (20, "success"),
    "B4": (1, "withhold", 12, "withhold"),
    "B5": (1, "withhold", 12, "success"),
}
PROBE_PATH = "/readyz"
BOOT_ID = Path("/proc/sys/kernel/random/boot_id")


class ReceiptJournal:
    def __init__(self, path: Path, boot_id: str) -> None:
        self.boot_id = boot_id
        self._lock = threading.Lock()
        self._seen: set[str] = set()
        self._ordinals: set[tuple[str, int]] = set()
        self._seq = 0
        with path.open("a+b", buffering=0) as source:
            source.seek(0)
            for row in source:
                record = strict_json(row)
                if not row.endswith(b"\n") or record["sequence"] != self._seq + 1:
                    raise ValueError("incomplete or out-of-order J4 receipt journal")
                if record["boot_id"] != boot_id:
                    raise ValueError("J4 journal boot identity changed")
                self._seq += 1
                if record["event"] == "receipt" and record.get("call_id") is not None:
                    self._seen.add(record["call_id"])
                    if type(record["branch"]) is int or record["branch"] in {"withhold", "success"}:
                        self._ordinals.add((record["cell"], record["ordinal"]))
        self._file = path.open("ab")
        if sys.platform != "win32" and os.name == "posix":
            directory = os.open(path.parent, os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)

    def close(self) -> None:
        with self._lock:
            self._file.close()

    def _append(self, event: str, fields: dict[str, Any]) -> int:
        sequence = self._seq + 1
        record = {**fields, "sequence": sequence, "event": event, "boot_id": self.boot_id}
        self._file.write(json.dumps(record, separators=(",", ":")).encode() + b"\n")
        self._file.flush()
        os.fsync(self._file.fileno())
        self._seq = sequence
        return sequence

    def receipt(
        self,
        identity: CallIdentity | None,
        digest: str | None,
        branch: int | str,
        at_ns: int,
        header_names: list[str],
    ) -> tuple[int, int | str]:
        with self._lock:
            fields = asdict(identity) if identity is not None else {}
            if identity is not None:
                if identity.call_id in self._seen:
                    branch = "duplicate_id"
                self._seen.add(identity.call_id)
                if type(branch) is int or branch in {"withhold", "success"}:
                    slot = (identity.cell, identity.ordinal)
                    if slot in self._ordinals:
                        branch = "duplicate_ordinal"
                    else:
                        self._ordinals.add(slot)
            sequence = self._append(
                "receipt",
                {
                    **fields,
                    "at_ns": at_ns,
                    "request_sha256": digest,
                    "matched": digest == EXPECTED_HASH,
                    "branch": branch,
                    "header_names": header_names,
                },
            )
            return sequence, branch

    def record(self, event: str, receipt_sequence: int, **fields: Any) -> None:
        with self._lock:
            self._append(
                event,
                {"receipt_sequence": receipt_sequence, "at_ns": time.monotonic_ns(), **fields},
            )


class StubServer(ThreadingHTTPServer):
    daemon_threads = False

    def __init__(
        self,
        address: tuple[str, int],
        journal: ReceiptJournal,
        cells: dict[str, str],
    ) -> None:
        if not cells or any(
            IDENTITY.fullmatch(cell) is None or arm not in SCRIPTS for cell, arm in cells.items()
        ):
            raise ValueError("invalid J4 cell manifest")
        self.journal = journal
        self.cells = cells
        super().__init__(address, StubHandler)


class StubHandler(BaseHTTPRequestHandler):
    server: StubServer
    protocol_version = "HTTP/1.1"
    timeout = 15

    def log_message(self, _format: str, *_args: object) -> None:
        pass

    def handle_expect_100(self) -> bool:
        # The fixture rejects the extra header in do_POST. No interim
        # response may leave the stub before its receipt is synced.
        return True

    def do_GET(self) -> None:  # noqa: N802
        if self.path != PROBE_PATH:
            self.send_error(404)
            return
        self.respond(
            200,
            {"boot_id": self.server.journal.boot_id, "monotonic_ns": time.monotonic_ns()},
        )

    def header(self, name: str) -> str:
        values = self.headers.get_all(name, [])
        if len(values) != 1:
            raise ValueError(f"expected one {name} header")
        return values[0]

    def respond(self, status: int, value: Any, retry_after: int | None = None) -> None:
        body = json.dumps(value, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        if retry_after is not None:
            self.send_header("Retry-After", str(retry_after))
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()
        self.close_connection = True

    def do_POST(self) -> None:  # noqa: N802
        if self.path != STUB_PATH:
            self.send_error(404)
            return
        id_ = None
        digest = None
        branch: int | str = "malformed"
        names = sorted(name.lower() for name in self.headers)
        try:
            id_ = CallIdentity(
                self.header("X-J4-Cell"),
                self.header("X-J4-Workflow"),
                self.header("X-J4-Node"),
                int(self.header("X-J4-Iteration")),
                int(self.header("X-J4-Attempt")),
                int(self.header("X-J4-Ordinal")),
                self.header("X-J4-Call"),
            )
            allowed = {
                "host",
                "content-type",
                "content-length",
                "connection",
                *(name.lower() for name in id_.headers()),
            }
            if set(names) != allowed or len(names) != len(allowed):
                raise ValueError("unexpected provider header")
            length = int(self.header("Content-Length"))
            if not 0 < length <= MAX_BODY or self.header("Content-Type") != "application/json":
                raise ValueError("invalid fixture body framing")
            body = self.rfile.read(length)
            if len(body) != length:
                raise ValueError("incomplete fixture body")
            digest = request_hash(canonical_request(strict_json(body)))
            script = SCRIPTS[self.server.cells[id_.cell]]
            if digest != EXPECTED_HASH:
                branch = "mismatch"
            elif id_.ordinal <= len(script) and script[id_.ordinal - 1] != "unused":
                branch = script[id_.ordinal - 1]
            else:
                branch = "unexpected_ordinal"
        except (KeyError, OSError, ValueError):
            pass
        journal = self.server.journal
        seq, branch = journal.receipt(id_, digest, branch, time.monotonic_ns(), names)
        disposition = "response_complete"
        try:
            if branch == "withhold":
                journal.record("withhold", seq)
                disposition = "peer_closed" if self.connection.recv(1) == b"" else "unexpected_data"
                return
            if branch == "success" and id_ is not None:
                code = 200
                value = {
                    "completion": "fixture alpha",
                    "request_sha256": digest,
                    "provider_call_id": id_.call_id,
                }
            elif isinstance(branch, int):
                code, value = 429, {"reason": "SCRIPTED_RATE_LIMIT"}
            elif branch == "mismatch":
                code, value = 422, {"reason": MISMATCH}
            else:
                code, value = 400, {"reason": "UNEXPECTED_FIXTURE_REQUEST"}
            retry_after = branch if isinstance(branch, int) else None
            self.respond(code, value, retry_after)
            journal.record("response", seq, status=code, retry_after=retry_after, payload=value)
        except TimeoutError:
            disposition = "stub_timeout"
        except ConnectionResetError:
            disposition = "peer_reset"
        except OSError:
            disposition = "socket_error"
        finally:
            self.close_connection = True
            journal.record("close", seq, disposition=disposition)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cells", type=Path, required=True)
    parser.add_argument("--journal", type=Path, required=True)
    args = parser.parse_args()
    cells = strict_json(args.cells.read_bytes())
    if not isinstance(cells, dict):
        raise ValueError("expected a J4 cell map")
    journal = ReceiptJournal(args.journal, BOOT_ID.read_text().strip())
    try:
        with StubServer(("0.0.0.0", STUB_PORT), journal, cells) as server:
            server.serve_forever()
    finally:
        journal.close()


if __name__ == "__main__":
    main()
