"""Opt-in K6 worker ledger persisted before an invocation enters its payload."""

from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import time
import uuid
from contextvars import ContextVar
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from workers.k4_witness import persistence_failure, sync_directory
from workers.lifecycle import worker_identity


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def utc(ns: int) -> str:
    sec, fraction = divmod(ns, 10**9)
    return f"{datetime.fromtimestamp(sec, UTC):%Y-%m-%dT%H:%M:%S}.{fraction:09d}Z"


def identifier(value: str) -> str:
    if (
        not re.fullmatch(r"[a-zA-Z0-9_.:@/+<>&=-]{1,512}", value)
        or "://" in value
        or re.fullmatch(r"eyJ[\w-]+\.[\w-]+\.[\w-]+", value)
    ):
        raise ValueError("K6 worker evidence identity")
    return value


def encoded(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n"
    ).encode()


def wire(body: bytes) -> dict[str, int | str]:
    return {"length": len(body), "sha256": digest(body)}


class WorkerLedger:
    def __init__(self, directory: Path) -> None:
        self.wall = time.time_ns()
        self.monotonic = time.monotonic_ns()
        self.session_id = str(uuid.uuid4())
        self.path = directory / f"{self.session_id}.jsonl"
        self.lock = threading.Lock()
        self.rows: list[bytes] = []
        self.closed = False
        self.session = {
            "id": self.session_id,
            "claimant": identifier(os.environ.get("DUR053_K6_G_CLAIMANT") or worker_identity()),
            "pod_uid": identifier(os.environ["POD_UID"]),
            "container_id": identifier(os.environ["CONTAINER_ID"]),
            "restart": int(os.environ["CONTAINER_RESTART_COUNT"]),
            "container_started_utc": utc(int(os.environ["CONTAINER_STARTED_NS"])),
            "boot_id": self.session_id,
            "started_utc": utc(self.wall),
        }
        self.header = encoded(
            {
                "schema": "dur053.k6-worker.v1",
                "opened_utc": self.session["started_utc"],
                "source": "worker-ledger",
                "binding": {
                    "namespace": identifier(os.environ["RUNTIME_NAMESPACE"]),
                    "episode": identifier(os.environ["DUR053_K6_G_EPISODE"]),
                    "reset_id": identifier(os.environ["DUR053_K6_G_RESET"]),
                    "barrier_id": identifier(os.environ["DUR053_K6_G_BARRIER"]),
                },
            }
        )
        self.append("session", session=self.session)
        if os.getenv("DUR053_K6_G_CLAIMANT"):
            from workers.k6_effect import emit_observation

            emit_observation("k6_g_session " + encoded(self.session).decode().rstrip("\n"))

    def clock(self) -> str:
        return utc(self.wall + time.monotonic_ns() - self.monotonic)

    def append(
        self,
        kind: str,
        invocation: str = "",
        *,
        session: dict[str, Any] | None = None,
        attempt: dict[str, Any] | None = None,
        traffic: dict[str, Any] | None = None,
        response: dict[str, int | str] | None = None,
    ) -> None:
        with self.lock:
            if self.closed:
                persistence_failure()
            self.rows.append(
                encoded(
                    {
                        "position": {"sequence": len(self.rows) + 1, "utc": self.clock()},
                        "data": {
                            "kind": kind,
                            "session": session,
                            "invocation": invocation,
                            "attempt": attempt,
                            "traffic": traffic,
                            "response": response,
                            "response_wire": None,
                        },
                    }
                )
            )
            self._persist("checkpoint")

    def _persist(self, mode: str) -> None:
        raw = b"".join(self.rows)
        count = len(self.rows)
        seal = encoded(
            {
                "source": "worker-ledger",
                "mode": mode,
                "first": 1,
                "last": count,
                "attempted": count,
                "written": count,
                "errors": 0,
                "length": len(raw),
                "sha256": digest(raw),
                "through_utc": self.clock(),
            }
        )
        temporary = self.path.with_suffix(".pending")
        try:
            with temporary.open("wb") as handle:
                handle.write(self.header + raw + seal)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
            sync_directory(self.path.parent)
        except OSError:
            persistence_failure()

    def checkpoint(self, terminal: bool = False) -> None:
        with self.lock:
            if not self.closed:
                self._persist("terminal" if terminal else "checkpoint")
                self.closed = terminal

    def begin(
        self,
        workflow: str,
        node: str,
        iteration: int,
        number: int,
        claim_token: str,
        claim_request: str,
    ) -> tuple[str, dict[str, Any]]:
        invocation = str(uuid.uuid4())
        attempt: dict[str, Any] = {
            "workflow": identifier(workflow),
            "node": identifier(node),
            "iteration": iteration,
            "attempt": number,
            "claim_hash": digest(claim_token.encode()),
            "claim_request_id": identifier(claim_request),
        }
        self.append("start", invocation, attempt=attempt)
        return invocation, attempt


def transfer(
    operation: str, request: dict[str, Any], invocation: str, body: bytes
) -> dict[str, Any]:
    return {
        "method": "POST",
        "operation": operation,
        "message_hash": digest(invocation.encode()),
        "request_id_hash": digest(str(request["request_id"]).encode()),
        "key_hash": digest(str(request["logical_key"]).encode()),
        "intent": str(uuid.UUID(request["intent_id"])),
        "resource_hash": digest(str(request["resource_id"]).encode()),
        "grant_hash": digest(str(request["grant_token"]).encode()),
        "attempt": int(request["attempt"]),
        "worker_binding": "",
        "request": wire(body),
        "wire": None,
    }


active: ContextVar[tuple[WorkerLedger, str] | None] = ContextVar("k6-active-exchange", default=None)
_ledger: WorkerLedger | None = None


def initialize() -> WorkerLedger | None:
    global _ledger
    if directory := os.getenv("DUR053_K6_G_WORKER_DIR"):
        _ledger = WorkerLedger(Path(directory))
    return _ledger


def ledger() -> WorkerLedger | None:
    return _ledger
