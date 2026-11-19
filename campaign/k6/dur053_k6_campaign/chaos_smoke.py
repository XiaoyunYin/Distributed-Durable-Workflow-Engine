"""Timing fixture and observations for F1a's tagged engineering worker."""

from __future__ import annotations

import hashlib
import os
import re
import threading
import time
from pathlib import Path
from typing import Any

from workers.control import ControlError
from workers.k4_control import CURRENT
from workers.kafka_worker import RetryingControl
from workers.lifecycle import WorkerLifecycle
from workers.witness import campaign_witness

LEASE_MS = 16_000
GATE_SECONDS = 60


class SmokeControl(RetryingControl):
    def __init__(self, url: str, stop: threading.Event, lifecycle: WorkerLifecycle) -> None:
        super().__init__(url, stop, lifecycle)
        self.timeout = 2
        stat = Path("/proc/self/stat").read_text().rsplit(")", 1)[1].split()
        self.process = f"{os.getpid()}:{stat[19]}"

    def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        operation = path.rsplit("/", 1)[-1]
        if operation == "claim":
            body = {**body, "attempt_lease_ms": LEASE_MS}
        elif operation == "heartbeat":
            body = {**body, "extension_ms": LEASE_MS}
        return super()._post(path, body)

    def _post_once(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        witness = campaign_witness()
        if witness is None:
            raise ValueError("F1A_WORKER_WITNESS_REQUIRED")
        route = re.fullmatch(
            r"/v1/workflows/([^/]+)/nodes/([^/]+)/iterations/(\d+)/(claim|heartbeat|result)", path
        )
        if route is None:
            return super()._post_once(path, body)
        workflow, node, iteration, operation = route.groups()
        token = body.get("claim_token")
        fields = {
            "workflow": workflow,
            "node": node,
            "iteration": int(iteration),
            "operation": operation,
            "base_url": self.base_url,
            "path": path,
            "process": self.process,
            "attempt_number": body.get("attempt_number", body.get("expected_attempt")),
            "token_hash": hashlib.sha256(token.encode()).hexdigest() if token else None,
            "request": trace.request if (trace := CURRENT.get()) else None,
            "sent_ns": time.time_ns(),
        }
        witness.trace("f1a_http_start", {**fields, "at_ns": time.time_ns()})
        try:
            result = super()._post_once(path, body)
        except ControlError as error:
            witness.trace(
                "f1a_http",
                {**fields, "at_ns": time.time_ns(), "status": error.status, "code": error.code},
            )
            raise
        witness.trace("f1a_http", {**fields, "at_ns": time.time_ns(), "status": 200, "code": "OK"})
        return result


def activity_gate(boundary: str, fields: dict[str, str]) -> None:
    if boundary != "activity_start":
        return
    witness = campaign_witness()
    if witness is None:
        raise ValueError("F1A_WORKER_WITNESS_REQUIRED")
    deadline = time.monotonic() + GATE_SECONDS
    release = Path("/witness") / "f1a-release"
    invocation_release = release.with_name(f"f1a-release-{fields['invocation']}")
    while time.monotonic() < deadline:
        if release.is_file() or invocation_release.is_file():
            witness.trace("f1a_activity_release", {**fields, "at_ns": time.time_ns()})
            return
        witness.trace("f1a_activity_wait", {**fields, "at_ns": time.time_ns()})
        time.sleep(0.5)
    witness.trace("f1a_activity_expired", {**fields, "at_ns": time.time_ns()})
    raise TimeoutError("F1A_ACTIVITY_GATE_EXPIRED")
