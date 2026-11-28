"""J4 B2 worker path. The private Go control service owns the retry schedule."""

from __future__ import annotations

import re
import time
from collections.abc import Callable
from dataclasses import asdict
from pathlib import Path
from typing import Any
from uuid import uuid4

from workers.recorded_activity import ProviderConfig, call_provider
from workers.recorded_wire import (
    EXPECTED_HASH,
    MISMATCH,
    CallIdentity,
    ProviderReply,
    Trace,
    canonical_request,
    request_hash,
)

BOOT_ID = Path("/proc/sys/kernel/random/boot_id")
Exchange = Callable[[str, dict[str, Any]], dict[str, Any]]


def wait_until(deadline_ns: int) -> None:
    while (remaining := deadline_ns - time.monotonic_ns()) > 0:
        time.sleep(remaining / 1_000_000_000)


def provider_outcome(reply: ProviderReply, call: CallIdentity, digest: str) -> tuple[str, int]:
    if reply.status == 0:
        return "timeout", 0
    if reply.status == 429:
        raw = reply.retry_after
        if raw is None or not re.fullmatch(r"[0-9]{1,10}", raw, re.ASCII):
            raise ValueError("expected a valid J4 Retry-After delta")
        seconds = int(raw)
        if seconds > 2_147_483_647:
            raise ValueError("J4 Retry-After delta is out of range")
        return "rate_limited", seconds
    if reply.status == 422 and reply.payload == {"reason": MISMATCH}:
        return "mismatch", 0
    expected = {
        "completion": "fixture alpha",
        "request_sha256": EXPECTED_HASH,
        "provider_call_id": call.call_id,
    }
    if reply.status == 200 and digest == EXPECTED_HASH and reply.payload == expected:
        return "success", 0
    raise ValueError("unexpected J4 provider outcome")


def complete_resilient(
    value: Any,
    *,
    workflow_id: str,
    node_id: str,
    iteration: int,
    attempt: int,
    claim_token: str,
    exchange: Exchange,
    trace: Trace,
) -> tuple[str, dict[str, str]]:
    digest = request_hash(canonical_request(value))
    config = ProviderConfig.load()
    boot_id = BOOT_ID.read_text().strip()
    authority = {"attempt_number": attempt, "claim_token": claim_token, "boot_id": boot_id}

    def post(action: str, fields: dict[str, Any]) -> dict[str, Any]:
        result = exchange(action, {**authority, **fields})
        if not boot_id or result.get("boot_id") != boot_id:
            raise RuntimeError("J4 shared host boot identity mismatch")
        return result

    schedule = post("reserve", {})
    trace(
        {
            "event": "schedule_read",
            "at_ns": time.monotonic_ns(),
            "boot_id": boot_id,
            "outcome_ns": schedule["outcome_ns"],
            "eligible_ns": schedule["eligible_ns"],
        }
    )
    used_ids: set[str] = set()
    while True:
        if terminal := schedule.get("terminal_state"):
            if terminal not in {"SUCCEEDED", "FAILED_FINAL"} or not isinstance(
                schedule.get("payload"), dict
            ):
                raise ValueError("invalid durable J4 terminal outcome")
            return terminal, schedule["payload"]

        wait_until(schedule["eligible_ns"])
        # The normal worker control client reuses this exact body after any
        # uncertain response. The worker never retries the provider transport.
        schedule = post("reserve", {"request_id": uuid4().hex})
        if not (permit := schedule.get("permit")):
            if not schedule.get("terminal_state") and schedule["eligible_ns"] <= 0:
                raise ValueError("J4 control granted neither a permit nor a future eligibility")
            continue
        sequence, call_id = permit["sequence"], permit["call_id"]
        if (
            type(sequence) is not int
            or not 1 <= sequence <= 4
            or permit["attempt"] != attempt
            or not isinstance(call_id, str)
            or call_id in used_ids
            or len(used_ids) >= 4
        ):
            raise ValueError("invalid or already used J4 permit")
        issued_ns, deadline_ns = permit["issued_ns"], permit["deadline_ns"]
        if deadline_ns - issued_ns != 2_000_000_000:
            raise ValueError("invalid J4 permit deadline")
        call = CallIdentity(
            config.cell, workflow_id, node_id, iteration, attempt, sequence, call_id
        )
        used_ids.add(call_id)
        details = {**asdict(call), "issued_ns": issued_ns, "deadline_ns": deadline_ns}
        trace({"event": "reserved", **details, "at_ns": time.monotonic_ns()})

        def record_send(send: dict[str, Any], fields: dict[str, Any] = details) -> None:
            trace({**send, **fields})

        reply = call_provider(config, call, value, deadline_ns, record_send)
        kind, retry_after = provider_outcome(reply, call, digest)
        trace(
            {
                "event": "reply",
                **details,
                "status": reply.status,
                "at_ns": reply.observed_ns,
                "retry_after_seconds": retry_after,
            }
        )
        schedule = post(
            "outcome",
            {
                "call_id": call_id,
                "kind": kind,
                "observed_ns": reply.observed_ns,
                "retry_after_seconds": retry_after,
            },
        )
        trace(
            {
                "event": "outcome_committed",
                **details,
                "at_ns": time.monotonic_ns(),
                "outcome_ns": schedule["outcome_ns"],
                "eligible_ns": schedule["eligible_ns"],
            }
        )
