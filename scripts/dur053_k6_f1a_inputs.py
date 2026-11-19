"""Offline predicates for F1a engineering evidence."""

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

from dur053_k6_chaos import Target
from dur053_k6_e1_join import expect

CONTROL_URL = "http://worker-control:8080"


def route_rows(
    rows: Iterable[dict[str, Any]], target: Target, workflow: str, *, attempt: int | None = None
) -> list[dict[str, Any]]:
    observed = [
        row
        for row in rows
        if row["event"] == "f1a_http"
        and row["pod_uid"] == target.uid
        and row["session"] == target.session
        and row["workflow"] == workflow
        and (attempt is None or row["attempt_number"] == attempt)
    ]
    for row in observed:
        expect("F1A_ROUTE_PROCESS", row["process"], target.process)
        expect("F1A_CONTROL_ROUTE", row["base_url"], CONTROL_URL)
        expect(
            "F1A_CONTROL_PATH",
            row["path"],
            f"/v1/workflows/{workflow}/nodes/{row['node']}/iterations/{row['iteration']}/{row['operation']}",
        )
    return observed


def require_stale(
    rows: list[dict[str, Any]],
    *,
    token_hash: str,
    replacement_observed_ns: int,
    restored_ns: int,
    successful_ns: int | None = None,
) -> dict[str, Any]:
    matched = [
        row
        for row in rows
        if row["operation"] in {"heartbeat", "result"}
        and row["token_hash"] == token_hash
        and row["sent_ns"] > max(replacement_observed_ns, restored_ns)
        and row["at_ns"] >= row["sent_ns"]
        and (successful_ns is None or row["at_ns"] < successful_ns)
    ]
    typed = [
        row
        for row in matched
        if row["status"] == 409 and row["code"] in {"STALE_CLAIM", "STALE_ATTEMPT"}
    ]
    expect(
        "F1A_TYPED_STALE_REJECTION",
        True if typed else [(r["operation"], r["status"], r["code"]) for r in matched],
        True,
    )
    return typed[0]


def chain_counters(raw: str, chain: str) -> dict[str, int] | None:
    # Recovery clears the parent jump. The daemon can leave an unreferenced chain.
    if not re.search(rf" -A CHAOS-OUTPUT -j {re.escape(chain)}$", raw, re.M):
        return None
    counts = re.findall(rf"^\[(\d+):(\d+)\] -A {re.escape(chain)} .* -j DROP$", raw, re.M)
    expect("F1A_DROP_RULE_COUNT", len(counts), 1)
    packets, octets = counts[0]
    return {"packets": int(packets), "bytes": int(octets)}


def require_drop_rise(before: dict[str, int], during: dict[str, int]) -> None:
    expect(
        "F1A_DROP_COUNTER_RISE",
        True
        if all(during[key] > before[key] for key in ("packets", "bytes"))
        else [before, during],
        True,
    )


def first_fenced_transition(
    before: dict[str, Any],
    after: dict[str, Any],
    issued_ns: int,
    target: Target,
    live_sessions: dict[str, set[str]],
) -> dict[str, Any]:
    prior = {row["transition_id"] for row in before["history"]}
    history = [
        row
        for row in after["audit"]
        if row["kind"] == "history"
        and row["epoch"] > 0
        and row["transition_id"] not in prior
        and row["observed_ns"] > issued_ns
    ]
    expect("F1A_FENCED_TRANSITION_MISSING", bool(history), True)
    first = history[0]
    expect("F1A_RECOVERY_SESSION", first["session_id"] != target.session, True)
    expect(
        "F1A_RECOVERY_SCHEDULER",
        first["session_id"] in live_sessions.get(first["pod_uid"], set()),
        True,
    )
    acquired = [
        row
        for row in after["audit"]
        if row["kind"] == "acquire"
        and all(
            row[key] == first[key] for key in ("partition_id", "epoch", "owner_id", "session_id")
        )
        and row["event_order"] < first["event_order"]
    ]
    expect("F1A_RECOVERY_COMMITTED_LEASE", len(acquired), 1)
    return {"acquire": acquired[0], "first_transition": first}
