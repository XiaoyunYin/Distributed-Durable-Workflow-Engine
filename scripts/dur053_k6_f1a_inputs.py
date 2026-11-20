"""Offline predicates for F1a engineering evidence."""

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

from dur053_k6_b_evidence import SmokeFailure
from dur053_k6_chaos import Target
from dur053_k6_e1_join import by, expect, one

CONTROL_URL = "http://worker-control:8080"
ATTEMPT_KEY = ("workflow_id", "node_id", "iteration", "attempt_number")
LEASE_KEY = ("partition_id", "owner_id", "epoch")


def worker_principal(pod: dict[str, Any]) -> str:
    # internal/api/identity.go: TokenReviewer verifies the ServiceAccount and pod extras.
    metadata = pod["metadata"]
    return (
        f"system:serviceaccount:{metadata['namespace']}:{pod['spec']['serviceAccountName']}"
        f":pod:{metadata['name']}:uid:{metadata['uid']}"
    )


def attempt_fields(attempt: dict[str, Any]) -> dict[str, Any]:
    return {key: attempt[key] for key in ATTEMPT_KEY}


def claim_rows(cut: dict[str, Any], attempt: dict[str, Any], principal: str) -> list:
    return by(
        cut["claims"],
        **attempt_fields(attempt),
        token_hash=attempt["token_hash"],
        worker_id=principal,
        worker_request_id=attempt["worker_request_id"],
    )


def invocation_identity(attempt: dict[str, Any], namespace: str) -> dict[str, Any]:
    return {
        "namespace": namespace,
        "workflow": attempt["workflow_id"],
        "node": attempt["node_id"],
        "iteration": attempt["iteration"],
        "attempt": attempt["attempt_number"],
        "token_hash": attempt["token_hash"],
    }


def terminal_winner(
    cut: dict[str, Any], rows: list[dict[str, Any]], pods: list[dict[str, Any]], namespace: str
) -> dict[str, Any]:
    winner = one("F1A_WINNING_ATTEMPT", by(cut["attempts"], is_current=False, state="SUCCEEDED"))
    started = one(
        "F1A_WINNING_INVOCATION",
        by(rows, event="start", identity=invocation_identity(winner, namespace)),
    )
    pod = one(
        "F1A_WINNING_POD_UID", [p for p in pods if p["metadata"]["uid"] == started["pod_uid"]]
    )
    principal = worker_principal(pod)
    expect("F1A_WINNING_WORKER_PRINCIPAL", winner["worker_id"], principal)
    claim = one("F1A_WINNING_CLAIM", claim_rows(cut, winner, principal))
    scope = {key: started[key] for key in ("session", "pod_uid")}
    one(
        "F1A_WINNING_ACTIVITY_FINISHED",
        by(rows, event="finish", id=started["id"], identity=started["identity"], **scope),
    )
    identity = started["identity"]
    results = by(
        rows,
        event="f1a_http",
        operation="result",
        workflow=identity["workflow"],
        node=identity["node"],
        iteration=identity["iteration"],
        attempt_number=identity["attempt"],
        token_hash=identity["token_hash"],
        status=200,
        **scope,
    )
    expected_path = (
        f"/v1/workflows/{identity['workflow']}/nodes/{identity['node']}"
        f"/iterations/{identity['iteration']}/result"
    )
    expect(
        "F1A_WINNING_RESULT_ROUTE",
        bool(results)
        and all(
            result["base_url"] == CONTROL_URL and result["path"] == expected_path
            for result in results
        ),
        True,
    )
    return {"attempt": winner, "claim": claim, "invocation": started, "result": results[0]}


def partition_replacement(cut: dict[str, Any], attempt: dict[str, Any]) -> dict[str, Any]:
    key = attempt_fields(attempt)
    old = one(
        "F1A_PARTITION_OLD_ATTEMPT_TIMED_OUT",
        by(
            cut["attempts"],
            **key,
            token_hash=attempt["token_hash"],
            is_current=False,
            state="TIMED_OUT",
        ),
    )
    replacements = [r for r in by(cut["attempts"], is_current=True) if r["attempt_number"] > 1]
    replacement = one("F1A_PARTITION_REPLACEMENT", replacements)
    expect(
        "F1A_REPLACEMENT_SAME_INVOCATION_KEY",
        attempt_fields(replacement),
        {**key, "attempt_number": old["attempt_number"] + 1},
    )
    # store.go writes TIMEOUT_REPLACEMENT against the old attempt in the same transaction.
    history = one(
        "F1A_REPLACEMENT_COMMITTED_HISTORY",
        by(cut["history"], reason="TIMEOUT_REPLACEMENT", **key),
    )
    return {"replacement": replacement, "old": old, "replacement_history": history}


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


def stale_candidate_failure(
    row: dict[str, Any],
    *,
    token_hash: str,
    replacement_observed_ns: int,
    restored_ns: int,
    successful_ns: int | None = None,
) -> str | None:
    if row["operation"] not in {"heartbeat", "result"}:
        return "F1A_CANDIDATE_STALE_OPERATION"
    if row["token_hash"] != token_hash:
        return "F1A_CANDIDATE_STALE_TOKEN_HASH_MISMATCH"
    if row["sent_ns"] <= replacement_observed_ns:
        return "F1A_CANDIDATE_STALE_SENT_BEFORE_REPLACEMENT"
    if row["sent_ns"] <= restored_ns:
        return "F1A_CANDIDATE_STALE_SENT_BEFORE_RESTORATION"
    if row["at_ns"] < row["sent_ns"] or (
        successful_ns is not None and row["at_ns"] >= successful_ns
    ):
        return "F1A_CANDIDATE_STALE_RESPONSE_ORDER"
    if row["status"] == 409 and row["code"] in {"STALE_CLAIM", "STALE_ATTEMPT"}:
        return None
    return "F1A_CANDIDATE_STALE_UNTYPED_RESPONSE"


def require_stale(
    rows: list[dict[str, Any]],
    *,
    token_hash: str,
    replacement_observed_ns: int,
    restored_ns: int,
    successful_ns: int | None = None,
) -> dict[str, Any]:
    failures = []
    for row in rows:
        failure = stale_candidate_failure(
            row,
            token_hash=token_hash,
            replacement_observed_ns=replacement_observed_ns,
            restored_ns=restored_ns,
            successful_ns=successful_ns,
        )
        if failure is None:
            return row
        failures.append(failure)
    raise SmokeFailure(
        f"F1A_TYPED_STALE_REJECTION observed={failures!r} "
        "expected=typed_stale_response_after_replacement_and_restoration"
    )


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
    workflow_transitions = {row["transition_id"]: row for row in after["history"]}
    history = [
        row
        for row in after["audit"]
        if row["kind"] == "history"
        and row["epoch"] > 0
        and row["transition_id"] in workflow_transitions
        and row["transition_id"] not in prior
        and row["observed_ns"] > issued_ns
    ]
    expect("F1A_FENCED_TRANSITION_MISSING", bool(history), True)
    first = min(history, key=lambda row: row["event_order"])
    transition = workflow_transitions[first["transition_id"]]
    expect("F1A_RECOVERY_HISTORY_EPOCH", transition["scheduler_epoch"], first["epoch"])
    expect("F1A_RECOVERY_SESSION", first["session_id"] != target.session, True)
    expect(
        "F1A_RECOVERY_SCHEDULER",
        first["session_id"] in live_sessions.get(first["pod_uid"], set()),
        True,
    )
    acquisitions = [
        row
        for row in after["audit"]
        if row["kind"] == "acquire"
        and row["partition_id"] == first["partition_id"]
        and row["event_order"] < first["event_order"]
    ]
    expect("F1A_RECOVERY_COMMITTED_LEASE", bool(acquisitions), True)
    acquired = max(acquisitions, key=lambda row: row["event_order"])
    keys = (*LEASE_KEY, "session_id", "pod_uid", "container_id")
    expect(
        "F1A_RECOVERY_COMMITTED_LEASE",
        [acquired[key] for key in keys],
        [first[key] for key in keys],
    )
    return {"acquire": acquired, "first_transition": first}
