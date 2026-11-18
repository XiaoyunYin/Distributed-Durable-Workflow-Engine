"""Check sealed D2b exchanges and the four boundary-specific outcomes."""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime

from dur053_k6_hook import encode
from dur053_k6_join import META, digest, identity, require, unique, verified_exchange

TRANSFER_BYTES = ("body_hash", "length", "wire_hash", "wire_length", "status")


def same(left: dict, right: dict, fields: tuple[str, ...]) -> bool:
    return all(left.get(k) == right.get(k) for k in fields)


def exchanges(deliveries: dict, faults: dict[str, str]):
    fault_count: Counter = Counter()
    for rows in deliveries.values():
        inbound = rows[0]
        cell = faults.get(inbound.get("request_id_hash"))
        fault_count[cell] += 1
        if cell not in {"medium-13", "medium-14", "medium-15"}:
            first, response = verified_exchange(rows)
            yield first, response, True
            continue
        primary = [r for r in rows if not r.get("copy")]
        by_event = unique(primary, lambda r: r["event"], "D2B_REPEATED_PRIMARY_EVENT")
        if cell == "medium-14":
            first, response = verified_exchange(primary)
            extra = [r for r in rows if r.get("copy") == 2]
            require(
                tuple(r["event"] for r in extra)
                == ("copy_open", "sink_send", "sink_response", "extra_reply", "close"),
                "D2B_SECOND_DELIVERY_INCOMPLETE",
            )
            copy, _, copy_response, disposition, _ = extra
            require(
                copy["connection_hash"] != first["connection_hash"]
                and same(
                    copy, first, tuple(k for k in META if k != "connection_hash") + TRANSFER_BYTES
                )
                and copy_response["body_hash"] == response["body_hash"]
                and all(r["copy"] == 2 for r in extra)
                and len(primary) + len(extra) == len(rows)
                and primary[1]["sequence"]
                < response["sequence"]
                < copy["sequence"]
                < disposition["sequence"]
                < by_event["worker_delivery"]["sequence"],
                "D2B_SECOND_DELIVERY_IDENTITY",
            )
            # Reuse the forward-only byte and identity checks per sink connection.
            # The second disposition remains an extra reply in the raw journal.
            second, second_response = verified_exchange(
                [
                    {**copy, "event": "inbound"},
                    extra[1],
                    copy_response,
                    {**disposition, "event": "worker_delivery"},
                    extra[-1],
                ]
            )
            yield first, response, True
            yield second, second_response, False
        else:
            expected = (
                ("inbound", "sink_send", "caller_closed", "sink_response", "worker_drop", "close")
                if cell == "medium-13"
                else (
                    "inbound",
                    "sink_send",
                    "sink_response",
                    "caller_closed",
                    "fault_drop",
                    "worker_drop",
                    "close",
                )
            )
            require(tuple(r["event"] for r in rows) == expected, "D2B_WITHHELD_EXCHANGE_INCOMPLETE")
            require(all(same(inbound, row, META) for row in rows), "D2B_WITHHELD_IDENTITY")
            require(
                all(
                    r["length"] == 0
                    and not any(
                        r.get(k) for k in ("wire_length", "body_hash", "wire_hash", "status")
                    )
                    for r in (by_event["caller_closed"], by_event["worker_drop"])
                ),
                "D2B_NONZERO_WORKER_BYTES",
            )
            response = by_event["sink_response"]
            if cell == "medium-15":
                require(
                    same(response, by_event["fault_drop"], TRANSFER_BYTES),
                    "D2B_DROP_REPLY_MISMATCH",
                )
            first, response = verified_exchange(
                [
                    inbound,
                    by_event["sink_send"],
                    response,
                    {**response, "event": "worker_delivery"},
                    by_event["close"],
                ]
            )
            yield first, response, False
    require(not fault_count["medium-12"], "D2B_KILLED_INVOCATION_SENT")
    require(
        all(fault_count[cell] == 1 for cell in set(faults.values()) - {"medium-12"}),
        "D2B_FAULT_REQUEST_INVENTORY",
    )


def timestamp(value: str) -> int:
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = datetime.fromisoformat(value) - epoch
    # Retained Go strings carry any extra sub-microsecond precision.
    return (delta.days * 86400 + delta.seconds) * 10**9 + delta.microseconds * 1000


def validate_cell(
    cell: str,
    sample: dict,
    operation: dict,
    engine: dict,
    snapshot: dict,
    transfers: list[dict],
    records: list[dict],
    hook_rows: list[dict],
    approver: str,
) -> dict:
    logical_key = operation["logical_key"]
    key_hash = identity(logical_key)
    all_calls = sorted(
        (r for r in snapshot["calls"] if r["logical_key"] == logical_key),
        key=lambda r: r["call_id"],
    )
    calls = [r for r in all_calls if r["operation"] != "allocate"]
    outcomes = {
        "medium-12": [("lookup", "MISSING", 2), ("apply", "APPLIED", 2)],
        "medium-13": [("apply", "APPLIED", 1), ("lookup", "FOUND", 2)],
        "medium-14": [("apply", "APPLIED", 1), ("apply", "DUPLICATE", 1)],
        "medium-15": [("apply", "APPLIED", 1), ("lookup", "FOUND", 1)],
        "unarmed-control": [("apply", "APPLIED", 1)],
    }[cell]
    require(
        [(r["operation"], r["outcome"], r["attempt"]) for r in calls] == outcomes,
        "D2B_CALLED_WRONG_PATH",
    )
    recovery = cell in {"medium-12", "medium-13"}
    applied = next(r for r in calls if r["outcome"] == "APPLIED")
    receipt = next(r for r in snapshot["receipts"] if r["logical_key"] == logical_key)
    require(
        receipt["workflow_id"] == sample["workflow_id"]
        and receipt["resource_id"] == operation["resource"]
        and receipt["expected_revision"] == 0
        and receipt["applied_revision"] == 1
        and receipt["canonical_hash"] == operation["canonical_hash"]
        and engine["state"] == "SUCCEEDED"
        and engine["result"] == receipt
        and engine["grant_transitions"] == 1
        and engine["approvals"]
        == [
            dict(
                intent_id=receipt["intent_id"],
                decision="APPROVED",
                status="DISPATCHED",
                approver=approver,
                key=logical_key,
                target=receipt["resource_id"],
                scope_hash=receipt["scope_hash"],
            )
        ],
        "D2B_ENGINE_RECEIPT_OR_GRANT",
    )
    attempts = sorted(engine["attempts"], key=lambda r: r["number"])
    winner = attempts[-1]
    require(
        len(attempts) == (2 if recovery else 1)
        and winner
        == dict(
            number=2 if recovery else 1,
            state="SUCCEEDED",
            fence=2 if recovery else 1,
            proof_operation="lookup" if cell in {"medium-13", "medium-15"} else "apply",
            signed=True,
        )
        | {"class": "COOPERATING_EFFECT"},
        "D2B_WINNING_RESULT",
    )
    require(
        [r["attempt"] for r in all_calls if r["operation"] == "allocate"]
        == list(range(1, len(attempts) + 1)),
        "D2B_FENCE_ALLOCATION",
    )
    if recovery:
        require(
            attempts[0]["number"] == 1 and attempts[0]["state"] != "SUCCEEDED",
            "D2B_INTERRUPTED_WINNER",
        )
    if cell in {"medium-13", "medium-14", "medium-15"}:
        second = calls[1]
        require(
            second["application_id"] == applied["application_id"]
            and second["response_hash"] == applied["response_hash"],
            "D2B_RECEIPT_CHANGED",
        )

    arm = sample.get("arm")
    relevant = [r for r in hook_rows if arm and r.get("arm") == arm["arm"]]
    cell_events = [r for r in transfers if r.get("key") == key_hash]
    sent = unique(
        [r for r in cell_events if r["stage"] == "sent"],
        lambda r: r["request"],
        "D2B_WORKER_SENT_REPLAY",
    )
    require(set(sent) == {identity(r["request_id"]) for r in calls}, "D2B_WORKER_SENT_INVENTORY")
    for call in calls:
        evidence = sent[identity(call["request_id"])]
        claimant = sample["new"] if call["attempt"] == 2 else sample["first_worker"]
        require(
            evidence["body"] == call["body_hash"]
            and evidence["invocation"] == identity(call["message_id"])
            and evidence["attempt"] == call["attempt"]
            and same(evidence, claimant, ("uid", "session", "process_start_id")),
            "D2B_WORKER_SENT_IDENTITY",
        )
    resolved = [r for r in cell_events if r["stage"] == "SUCCEEDED"]
    require(
        len(resolved) == 1 and resolved[0]["invocation"] == identity(calls[-1]["message_id"]),
        "D2B_WORKER_RESOLUTION",
    )

    metrics = {
        "terminal_ns": sample["terminal_ns"],
        "receipt_ns": resolved[0]["at_ns"],
        "offered_to_terminal_ns": sample["terminal_ns"] - sample["offered_ns"],
    }
    if cell == "unarmed-control":
        require(arm is None and not relevant, "D2B_CONTROL_ARMED")
        return metrics
    names = [r["event"] for r in relevant]
    expected = ["arm", "reach", "held", "action", "command_issued", "release"]
    if cell == "medium-12":
        expected = ["arm", "reach", "held", "inject", "backend_confirmed", "action"]
    if cell == "medium-13":
        expected = [
            "arm",
            "reach",
            "held",
            "inject",
            "backend_confirmed",
            "action",
            "command_issued",
            "release",
        ]
    require(names == expected, "D2B_HOOK_INVENTORY")
    persisted, reach, held = relevant[:3]
    action = next(r for r in relevant if r["event"] == "action")
    require(
        action["command"]
        == {
            "medium-12": "killed",
            "medium-13": "release",
            "medium-14": "duplicate",
            "medium-15": "drop",
        }[cell],
        "D2B_WRONG_ACTION",
    )
    binding = {
        "sequence": reach["sequence"],
        "sha256": reach["message_sha256"],
        "journal_sha256": digest(encode(reach)),
    }
    require(
        arm["arm_ack"] == {"sequence": persisted["sequence"], "sha256": digest(encode(persisted))},
        "D2B_ARM_DURABILITY_UNJOINED",
    )
    require(
        all(
            row["ack"] == binding and row["at_ns"] < persisted["until_ms"] * 10**6
            for row in relevant[2:]
        ),
        "D2B_DURABLE_ACK_UNJOINED",
    )
    fields = reach["fields"]
    require(
        reach["target"] == arm["target"] == persisted["target"]
        and all(fields[k] == v for k, v in persisted["selector"].items())
        and fields["key"] == logical_key
        and fields["resource"] == receipt["resource_id"]
        and fields["intent"] == receipt["intent_id"]
        and fields["scope"] == receipt["scope_hash"],
        "D2B_HOOK_OBJECT_UNJOINED",
    )
    committed = timestamp(applied["observed_at"])
    local = [r for r in records if r.get("request_id_hash") == identity(fields["request"])]
    if cell == "medium-12":
        require(
            not local
            and not any(r.get("invocation_hash") == identity(fields["invocation"]) for r in records)
            and not any(r["invocation"] == identity(fields["invocation"]) for r in cell_events),
            "D2B_KILLED_INVOCATION_SENT",
        )
        metrics.update(
            replacement_first_send_ns=min(r["at_ns"] for r in sent.values()),
            first_sink_call_ns=timestamp(calls[0]["observed_at"]),
        )
    else:
        require(
            fields["invocation"] == applied["message_id"]
            and fields["request"] == applied["request_id"]
            and fields["binding"] == applied["connection_hash"],
            "D2B_HOOK_CALL_UNJOINED",
        )
        require(
            any(r["session"] == reach["target"]["session"] for r in local) or cell == "medium-13",
            "D2B_RELAY_SESSION",
        )
        if cell in {"medium-13", "medium-15"}:
            require(
                fields["application"] == applied["application_id"]
                and fields["reply"] == applied["response_hash"]
                and committed <= reach["at_ns"],
                "D2B_HOOK_COMMIT_UNJOINED",
            )
            metrics["commit_bracket_ns"] = [committed, reach["at_ns"]]
    if recovery:
        injection = next(r for r in relevant if r["event"] == "inject")
        confirmed = next(r for r in relevant if r["event"] == "backend_confirmed")
        death = confirmed["death_ns"]
        first_after_death = min(r["at_ns"] for r in sent.values() if r["attempt"] == 2)
        replacement_claim = sample.get("replacement_claim_observed_ns", 0)
        require(
            injection["target"] == sample["first_worker"]
            and (
                cell != "medium-12"
                or same(
                    reach["target"], injection["target"], ("uid", "session", "process_start_id")
                )
            )
            and held["at_ns"] <= injection["issued_ns"] <= death <= confirmed["at_ns"]
            and death < replacement_claim < first_after_death,
            "D2B_RECOVERY_BEFORE_DEATH",
        )
        metrics.update(
            death_ns=death,
            replacement_ns=sample["replacement_ns"],
            replacement_claim_observed_ns=replacement_claim,
            death_to_replacement_claim_ns=replacement_claim - death,
        )
    if cell == "medium-13":
        closed = next(r for r in local if r["event"] == "caller_closed")
        require(
            timestamp(closed["time"]) <= action["at_ns"]
            and closed["sequence"] == action["evidence"]["caller_close_sequence"]
            and not any(
                r["stage"] == "timeout" and r["invocation"] == identity(fields["invocation"])
                for r in cell_events
            ),
            "D2B_CALLER_NOT_WITHHELD_AT_DEATH",
        )
        lookup_ns = timestamp(calls[1]["observed_at"])
        metrics.update(
            recovery_lookup_ns=lookup_ns,
            commit_to_recovery_ns=[lookup_ns - reach["at_ns"], lookup_ns - committed],
        )
    if cell == "medium-14":
        first_send = next(r for r in local if r["event"] == "sink_send")
        require(action["at_ns"] <= timestamp(first_send["time"]), "D2B_DUPLICATE_BEFORE_ACK")
        metrics.update(
            second_call_ns=timestamp(calls[1]["observed_at"]), second_call_disposition="DUPLICATE"
        )
    if cell == "medium-15":
        timeout = [r for r in cell_events if r["stage"] == "timeout"]
        unknown = [r for r in cell_events if r["stage"] == "OUTCOME_UNKNOWN"]
        lookup = sent[identity(calls[1]["request_id"])]
        first = sent[identity(applied["request_id"])]
        require(
            len(timeout) == len(unknown) == 1
            and same(
                timeout[0],
                first,
                ("session", "uid", "process_start_id", "request", "invocation", "body"),
            )
            and same(lookup, first, ("session", "uid", "process_start_id", "invocation"))
            and timeout[0]["at_ns"] >= first["at_ns"] + 11_000_000_000
            and action["at_ns"]
            < timeout[0]["at_ns"]
            <= unknown[0]["at_ns"]
            <= lookup["at_ns"]
            <= resolved[0]["at_ns"],
            "D2B_TIMEOUT_OR_SAME_CLAIM_LOOKUP",
        )
        metrics.update(
            timeout_ns=timeout[0]["at_ns"],
            unknown_ns=unknown[0]["at_ns"],
            lookup_send_ns=lookup["at_ns"],
            time_unknown_ns=resolved[0]["at_ns"] - unknown[0]["at_ns"],
            timeout_to_resolution_ns=resolved[0]["at_ns"] - timeout[0]["at_ns"],
        )
    return metrics
