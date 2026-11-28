"""Independent J4 replay oracle. Consumes retained rows, never runtime policy."""

from __future__ import annotations

import base64
import json
from copy import deepcopy
from pathlib import Path

from deploy.j4.evidence import SECOND, write_json

PROVIDER = "j4.recorded-llm"
SUCCESSOR = "pure.echo"
MATCH = "92b3bc136a6617117cfac28e4324b272b6bf6b72ab5d137a948cb9fc0d25e751"
WRONG = "7fd339f77e3935f75c39a7436f7c71dd74f4644f87c3f5968120ce9095e06522"
GROUP = "runtime-workers-v1"
TASK_TOPIC = "durable-agent.tasks.v1"
RETRY = [2302, 4484, 8486, 16039]
SCRIPTS = {
    "C0": (200,),
    "C1": ("cut", 200),
    "C2": (200,),
    "C3": (422,),
    "C4": (200,),
    "B0": (1, "timeout", 12, 200),
    "B1": ("unused", 1, 200),
    "B2": ("cut", 1, 200),
    "B3": (20, 200),
    "B4": (1, "timeout", 12, "timeout"),
    "B5": (1, "timeout", 12, 200),
}
KILLS = {"C1", "C2", "C4", "B1", "B2", "B3"}
REPLACEMENTS = {"C1", "B1", "B2", "B3"}
REJECTIONS = {
    "C3": "RECORDED_REQUEST_MISMATCH",
    "C4": "IDLE_KILL_NO_OVERLAP",
    "B4": "PROVIDER_BUDGET_EXHAUSTED",
    "B5": "FIXED_DELAY_REJECTED",
}


class VerdictError(ValueError):
    def __init__(self, code: str, missing: bool = False) -> None:
        super().__init__(code)
        self.code = code
        self.missing = missing


def require(condition: bool, code: str, *, missing: bool = False) -> None:
    if not condition:
        raise VerdictError(code, missing)


def one(rows: list[dict], code: str) -> dict:
    require(len(rows) == 1, code, missing=not rows)
    return rows[0]


def attempt_key(row: dict) -> tuple[str, int, int]:
    return row["node_id"], row["iteration"], row["attempt_number"]


def by_key(rows: list[dict], key, code: str) -> dict:
    result = {key(row): row for row in rows}
    require(len(result) == len(rows), code)
    return result


def hook_events(e: dict, name: str, kind: str | None = None) -> list[dict]:
    return [
        row
        for row in e["worker"]
        if row["hook"] == name and (kind is None or row["fields"].get("event") == kind)
    ]


def int_field(row: dict, name: str) -> int:
    return int(row["fields"][name])


def encoded_payload(row: dict) -> dict:
    return json.loads(base64.b64decode(row["value_b64"], validate=True))


def check_clocks(e: dict) -> None:
    start, end = e["clock_start"], e["clock_end"]
    boot = start["boot_id"]
    require(bool(boot) and boot == end["boot_id"], "BOOT_CHANGED")
    roles = {"runtime", "worker", "stub"}
    for report in (start, end):
        require(set(report["samples"]) == roles, "MISSING_CLOCK", missing=True)
        for sample in report["samples"].values():
            before, stamp, after = (sample[k] for k in ("before_ns", "remote_ns", "after_ns"))
            require(before <= stamp <= after and after - before <= 20_000_000, "CLOCK_BRACKET")
    require(
        all(
            start["samples"][role]["after_ns"] <= end["samples"][role]["before_ns"]
            for role in roles
        ),
        "CLOCK_ORDER",
    )
    for source in ("worker", "stub", "broker"):
        require(all(row["boot_id"] == boot for row in e[source]), "FOREIGN_WITNESS")
    require(all(row["boot_id"] == boot for row in e["scheduler"]), "FOREIGN_SCHEDULER_CLOCK")


def check_calls(e: dict) -> tuple[dict, dict]:
    arm, db, workflow = e["arm"], e["db"], e["workflow_id"]
    script = SCRIPTS[arm]
    journal = e["stub"]
    records = [row for row in journal if row["event"] == "receipt"]
    expected_ord = {n for n, kind in enumerate(script, 1) if kind != "unused"}
    calls = by_key(records, lambda r: r["ordinal"], "DUPLICATE_CALL_ORDINAL")
    require(expected_ord <= calls.keys(), "MISSING_CALL", missing=True)
    require(set(calls) == expected_ord, "EXTRA_CALL")
    require(
        [row["sequence"] for row in journal] == list(range(1, len(journal) + 1)), "STUB_SEQUENCE"
    )
    by_key(records, lambda r: r["call_id"], "DUPLICATE_CALL_ID")
    reservations = by_key(db["reservations"], lambda row: row["sequence"], "DUPLICATE_RESERVATION")
    expected = set(range(1, len(script) + 1)) if arm.startswith("B") else set()
    require(expected <= reservations.keys(), "MISSING_RESERVATION", missing=True)
    require(set(reservations) == expected and len(reservations) <= 4, "RESERVATION_LIMIT")
    by_key(db["reservations"], lambda r: r["call_id"], "DUPLICATE_RESERVATION_ID")
    allowed_headers = {
        "host",
        "content-type",
        "content-length",
        "connection",
        "x-j4-cell",
        "x-j4-workflow",
        "x-j4-node",
        "x-j4-iteration",
        "x-j4-attempt",
        "x-j4-ordinal",
        "x-j4-call",
    }
    for ordinal, call in calls.items():
        require(
            (call["workflow_id"], call["node_id"], call["iteration"], call["cell"])
            == (workflow, PROVIDER, 0, e["cell"]),
            "FOREIGN_CALL",
        )
        require(
            call["request_sha256"] == (WRONG if arm == "C3" else MATCH)
            and call["matched"] == (arm != "C3"),
            "CANONICAL_REQUEST",
        )
        require(
            set(call["header_names"]) == allowed_headers
            and len(call["header_names"]) == len(allowed_headers),
            "PROVIDER_HEADERS",
        )
        related = [r for r in journal if r.get("receipt_sequence") == call["sequence"]]
        closes = [r for r in related if r["event"] == "close"]
        close = one(closes, "MISSING_SOCKET_CLOSE")
        require(close["at_ns"] >= call["at_ns"], "SOCKET_CLOSE_ORDER")
        responses = [r for r in related if r["event"] == "response"]
        kind = script[ordinal - 1]
        if kind in {"timeout", "cut"}:
            hold = one([r for r in related if r["event"] == "withhold"], "MISSING_WITHHOLD")
            require(
                not responses
                and call["branch"] == "withhold"
                and call["at_ns"] <= hold["at_ns"] <= close["at_ns"]
                and close["disposition"] in {"peer_closed", "peer_reset"},
                "WITHHOLD_VIOLATION",
            )
        else:
            response = one(responses, "MISSING_PROVIDER_RESPONSE")
            status = kind if kind >= 200 else 429
            require(
                response["status"] == status and response["at_ns"] >= call["at_ns"],
                "WRONG_PROVIDER_RESPONSE",
            )
            if status == 429:
                require(
                    response["retry_after"] == kind and call["branch"] == kind, "RETRY_AFTER_SCRIPT"
                )
    require(
        all(
            r["event"] == "receipt" or r.get("receipt_sequence") in {c["sequence"] for c in records}
            for r in journal
        ),
        "ORPHAN_STUB_EVENT",
    )
    return calls, reservations


def check_attempts(e: dict, calls: dict) -> dict:
    arm, db, workflow = e["arm"], e["db"], e["workflow_id"]
    initial = e["submit"]
    w = one(db["workflows"], "MISSING_WORKFLOW")
    acknowledged = initial["response"]
    require(
        initial["status"] == 201
        and acknowledged["created"] is True
        and acknowledged["workflow"]["workflow_id"] == w["workflow_id"] == workflow
        and w["namespace"] == e["namespace"]
        and w["submission_key"] == initial["request"]["submission_key"] == workflow,
        "SUBMISSION_IDENTITY",
    )
    require(
        w["definition_id"] == "j4.recorded-llm-recovery"
        and w["definition_version"] == 1
        and 0 < acknowledged["workflow"]["revision"] <= w["revision"],
        "DEFINITION_IDENTITY",
    )
    state = "FAILED" if arm in {"C3", "B4"} else "SUCCEEDED"
    require(w["state"] == state, "WRONG_TERMINAL_STATE")
    for source in ("history", "nodes", "attempts", "outbox", "reservations"):
        require(all(row["workflow_id"] == workflow for row in db[source]), "FOREIGN_DB_ROW")
    history = db["history"]
    reasons = by_key(history, lambda row: row["revision"], "DUPLICATE_HISTORY")
    expected = {(PROVIDER, 0, 1)}
    winning = 2 if arm in REPLACEMENTS else 1
    expected.add((PROVIDER, 0, winning))
    if state == "SUCCEEDED":
        expected.add((SUCCESSOR, 0, 1))
    attempts = by_key(db["attempts"], attempt_key, "DUPLICATE_ATTEMPT")
    require(set(attempts) == expected, "ATTEMPT_POPULATION")
    require(
        all(not attempt["is_current"] for attempt in attempts.values())
        and db["unexpected_results"] == 0,
        "UNEXPECTED_DURABLE_RESULT",
    )
    if winning == 2:
        old = attempts[PROVIDER, 0, 1]
        require(old["state"] == "TIMED_OUT" and old["result"] is None, "UNWITNESSED_REPLACEMENT")
        retirement = one(
            [r for r in history if r["reason"] == "TIMEOUT_REPLACEMENT"],
            "MISSING_REPLACEMENT_HISTORY",
        )
        replacement = one(
            [
                r
                for r in db["outbox"]
                if r["event_type"] == "attempt.dispatch"
                and r["aggregate_revision"] == retirement["revision"]
            ],
            "MISSING_REPLACEMENT_DISPATCH",
        )
        require(
            attempt_key(retirement) == (PROVIDER, 0, 1)
            and attempt_key(replacement["payload"]) == (PROVIDER, 0, 2),
            "REPLACEMENT_HISTORY_JOIN",
        )
    success = attempts[PROVIDER, 0, winning]
    nodes = by_key(db["nodes"], lambda row: row["node_id"], "DUPLICATE_NODE")
    require(
        set(nodes) == ({PROVIDER, SUCCESSOR} if state == "SUCCEEDED" else {PROVIDER})
        and all(n["iteration"] == 0 for n in nodes.values()),
        "SUCCESSOR_POPULATION",
    )
    fixture = {
        "model": "recorded-llm-v1",
        "prompt": "Return fixture beta." if arm == "C3" else "Return fixture alpha.",
    }
    require(
        nodes[PROVIDER]["input"] == initial["request"]["initial_input"] == fixture
        and initial["request"]["payload"] == fixture,
        "PROVIDER_INPUT_JOIN",
    )
    if state == "SUCCEEDED":
        last = calls[max(calls)]
        result = {
            "completion": "fixture alpha",
            "request_sha256": MATCH,
            "provider_call_id": last["call_id"],
        }
        require(
            last["attempt"] == winning
            and success["state"] == "SUCCEEDED"
            and success["result"] == result,
            "PROVIDER_RESULT_JOIN",
        )
        require(
            nodes[PROVIDER]["accepted_result"] == result
            and nodes[SUCCESSOR]["input"] == result
            and nodes[SUCCESSOR]["accepted_result"] == result
            and attempts[SUCCESSOR, 0, 1]["result"] == result
            and attempts[SUCCESSOR, 0, 1]["state"] == "SUCCEEDED",
            "SUCCESSOR_RESULT_JOIN",
        )
        reply = one(
            [
                r
                for r in e["stub"]
                if r["event"] == "response" and r["receipt_sequence"] == last["sequence"]
            ],
            "MISSING_PROVIDER_RESPONSE",
        )
        require(reply["payload"] == result, "PROVIDER_PAYLOAD_JOIN")
    else:
        require(
            nodes[PROVIDER]["accepted_result"] is None
            and success["state"] == "FAILED_FINAL"
            and success["result"] == {"reason": REJECTIONS[arm]},
            "FINAL_FAILURE_REASON",
        )
    recorded = [r for r in history if r["reason"] == "ATTEMPT_RESULT_RECORDED"]
    consumed = [r for r in history if r["reason"] == "RESULT_CONSUMED"]
    winning_steps = expected - ({(PROVIDER, 0, 1)} if winning == 2 else set())
    result_events = [r for r in db["outbox"] if r["event_type"] == "activity.result"]
    consumed_events = [r for r in db["outbox"] if r["event_type"] == "workflow.result_consumed"]
    for rows in (recorded, consumed):
        result_keys = by_key(rows, attempt_key, "DUPLICATE_RESULT_HISTORY")
        require(winning_steps <= result_keys.keys(), "MISSING_HISTORY", missing=True)
        require(set(result_keys) == winning_steps, "ORPHAN_RESULT_HISTORY")
    require(
        set(reasons) == set(range(1, w["revision"] + 1))
        and reasons[w["revision"]]["new_state"] == state,
        "TERMINAL_HISTORY",
    )
    require(len(result_events) == len(winning_steps), "RESULT_EVENT_POPULATION")
    require(len(consumed_events) == len(winning_steps), "CONSUMPTION_EVENT_POPULATION")
    for row in recorded:
        other = one([r for r in consumed if attempt_key(r) == attempt_key(row)], "MISSING_HISTORY")
        event = one(
            [r for r in result_events if r["aggregate_revision"] == row["revision"]],
            "MISSING_RESULT_EVENT",
        )
        require(
            row["revision"] < other["revision"]
            and event["payload"] == attempts[attempt_key(row)]["result"]
            and row["actor_id"] == attempts[attempt_key(row)]["worker_id"]
            and row["transaction_id"] == attempts[attempt_key(row)]["transaction_id"]
            and row["committed_at"] == attempts[attempt_key(row)]["committed_at"]
            and bool(row["committed_at"]),
            "RECEIPT_EVENT_JOIN",
        )
        consumed_event = one(
            [r for r in consumed_events if r["aggregate_revision"] == other["revision"]],
            "MISSING_HISTORY",
        )
        require(
            attempt_key(consumed_event["payload"]) == attempt_key(row)
            and consumed_event["payload"]["attempt_state"] == attempts[attempt_key(row)]["state"],
            "CONSUMPTION_EVENT_JOIN",
        )
    return attempts


def check_transport(e: dict, attempts: dict) -> dict:
    db = e["db"]
    emitted = by_key(db["outbox"], lambda r: r["event_id"], "DUPLICATE_OUTBOX_ID")
    inbox = by_key(
        [r for r in db["inbox"] if r["consumer_id"] == GROUP],
        lambda r: r["event_id"],
        "DUPLICATE_INBOX",
    )
    notices = by_key(
        [r for r in e["broker"] if r["kind"] == "notice"],
        lambda r: r["offset"],
        "DUPLICATE_BROKER_OFFSET",
    )
    deliveries = by_key(
        hook_events(e, "j4_offset_committed"),
        lambda r: int_field(r, "offset"),
        "DUPLICATE_DELIVERY",
    )
    if not notices or set(notices) != set(deliveries):
        raise VerdictError("BROKER_DELIVERY_JOIN", missing=not notices or not deliveries)
    offsets = one([r for r in db["offsets"] if r["consumer_id"] == GROUP], "MISSING_DURABLE_OFFSET")
    require(
        offsets["next_offset"] == max(notices) + 1
        and offsets["kafka_partition"] == 0
        and offsets["topic"] == TASK_TOPIC
        and set(notices) == set(range(max(notices) + 1)),
        "OFFSET_PREFIX",
    )
    broker_offsets = [r for r in e["broker"] if r["kind"] == "offset" and r["offset"] is not None]
    require(
        bool(broker_offsets) and broker_offsets[-1]["offset"] == offsets["next_offset"],
        "BROKER_COMMITTED_OFFSET",
        missing=not broker_offsets,
    )
    claims = by_key(
        hook_events(e, "attempt_claimed"),
        lambda r: (r["fields"]["node"], int_field(r, "iteration"), int_field(r, "attempt")),
        "DUPLICATE_CLAIM",
    )
    require(set(claims) == set(attempts), "CLAIM_POPULATION", missing=not claims)
    for key, claim in claims.items():
        data = claim["fields"]
        require(
            data["workflow"] == e["workflow_id"]
            and data["worker"] == attempts[key]["worker_id"]
            and data["token_hash"] == attempts[key]["claim_token_hash"]
            and data["request"] == attempts[key]["worker_request_id"],
            "CLAIM_DB_JOIN",
        )
    copies = by_key(e["redelivery"], lambda r: r["offset"], "DUPLICATE_REDELIVERY")
    originals = {}
    for offset, notice in sorted(notices.items()):
        event_id = notice["event_id"]
        require(event_id in inbox and event_id in emitted, "BROKER_UNKNOWN_EVENT")
        event, delivery, payload = emitted[event_id], deliveries[offset], encoded_payload(notice)
        headers = dict(notice["headers"])
        require(
            notice["partition"] == 0
            and event["topic"] == TASK_TOPIC
            and event["publish_state"] == "PUBLISHED"
            and payload == event["payload"]
            and notice["event_type"] == event["event_type"]
            and notice["schema_version"] == event["schema_version"]
            and payload["workflow_id"] == e["workflow_id"]
            and base64.b64decode(notice["key_b64"], validate=True).decode() == event_id
            and base64.b64decode(headers["workflow-id"], validate=True).decode()
            == e["workflow_id"],
            "BROKER_OUTBOX_JOIN",
        )
        require(
            delivery["fields"]["event"] == event_id
            and int_field(delivery, "next_offset") == offset + 1
            and int_field(delivery, "partition") == 0
            and delivery["fields"]["consumer"] == GROUP
            and delivery["fields"]["topic"] == TASK_TOPIC,
            "OFFSET_ACK_JOIN",
        )
        if offset in copies:
            original = notices[copies[offset]["original_offset"]]
            require(
                all(notice[k] == original[k] for k in ("headers", "key_b64", "value_b64"))
                and delivery["fields"]["disposition"] in {"ALREADY_HANDLED", "STALE"},
                "REDELIVERY_WIRE_MISMATCH",
            )
            require(
                copies[offset]["request_ns"]
                <= delivery["monotonic_ns"]
                <= copies[offset]["request_ns"] + 5 * SECOND,
                "REDELIVERY_ACK_DEADLINE",
            )
            pub = one(
                [r for r in e["broker"] if r["kind"] == "republished" and r["offset"] == offset],
                "MISSING_REDELIVERY_PUBLISH",
            )
            require(
                pub["partition"] == 0
                and pub["original_offset"] == original["offset"]
                and copies[offset]["request_ns"] <= pub["send_ns"] <= delivery["monotonic_ns"],
                "REDELIVERY_PUBLISH_IDENTITY",
            )
            after = copies[offset]["durable"]
            require(
                after["observed_ns"] <= copies[offset]["request_ns"] + 5 * SECOND
                and any(
                    r["consumer_id"] == GROUP and r["next_offset"] == offset + 1
                    for r in after["db"]["offsets"]
                )
                and after["db"]["attempts"] == db["attempts"]
                and after["db"]["reservations"] == db["reservations"],
                "REDELIVERY_DURABLE_JOIN",
            )
            require(
                any(
                    sample["offset"] >= offset + 1
                    and copies[offset]["request_ns"]
                    <= sample["at_ns"]
                    <= copies[offset]["request_ns"] + 5 * SECOND
                    for sample in broker_offsets
                ),
                "REDELIVERY_BROKER_OFFSET",
            )
            continue
        require(
            inbox[event_id]["kafka_offset"] == offset
            and inbox[event_id]["kafka_partition"] == 0
            and inbox[event_id]["disposition"] == "ACCEPTED"
            and delivery["fields"]["disposition"] == "ACCEPTED",
            "INBOX_OFFSET_JOIN",
        )
        key = attempt_key(payload)
        require(key in claims and key not in originals, "UNEXPECTED_OR_DUPLICATE_TASK")
        originals[key] = event_id
        require(
            delivery["process_session"] == claims[key]["process_session"]
            and delivery["monotonic_ns"] <= claims[key]["monotonic_ns"]
            and notice["at_ns"] >= 0,
            "OFFSET_BEFORE_CLAIM",
        )
    require(
        set(originals) == set(claims)
        and set(originals.values()) == set(inbox)
        and set(inbox) == {r["event_id"] for r in db["outbox"] if r["topic"] == TASK_TOPIC}
        and copies.keys() <= notices.keys(),
        "MISSING_REVERSE_TRANSPORT_JOIN",
    )
    return claims


def check_provider_claims(e: dict, calls: dict, reservations: dict, claims: dict) -> None:
    sends = by_key(
        hook_events(e, "j4_provider", "send"), lambda r: r["fields"]["call_id"], "DUPLICATE_SEND"
    )
    require(set(sends) == {r["call_id"] for r in calls.values()}, "MISSING_SEND", missing=True)
    for call in calls.values():
        send = sends[call["call_id"]]
        claim = claims[PROVIDER, 0, call["attempt"]]
        require(
            send["process_session"] == claim["process_session"]
            and all(
                send["fields"][k] == claim["fields"][k] for k in ("worker", "request", "token_hash")
            )
            and send["fields"]["workflow_id"] == call["workflow_id"]
            and int_field(send, "attempt") == call["attempt"]
            and int_field(send, "ordinal") == call["ordinal"]
            and claim["monotonic_ns"] <= int_field(send, "before_ns") <= call["at_ns"],
            "PROVIDER_SEND_CLAIM_JOIN",
        )
    terminal = one(hook_events(e, "j4_result_receipt"), "MISSING_WORKER_RECEIPT")
    winner = claims[PROVIDER, 0, 2 if e["arm"] in REPLACEMENTS else 1]
    require(
        terminal["fields"] == winner["fields"]
        and terminal["process_session"] == winner["process_session"],
        "WORKER_RECEIPT_CLAIM_JOIN",
    )
    if not reservations:
        replies = by_key(
            hook_events(e, "j4_provider", "reply"),
            lambda r: r["fields"]["call_id"],
            "DUPLICATE_PROVIDER_REPLY",
        )
        final = calls[max(calls)]
        require(set(replies) == {final["call_id"]}, "CORE_REPLY_POPULATION", missing=not replies)
        reply = replies[final["call_id"]]
        require(
            reply["process_session"] == winner["process_session"]
            and int_field(reply, "status") == (422 if e["arm"] == "C3" else 200)
            and final["at_ns"] <= int_field(reply, "at_ns") < int_field(reply, "deadline_ns")
            and int_field(reply, "at_ns") <= terminal["monotonic_ns"],
            "CORE_REPLY_JOIN",
        )
    else:
        for row in hook_events(e, "j4_provider", "reserved"):
            reservation = reservations[int_field(row, "ordinal")]
            owner = claims[attempt_key(reservation)]
            require(
                row["process_session"] == owner["process_session"]
                and all(
                    row["fields"][k] == owner["fields"][k]
                    for k in ("worker", "request", "token_hash")
                )
                and owner["monotonic_ns"] <= reservation["issued_ns"],
                "RESERVATION_OWNER_JOIN",
            )


def check_cut(e: dict, calls: dict, reservations: dict, claims: dict) -> None:
    arm = e["arm"]
    cut = e.get("cut")
    require(bool(cut) == (arm in KILLS), "KILL_POPULATION", missing=arm in KILLS)
    started = e["submit"]["accepted_ns"]
    first = claims[PROVIDER, 0, 1]
    initial = reservations[1]["issued_ns"] if reservations else calls[1]["at_ns"]
    require(
        e["submit"]["request_ns"] <= first["monotonic_ns"] <= initial <= started + 30 * SECOND,
        "FIRST_CLAIM_OR_SEND_DEADLINE",
    )
    if cut is None:
        return
    before, kill, dead, ready = (
        cut[k] for k in ("boundary_ns", "request_ns", "death_ns", "ready_ns")
    )
    require(
        before <= kill <= dead <= ready
        and (arm == "B3" or kill - before <= SECOND // 2)
        and dead - kill <= SECOND // 2
        and ready - dead <= 18 * SECOND,
        "KILL_TIMING",
    )
    require(
        cut["exit_code"] == 137
        and cut["old_process_gone"] is True
        and cut["old_container"] != cut["new_container"]
        and cut["old_session"] != cut["new_session"],
        "KILL_PROCESS_IDENTITY",
    )
    old = cut["pre_db"]
    require(old["request_ns"] <= old["observed_ns"] <= kill, "POST_CUT_PREFLIGHT")
    pre = old["db"]
    if arm == "C4":
        require(
            not pre["workflows"]
            and not pre["attempts"]
            and not cut["open_calls"]
            and ready <= e["submit"]["request_ns"] <= calls[1]["at_ns"],
            "IDLE_SCOPE",
        )
        return
    require(
        bool(pre["workflows"]) and pre["workflows"][0]["workflow_id"] == e["workflow_id"],
        "PRE_CUT_WORKFLOW",
    )
    offset = one(
        [
            r
            for r in hook_events(e, "j4_offset_committed")
            if r["process_session"] == cut["old_session"] and int_field(r, "offset") == 0
        ],
        "MISSING_PREKILL_OFFSET",
    )
    require(
        offset["monotonic_ns"] <= first["monotonic_ns"] <= before
        and any(
            row["kind"] == "offset" and row["offset"] == 1 and row["at_ns"] <= kill
            for row in e["broker"]
        ),
        "PREKILL_BROKER_ACK",
    )
    require(first["process_session"] == cut["old_session"], "CUT_WRONG_WORKER")
    before_attempt = one(
        [r for r in pre["attempts"] if attempt_key(r) == (PROVIDER, 0, 1)],
        "MISSING_PREKILL_ATTEMPT",
    )
    if arm == "C2":
        require(
            before_attempt["state"] == "SUCCEEDED"
            and any(r["reason"] == "ATTEMPT_RESULT_RECORDED" for r in pre["history"])
            and any(r["event_type"] == "activity.result" for r in pre["outbox"])
            and not any(r["reason"] == "RESULT_CONSUMED" for r in pre["history"])
            and not any(r["node_id"] == SUCCESSOR for r in pre["nodes"]),
            "C2_COMMIT_BOUNDARY",
        )
        hold = one([r for r in e["scheduler"] if r["event"] == "held"], "MISSING_C2_GATE")
        release = one([r for r in e["scheduler"] if r["event"] == "released"], "MISSING_C2_RELEASE")
        require(
            hold["workflow_id"] == release["workflow_id"] == e["workflow_id"]
            and hold["monotonic_ns"] <= kill
            and ready <= cut["release_request_ns"] <= ready + 2 * SECOND
            and release["monotonic_ns"] >= cut["release_request_ns"]
            and all(r["at_ns"] < kill for r in calls.values()),
            "C2_GATE_OR_REINVOCATION",
        )
        successor = claims[SUCCESSOR, 0, 1]
        require(
            successor["process_session"] == cut["new_session"]
            and release["monotonic_ns"] <= successor["monotonic_ns"],
            "C2_SUCCESSOR_GATE_JOIN",
        )
        require(
            any(
                r["fields"].get("arm") == "C2"
                and r["process_session"] == cut["old_session"]
                and r["monotonic_ns"] <= kill
                for r in hook_events(e, "cut_held")
            ),
            "MISSING_WORKER_HOLD",
        )
        return
    require(
        before_attempt["state"] == "CLAIMED"
        and before_attempt["is_current"]
        and before_attempt["result"] is None
        and before_attempt["claim_token_hash"] == first["fields"]["token_hash"]
        and not any(r["reason"] == "ATTEMPT_RESULT_RECORDED" for r in pre["history"]),
        "PRE_RESULT_CUT_BOUNDARY",
    )
    replacement = claims[PROVIDER, 0, 2]
    require(
        dead <= replacement["monotonic_ns"]
        and replacement["process_session"] == cut["new_session"],
        "REPLACEMENT_IDENTITY",
    )
    if arm in {"C1", "B2"}:
        first_call = calls[1]
        close = one(
            [
                r
                for r in e["stub"]
                if r["event"] == "close" and r["receipt_sequence"] == first_call["sequence"]
            ],
            "MISSING_SOCKET_CLOSE",
        )
        require(
            first_call["call_id"] in cut["open_calls"]
            and first_call["at_ns"] < kill <= close["at_ns"],
            "CUT_NOT_IN_FLIGHT",
        )
        if arm == "C1":
            require(
                dead <= replacement["monotonic_ns"] <= calls[2]["at_ns"] <= dead + 30 * SECOND,
                "C1_RECOVERY_TIMING",
            )
    if arm in {"B1", "B2", "B3"}:
        first_reservation = one(pre["reservations"], "MISSING_PREKILL_RESERVATION")
        require(
            first_reservation["call_id"] == reservations[1]["call_id"],
            "B2_CUT_RESERVATION_IDENTITY",
        )
        if arm == "B3":
            origin = first_reservation["observed_ns"]
            require(
                first_reservation["outcome"] == "rate_limited"
                and first_reservation["eligible_ns"] == origin + 20 * SECOND
                and origin + SECOND // 2 <= kill <= dead <= origin + SECOND,
                "B3_BACKOFF_CUT",
            )
        else:
            require(
                first_reservation["outcome"] is None and dead < first_reservation["deadline_ns"],
                "B2_CUT_AFTER_OUTCOME_OR_DEADLINE",
            )
        if arm == "B1":
            require(
                any(
                    r["fields"].get("arm") == "B1"
                    and r["process_session"] == cut["old_session"]
                    and r["monotonic_ns"] <= kill
                    for r in hook_events(e, "cut_held")
                )
                and not cut["open_calls"],
                "B1_GATE_OR_UNUSED",
            )


def check_retry(e: dict, calls: dict, reservations: dict) -> list[dict]:
    if not reservations:
        return []
    arm, script, boot = e["arm"], SCRIPTS[e["arm"]], e["clock_start"]["boot_id"]
    traces = {
        kind: by_key(
            hook_events(e, "j4_provider", kind),
            lambda row: row["fields"]["call_id"],
            "DUPLICATE_PROVIDER_WITNESS",
        )
        for kind in ("reserved", "send", "reply", "outcome_committed")
    }
    require(
        set(traces["reserved"]) == {r["call_id"] for r in reservations.values()},
        "RESERVATION_WITNESS_JOIN",
        missing=True,
    )
    require(
        set(traces["send"]) == {r["call_id"] for r in calls.values()},
        "OUTBOUND_WITNESS_JOIN",
        missing=True,
    )
    completed = {
        reservations[i]["call_id"]
        for i, kind in enumerate(script, 1)
        if kind not in {"unused", "cut"}
    }
    for key in ("reply", "outcome_committed"):
        require(set(traces[key]) == completed, "OUTCOME_WITNESS_JOIN", missing=True)
    timings = []
    violations = []
    for ordinal, kind in enumerate(script, 1):
        r = reservations[ordinal]
        ident = r["call_id"]
        issued, deadline = (r[k] for k in ("issued_ns", "deadline_ns"))
        reserved = traces["reserved"][ident]
        require(
            deadline - issued == 2 * SECOND
            and boot == r["boot_id"]
            and int_field(reserved, "ordinal") == ordinal
            and issued <= int_field(reserved, "at_ns")
            and bool(r["committed_at"]),
            "RESERVATION_DEADLINE",
        )
        expected_attempt = 2 if arm in {"B1", "B2", "B3"} and ordinal > 1 else 1
        require(
            r["attempt_number"] == expected_attempt
            and int_field(reserved, "attempt") == expected_attempt,
            "RESERVATION_CLAIM_JOIN",
        )
        if ordinal in calls:
            call = calls[ordinal]
            send = traces["send"][ident]
            require(
                call["call_id"] == ident
                and call["attempt"] == expected_attempt
                and reserved["process_session"] == send["process_session"]
                and int_field(reserved, "at_ns")
                <= int_field(send, "before_ns")
                <= int_field(send, "at_ns")
                <= issued + 250_000_000
                and int_field(send, "before_ns") <= call["at_ns"] < deadline,
                "SEND_BEFORE_RESERVATION_OR_LATE",
            )
        seconds = kind if isinstance(kind, int) and kind < 200 else 0
        if kind in {"unused", "cut", "timeout"}:
            origin, outcome = deadline, "timeout" if kind == "timeout" else "ambiguous"
            if kind == "timeout":
                observed = int_field(traces["reply"][ident], "at_ns")
                require(
                    origin <= observed <= origin + 100_000_000
                    and int_field(traces["reply"][ident], "status") == 0,
                    "TIMEOUT_OBSERVATION",
                )
        else:
            reply = traces["reply"][ident]
            origin, outcome = int_field(reply, "at_ns"), "rate_limited" if seconds else "success"
            require(
                calls[ordinal]["at_ns"] <= origin < deadline
                and int_field(reply, "status") == (429 if seconds else 200)
                and int_field(reply, "retry_after_seconds") == seconds,
                "REPLY_WITNESS",
            )
        require(
            r["outcome"] == outcome
            and r["observed_ns"] == origin
            and r["retry_after_seconds"] == seconds,
            "DURABLE_RETRY_ORIGIN",
        )
        if ident in completed:
            committed = traces["outcome_committed"][ident]
            require(
                committed["process_session"] == reserved["process_session"]
                and int_field(committed, "outcome_ns") == origin
                and int_field(committed, "eligible_ns") == (r["eligible_ns"] or 0)
                and int_field(committed, "at_ns") >= int_field(traces["reply"][ident], "at_ns"),
                "OUTCOME_COMMIT_JOIN",
            )
        if ordinal == len(script):
            final = "FAILED_FINAL" if arm == "B4" else "SUCCEEDED"
            attempt = one(
                [
                    row
                    for row in e["db"]["attempts"]
                    if row["node_id"] == PROVIDER and row["state"] == final
                ],
                "MISSING_B2_TERMINAL",
            )
            require(
                r["terminal_state"] == final and r["terminal_payload"] == attempt["result"],
                "B2_TERMINAL_JOIN",
            )
        else:
            require(r["terminal_state"] is None, "EARLY_B2_TERMINAL")
        if outcome == "success":
            require(r["eligible_ns"] is None, "SUCCESS_RETRY_SCHEDULE")
            continue
        ordinary = origin + max(RETRY[ordinal - 1] * 1_000_000, seconds * SECOND)
        policy = origin + max(3_400_000_000, seconds * SECOND) if arm == "B5" else ordinary
        require(r["eligible_ns"] == policy, "DURABLE_RETRY_SCHEDULE")
        if ordinal == len(script):
            continue
        next_call, next_r = calls[ordinal + 1], reservations[ordinal + 1]
        allowance = 0
        if arm in {"B1", "B2", "B3"} and ordinal == 1:
            read = one(
                [
                    row
                    for row in hook_events(e, "j4_provider", "schedule_read")
                    if row["process_session"] == e["cut"]["new_session"]
                ],
                "MISSING_RESUME_SCHEDULE",
            )
            ready = int_field(read, "at_ns")
            require(
                e["cut"]["death_ns"] <= ready <= e["cut"]["death_ns"] + 18 * SECOND
                and ready <= next_r["issued_ns"]
                and int_field(read, "eligible_ns") == ordinary
                and int_field(read, "outcome_ns") == origin,
                "RESTART_RETRY_SCHEDULE",
            )
            if arm == "B3":
                require(ready <= origin + 17 * SECOND, "B3_RESTART_ALLOWANCE")
            allowance = max(0, ready - ordinary)
        timestamps = (
            next_r["issued_ns"],
            int_field(traces["send"][next_r["call_id"]], "before_ns"),
            next_call["at_ns"],
        )
        require(timestamps == tuple(sorted(timestamps)), "RETRY_CAUSAL_ORDER")
        if arm == "B5":
            require(
                policy <= timestamps[0] <= timestamps[-1] <= policy + 750_000_000,
                "FIXED_DELAY_CONTROL_WINDOW",
            )
        latest = ordinary + 750_000_000 + allowance
        verdict = (
            "early" if timestamps[0] < ordinary else "late" if timestamps[-1] > latest else "inside"
        )
        timings.append(
            {
                "ordinal": ordinal,
                "origin_ns": origin,
                "eligible_ns": ordinary,
                "latest_ns": latest,
                "restart_allowance_ns": allowance,
                "next_issue_ns": timestamps[0],
                "next_receipt_ns": timestamps[-1],
                "deadline_slack_ns": latest - timestamps[-1],
                "verdict": verdict,
            }
        )
        if verdict != "inside":
            violations.append((ordinal, verdict))
        require(timestamps[0] >= origin + seconds * SECOND, "RETRY_AFTER_FLOOR")
    require(
        violations == ([(1, "late"), (2, "early")] if arm == "B5" else []),
        "RETRY_WINDOW_OR_FIXED_DELAY_CONTROL",
    )
    return timings


def check_observation(e: dict, reservations: dict) -> None:
    arm, terminal = e["arm"], e["terminal"]
    lower, upper, ended = terminal["lower_ns"], terminal["observed_ns"], e["observation_end_ns"]
    require(
        lower <= upper <= ended
        and upper <= e["submit"]["accepted_ns"] + (120 if arm.startswith("B") else 90) * SECOND,
        "TERMINAL_TIMING",
    )
    required_redeliveries = 2 if arm == "B4" else 1 if arm == "C2" else 0
    require(len(e["redelivery"]) == required_redeliveries, "REDELIVERY_POPULATION", missing=True)
    if required_redeliveries:
        first = e["redelivery"][0]
        require(upper <= first["request_ns"] <= lower + 2 * SECOND, "TERMINAL_REDELIVERY_TIMING")
        notices = {r["offset"]: r for r in e["broker"] if r["kind"] == "notice"}
        for delivery in e["redelivery"]:
            raw = notices[delivery["original_offset"]]
            require(
                attempt_key(encoded_payload(raw)) == (PROVIDER, 0, 1), "REDELIVERY_WRONG_NOTICE"
            )
    if arm.startswith("C"):
        floor = upper + 30 * SECOND
        if arm == "C2":
            floor = max(
                floor,
                e["cut"]["request_ns"] + 30 * SECOND,
                e["redelivery"][0]["ack_ns"] + 30 * SECOND,
            )
        require(ended >= floor, "CORE_QUIET_WINDOW")
    if arm == "B4":
        deadline = reservations[4]["deadline_ns"]
        latest = deadline + (16_039 + 750 + 18_000) * 1_000_000
        second = e["redelivery"][1]
        require(
            upper <= deadline + 5 * SECOND
            and latest + SECOND <= second["request_ns"] <= latest + 3 * SECOND
            and ended >= max(latest + 10 * SECOND, second["ack_ns"] + 5 * SECOND),
            "EXHAUSTION_OBSERVATION",
        )
    delivered = {
        int_field(row, "offset"): row["monotonic_ns"]
        for row in hook_events(e, "j4_offset_committed")
    }
    require(
        all(delivered.get(row["offset"]) == row["ack_ns"] for row in e["redelivery"]),
        "REDELIVERY_ACK_WITNESS",
    )


def check(e: dict) -> dict:
    try:
        require(e.get("schema") == "j4.cell.v1", "INVALID_EVIDENCE_SCHEMA", missing=True)
        check_clocks(e)
        calls, reservations = check_calls(e)
        attempts = check_attempts(e, calls)
        claims = check_transport(e, attempts)
        check_provider_claims(e, calls, reservations, claims)
        check_cut(e, calls, reservations, claims)
        timing = check_retry(e, calls, reservations)
        check_observation(e, reservations)
        return {
            "status": "pass",
            "control": REJECTIONS.get(e["arm"]),
            "calls": len(calls),
            "rejected_calls": int(e["arm"] == "C3"),
            "calls_after_kill": sum(
                r["at_ns"] >= (e.get("cut") or {}).get("request_ns", 1 << 65)
                for r in calls.values()
            ),
            "reservations": len(reservations),
            "unused": int(e["arm"] == "B1"),
            "completions": int(e["arm"] not in {"C3", "B4"}),
            "exhaustions": int(e["arm"] == "B4"),
            "timing": timing,
        }
    except VerdictError as exc:
        return {"status": "invalid" if exc.missing else "fail", "reason": exc.code}
    except (KeyError, TypeError, ValueError, IndexError) as exc:
        return {
            "status": "invalid",
            "reason": "MALFORMED_OR_INCOMPLETE_EVIDENCE",
            "error_type": type(exc).__name__,
        }


def negative_controls(source: dict, destination: Path) -> dict:
    if source["arm"] != "B0" or check(source)["status"] != "pass":
        raise ValueError("J4 controls need a passing B0 qualification cell")
    verdicts = {}
    for tag, reason in (
        ("missing-call", "MISSING_CALL"),
        ("missing-history", "MISSING_HISTORY"),
        ("missing-reservation", "MISSING_RESERVATION"),
    ):
        copy = deepcopy(source)
        if tag == "missing-call":
            first = next(r for r in copy["stub"] if r["event"] == "receipt")
            copy["stub"].remove(first)
        elif tag == "missing-history":
            first = next(r for r in copy["db"]["history"] if r["reason"] == "RESULT_CONSUMED")
            copy["db"]["history"].remove(first)
        else:
            copy["db"]["reservations"].pop(0)
        write_json(destination / f"{tag}.json", copy)
        verdict = check(copy)
        if verdict["status"] == "pass" or verdict["reason"] != reason:
            raise ValueError(f"J4 negative control {tag} escaped its independent join")
        verdicts[tag] = verdict
    write_json(destination / "verdicts.json", verdicts)
    return verdicts
