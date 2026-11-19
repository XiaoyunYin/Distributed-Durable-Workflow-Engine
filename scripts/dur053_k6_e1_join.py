"""Independent engineering joins for the five deployed hook cells."""

from __future__ import annotations

import json
from datetime import datetime
from hashlib import sha256

from dur053_k6_b_evidence import SmokeFailure

BASE = ("I1", "I2", "I4", "I6", "I11")
GROUP = "runtime-workers-v1"
SCHEDULERS = "runtime-schedulers-v1"
TASKS = "durable-agent.tasks.v1"
EVENTS = "durable-agent.events.v1"


def expect(name: str, observed, expected) -> None:
    if observed != expected:
        raise SmokeFailure(
            f"{name} observed={json.dumps(observed)} expected={json.dumps(expected)}"
        )


def one(name: str, rows: list[dict]) -> dict:
    expect(name + "_MATCHED_LINES", len(rows), 1)
    return rows[0]


def by(rows: list[dict], **values) -> list[dict]:
    return [row for row in rows if all(row.get(key) == value for key, value in values.items())]


def journal(raw: bytes, arm: dict, *, complete: bool = False) -> dict:
    lines = raw.splitlines()
    expect("E1_JOURNAL_FRAMED", bool(raw) and raw.endswith(b"\n"), True)
    try:
        rows = [json.loads(line) for line in lines]
    except (ValueError, UnicodeError):
        raise SmokeFailure("E1_JOURNAL_INVALID_JSON") from None
    expect("E1_JOURNAL_SEQUENCE", [row["sequence"] for row in rows], list(range(1, len(rows) + 1)))
    expect("E1_JOURNAL_TIME_ORDER", [r["at_ns"] for r in rows], sorted(r["at_ns"] for r in rows))
    selected = by(rows, arm=arm["arm"])
    expected = ["arm", "reach", "held"]
    if complete:
        expected += ["inject", "backend_confirmed", "process_exit", "action", "recovery"]
    expect("E1_JOURNAL_TIMELINE", [r["event"] for r in selected], expected)
    armed, reached, held = selected[:3]
    for row, binding in ((armed, arm["arm_ack"]), (reached, arm["ack"])):
        hash_field = "journal_sha256" if row is reached else "sha256"
        expect("E1_JOURNAL_BOUND_RECORD", row["sequence"], binding["sequence"])
        expect(
            "E1_JOURNAL_BOUND_HASH",
            sha256(lines[row["sequence"] - 1]).hexdigest(),
            binding[hash_field],
        )
    expect("E1_JOURNAL_WIRE_HASH", reached["message_sha256"], arm["ack"]["sha256"])
    expect("E1_JOURNAL_ACK", held["ack"], arm["ack"])
    expect("E1_JOURNAL_ACTOR", [armed["target"], reached["target"]], [arm["target"]] * 2)
    expect("E1_JOURNAL_BOUNDARY", reached["boundary"], armed["boundary"])
    expect("E1_JOURNAL_REACH", {k: reached[k] for k in arm["reach"]}, arm["reach"])
    for name in ("hook_id", "nonce"):
        expect("E1_JOURNAL_" + name.upper(), len(by(rows, **{name: reached[name]})), 1)
    cutoff = armed["until_ms"] * 1_000_000
    expect("E1_JOURNAL_HELD_IN_TIME", held["at_ns"] < cutoff, True)
    if complete:
        injected, confirmed, exited, action, recovered = selected[3:]
        for row in (injected, confirmed, exited, action):
            expect("E1_JOURNAL_ACTION_ACK", row["ack"], arm["ack"])
        expected_target = {key: injected["target"][key] for key in arm["target"]}
        expect("E1_JOURNAL_KILL_TARGET", expected_target, arm["target"])
        expect("E1_JOURNAL_CRI_TARGET", exited["target"], injected["target"])
        expect("E1_JOURNAL_KILLED", action["command"], "killed")
        expect("E1_JOURNAL_KILL_IN_TIME", action["at_ns"] < cutoff, True)
        expect(
            "E1_JOURNAL_REPLACEMENT_CONTAINER",
            recovered["replacement"]["container_id"] != injected["target"]["container_id"],
            True,
        )
        expect(
            "E1_JOURNAL_INJECTION_ORDER",
            held["at_ns"] <= injected["issued_ns"] <= confirmed["acknowledged_ns"],
            True,
        )
        expect(
            "E1_JOURNAL_DEATH_ORDER",
            injected["issued_ns"] <= exited["observed_ns"] <= recovered["at_ns"]
            and exited["observed_ns"] < cutoff,
            True,
        )
    return {row["event"]: row for row in selected}


def broker_records(broker: list[dict], partitions: int) -> list[dict]:
    expect("E1_KAFKA_PARTITION_COUNT", len({row["partition"] for row in broker}), partitions)
    result = []
    for part in broker:
        expect("E1_KAFKA_RETENTION_START", part["first"], 0)
        expect(
            "E1_KAFKA_READ_RANGE",
            [row["offset"] for row in part["records"]],
            list(range(part["first"], part["next"])),
        )
        result.extend({**row, "partition": part["partition"]} for row in part["records"])
    return result


def relay_at_hold(cell: str, workflow: str, objects: dict, db: dict, broker: list[dict]) -> list:
    event = one("E1_HELD_OUTBOX", by(db["outbox"], event_id=objects["event"]))
    expected = {
        "event_type": "workflow.created",
        "topic": EVENTS,
        "publish_state": "CLAIMED",
        "claim_owner": objects["owner"],
        "relay_attempts": 1,
        "published_at": None,
    }
    expect("E1_FIRST_OUTBOX_CLAIM", {k: event[k] for k in expected}, expected)
    expect("E1_OUTBOX_HOOK_IDENTITY", (objects["attempt"], objects["topic"]), ("1", EVENTS))
    expect("E1_UNFINALIZED_OUTBOX", by(db["publications"], event_id=objects["event"]), [])
    records = by(broker_records(broker, 8), event=objects["event"])
    if cell == "hook-0":
        expect("E1_BEFORE_PUBLISH_BROKER_COPIES", len(records), 0)
    else:
        expect("E1_AFTER_ACK_BROKER_COPIES", len(records), 1)
    for row in records:
        expect(
            "E1_FIRST_BROKER_IDENTITY",
            (row["key"], row["workflow"], row["traceparent"]),
            (objects["event"], workflow, event["traceparent"]),
        )
        expect("E1_FIRST_BROKER_TRACE", bool(row["traceparent"]), True)
    return records


def relay_after(
    objects: dict, before: list, db: dict, broker: list[dict], replacement: dict
) -> dict:
    event = one("E1_RECOVERED_OUTBOX", by(db["outbox"], event_id=objects["event"]))
    publication = one("E1_RECOVERED_PUBLICATION", by(db["publications"], event_id=objects["event"]))
    expect(
        "E1_RECOVERED_OUTBOX_STATE",
        (event["publish_state"], event["relay_attempts"]),
        ("PUBLISHED", 2),
    )
    expect(
        "E1_REPLACEMENT_RELAY",
        (
            publication["broker_acknowledged"],
            publication["relay_attempt"],
            publication["relay_owner"],
        ),
        (True, 2, replacement["session_uuid"]),
    )
    expect("E1_RELAY_NEW_OWNER", replacement["session_uuid"] != objects["owner"], True)
    copies = by(broker_records(broker, 8), event=objects["event"])
    expect("E1_REPLACEMENT_BROKER_COPIES", len(copies), len(before) + 1)
    expect("E1_REPLAY_PRESERVES_ORIGINAL", copies[: len(before)], before)
    for row in copies:
        expect(
            "E1_REPLAY_TRACE_AND_ID",
            (row["key"], row["workflow"], row["traceparent"], row["partition"]),
            (event["event_id"], event["workflow_id"], event["traceparent"], copies[0]["partition"]),
        )
    inbox = one(
        "E1_LOGICAL_DISPOSITION", by(db["inbox"], event_id=objects["event"], consumer_id=SCHEDULERS)
    )
    expect(
        "E1_LOGICAL_DISPOSITION_OFFSET",
        (inbox["kafka_partition"], inbox["kafka_offset"], inbox["disposition"]),
        (copies[0]["partition"], copies[0]["offset"], "ACCEPTED"),
    )
    return {"publication": publication, "copies": copies, "inbox": inbox}


def claim_at_hold(db: dict, objects: dict, target: dict, namespace: str, witness: list) -> dict:
    attempt = one("E1_HELD_ATTEMPT", db["attempts"])
    claim = one("E1_WORKER_CONTROL_COMMIT_AUDIT", db["claims"])
    worker = f"system:serviceaccount:{namespace}:worker:pod:{target['pod']}:uid:{target['uid']}"
    expected = {
        "node_id": objects["node"],
        "iteration": int(objects["iteration"]),
        "attempt_number": 1,
        "worker_id": worker,
        "worker_request_id": objects["request"],
        "token_hash": objects["token_hash"],
    }
    for row in (attempt, claim):
        expect("E1_CLAIM_PID_JOIN", {k: row[k] for k in expected}, expected)
    expect("E1_CLAIM_WORKER_POD", objects["worker"].rsplit("-", 1)[0], target["pod"])
    expect(
        "E1_CLAIM_UNEXECUTED",
        (attempt["state"], attempt["is_current"], attempt["result"]),
        ("CLAIMED", True, None),
    )
    expect("E1_CLAIM_NO_EARLIER_RESULT", by(db["history"], reason="ATTEMPT_RESULT_RECORDED"), [])
    expect("E1_CLAIM_NO_RESULT_EVIDENCE", db["result_evidence"], [])
    expect("E1_CLAIM_IS_NOT_INVOCATION", witness, [])
    return attempt


def claim_after(old: dict, db: dict, witness: list[dict], replacement: dict) -> dict:
    previous = one("E1_EXPIRED_CLAIM", by(db["attempts"], attempt_number=1))
    winner = one("E1_REPLACEMENT_RECEIPT", by(db["attempts"], attempt_number=2, state="SUCCEEDED"))
    claim = one("E1_REPLACEMENT_CLAIM_COMMIT", by(db["claims"], attempt_number=2))
    timeout = one("E1_TTL_TRANSITION", by(db["history"], reason="TIMEOUT_REPLACEMENT"))
    expect("E1_EXPIRED_CLAIM_RESULT", (previous["state"], previous["result"]), ("TIMED_OUT", None))
    expect(
        "E1_TTL_RECOVERY_TIME",
        datetime.fromisoformat(timeout["created_at"])
        >= datetime.fromisoformat(old["heartbeat_deadline"]),
        True,
    )
    expect(
        "E1_REPLACEMENT_CLAIM_WINNER",
        (claim["token_hash"], claim["order"] > db["claims"][0]["order"]),
        (winner["token_hash"], True),
    )
    expect("E1_EXPIRED_ATTEMPT_NEVER_STARTED", by(witness, attempt_number=1), [])
    start = one("E1_REPLACEMENT_INVOCATION", by(witness, attempt_number=2, event="start"))
    expect(
        "E1_REPLACEMENT_WORKER_JOIN",
        (start["pod_uid"], start["token_hash"], start["pod_name"]),
        (replacement["uid"], winner["token_hash"], replacement["pod"]),
    )
    return {"timeout": timeout, "receipt": winner, "claim": claim, "start": start}


def offset_at_hold(objects: dict, db: dict, sample: dict, broker: list[dict]) -> list[dict]:
    part, offset, next_offset = (int(objects[k]) for k in ("partition", "offset", "next_offset"))
    expect(
        "E1_I7_GROUP_SAMPLE",
        (sample.get("group"), sample.get("topic"), sample.get("partition")),
        (GROUP, TASKS, part),
    )
    expect("E1_I7_HELD_KAFKA_COMMIT", sample["next"] if sample["next"] != -1 else 0, offset)
    expect("E1_I7_CONTROLLING_NEXT", db["next_offset"], next_offset)
    expect("E1_I7_HELD_DELIVERY", (next_offset, objects["disposition"]), (offset + 1, "ACCEPTED"))
    records = [r for r in broker_records(broker, 1) if r["offset"] < next_offset]
    expect("E1_I7_BROKER_LOW_RANGE", [r["offset"] for r in records], list(range(next_offset)))
    expect("E1_I7_HELD_EVENT", records[-1]["event"], objects["event"])
    dispositions = []
    for row in records:
        expect("E1_I7_RECORD_PARTITION", (row["partition"], row["key"]), (part, row["event"]))
        dispositions.append(
            one(
                "E1_I7_LOWER_DISPOSITION",
                by(db["rows"], offset=row["offset"], event_id=row["event"]),
            )
        )
    expect("E1_I7_HELD_DB_DISPOSITION", dispositions[-1]["disposition"], "ACCEPTED")
    return dispositions


def offset_after(
    objects: dict, before: list, db: dict, sample: dict, logs: list, replacement: dict
) -> dict:
    next_offset = int(objects["next_offset"])
    expect(
        "E1_I7_REPLACEMENT_BROKER_COMMIT",
        (sample.get("group"), sample.get("topic"), sample.get("partition"), sample.get("next")),
        (GROUP, TASKS, int(objects["partition"]), next_offset),
    )
    expect("E1_I7_REPLACEMENT_DB_NEXT", db["next_offset"], next_offset)
    expect(
        "E1_I7_DURABLE_LOWER_DISPOSITIONS",
        [r for r in db["rows"] if r["offset"] < next_offset],
        before,
    )
    redelivery = one(
        "E1_I7_REDELIVERY",
        by(
            logs,
            boundary="before_offset_ack",
            session=replacement["session"],
            event=objects["event"],
            offset=objects["offset"],
            next_offset=objects["next_offset"],
            disposition="ALREADY_HANDLED",
        ),
    )
    return {"broker": sample, "all_lower_dispositions": before, "redelivery": redelivery}


def receipt_at_hold(objects: dict, db: dict, target: dict) -> dict:
    receipt = one(
        "E1_EXACT_RECEIPT",
        by(db["attempts"], attempt_number=int(objects["attempt"]), state="SUCCEEDED"),
    )
    record = one("E1_RESULT_RECORD", by(db["history"], reason="ATTEMPT_RESULT_RECORDED"))
    expected = (
        objects["node"],
        int(objects["iteration"]),
        objects["token_hash"],
        objects["result_hash"],
        False,
    )
    expect(
        "E1_RECEIPT_IDENTITY",
        tuple(
            receipt[k] for k in ("node_id", "iteration", "token_hash", "result_hash", "is_current")
        ),
        expected,
    )
    expect(
        "E1_PRE_CONSUMPTION_REVISION",
        (db["workflow"]["revision"], record["revision"]),
        (int(objects["revision"]),) * 2,
    )
    expect("E1_NOT_YET_CONSUMED", by(db["history"], reason="RESULT_CONSUMED"), [])
    lease = one(
        "E1_HELD_LEASE",
        by(
            db["leases"],
            partition_id=int(objects["partition"]),
            owner_id=objects["owner"],
            epoch=int(objects["epoch"]),
            live=True,
        ),
    )
    one(
        "E1_HELD_LEASE_OWNER_COMMIT",
        by(
            db["audit"],
            kind="acquire",
            owner_id=objects["owner"],
            epoch=int(objects["epoch"]),
            session_id=target["session_uuid"],
            pod_uid=target["uid"],
        ),
    )
    event = one(
        "E1_WORKER_RECEIPT_EVENT",
        by(db["outbox"], event_type="activity.result", aggregate_revision=record["revision"]),
    )
    one(
        "E1_DEPLOYED_KAFKA_RECEIPT",
        by(db["inbox"], consumer_id=SCHEDULERS, event_id=event["event_id"], disposition="ACCEPTED"),
    )
    return {"receipt": receipt, "record": record, "lease": lease}


def receipt_after(before: dict, db: dict, replacement: dict) -> dict:
    receipt = one(
        "E1_SAME_RESULT_RECEIPT",
        by(db["attempts"], attempt_number=before["receipt"]["attempt_number"]),
    )
    expect("E1_RESULT_RECEIPT_UNCHANGED", receipt, before["receipt"])
    consumed = one("E1_OWNER_CONSUMPTION", by(db["history"], reason="RESULT_CONSUMED"))
    expect("E1_OWNER_CONSUMPTION_REVISION", consumed["revision"], before["record"]["revision"] + 1)
    expect("E1_OWNER_CONSUMPTION_ATTEMPT", consumed["attempt_number"], receipt["attempt_number"])
    audit = one(
        "E1_REPLACEMENT_OWNER_COMMIT",
        by(
            db["audit"],
            kind="history",
            transition_id=consumed["transition_id"],
            session_id=replacement["session_uuid"],
            pod_uid=replacement["uid"],
        ),
    )
    expect(
        "E1_OWNER_EPOCH_ADVANCED",
        (
            audit["epoch"] > before["lease"]["epoch"],
            audit["epoch"],
            audit["owner_id"] != before["lease"]["owner_id"],
        ),
        (True, consumed["scheduler_epoch"], True),
    )
    one(
        "E1_REPLACEMENT_LEASE_ACQUIRED",
        by(
            db["audit"],
            kind="acquire",
            owner_id=audit["owner_id"],
            epoch=audit["epoch"],
            session_id=audit["session_id"],
        ),
    )
    return {"receipt": receipt, "consumed": consumed, "owner_audit": audit}


def base_inputs(cell: str, workflow: str, payload_hash: str, db: dict, witness: list[dict]) -> dict:
    wf = db["workflow"]
    expect(
        "E1_I1_SUBMISSION",
        (wf["workflow_id"], wf["submission_key"], wf["submission_payload_hash"]),
        (workflow, workflow, payload_hash),
    )
    history = db["history"]
    expect(
        "E1_I2_REVISIONS", [row["revision"] for row in history], list(range(1, wf["revision"] + 1))
    )
    expect(
        "E1_I2_TRANSITIONS",
        [row["old_state"] for row in history[1:]],
        [row["new_state"] for row in history[:-1]],
    )
    receipt = one("E1_I4_RECEIPT", by(db["attempts"], state="SUCCEEDED"))
    recorded = one("E1_I4_RESULT_RECORDED", by(history, reason="ATTEMPT_RESULT_RECORDED"))
    claim = one("E1_I4_CLAIM_AUDIT", by(db["claims"], attempt_number=receipt["attempt_number"]))
    expect(
        "E1_I4_WINNING_RECEIPT",
        (recorded["attempt_number"], claim["token_hash"]),
        (receipt["attempt_number"], receipt["token_hash"]),
    )
    start = one(
        "E1_I4_INVOCATION_START", by(witness, event="start", token_hash=receipt["token_hash"])
    )
    one(
        "E1_I4_INVOCATION_END",
        by(witness, event="end", invocation_id=start["invocation_id"], outcome="SUCCEEDED"),
    )
    for event in db["outbox"]:
        expect("E1_I6_PUBLISHED", event["publish_state"], "PUBLISHED")
        one(
            "E1_I6_BROKER_ACK",
            by(db["publications"], event_id=event["event_id"], broker_acknowledged=True),
        )
        consumer = GROUP if event["topic"] == TASKS else SCHEDULERS
        one(
            "E1_I6_DISPOSITION",
            by(
                db["inbox"],
                event_id=event["event_id"],
                consumer_id=consumer,
                disposition="ACCEPTED",
            ),
        )
    expect("E1_I6_DISPATCH", bool(by(db["outbox"], event_type="attempt.dispatch")), True)
    terminal = one("E1_I11_TERMINAL", by(history, new_state="SUCCEEDED"))
    expect(
        "E1_I11_WORKFLOW_STATE", (wf["state"], wf["revision"]), ("SUCCEEDED", terminal["revision"])
    )
    labels = [f"I{i}" for i in range(1, 14)]
    registered = set(BASE) | ({"I7"} if cell == "hook-3" else set())
    return {
        "registered": [label for label in labels if label in registered],
        "not_covered": [label for label in labels if label not in registered],
        "predicates": {
            "I1": {"submission": wf["submission_key"], "body_hash": payload_hash},
            "I2": {"transitions": [row["transition_id"] for row in history]},
            "I4": {
                "claim_order": claim["order"],
                "result_transition": recorded["transition_id"],
                "invocation": start["invocation_id"],
            },
            "I6": {
                "events": [row["event_id"] for row in db["outbox"]],
                "publications": [row["publication_id"] for row in db["publications"]],
            },
            "I11": {"terminal_transition": terminal["transition_id"]},
        },
    }
