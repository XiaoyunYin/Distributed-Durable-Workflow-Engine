"""Independent evidence joins for the deployed scheduler lease takeover."""

from __future__ import annotations

import json
from datetime import datetime

from dur053_k6_b_evidence import SmokeFailure
from dur053_k6_e1_join import base_inputs, by, expect, journal, one

CELL = "hook-5"
PRIMARY = "lease_takeover"
COMPANION = "lease_takeover_acquired"
WATCHED = ("partition_leases", "activity_attempts", "outbox", "transition_history")


def acknowledged(raw: bytes, arm: dict) -> dict:
    """Verify the durable ACK prefix with the existing E1 journal checks."""
    lines = raw.splitlines(keepends=True)
    held = one(
        "E2_DURABLE_ACK",
        by([json.loads(line) for line in lines], arm=arm["arm"], event="held"),
    )
    return journal(b"".join(lines[: held["sequence"]]), arm)["reach"]


def lease_at_hold(arm: dict, reach: dict, target: dict, cut: dict, partition: int) -> dict:
    fields = reach["fields"]
    objects = fields["objects"]
    expect("E2_REACH_BINDING", reach["target"], arm["target"])
    expect("E2_REACH_TARGET", reach["target"], {key: target[key] for key in reach["target"]})
    expect("E2_REACH_WORKFLOW", fields["key"], arm["selector"]["key"])
    expect("E2_REACH_INVOCATION", bool(fields["invocation"]), True)
    expect(
        "E2_WORKFLOW_AT_HOLD",
        cut["workflow"],
        {"id": fields["key"], "revision": int(objects["revision"])},
    )
    lease = cut["lease"]
    expect(
        "E2_UNEXPIRED_OWNER",
        (lease["partition_id"], lease["owner_id"], lease["epoch"], lease["live"]),
        (partition, objects["owner"], int(objects["epoch"]), True),
    )
    expect("E2_REACH_PARTITION", int(objects["partition"]), partition)
    expect("E2_REACH_CELL", objects["cell"], CELL)
    backend = one("E2_HOLD_DB_BACKEND", by(cut["backends"], pid=int(objects["backend_pid"])))
    expect(
        "E2_IDLE_BACKEND_LIFETIME",
        (backend["start_us"], backend["application"], backend["state"], backend["in_transaction"]),
        (int(objects["backend_start_us"]), "dur053-e2-" + target["session_uuid"], "idle", False),
    )
    owner = one(
        "E2_OWNER_COMMIT",
        by(cut["acquisitions"], owner_id=lease["owner_id"], epoch=lease["epoch"]),
    )
    expect(
        "E2_OWNER_AUDIT_PROCESS",
        (owner["session_id"], owner["pod_uid"], owner["container_id"]),
        (target["session_uuid"], target["uid"], target["uid"]),
    )
    return owner


def takeover(primary: dict, before: dict, successor: dict, after: dict, reach: dict) -> dict:
    old, new = before["lease"], after["lease"]
    expect("E2_HIGHER_EPOCH", new["epoch"], old["epoch"] + 1)
    expect("E2_DISTINCT_OWNER", new["owner_id"] != old["owner_id"], True)
    expect("E2_ACQUISITION_COMMIT_ORDER", primary["event_order"] < successor["event_order"], True)
    expect(
        "E2_DB_EXPIRY_ORDER",
        datetime.fromisoformat(before["db_at"])
        < datetime.fromisoformat(old["lease_expires_at"])
        <= datetime.fromisoformat(new["updated_at"])
        <= datetime.fromisoformat(after["db_at"]),
        True,
    )
    objects = reach["fields"]["objects"]
    expect("E2_SUCCESSOR_ACQUISITION_XID", objects["transaction"], successor["transaction_id"])
    write = lease_commit(after, successor)
    expect(
        "E2_DB_ACQUISITION_BACKEND",
        (write["pid"], write["session_id"], write["owner_id"], write["epoch"]),
        (int(objects["backend_pid"]), successor["session_id"], new["owner_id"], new["epoch"]),
    )
    last_old_write = max(
        by(before["writes"], relation="partition_leases"), key=lambda row: row["commit_order"]
    )
    expect(
        "E2_PREDECESSOR_LEASE_EXPIRY_WITNESS",
        (
            last_old_write["owner_id"],
            last_old_write["epoch"],
            datetime.fromisoformat(last_old_write["row_identity"]["expires_at"]).timestamp(),
        ),
        (
            old["owner_id"],
            old["epoch"],
            datetime.fromisoformat(old["lease_expires_at"]).timestamp(),
        ),
    )
    expect(
        "E2_NO_INTERVENING_LEASE_MUTATION",
        [
            row
            for row in by(after["writes"], relation="partition_leases")
            if last_old_write["commit_order"] < row["commit_order"] <= write["commit_order"]
        ],
        [write],
    )
    return write


def lease_commit(cut: dict, audit: dict) -> dict:
    write = one(
        "E2_LEASE_WRITE",
        by(cut["writes"], relation="partition_leases", xid=audit["transaction_id"]),
    )
    expect(
        "E2_LEASE_WRITE_AUDIT",
        (write["owner_id"], write["epoch"], write["session_id"]),
        (audit["owner_id"], audit["epoch"], audit["session_id"]),
    )
    return write


def reject_stale_writes(
    cut: dict, acquisition: dict, primary: dict, *, successor_held: bool
) -> None:
    later = [row for row in cut["writes"] if row["commit_order"] > acquisition["commit_order"]]
    old = [
        row
        for row in later
        if row["epoch"] == primary["epoch"]
        or (
            row["session_id"] == primary["session_id"]
            and (
                successor_held
                or not any(
                    owner["session_id"] == row["session_id"]
                    and owner["owner_id"] == row["owner_id"]
                    and owner["epoch"] == row["epoch"]
                    and owner["epoch"] > acquisition["epoch"]
                    and any(
                        earlier["xid"] == owner["transaction_id"]
                        and earlier["relation"] == "partition_leases"
                        and earlier["commit_order"] <= row["commit_order"]
                        for earlier in later
                    )
                    for owner in cut["acquisitions"]
                )
            )
        )
    ]
    expect("E2_I3_STALE_MUTATION", old, [])


def confirm_rejection(call: dict, reach: dict, before: dict, after: dict, cut: dict) -> None:
    if call["outcome"] == "COMMITTED":
        raise SmokeFailure(
            "E2_I3_STALE_CALL_COMMITTED observed=COMMITTED expected=ErrLeaseNotOwned"
        )
    expect("E2_TYPED_REJECTION", call["outcome"], "ErrLeaseNotOwned")
    expect("E2_CONTINUOUS_PREDECESSOR", after, before)
    expect(
        "E2_DEPLOYED_CALL_ORIGIN",
        (
            call["operation"],
            call["pid"],
            call["process_start_id"],
            call["session"],
            call["pod_uid"],
        ),
        (
            "CreateAttempt",
            int(before["process_start_id"].split(":")[0]),
            before["process_start_id"],
            before["session_uuid"],
            before["uid"],
        ),
    )
    fields = reach["fields"]
    expect(
        "E2_EXACT_RESUMED_INVOCATION",
        (call["workflow"], call["invocation"]),
        (fields["key"], fields["invocation"]),
    )
    backend_keys = {"backend_pid", "backend_start_us"}
    expect(
        "E2_ORIGINAL_LEASE_REF",
        {k: v for k, v in call["objects"].items() if k not in backend_keys},
        {k: v for k, v in fields["objects"].items() if k not in backend_keys},
    )
    expect(
        "E2_RESUMED_DB_BACKEND",
        {k: call["objects"].get(k) for k in backend_keys},
        {k: fields["objects"][k] for k in backend_keys},
    )
    backend = one(
        "E2_RESUMED_BACKEND_SAMPLE", by(cut["backends"], pid=int(fields["objects"]["backend_pid"]))
    )
    expect(
        "E2_RESUMED_DB_SESSION",
        (backend["start_us"], backend["application"]),
        (int(fields["objects"]["backend_start_us"]), "dur053-e2-" + before["session_uuid"]),
    )


def timeline(raw: bytes, primary: dict, successor: dict) -> dict:
    reaches = [acknowledged(raw, arm) for arm in (primary, successor)]
    expect("E2_JOURNAL_FRAMED", raw.endswith(b"\n"), True)
    rows = [json.loads(line) for line in raw.splitlines()]
    expect("E2_JOURNAL_SEQUENCE", [r["sequence"] for r in rows], list(range(1, len(rows) + 1)))
    expect(
        "E2_JOURNAL_OBSERVER_ORDER", [r["at_ns"] for r in rows], sorted(r["at_ns"] for r in rows)
    )
    expected = [
        (primary, "arm"),
        (primary, "reach"),
        (primary, "held"),
        (primary, "prolonged"),
        (successor, "arm"),
        (successor, "reach"),
        (successor, "held"),
        (successor, "takeover_verified"),
        (primary, "action"),
        (primary, "command_issued"),
        (primary, "release"),
        (successor, "rejection_observed"),
        (successor, "action"),
        (successor, "command_issued"),
        (successor, "release"),
    ]
    selected = [r for r in rows if r.get("arm") in {primary["arm"], successor["arm"]}]
    expect(
        "E2_RELEASE_ORDER",
        [(r["arm"], r["event"]) for r in selected],
        [(arm["arm"], event) for arm, event in expected],
    )
    expect("E2_SHARED_DEADLINE", successor["until_ms"], primary["until_ms"])
    arms = {arm["arm"]: arm for arm in (primary, successor)}
    for row in selected:
        arm, event = arms[row["arm"]], row["event"]
        expect("E2_BEFORE_DEADLINE", row["at_ns"] < arm["until_ms"] * 1_000_000, True)
        if event in {
            "prolonged",
            "takeover_verified",
            "rejection_observed",
            "action",
            "command_issued",
            "release",
        }:
            expect("E2_ORDERED_ACK", row["ack"], arm["ack"])
        if event in {"action", "command_issued"}:
            expect("E2_RESUME_COMMAND", row["command"], "release")
    expect("E2_DISTINCT_TARGETS", primary["target"]["uid"] != successor["target"]["uid"], True)
    expect("E2_DISTINCT_INVOCATIONS", len({r["fields"]["invocation"] for r in reaches}), 2)
    return {f"{r['arm']}/{r['event']}": r["sequence"] for r in selected}


def recovery(
    db: dict,
    target: dict,
    acquired: dict,
    revision: int,
    workflow: str,
    body_hash: str,
    witness: list[dict],
) -> dict:
    first = one("E2_FIRST_USEFUL_TRANSITION", by(db["history"], revision=revision + 1))
    expect("E2_SUCCESSOR_CREATED_ATTEMPT", first["reason"], "ATTEMPT_CREATED")
    audit = one("E2_SUCCESSOR_FIRST_COMMIT", by(db["audit"], transition_id=first["transition_id"]))
    expect(
        "E2_SUCCESSOR_FIRST_OWNER",
        (
            audit["session_id"],
            audit["pod_uid"],
            audit["owner_id"],
            audit["epoch"],
            first["scheduler_epoch"],
        ),
        (
            target["session_uuid"],
            target["uid"],
            acquired["owner_id"],
            acquired["epoch"],
            acquired["epoch"],
        ),
    )
    expect(
        "E2_FIRST_COMMIT_AFTER_ACQUISITION", audit["event_order"] > acquired["event_order"], True
    )
    for transition in db["history"]:
        if transition["revision"] <= revision or transition["scheduler_epoch"] is None:
            continue
        committed = one(
            "E2_OWNER_HISTORY",
            by(db["audit"], transition_id=transition["transition_id"], kind="history"),
        )
        expect("E2_OWNER_HISTORY_EPOCH", committed["epoch"], transition["scheduler_epoch"])
        lease = one(
            "E2_OWNER_HISTORY_ACQUISITION",
            by(
                db["audit"],
                kind="acquire",
                epoch=committed["epoch"],
                owner_id=committed["owner_id"],
                session_id=committed["session_id"],
                partition_id=committed["partition_id"],
            ),
        )
        expect("E2_OWNER_HISTORY_ORDER", lease["event_order"] < committed["event_order"], True)
    evidence = base_inputs(CELL, workflow, body_hash, db, witness)
    evidence["registered"].insert(2, "I3")
    evidence["not_covered"].remove("I3")
    evidence["predicates"]["I3"] = {"successor_first_transition": first["transition_id"]}
    return evidence
