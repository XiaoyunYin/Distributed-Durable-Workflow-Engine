"""Independent joins for the two scheduler-to-PostgreSQL partition cells."""

from __future__ import annotations

import ipaddress
import json
import shlex
from datetime import datetime
from itertools import pairwise
from typing import Any
from uuid import UUID

from dur053_k6_b_evidence import SmokeFailure
from dur053_k6_chaos import Target, prove_restoration
from dur053_k6_e1_join import by, expect, one
from dur053_k6_e2_join import acknowledged, lease_commit, reject_stale_writes

JsonObject = dict[str, Any]

CELLS = ("medium-2", "medium-3")
BOUNDARIES = {"medium-2": "renew_lease_locked", "medium-3": "lease_takeover"}
TARGET_ROLE = "scheduler-f1b-target"
PEER_ROLE = "scheduler-f1b-peer"
TARGET_APP = "durable-" + TARGET_ROLE
PEER_APP = "durable-" + PEER_ROLE
IDLE_TIMEOUT_MS = 12_000


def partition_options(engine: str, exclusions: dict[str, list[str]]) -> JsonObject:
    try:
        engine_ip = ipaddress.IPv4Address(engine)
        collisions = [
            key
            for key, values in exclusions.items()
            if engine_ip in map(ipaddress.ip_address, values)
        ]
    except ValueError:
        raise SmokeFailure("F1B_POSTGRES_TARGET_ADDRESS_INVALID") from None
    expect("F1B_POSTGRES_TARGET_COLLISION", collisions, [])
    return {"externalTargets": [str(engine_ip)]}


def durable_hold(raw: bytes, arm: JsonObject, issued_ns: int) -> JsonObject:
    reach = acknowledged(raw, arm)
    held = one(
        "F1B_ACK_HELD",
        by([json.loads(line) for line in raw.splitlines()], arm=arm["arm"], event="held"),
    )
    expect("F1B_ACK_BEFORE_INJECTION", held["at_ns"] < issued_ns, True)
    return reach


def require_hold(cell: str, reach: JsonObject, target: Target, cut: JsonObject) -> JsonObject:
    identity = {
        "actor": "scheduler",
        "uid": target.uid,
        "process_start_id": target.process,
        "session": target.session,
    }
    expect("F1B_HOLD_IDENTITY", reach["target"], identity)
    expect("F1B_HOLD_BOUNDARY", reach["boundary"], BOUNDARIES[cell])
    fields = reach["fields"]
    objects = fields["objects"]
    workflow = cut["workflow"]
    lease = one("F1B_LEASE_ROW", cut["leases"])
    expect(
        "F1B_HOLD_WORKFLOW",
        (fields["key"], int(objects["revision"])),
        (workflow["workflow_id"], workflow["revision"]),
    )
    expect(
        "F1B_HOLD_LEASE",
        (objects["cell"], int(objects["partition"]), objects["owner"], int(objects["epoch"]), True),
        (cell, lease["partition_id"], lease["owner_id"], lease["epoch"], lease["live"]),
    )
    expect("F1B_PREHOLD_ATTEMPTS", cut["attempts"], [])
    if cell == "medium-2":
        expect("F1B_SERVER_IDLE_BOUND", int(objects["idle_timeout_ms"]), IDLE_TIMEOUT_MS)
        expect("F1B_LOCKED_DB_TIME", int(objects["db_time_us"]) < int(objects["expires_us"]), True)
        expect(
            "F1B_EXPIRY_AT_HOLD",
            int(datetime.fromisoformat(lease["lease_expires_at"]).timestamp() * 10**6),
            int(objects["expires_us"]),
        )
    else:
        expect("F1B_PENDING_ATTEMPT", (objects["node"], objects["iteration"]), ("pure.echo", "0"))
    audit = one(
        "F1B_ORIGINAL_ACQUISITION",
        by(cut["audit"], kind="acquire", owner_id=lease["owner_id"], epoch=lease["epoch"]),
    )
    expect(
        "F1B_ORIGINAL_ACQUISITION_PROCESS",
        (audit["session_id"], audit["pod_uid"], audit["container_id"]),
        (str(UUID(target.session)), target.uid, target.uid),
    )
    return audit


def require_lock(cell: str, reach: JsonObject, sample: JsonObject) -> None:
    fields = reach["fields"]["objects"]
    locked = cell == "medium-2"
    expect("F1B_NOWAIT_SQLSTATE", sample["sqlstate"], "55P03" if locked else "00000")
    expect("F1B_NOWAIT_ROLLBACK", sample["rollback"], "00000")
    expect("F1B_MONITOR_READ_ONLY", sample["read_only"], "on")
    backend = sample["backend"]
    expect("F1B_HOLD_BACKEND_PRESENT", backend is not None, True)
    expect(
        "F1B_HOLD_BACKEND_LIFETIME",
        (backend["pid"], backend["start_us"]),
        (int(fields["backend_pid"]), int(fields["backend_start_us"])),
    )
    expect(
        "F1B_HOLD_TRANSACTION",
        (backend["state"], backend["in_transaction"], backend["xid"]),
        ("idle in transaction", True, fields["transaction"]) if locked else ("idle", False, None),
    )
    application = TARGET_APP if locked else "dur053-e2-" + str(UUID(reach["target"]["session"]))
    expect("F1B_HOLD_BACKEND_APPLICATION", backend["application"], application)
    if not locked:
        expect("F1B_NOWAIT_OLD_EPOCH", sample["probe_epoch"], int(fields["epoch"]))


def clear_candidate(sample: JsonObject, old_epoch: int) -> bool:
    return (
        sample["backend"] is None
        and sample["sqlstate"] == "00000"
        and sample["rollback"] == "00000"
        and sample["probe_epoch"] == old_epoch
    )


def peer_takeover(
    cell: str,
    old: JsonObject,
    before: JsonObject,
    after: JsonObject,
    peer: Target,
    clear: JsonObject | None,
) -> tuple[JsonObject, JsonObject]:
    old_lease = one("F1B_OLD_LEASE_ROW", before["leases"])
    successor = min(
        (row for row in after["audit"] if row["kind"] == "acquire" and row["epoch"] > old["epoch"]),
        key=lambda row: row["event_order"],
    )
    expect(
        "F1B_PEER_ACQUISITION",
        (
            successor["epoch"],
            successor["pod_uid"],
            successor["container_id"],
            successor["owner_id"] != old["owner_id"],
        ),
        (old["epoch"] + 1, peer.uid, peer.uid, True),
    )
    expect("F1B_PEER_SESSION", successor["session_id"], str(UUID(peer.session)))
    write = lease_commit(after, successor)
    expect(
        "F1B_TAKEOVER_AFTER_EXPIRY",
        datetime.fromisoformat(old_lease["lease_expires_at"])
        <= datetime.fromisoformat(write["row_identity"]["updated_at"])
        <= datetime.fromisoformat(successor["finalized_at"]),
        True,
    )
    if cell == "medium-2":
        expect(
            "F1B_TAKEOVER_REQUIRES_LOCK_CLEAR",
            clear is not None and clear_candidate(clear, old["epoch"]),
            True,
        )
        expect(
            "F1B_CLEAR_BEFORE_TAKEOVER",
            clear is not None
            and datetime.fromisoformat(clear["db_at"])
            <= datetime.fromisoformat(write["row_identity"]["updated_at"]),
            True,
        )
    reject_stale_writes(after, write, old, successor_held=True)
    return successor, write


def peer_progress(cut: JsonObject, revision: int, peer: Target, takeover: JsonObject) -> JsonObject:
    advance = one(
        "F1B_PEER_ATTEMPT_CREATED",
        [
            row
            for row in cut["history"]
            if row["revision"] > revision and row["reason"] == "ATTEMPT_CREATED"
        ],
    )
    audit = one(
        "F1B_PEER_PROGRESS_HISTORY",
        by(cut["audit"], kind="history", transition_id=advance["transition_id"]),
    )
    expect(
        "F1B_PEER_PROGRESS_OWNER",
        (audit["pod_uid"], audit["session_id"], audit["owner_id"]),
        (peer.uid, str(UUID(peer.session)), takeover["owner_id"]),
    )
    expect(
        "F1B_PEER_PROGRESS_ORDER",
        audit["event_order"] > takeover["event_order"] and audit["epoch"] >= takeover["epoch"],
        True,
    )
    acquisition = one(
        "F1B_PEER_PROGRESS_ACQUISITION",
        by(
            cut["audit"],
            kind="acquire",
            epoch=audit["epoch"],
            owner_id=audit["owner_id"],
            session_id=audit["session_id"],
        ),
    )
    expect(
        "F1B_PEER_PROGRESS_EPOCH",
        advance["scheduler_epoch"] == audit["epoch"]
        and acquisition["event_order"] < audit["event_order"],
        True,
    )
    return advance


def verify_rule(spec: JsonObject, source: str, engine: str) -> tuple[str, str, str, str]:
    rule = one("F1B_SINGLE_DROP_RULE", by(spec.get("iptables", []), source=source))
    expect("F1B_EGRESS_ONLY", rule["direction"], "output")
    sets = by(spec.get("ipsets", []), source=source)
    expect("F1B_DROP_IPSET_COUNT", len(sets), 3)
    outer = one("F1B_DROP_IPSET_LIST", by(sets, ipsetType="list:set"))
    net = one("F1B_DROP_IPSET_NETWORK", by(sets, ipsetType="hash:net"))
    ports = one("F1B_DROP_IPSET_PORTS", by(sets, ipsetType="hash:net,port"))
    expect("F1B_DROP_IPSET_BINDING", rule["ipsets"], [outer["name"]])
    expect(
        "F1B_DROP_IPSET_MEMBERS", sorted(outer["setNames"]), sorted([net["name"], ports["name"]])
    )
    expect("F1B_DROP_IPSET_PORTS_EMPTY", ports.get("cidrAndPorts", []), [])
    expect(
        "F1B_POSTGRES_DROP_SCOPE",
        [str(ipaddress.ip_network(address)) for address in net["cidrs"]],
        [f"{engine}/32"],
    )
    return rule["name"], outer["name"], net["name"], ports["name"]


def verify_kernel_rule(
    rules: str, sets: str, chain: str, ipset: str, net: str, ports: str, engine: str
) -> None:
    drops = [
        shlex.split(line)
        for line in rules.splitlines()
        if f" -A {chain} " in line and line.endswith(" -j DROP")
    ]
    expect("F1B_KERNEL_DROP_RULE", len(drops), 1)
    drop = drops[0]
    index = drop.index("--match-set") if "--match-set" in drop else len(drop)
    expect("F1B_KERNEL_MATCH_DESTINATION", drop[index + 1 : index + 3], [ipset, "dst,dst"])

    def members(name: str) -> list[str]:
        return [line.split()[2] for line in sets.splitlines() if line.startswith(f"add {name} ")]

    expect("F1B_KERNEL_DROP_SET_MEMBERS", sorted(members(ipset)), sorted([net, ports]))
    expect("F1B_KERNEL_DROP_SET_PORTS_EMPTY", members(ports), [])
    expect(
        "F1B_KERNEL_POSTGRES_DROP_SCOPE",
        [str(ipaddress.ip_network(value)) for value in members(net)],
        [f"{engine}/32"],
    )


def process_rows(rows: list[JsonObject], target: Target, reach: JsonObject) -> list[JsonObject]:
    selected = by(rows, workflow=reach["fields"]["key"], invocation=reach["fields"]["invocation"])
    for row in selected:
        expect(
            "F1B_SCHEDULER_CALL_SOURCE",
            (row["pod_uid"], row["process_start_id"], row["pid"], row["session"]),
            (
                target.uid,
                target.process,
                int(target.process.split(":")[0]),
                str(UUID(target.session)),
            ),
        )
    return selected


def restored(absent_ns: int, observation: dict[str, Any], target: Target) -> None:
    prove_restoration(
        absent_ns,
        observation,
        {"outcome": "connected", "pod_uid": target.uid, "process_start_id": target.process},
    )
    expect(
        "F1B_RESTORE_NEW_PROBE_AFTER_ABSENCE",
        observation["started_ns"] > absent_ns and observation["backend_pid"] > 0,
        True,
    )


def fenced_calls(
    cell: str, rows: list[JsonObject], reach: JsonObject, released_ns: int
) -> list[JsonObject]:
    calls = by(rows, event="fenced_call")
    expected = (
        [("RenewLease", "SESSION_LOST"), ("ReleaseLease", "ErrLeaseNotOwned")]
        if cell == "medium-2"
        else [("CreateAttempt", "ErrLeaseNotOwned")]
    )
    expect("F1B_STALE_CALL_RESULT", [(row["operation"], row["outcome"]) for row in calls], expected)
    expect(
        "F1B_STALE_CALL_BOUNDARY",
        all(row["objects"] == reach["fields"]["objects"] for row in calls),
        True,
    )
    times = [released_ns, *(row["at_ns"] for row in calls)]
    expect("F1B_STALE_CALL_ORDER", all(a < b for a, b in pairwise(times)), True)
    return calls
