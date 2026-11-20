"""Offline confirmation gates against hosted F1a row shapes and F1b controls."""

from __future__ import annotations

import json
import sys
import time
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from dur053_k6_b_evidence import SmokeFailure  # noqa: E402
from dur053_k6_chaos import Target, render_chaos  # noqa: E402
from dur053_k6_f1a_inputs import chain_counters, claim_rows  # noqa: E402
from dur053_k6_f1b_inputs import (  # noqa: E402
    BOUNDARIES,
    CELLS,
    TARGET_APP,
    durable_hold,
    fenced_calls,
    partition_options,
    peer_progress,
    peer_takeover,
    process_rows,
    require_hold,
    require_lock,
    restored,
    verify_kernel_rule,
    verify_rule,
)
from dur053_k6_hook import Receiver, encode  # noqa: E402
from dur053_k6_registry import read_registry  # noqa: E402

ENGINE = "172.19.0.5"
HOSTED = json.loads((ROOT / "tests/fixtures/k6_f1a_hosted.json").read_bytes())["medium-1"]
TARGET = Target(**{**HOSTED["target"], "session": UUID(HOSTED["target"]["session"]).hex})
IDENTITY = {
    "actor": "scheduler",
    "uid": TARGET.uid,
    "process_start_id": TARGET.process,
    "session": TARGET.session,
}
WORKFLOW = HOSTED["workflow"]["workflow_id"]
OLD = HOSTED["before"]["audit"][0]


def reject(action, label, survived) -> None:
    try:
        action()
    except SmokeFailure as error:
        assert str(error).startswith(label), str(error)
    else:
        pytest.fail(survived)


def lock_fixture(cell):
    fields = {
        "cell": cell,
        "backend_pid": "501",
        "backend_start_us": "6",
        "partition": str(OLD["partition_id"]),
        "owner": OLD["owner_id"],
        "epoch": str(OLD["epoch"]),
        "revision": "1",
    }
    locked = cell == "medium-2"
    if locked:
        fields.update(transaction="13000", idle_timeout_ms="12000", db_time_us="1", expires_us="2")
    else:
        fields.update(node="pure.echo", iteration="0")
    reach = {
        "target": IDENTITY,
        "boundary": BOUNDARIES[cell],
        "fields": {"key": WORKFLOW, "invocation": "call-1", "objects": fields},
    }
    backend = {
        "pid": 501,
        "start_us": 6,
        "application": TARGET_APP if locked else "dur053-e2-" + str(UUID(TARGET.session)),
        "state": "idle in transaction" if locked else "idle",
        "in_transaction": locked,
        "xid": fields.get("transaction"),
    }
    return reach, {
        "sqlstate": "55P03" if locked else "00000",
        "probe_epoch": -1 if locked else OLD["epoch"],
        "rollback": "00000",
        "read_only": "on",
        "backend": backend,
    }


@pytest.mark.parametrize("cell", CELLS)
def test_exact_postgres_partition(cell):
    row = next(row for row in read_registry()["episodes"] if row["cell"] == cell)
    rendered = render_chaos(row, TARGET, "trial", options=partition_options(ENGINE, {}))
    assert rendered["kind"] == "NetworkChaos"
    assert rendered["spec"] == {
        "selector": {"pods": {TARGET.namespace: [TARGET.pod]}},
        "mode": "one",
        "action": "partition",
        "direction": "to",
        "duration": "60s",
        "externalTargets": [ENGINE],
    }, "F1B_PARTITION_SPEC_DRIFT"


@pytest.mark.parametrize("route", ["broker", "receiver_gateway", "kubernetes_api", "service_ips"])
def test_postgres_target_collision(route):
    reject(
        lambda: partition_options(ENGINE, {route: [ENGINE]}),
        "F1B_POSTGRES_TARGET_COLLISION",
        "F1B_COLLIDING_PARTITION_TARGET_ACCEPTED",
    )


def test_missing_and_late_durable_ack(tmp_path):
    p = tmp_path / "journal"
    token = "b" * 64
    receiver = Receiver(p, {"scheduler": token}, record_waits=True)
    with receiver.file:

        def submit(message):
            return receiver.event(encode({"identity": IDENTITY, **message}), "Bearer " + token)

        submit({"event": "register"})
        cell = "medium-2"
        boundary = BOUNDARIES[cell]
        receiver.arm(cell, boundary, IDENTITY, {"key": WORKFLOW}, 60)
        message = {
            "event": "boundary_reached",
            "boundary": boundary,
            "fields": {"key": WORKFLOW},
            "hook_id": "1" * 32,
            "nonce": "2" * 32,
        }
        response = submit(message)
        assert response["command"] == "ack"
        before_held = p.read_bytes()
        arm = receiver.observe(cell)
        reject(
            lambda: durable_hold(before_held, arm, time.time_ns()),
            "E2_DURABLE_ACK",
            "F1B_MISSING_ACK_ACCEPTED",
        )
        poll = {
            **message,
            "event": "held",
            **{k: response[k] for k in ("arm", "sequence", "sha256", "journal_sha256")},
        }
        assert submit(poll)["command"] == "wait"
        assert submit(poll)["command"] == "wait"
        assert receiver.rows[-1]["event"] == "wait"
        arm = receiver.observe(cell)
        held_ns = receiver.rows[-2]["at_ns"]
        assert durable_hold(p.read_bytes(), arm, held_ns + 1)["target"] == IDENTITY
        reject(
            lambda: durable_hold(p.read_bytes(), arm, held_ns),
            "F1B_ACK_BEFORE_INJECTION",
            "F1B_LATE_ACK_ACCEPTED",
        )


def test_medium2_requires_nowait_lock():
    reach, monitor = lock_fixture("medium-2")
    require_lock("medium-2", reach, monitor)
    reject(
        lambda: require_lock("medium-2", reach, {**monitor, "sqlstate": "00000"}),
        "F1B_NOWAIT_SQLSTATE",
        "F1B_MISSING_NOWAIT_55P03_ACCEPTED",
    )


def test_medium3_requires_idle_backend():
    reach, monitor = lock_fixture("medium-3")
    require_lock("medium-3", reach, monitor)
    monitor["backend"].update(state="idle in transaction", in_transaction=True)
    reject(
        lambda: require_lock("medium-3", reach, monitor),
        "F1B_HOLD_TRANSACTION",
        "F1B_MEDIUM3_OPEN_TRANSACTION_ACCEPTED",
    )


def test_takeover_requires_committed_peer_after_lock_clear():
    # Takeover fields are an F1b augmentation of the hosted K4 audit row schema.
    peer = replace(TARGET, uid="peer-uid", session="c" * 32)
    expires = datetime.fromisoformat(OLD["at"]) + timedelta(seconds=15)
    updated = expires + timedelta(seconds=1)
    acquire = {
        **OLD,
        "event_order": OLD["event_order"] + 1,
        "epoch": OLD["epoch"] + 1,
        "owner_id": "12345678-aabb-ccdd-eeff-123456789abc",
        "transaction_id": "13001",
        "pod_uid": peer.uid,
        "container_id": peer.uid,
        "session_id": str(UUID(peer.session)),
        "finalized_at": (updated + timedelta(milliseconds=1)).isoformat(),
    }
    write = {
        "relation": "partition_leases",
        "commit_order": 10,
        "xid": acquire["transaction_id"],
        "owner_id": acquire["owner_id"],
        "epoch": acquire["epoch"],
        "session_id": acquire["session_id"],
        "row_identity": {"updated_at": updated.isoformat()},
    }
    before = {"leases": [{"lease_expires_at": expires.isoformat()}]}
    clear = {
        "backend": None,
        "sqlstate": "00000",
        "rollback": "00000",
        "probe_epoch": OLD["epoch"],
        "db_at": (expires - timedelta(seconds=1)).isoformat(),
    }
    after = {"audit": [OLD, acquire], "writes": [write]}
    assert peer_takeover("medium-2", OLD, before, after, peer, clear) == (acquire, write)
    reject(
        lambda: peer_takeover(
            "medium-2", OLD, before, after, peer, {**clear, "backend": {"pid": 501}}
        ),
        "F1B_TAKEOVER_REQUIRES_LOCK_CLEAR",
        "F1B_TAKEOVER_WITHOUT_LOCK_CLEAR_ACCEPTED",
    )
    reject(
        lambda: peer_takeover(
            "medium-2", OLD, before, after, peer, {**clear, "db_at": acquire["finalized_at"]}
        ),
        "F1B_CLEAR_BEFORE_TAKEOVER",
        "F1B_LATE_LOCK_CLEAR_ACCEPTED",
    )
    history = {**HOSTED["history"], "scheduler_epoch": acquire["epoch"]}
    audit = {
        **acquire,
        "event_order": acquire["event_order"] + 1,
        "kind": "history",
        "transition_id": history["transition_id"],
    }
    assert (
        peer_progress({"history": [history], "audit": [OLD, acquire, audit]}, 2, peer, acquire)
        == history
    )
    assert claim_rows(
        {"claims": [HOSTED["claim"]]}, HOSTED["attempt"], HOSTED["attempt"]["worker_id"]
    ) == [HOSTED["claim"]]
    assert HOSTED["attempt"]["worker_id"].startswith("system:serviceaccount:")


def test_deployed_stale_call_is_typed_and_same_process():
    reach, _ = lock_fixture("medium-3")
    row = {
        "event": "fenced_call",
        "pid": 1,
        "process_start_id": TARGET.process,
        "pod_uid": TARGET.uid,
        "session": str(UUID(TARGET.session)),
        "workflow": WORKFLOW,
        "invocation": "call-1",
        "at_ns": 13,
        "objects": reach["fields"]["objects"],
        "operation": "CreateAttempt",
        "outcome": "ErrLeaseNotOwned",
    }
    assert fenced_calls("medium-3", process_rows([row], TARGET, reach), reach, 12) == [row]
    reject(
        lambda: fenced_calls("medium-3", [{**row, "outcome": "COMMITTED"}], reach, 12),
        "F1B_STALE_CALL_RESULT",
        "F1B_COMMITTED_STALE_CALL_ACCEPTED",
    )
    reject(
        lambda: process_rows([{**row, "process_start_id": "1:32"}], TARGET, reach),
        "F1B_SCHEDULER_CALL_SOURCE",
        "F1B_HARNESS_OR_RESTARTED_STALE_CALL_ACCEPTED",
    )


def test_restoration_requires_new_pool_query():
    row = {
        "event": "pool_probe",
        "outcome": "connected",
        "process_start_id": TARGET.process,
        "pod_uid": TARGET.uid,
        "started_ns": 12,
        "at_ns": 13,
        "backend_pid": 88,
    }
    restored(11, row, TARGET)
    reject(
        lambda: restored(11, {**row, "started_ns": 9}, TARGET),
        "F1B_RESTORE_NEW_PROBE_AFTER_ABSENCE",
        "F1B_PRE_REMOVAL_RECONNECT_ACCEPTED",
    )


def test_pinned_chaos_mesh_ipset_and_kernel_scope():
    source = "episode/trial"
    spec = {
        "iptables": [{"name": "out", "direction": "output", "ipsets": ["all"], "source": source}],
        "ipsets": [
            {
                "name": "all",
                "ipsetType": "list:set",
                "setNames": ["net", "ports"],
                "source": source,
            },
            {"name": "net", "ipsetType": "hash:net", "cidrs": [f"{ENGINE}/32"], "source": source},
            {"name": "ports", "ipsetType": "hash:net,port", "source": source},
        ],
    }
    chain, *sets = verify_rule(spec, source, ENGINE)
    rules = (
        "[12:1008] -A CHAOS-OUTPUT -j out\n"
        "[12:1008] -A out -m set --match-set all dst,dst -j DROP\n"
    )
    members = f"add all net\nadd all ports\nadd net {ENGINE}\n"
    verify_kernel_rule(rules, members, chain, *sets, ENGINE)
    assert chain_counters(rules, chain) == {"packets": 12, "bytes": 1008}
    reject(
        lambda: verify_kernel_rule(rules, members + "add net 172.19.0.6\n", chain, *sets, ENGINE),
        "F1B_KERNEL_POSTGRES_DROP_SCOPE",
        "F1B_EXTRA_KERNEL_DROP_TARGET_ACCEPTED",
    )


def test_lock_and_lease_confirmations_use_hosted_acquire_shape():
    reach, _ = lock_fixture("medium-2")
    expiry = "2026-11-20T01:11:08.180351+00:00"
    reach["fields"]["objects"]["expires_us"] = str(
        int(datetime.fromisoformat(expiry).timestamp() * 10**6)
    )
    cut = {
        "workflow": {"workflow_id": WORKFLOW, "revision": 1},
        "attempts": [],
        "audit": [OLD],
        "leases": [
            {
                "partition_id": OLD["partition_id"],
                "owner_id": OLD["owner_id"],
                "epoch": OLD["epoch"],
                "live": True,
                "lease_expires_at": expiry,
            }
        ],
    }
    assert require_hold("medium-2", reach, TARGET, cut) == OLD
