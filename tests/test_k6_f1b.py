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
from dur053_k6_f1b_smoke import F1bEvidence, F1bSmoke  # noqa: E402
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


@pytest.fixture
def f1b_smoke(tmp_path):
    # Exercise the real F1b methods and evidence writes without starting kind.
    smoke = object.__new__(F1bSmoke)
    smoke.cell_id = "medium-2"
    smoke.evidence = F1bEvidence(tmp_path)
    smoke.monitor_samples = []
    return smoke


def test_scheduler_rows_parse_timestamped_observations(f1b_smoke, tmp_path, monkeypatch):
    # The probe is from run 44678222396. The call uses the same hosted identity
    # and hold fields with the fenced_call shape emitted after release.
    raw = (ROOT / "tests/fixtures/k6_f1b_scheduler_log.txt").read_bytes()
    # The exact prefix must occur at the start of the application message.
    raw += b"2026-11-20T05:13:26.000000001Z ignored DUR053_K6_OBSERVATION f1b bad JSON\n"
    target = replace(
        TARGET,
        pod="scheduler-bcd7ddf69-ggg4w",
        uid="c62b26eb-b491-4960-bf68-9ae6a2bd283a",
        process="1:111146",
        session="52b8b7a2cc344f08b08d5f6ebb2a476b",
    )
    reach = {"fields": {"key": "e1-medium-2", "invocation": "a726d3f9-5dc6-41b9-a277-c2b53cd11de2"}}
    monkeypatch.setattr(f1b_smoke, "role_log", lambda pod, container: raw)
    rows = f1b_smoke.scheduler_rows(target, reach)
    assert [(row["event"], row["outcome"]) for row in rows] == [
        ("pool_probe", "unreachable"),
        ("fenced_call", "SESSION_LOST"),
    ], "F1B_TIMESTAMPED_SCHEDULER_OBSERVATIONS_MISSING"
    assert rows[0]["started_ns"] == 1795151585085112604
    assert rows[1]["operation"] == "RenewLease"
    assert rows[1]["objects"]["transaction"] == "1805"
    assert (tmp_path / "medium-2-scheduler-observations.log").read_bytes() == raw
    assert json.loads((tmp_path / "medium-2-scheduler-observations.json").read_bytes()) == rows


@pytest.mark.parametrize("payload", [b'{"event":', b"[]", b'{"event":"\xff"}'])
def test_scheduler_rows_reject_malformed_observation(f1b_smoke, tmp_path, monkeypatch, payload):
    raw = b"2026-11-20T05:13:05.785902401Z DUR053_K6_OBSERVATION f1b " + payload + b"\n"
    monkeypatch.setattr(f1b_smoke, "role_log", lambda pod, container: raw)
    with pytest.raises(SmokeFailure, match=r"^F1B_SCHEDULER_OBSERVATION_INVALID line=1$"):
        f1b_smoke.scheduler_rows(TARGET, {})
    assert (tmp_path / "medium-2-scheduler-observations.log").read_bytes() == raw


@pytest.mark.parametrize("gone_at", ["while-injected", "after-network-confirmation", None])
def test_during_injection_records_idle_bound_race(f1b_smoke, tmp_path, monkeypatch, gone_at):
    reach, monitor = lock_fixture("medium-2")
    reach["hold_ack_ns"] = ack_ns = 1795151583337513227
    active_ns = 1795151584384985513
    gone_ns = 1795151595763796416
    confirmed_ns = active_ns + 900_000_000
    arm = {"arm": "test-arm"}

    def sample(_reach, stage):
        if stage == gone_at:
            return {
                **monitor,
                "at_ns": gone_ns,
                "backend": None,
                "sqlstate": "00000",
                "probe_epoch": OLD["epoch"],
                "peer_waiters": [],
            }
        return {**monitor, "at_ns": confirmed_ns, "peer_waiters": [938]}

    monkeypatch.setattr(f1b_smoke, "monitor", sample)
    monkeypatch.setattr(f1b_smoke, "held", lambda: arm)
    monkeypatch.setattr(f1b_smoke, "counters", lambda *_: {"packets": 43, "bytes": 2606})
    monkeypatch.setattr(f1b_smoke, "reobserve", lambda _: TARGET)
    monkeypatch.setattr(
        f1b_smoke,
        "scheduler_rows",
        lambda *_: [{"event": "pool_probe", "outcome": "unreachable", "started_ns": active_ns + 1}],
    )
    poll = {**arm, "event": "wait", "at_ns": active_ns + 1}
    monkeypatch.setattr(f1b_smoke, "journal_bytes", lambda: json.dumps(poll).encode())

    baseline = {"packets": 2, "bytes": 125}
    if gone_at is None:
        result = f1b_smoke.confirm_partition(
            TARGET, reach, arm, active_ns, "out", baseline, time.monotonic() + 5
        )
        assert result["hold_ack_to_confirmation_ns"] == confirmed_ns - ack_ns
        missing = []
    else:
        with pytest.raises(SmokeFailure, match=r"^F1B_IDLE_BOUND_BEFORE_CONFIRMATION ") as failure:
            f1b_smoke.confirm_partition(
                TARGET, reach, arm, active_ns, "out", baseline, time.monotonic() + 5
            )
        missing = ["live_held_backend"]
        if gone_at == "while-injected":
            missing = [
                "drop_packet_rise",
                "drop_byte_rise",
                "failed_pool_probe",
                "authenticated_hook_poll",
                "peer_waiter",
                *missing,
            ]
        assert str(failure.value) == (
            f"F1B_IDLE_BOUND_BEFORE_CONFIRMATION hold_ack_ns={ack_ns} active_ns={active_ns} "
            f"idle_timeout_ms=12000 observed_ns={gone_ns} missing={','.join(missing)}"
        )
    evidence = json.loads((tmp_path / "medium-2-during-injection.json").read_bytes())
    assert (evidence["hold_ack_ns"], evidence["active_ns"], evidence["idle_timeout_ms"]) == (
        ack_ns,
        active_ns,
        12000,
    )
    assert evidence["missing"] == missing
    if gone_at is None:
        assert evidence["hold_ack_to_confirmation_ns"] == confirmed_ns - ack_ns
    else:
        assert evidence["monitor"]["backend"] is None


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
        held = durable_hold(p.read_bytes(), arm, held_ns + 1)
        assert held["target"] == IDENTITY
        assert held["hold_ack_ns"] == held_ns
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
