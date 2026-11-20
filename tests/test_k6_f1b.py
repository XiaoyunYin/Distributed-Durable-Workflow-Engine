"""Offline confirmation gates against hosted F1a row shapes and F1b controls."""

from __future__ import annotations

import json
import sys
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from dur053_k6_b_evidence import SmokeFailure  # noqa: E402
from dur053_k6_chaos import Target, render_chaos  # noqa: E402
from dur053_k6_e1_join import EVENTS, SCHEDULERS, TASKS, by  # noqa: E402
from dur053_k6_e2_join import reject_stale_writes  # noqa: E402
from dur053_k6_f1a_inputs import RELEASE_MARKER, chain_counters, claim_rows  # noqa: E402
from dur053_k6_f1a_smoke import READ_SEALS, RELEASE_ACTIVITY  # noqa: E402
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
REJECTION_FIXTURE = ROOT / "tests/fixtures/k6_f1b_rejection_hosted.json"
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


@pytest.fixture(params=CELLS)
def hosted_rejection(request, f1b_smoke):
    # Both rejected cuts and takeover joins come from hosted run 44685492351.
    fixture = json.loads(REJECTION_FIXTURE.read_bytes())
    episode = fixture[request.param]
    workflow = episode["rejected"]["workflow"]
    f1b_smoke.cell_id = request.param
    f1b_smoke.workflow = workflow["workflow_id"]
    f1b_smoke.partition = workflow["partition_id"]
    f1b_smoke.namespace = workflow["namespace"]
    return episode, fixture["completion_template"]


def test_hosted_reacquisition_allows_original_session(hosted_rejection, f1b_smoke, monkeypatch):
    episode, _ = hosted_rejection
    rejected = episode["rejected"]
    old = rejected["audit"][0]
    monkeypatch.setattr(f1b_smoke, "pg", lambda _sql: json.dumps(rejected))

    cut = f1b_smoke.fence_cut("after-original-rejection")
    reject_stale_writes(cut, episode["takeover"]["write"], old, successor_held=False)
    assert cut["acquisitions"] == by(rejected["audit"], kind="acquire")
    expected = [1, 9, 11] if f1b_smoke.cell_id == "medium-2" else [1, 9, 10]
    assert [r["epoch"] for r in by(cut["acquisitions"], session_id=old["session_id"])] == expected


@pytest.mark.parametrize("defect", ["old-epoch", "missing-acquisition", "late-acquisition"])
def test_hosted_rejection_rejects_stale_write(hosted_rejection, f1b_smoke, monkeypatch, defect):
    episode, _ = hosted_rejection
    cut = episode["rejected"]
    old = cut["audit"][0]
    takeover = episode["takeover"]["write"]
    acquire = next(r for r in cut["audit"] if r["kind"] == "acquire" and r["epoch"] == 9)
    committed = by(cut["writes"], relation="partition_leases", xid=acquire["transaction_id"])[0]
    followup = by(cut["writes"], commit_order=committed["commit_order"] + 1)[0]
    if defect == "old-epoch":
        followup["epoch"] = old["epoch"]
    elif defect == "missing-acquisition":
        cut["writes"].remove(committed)  # Keep the matching audit row without its durable write.
    else:
        committed["commit_order"], followup["commit_order"] = (
            followup["commit_order"],
            committed["commit_order"],
        )
        cut["writes"].sort(key=lambda row: row["commit_order"])
    monkeypatch.setattr(f1b_smoke, "pg", lambda _sql: json.dumps(cut))

    with pytest.raises(SmokeFailure, match=r"^E2_I3_STALE_MUTATION "):
        reject_stale_writes(
            f1b_smoke.fence_cut("after-original-rejection"), takeover, old, successor_held=False
        )


def complete_hosted_cut(rejected, template, finished_at):
    # The F1b run stopped with a claimed attempt. Extend it with the observed F1a
    # terminal row shapes, keeping F1b's submission, winning attempt and claim.
    workflow = rejected["workflow"]
    attempt = rejected["attempts"][0]
    acquisition = rejected["audit"][-1]
    history = [
        {
            **row,
            "workflow_id": workflow["workflow_id"],
            "created_at": finished_at,
            "actor_id": attempt["worker_id"] if row["actor_kind"] == "worker" else row["actor_id"],
            "scheduler_epoch": acquisition["epoch"] if row["scheduler_epoch"] else None,
        }
        for row in template["history"]
    ]
    events = [
        {
            **row,
            "workflow_id": workflow["workflow_id"],
            "traceparent": workflow["traceparent"],
            "published_at": finished_at,
        }
        for row in template["outbox"]
    ]
    return {
        **rejected,
        "workflow": {**workflow, "state": "SUCCEEDED", "revision": history[-1]["revision"]},
        "attempts": [{**attempt, "state": "SUCCEEDED", "is_current": False}],
        "history": rejected["history"] + history,
        "outbox": rejected["outbox"] + events,
        "publications": rejected["publications"]
        + [{**row, "published_at": finished_at} for row in template["publications"]],
    }


def complete_hosted_witness(rejected, template, released_ns):
    attempt = rejected["attempts"][0]
    workflow = attempt["workflow_id"]
    _, _, namespace, account, _, pod, _, uid = attempt["worker_id"].split(":")
    request = attempt["worker_request_id"]
    session = template["invocation"]["session"]
    delta_ns = released_ns - template["release"]["at_ns"]
    streams = {"trace": [], "invocations": []}
    for value in template.values():
        row = dict(value)
        stream = row.pop("stream")
        row.update(pod_uid=uid, session=session)
        if stream == "invocations":
            row["identity"] = {
                **row["identity"],
                "namespace": namespace,
                "workflow": workflow,
                "token_hash": attempt["token_hash"],
            }
        else:
            row.update(workflow=workflow, container_id=uid, token_hash=attempt["token_hash"])
            if row["event"] == "f1a_http":
                row["path"] = (
                    f"/v1/workflows/{workflow}/nodes/pure.echo/iterations/0/{row['operation']}"
                )
            else:
                row.update(request=request, worker=request.split(":", 1)[0])
            for key in ("at_ns", "sent_ns", "gate_started_ns"):
                if key in row:
                    row[key] += delta_ns
        streams[stream].append(json.dumps(row))
    seal = {
        "inventory": {"complete": True, "errors": 0, "pod_uid": uid, "session": session},
        "streams": {key: {"data": "\n".join(rows) + "\n"} for key, rows in streams.items()},
    }
    return seal, {
        "metadata": {"namespace": namespace, "name": pod, "uid": uid},
        "spec": {"serviceAccountName": account},
    }


def test_hosted_post_rejection_tail(hosted_rejection, f1b_smoke, tmp_path, monkeypatch):
    episode, template = hosted_rejection
    rejected, takeover, calls = (episode[key] for key in ("rejected", "takeover", "fenced_calls"))
    old = rejected["audit"][0]
    cell = f1b_smoke.cell_id
    workflow = f1b_smoke.workflow
    coverage = episode["coverage"]
    released_ns = rejected["sample_observed_ns"] + 10**7
    finished_at = datetime.fromtimestamp((released_ns + 10**8) / 10**9, UTC).isoformat()
    finished = complete_hosted_cut(rejected, template, finished_at)
    seal, pod = complete_hosted_witness(rejected, template["witness"], released_ns)
    inbox = [
        {
            **by(rejected["inbox"], consumer_id=SCHEDULERS)[0],
            "event_id": row["event_id"],
            "kafka_offset": index,
            "recorded_at": finished_at,
        }
        for index, row in enumerate(template["outbox"], start=1)
    ]
    reconciled = {**finished, "inbox": finished["inbox"] + inbox}
    samples = iter(
        [
            (True, rejected),
            (False, finished),  # F1a finish can see the winner before transport reconciles.
            (False, finished),
            (False, reconciled),
            (True, reconciled),
        ]
    )

    def pg(sql):
        writes, cut = next(samples)
        assert ("FROM k6_e2.writes" in sql) is writes
        return json.dumps(
            {
                key: value
                for key, value in cut.items()
                if writes or key not in {"writes", "read_only"}
            }
        )

    def exec_python(stage, deployment, script, *args):
        assert deployment == "deployment/observer"
        if stage == "RELEASE":
            assert (script, args) == (RELEASE_ACTIVITY, (RELEASE_MARKER, ""))
            return {"released": True, "marker": RELEASE_MARKER}
        assert (stage, script, args) == ("WITNESS", READ_SEALS, ())
        return [seal]

    monkeypatch.setattr(time, "time_ns", lambda: released_ns - 10**6)
    monkeypatch.setattr(f1b_smoke, "pg", pg)
    monkeypatch.setattr(f1b_smoke, "exec_python", exec_python)
    monkeypatch.setattr(f1b_smoke, "pods", lambda _role: [pod])
    monkeypatch.setattr(f1b_smoke, "kafka", lambda topic: {"topic": topic})
    inputs = {
        "workflow": workflow,
        "body_hash": rejected["workflow"]["submission_payload_hash"],
        "registered": coverage["invariants_covered"],
        "not_covered": coverage["invariants_not_covered"],
        "hold": {"fields": {"objects": calls[0]["objects"], "invocation": calls[0]["invocation"]}},
        "peer_acquisition": takeover["acquisition"],
        "peer_progress": takeover["progress"],
        "fenced_calls": calls,
    }
    f1b_smoke.reconcile_rejection(inputs, old, takeover["write"])

    def evidence(label):
        return json.loads((tmp_path / f"{cell}-{label}.json").read_bytes())

    checker = evidence("checker-inputs")
    assert checker["engine_cut"] == evidence("post-rejection-final-writes")
    assert (
        checker["engine_cut"]["acquisitions"]
        == evidence("after-original-rejection")["acquisitions"]
    )
    assert checker["engine_cut"]["inbox"] == reconciled["inbox"]
    assert checker["winning_claim"] == rejected["claims"][0]
    assert checker["witness"] == [seal]
    assert set(checker["predicates"]) == set(coverage["invariants_covered"])
    assert checker["predicates"]["I3"] == {
        "original_acquire_order": old["event_order"],
        "peer_acquire_order": takeover["acquisition"]["event_order"],
        "peer_write_commit_order": takeover["write"]["commit_order"],
        "original_stale_invocation": calls[0]["invocation"],
    }
    assert all(checker[key] == value for key, value in inputs.items())
    assert evidence("kafka-final-" + EVENTS) == {"topic": EVENTS}
    assert evidence("kafka-final-" + TASKS) == {"topic": TASKS}

    # Inject a stale write seen only after reconciliation to exercise the second join.
    writes = reconciled["writes"]
    original_write = by(writes, xid=old["transaction_id"], relation="partition_leases")[0]
    stale = {**original_write, "xid": "4000", "commit_order": writes[-1]["commit_order"] + 1}
    contaminated = {**reconciled, "writes": [*writes, stale]}
    samples = iter([(True, rejected), (False, finished), (False, reconciled), (True, contaminated)])
    with pytest.raises(SmokeFailure, match=r"^E2_I3_STALE_MUTATION "):
        f1b_smoke.reconcile_rejection(inputs, old, takeover["write"])

    if cell == "medium-2":
        # Replace the pre-takeover write witness with the held XID to isolate the rollback check.
        renewed = {**original_write, "xid": calls[0]["objects"]["transaction"]}
        with_renewal = {
            **reconciled,
            "writes": [renewed if row == original_write else row for row in writes],
        }
        samples = iter(
            [(True, rejected), (False, finished), (False, reconciled), (True, with_renewal)]
        )
        with pytest.raises(SmokeFailure, match=r"^F1B_HELD_RENEWAL_NEVER_COMMITTED "):
            f1b_smoke.reconcile_rejection(inputs, old, takeover["write"])


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
