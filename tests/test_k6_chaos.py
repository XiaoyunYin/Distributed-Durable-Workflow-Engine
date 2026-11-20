from __future__ import annotations

import ast
import hashlib
import json
import sys
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import dur053_k6_f1a_smoke as f1a  # noqa: E402
from dur053_k6_b_evidence import SmokeFailure  # noqa: E402
from dur053_k6_chaos import (  # noqa: E402
    Chaos,
    Target,
    backend_selected,
    prove_death,
    prove_restoration,
    render_chaos,
    select_target,
)
from dur053_k6_f1a_inputs import (  # noqa: E402
    CONTROL_URL,
    RELEASE_MARKER,
    chain_counters,
    first_fenced_transition,
    partition_replacement,
    require_drop_rise,
    require_stale,
    route_rows,
    terminal_winner,
)
from dur053_k6_f1a_smoke import (  # noqa: E402
    CONTROL_PROBE,
    OBSERVER_SCRIPT,
    READ_SEALS,
    RELEASE_ACTIVITY,
    SUBMIT_TRACED,
    audit_control_mounts,
    audit_topology,
    deployment_objects,
)
from dur053_k6_registry import read_registry  # noqa: E402

TARGET = Target(
    "episode",
    "worker-a",
    "uid-a",
    "worker",
    "worker",
    "containerd://a",
    0,
    "node",
    4242,
    "1:100",
    "session-a",
)
PEERS = ["control-b", "control-a"]
RUNTIME = "localhost:5001/dur053/runtime@sha256:" + "a" * 64
WORKER = "localhost:5001/dur053/worker@sha256:" + "b" * 64
TAGGED_WORKER = "localhost:5001/dur053-kind-worker@sha256:" + "c" * 64
TAGGED_RUNTIME = "localhost:5001/dur053-k6-f1a-runtime@sha256:" + "d" * 64


def episode(cell: str) -> dict:
    return next(row for row in read_registry()["episodes"] if row["cell"] == cell)


def test_embedded_scripts_compile():
    scripts = {
        "OBSERVER_SCRIPT": OBSERVER_SCRIPT,
        "SUBMIT_TRACED": SUBMIT_TRACED,
        "READ_SEALS": READ_SEALS,
        "CONTROL_PROBE": CONTROL_PROBE,
        "RELEASE_ACTIVITY": RELEASE_ACTIVITY,
    }
    for name, script in scripts.items():
        compile(script, name, "exec")


def test_partition_direction_exact():
    manifest = render_chaos(episode("medium-4"), TARGET, "trial", peers=PEERS)
    assert manifest == {
        "apiVersion": "chaos-mesh.org/v1alpha1",
        "kind": "NetworkChaos",
        "metadata": {"name": "trial", "namespace": "episode"},
        "spec": {
            "action": "partition",
            "direction": "to",
            "duration": "60s",
            "mode": "one",
            "selector": {"pods": {"episode": ["worker-a"]}},
            "target": {
                "selector": {"pods": {"episode": ["control-a", "control-b"]}},
                "mode": "all",
            },
        },
    }, "F1A_PARTITION_DIRECTION_OR_SELECTOR_CHANGED"


def test_kill_manifests_use_registered_target_and_action():
    pod = render_chaos(episode("medium-0"), TARGET, "trial")
    assert pod["kind"] == "PodChaos"
    assert pod["spec"] == {
        "action": "pod-kill",
        "gracePeriod": 0,
        "mode": "one",
        "selector": {"pods": {"episode": ["worker-a"]}},
    }
    scheduler = replace(TARGET, role="scheduler", container="observed-scheduler")
    container = render_chaos(episode("medium-1"), scheduler, "trial")
    assert container["spec"] == {
        "action": "container-kill",
        "containerNames": ["observed-scheduler"],
        "mode": "one",
        "selector": {"pods": {"episode": ["worker-a"]}},
    }


@pytest.mark.parametrize("change", [{"process": "1:101"}, {"pod": "worker-b", "uid": "uid-b"}])
def test_apply_rejects_repicked_or_restarted_target(tmp_path, change):
    class RecordingSmoke:
        def __init__(self):
            self.calls = []

        def retain(self, label, data):
            self.calls.append((label, data))

        def k(self, *args, **kwargs):
            self.calls.append((args, kwargs))
            return b'{"metadata":{"uid":"test-cr"}}'

    smoke = RecordingSmoke()
    active = select_target([replace(TARGET, uid="uid-b"), TARGET], 530000)
    assert active == TARGET
    chaos = Chaos(smoke, tmp_path, episode("medium-0"), active)
    try:
        chaos.apply(lambda: replace(active, **change))
    except SmokeFailure as error:
        assert "F1A_TARGET_DRIFT" in str(error)
    else:
        pytest.fail("F1A_REPICKED_OR_RESTARTED_TARGET_ACCEPTED")
    assert smoke.calls == []


def test_backend_ack_requires_exact_record_inventory():
    obj = render_chaos(episode("medium-4"), TARGET, "trial", peers=PEERS)
    obj["status"] = {
        "conditions": [{"type": "AllInjected", "status": "True"}],
        "experiment": {
            "containerRecords": [
                {"id": "episode/worker-a", "selectorKey": ".", "phase": "Injected"},
                {"id": "episode/control-a", "selectorKey": ".Target", "phase": "Injected"},
                {"id": "episode/control-b", "selectorKey": ".Target", "phase": "Injected"},
            ]
        },
    }
    assert backend_selected(obj, TARGET, PEERS)
    obj["status"]["experiment"]["containerRecords"][1]["id"] = "episode/control-c"
    with pytest.raises(SmokeFailure, match="F1A_BACKEND_SELECTION"):
        backend_selected(obj, TARGET, PEERS)


def test_restoration_requires_independent_connected_response():
    restored = {"status": 200, "at_ns": 80}
    prove_restoration(70, restored, {"status": 200})
    with pytest.raises(SmokeFailure, match="F1A_RESTORE_AFTER_ABSENCE"):
        prove_restoration(81, restored, {"status": 200})
    try:
        prove_restoration(70, {"status": 0, "at_ns": 80, "all_recovered": True}, {"status": 200})
    except SmokeFailure as error:
        assert "F1A_RESTORE_STATUS" in str(error)
    else:
        pytest.fail("F1A_BACKEND_ONLY_RESTORATION_ACCEPTED")


def test_ack_cannot_prove_death_of_live_process():
    backend = render_chaos(episode("medium-0"), TARGET, "trial")
    backend["status"] = {
        "conditions": [{"type": "AllInjected", "status": "True"}],
        "experiment": {
            "containerRecords": [
                {"id": "episode/worker-a", "selectorKey": ".", "phase": "Injected"},
            ]
        },
    }
    assert backend_selected(backend, TARGET)
    try:
        prove_death(TARGET, 100, TARGET.process, [])
    except SmokeFailure as error:
        assert "F1A_PROCESS_DEATH" in str(error)
    else:
        pytest.fail("F1A_ACK_ACCEPTED_AS_PROCESS_DEATH")


def test_scheduler_death_checks_explicit_cri_exit():
    scheduler = replace(TARGET, role="scheduler")
    event = {"container_id": "a", "at": 1.0, "exit_status": 137, "exit_status_source": "explicit"}
    assert prove_death(scheduler, 100, None, [event])["cri_exit"] == [event]
    with pytest.raises(SmokeFailure, match="F1A_CRI_EXIT_STATUS"):
        prove_death(scheduler, 100, None, [{**event, "exit_status": 0}])


def test_partition_requires_typed_stale_after_commit_and_removal():
    heartbeat = {
        "operation": "heartbeat",
        "token_hash": "old-hash",
        "sent_ns": 31,
        "at_ns": 35,
        "status": 409,
        "code": "STALE_CLAIM",
    }
    order = {
        "token_hash": "old-hash",
        "replacement_observed_ns": 20,
        "restored_ns": 30,
        "successful_ns": 40,
    }
    assert require_stale([heartbeat], **order) == heartbeat
    for invalid in (
        {**heartbeat, "at_ns": 19},
        {**heartbeat, "at_ns": 29},
        {**heartbeat, "at_ns": 41},
        {**heartbeat, "status": 200},
        {**heartbeat, "token_hash": "winner-hash"},
    ):
        with pytest.raises(SmokeFailure, match="F1A_TYPED_STALE_REJECTION"):
            require_stale([invalid], **order)
    try:
        require_stale([{**heartbeat, "code": "RESULT_CONFLICT"}], **order)
    except SmokeFailure as error:
        assert "F1A_TYPED_STALE_REJECTION" in str(error)
    else:
        pytest.fail("F1A_UNTYPED_STALE_REJECTION_ACCEPTED")


def test_route_and_drop_evidence_join_the_observed_process_and_rule():
    route = {
        "event": "f1a_http",
        "pod_uid": TARGET.uid,
        "session": TARGET.session,
        "process": TARGET.process,
        "workflow": "example",
        "node": "pure.echo",
        "iteration": 0,
        "operation": "heartbeat",
        "base_url": CONTROL_URL,
        "path": "/v1/workflows/example/nodes/pure.echo/iterations/0/heartbeat",
        "attempt_number": 1,
    }
    assert route_rows([route], TARGET, "example", attempt=1) == [route]
    with pytest.raises(SmokeFailure, match="F1A_ROUTE_PROCESS"):
        route_rows([{**route, "process": "1:500"}], TARGET, "example", attempt=1)
    with pytest.raises(SmokeFailure, match="F1A_CONTROL_ROUTE"):
        route_rows([{**route, "base_url": "http://api:8080"}], TARGET, "example", attempt=1)
    sample = (
        ":OUTPUT/fault - [0:0]\n"
        "[23:1102] -A CHAOS-OUTPUT -j OUTPUT/fault\n"
        "[23:1102] -A OUTPUT/fault -o eth0 -m set --match-set peers dst,dst -j DROP\n"
    )
    counters = chain_counters(sample, "OUTPUT/fault")
    assert counters == {"packets": 23, "bytes": 1102}
    assert chain_counters(sample, "OUTPUT/other") is None
    assert (
        chain_counters(sample.replace(" -A CHAOS-OUTPUT ", " -A UNUSED "), "OUTPUT/fault") is None
    )
    require_drop_rise({"packets": 10, "bytes": 600}, counters)
    with pytest.raises(SmokeFailure, match="F1A_DROP_COUNTER_RISE"):
        require_drop_rise(counters, counters)


# The hosted artifact stopped at warmup. The receipt, attempt, pod and role-log
# values come from run 44636173493. Claim, witness and recovery rows use the
# snapshot/view and seal schemas because that run never captured those rows.
HOSTED_RUN = {
    "medium-0": (
        1795132142706858801,
        "worker-64646bcb4d-f8vkr",
        "63257d70-21b7-44d3-a5b0-4927918e2937",
        1,
        "bcdc7ce8-a3bd-4461-97bc-455eeca03687",
        "0217840952158b7998411de50a8d02553dd919553dcfb89ba2271defd87599d7",
        "2026-11-19T23:49:48.895708+00:00",
    ),
    "medium-1": (
        1795132259133338880,
        "worker-6b56b798c9-5x4fg",
        "386c224b-8bfd-4b2b-a4e2-9052e1fb482d",
        0,
        "74391e95-2234-4258-a9db-c65001a5366e",
        "0105d99ae492be6cc0fe9d2a96623c358f4f791bc2acfa386365a4abcd402f68",
        "2026-11-19T23:51:44.31053+00:00",
    ),
    "medium-4": (
        1795132372604869217,
        "worker-697b7d66d8-wvc9f",
        "f13e8930-2da5-4743-a104-7bb85a1ec63f",
        0,
        "2eab4464-5f48-4ba9-9e53-545520d2781d",
        "c5582bcfa068e1adc278bb2ddb32ff3894fce3890e589044f34b9eecf76d3524",
        "2026-11-19T23:53:38.789822+00:00",
    ),
}


def pod_for(target):
    return {
        "metadata": {"namespace": target.namespace, "name": target.pod, "uid": target.uid},
        "spec": {"serviceAccountName": target.role, "nodeName": target.node},
        "status": {"conditions": [{"type": "Ready", "status": "True"}]},
    }


def hosted_worker(cell):
    accepted_ns, name, uid, slot, request_id, token_hash, deadline = HOSTED_RUN[cell]
    initial = {"attempts": 0, "offsets": 0, "outbox": 0, "workflows": 0}
    accepted = {
        "at_ns": accepted_ns,
        "response": {"created": True, "status": 201, "workflow_id": f"e1-{cell}"},
    }
    workflow = accepted["response"]["workflow_id"]
    target = replace(
        TARGET,
        namespace=f"dur053-k6-f1a-{cell}",
        pod=name,
        uid=uid,
        session="8cad6e51-a53a-4f66-adba-fe54102734e1",
    )
    worker = f"{name}-{slot}"
    attempt = {
        "state": "CLAIMED",
        "result": None,
        "node_id": "pure.echo",
        "iteration": 0,
        "worker_id": f"system:serviceaccount:{target.namespace}:worker:pod:{name}:uid:{uid}",
        "is_current": True,
        "token_hash": token_hash,
        "result_hash": None,
        "workflow_id": workflow,
        "effect_class": "PURE_ACTIVITY",
        "attempt_number": initial["attempts"] + 1,
        "worker_request_id": f"{worker}:{request_id}",
        "heartbeat_deadline": deadline,
        "outcome_disposition": "NONE",
    }
    claim = {
        **{
            key: attempt[key]
            for key in (
                "workflow_id",
                "node_id",
                "iteration",
                "attempt_number",
                "token_hash",
                "worker_id",
                "worker_request_id",
            )
        },
        "order": 40,
        "at": deadline,
        "finalized_at": deadline,
        "session": "worker-control-session",
        "pod_uid": "worker-control-pod-uid",
        "container_id": "worker-control-pod-uid",
        "transaction": "781",
    }
    context = {"session": target.session, "pod_uid": uid}
    invocation = "172cfdf9-9ca0-4a46-8a23-8020243ef8e5"
    identity = {
        "namespace": target.namespace,
        "workflow": workflow,
        "node": "pure.echo",
        "iteration": 0,
        "attempt": 1,
        "token_hash": token_hash,
    }
    started = {
        **context,
        "stream": "invocations",
        "event": "start",
        "id": invocation,
        "identity": identity,
        "at": 451.0,
    }
    waiting = {
        **context,
        "stream": "trace",
        "event": "f1a_activity_wait",
        "invocation": invocation,
        "workflow": workflow,
        "node": "pure.echo",
        "iteration": "0",
        "attempt": "1",
        "token_hash": token_hash,
        "worker": worker,
        "request": attempt["worker_request_id"],
        "at_ns": accepted_ns + 2,
        "at": 451.1,
        "container_id": uid,
        "timestamp": deadline,
    }
    return target, {"attempts": [attempt], "claims": [claim]}, [started, waiting]


def http_row(target, attempt, operation, *, status=200, code="OK", sent=31):
    workflow, node, iteration = (attempt[key] for key in ("workflow_id", "node_id", "iteration"))
    return {
        "event": "f1a_http",
        "stream": "trace",
        "session": target.session,
        "pod_uid": target.uid,
        "container_id": target.uid,
        "process": target.process,
        "workflow": workflow,
        "node": node,
        "iteration": iteration,
        "operation": operation,
        "base_url": CONTROL_URL,
        "path": f"/v1/workflows/{workflow}/nodes/{node}/iterations/{iteration}/{operation}",
        "attempt_number": attempt["attempt_number"],
        "token_hash": attempt["token_hash"],
        "request": "b5c2a94b-1e7f-4351-85ea-65a6e56a0dfb",
        "sent_ns": sent,
        "status": status,
        "code": code,
        "at_ns": sent + 4,
        "at": 451.2,
        "timestamp": "2026-11-19T23:50:59Z",
    }


def offline_smoke(tmp_path, monkeypatch, target, cut, rows):
    smoke = object.__new__(f1a.F1aSmoke)
    smoke.cell_id = target.namespace.removeprefix("dur053-k6-f1a-")
    smoke.namespace = target.namespace
    smoke.evidence = f1a.F1aEvidence(tmp_path)
    smoke.cut = lambda _: cut
    smoke.witness = lambda: (rows, [])
    smoke.pods = lambda _: [pod_for(target)]
    smoke.identity = lambda role, pod, session: replace(target, role=role, session=session)
    clock = SimpleNamespace(current=0)
    monkeypatch.setattr(
        f1a,
        "time",
        SimpleNamespace(
            monotonic=lambda: clock.current,
            sleep=lambda _: setattr(clock, "current", clock.current + 120),
            time_ns=lambda: f1a.ns("2026-11-19T23:50:59.300Z"),
        ),
    )
    return smoke


@pytest.mark.parametrize("cell", HOSTED_RUN)
def test_active_worker_joins_hosted_authenticated_principal(tmp_path, monkeypatch, cell):
    target, cut, rows = hosted_worker(cell)
    attempt = cut["attempts"][0]
    rows.extend(http_row(target, attempt, operation) for operation in ("claim", "heartbeat"))
    smoke = offline_smoke(tmp_path, monkeypatch, target, cut, rows)
    try:
        selected, proof = smoke.active_worker(attempt["workflow_id"], heartbeat=cell == "medium-4")
    except SmokeFailure as error:
        pytest.fail(f"F1A_BODY_ID_USED_AS_AUTHENTICATED_PRINCIPAL: {error}")
    assert selected == [target]
    assert proof[target.uid]["claim"] == cut["claims"][0]
    assert rows[1]["worker"] != attempt["worker_id"]
    assert rows[1]["request"] != rows[1]["invocation"] != rows[-1]["request"]


def test_worker_selection_timeout_retains_rows_and_first_failure(tmp_path, monkeypatch):
    target, cut, rows = hosted_worker("medium-0")
    attempt = cut["attempts"][0]
    attempt["worker_id"] = attempt["worker_id"].replace(":worker:pod:", ":different-account:pod:")
    smoke = offline_smoke(tmp_path, monkeypatch, target, cut, rows)
    label = "F1A_CANDIDATE_WORKER_PRINCIPAL_MISMATCH"
    with pytest.raises(SmokeFailure, match=label):
        smoke.active_worker(attempt["workflow_id"], heartbeat=True)
    path = tmp_path / "medium-0-warmup-active-worker-timeout-witness.json"
    evidence = json.loads(path.read_bytes())
    assert evidence["witness_rows"] == rows
    assert evidence["candidates"] == [{"attempt_number": 1, "pod_uid": target.uid, "label": label}]
    smoke.evidence.secret(rows[1]["request"])
    with pytest.raises(SmokeFailure, match="CREDENTIAL_SCAN_FAILED"):
        smoke.retain("scan-test", evidence)
    assert not (tmp_path / "medium-0-scan-test.json").exists()


def test_pure_claim_trace_uses_server_receipt(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "campaign" / "k6"))
    from dur053_k6_campaign import chaos_smoke

    token = "synthetic-claim-token"
    rows = []
    monkeypatch.setattr(
        chaos_smoke,
        "campaign_witness",
        lambda: SimpleNamespace(
            trace=lambda event, fields: rows.append({**fields, "event": event}),
        ),
    )
    monkeypatch.setattr(
        chaos_smoke.RetryingControl,
        "_post_once",
        lambda *args: {
            "attempt_number": 1,
            "claim_token": token,
        },
    )
    control = object.__new__(chaos_smoke.SmokeControl)
    control.base_url, control.process = CONTROL_URL, "1:100"
    control._post_once(
        "/v1/workflows/e1-medium-0/nodes/pure.echo/iterations/0/claim", {"expected_attempt": 0}
    )
    assert rows[0]["attempt_number"] == 0 and rows[0]["token_hash"] is None
    assert (
        rows[1]["attempt_number"] == 1
        and rows[1]["token_hash"] == hashlib.sha256(token.encode()).hexdigest()
    )
    assert token not in json.dumps(rows)


def hosted_scheduler():
    scheduler = replace(
        TARGET,
        namespace="dur053-k6-f1a-medium-1",
        pod="scheduler-5858c87678-plk58",
        uid="55db0771-9b4f-4511-820b-bf2c3f95603e",
        role="scheduler",
        container="scheduler",
        session="26438542-adc1-4030-b50e-59ed4b5b14b3",
    )
    lease = {
        "partition_id": 14,
        "owner_id": "f188e692-2344-42d0-8199-119e8163cf8b",
        "epoch": 1,
        "lease_expires_at": "2026-11-19T23:51:14.140000+00:00",
        "updated_at": "2026-11-19T23:50:59.140000+00:00",
        "live": True,
    }
    acquire = {
        "kind": "acquire",
        "event_order": 21,
        "partition_id": lease["partition_id"],
        "owner_id": lease["owner_id"],
        "epoch": lease["epoch"],
        "pod_uid": scheduler.uid,
        "container_id": scheduler.uid,
        "session_id": scheduler.session,
        "transaction_id": "775",
        "transition_id": None,
        "at": "2026-11-19T23:50:59.145000+00:00",
        "finalized_at": "2026-11-19T23:50:59.150000+00:00",
    }
    acquire["observed_ns"] = f1a.ns(acquire["finalized_at"])
    log = {
        "acquisition": "49d1d6ed-7834-4ccb-a835-e468aab53d9c",
        "boundary": "after_lease_acquired_hold_enter",
        "container_id": scheduler.uid,
        "epoch": 1,
        "event": "scheduler_acquisition",
        "monotonic_seconds": 8.967223301,
        "owner": lease["owner_id"],
        "partition": 14,
        "pid": 1,
        "pod_uid": scheduler.uid,
        "session": scheduler.session,
        "timestamp": "2026-11-19T23:50:59.15289976Z",
    }
    _, cut, _ = hosted_worker("medium-1")
    return scheduler, {**cut, "leases": [lease], "audit": [acquire], "history": []}, log


def role_log(log):
    return (
        f"{log['timestamp']} 2026/11/19 23:50:59 INFO k4 scheduler acknowledgment "
        f"data={json.dumps(json.dumps(log, separators=(',', ':')))}"
    ).encode()


def run_44647473830(cell):
    # Selected pre-release rows from the hosted run. Tests extend them with the
    # shapes emitted by ActivityRunner, the HTTP tracer and transition_history.
    source = ROOT / "tests/fixtures/k6_f1a_hosted.json"
    return json.loads(source.read_bytes())[cell]


def test_scheduler_selection_and_confirmation_join_hosted_role_log(tmp_path, monkeypatch):
    target, cut, log = hosted_scheduler()
    smoke = offline_smoke(tmp_path, monkeypatch, target, cut, [])
    smoke.role_log = lambda *_: role_log(log)
    assert smoke.active_scheduler("e1-medium-1") == ([target], cut)
    assert smoke.ensure_scheduler_lease(target, "e1-medium-1") == cut
    assert json.loads((tmp_path / "medium-1-scheduler-acquisition.json").read_bytes()) == log
    log["session"] = log["owner"]
    label = "F1A_CANDIDATE_SCHEDULER_ACQUISITION_LOG_MISSING"
    with pytest.raises(SmokeFailure, match=label):
        smoke.ensure_scheduler_lease(target, "e1-medium-1")
    evidence = json.loads(
        (tmp_path / "medium-1-bound-scheduler-lease-timeout-witness.json").read_bytes()
    )
    assert evidence["witness_rows"] == [log]
    assert evidence["candidates"] == [{"attempt_number": 1, "pod_uid": target.uid, "label": label}]


def test_first_fenced_transition_joins_the_committed_lease():
    killed, author, cut, after, issued_ns, _ = fenced_rows()
    acquisition = after["audit"][-2]
    transition = after["audit"][-1]
    live = {author.uid: {author.session}}
    assert first_fenced_transition(cut, after, issued_ns, killed, live) == {
        "acquire": acquisition,
        "first_transition": transition,
    }
    wrong_pod = {**acquisition, "pod_uid": "another-pod"}
    with pytest.raises(SmokeFailure, match="F1A_RECOVERY_COMMITTED_LEASE"):
        first_fenced_transition(
            cut, {**after, "audit": [wrong_pod, transition]}, issued_ns, killed, live
        )


def fenced_rows():
    fixture = run_44647473830("medium-1")
    killed = Target(**fixture["target"])
    author = replace(Target(**fixture["replacement_target"]), session="restarted-scheduler")
    before = fixture["before"]
    issued_ns = fixture["removed"]["issued_ns"]
    sample = before["audit"][-1]
    acquisition = {
        **sample,
        "owner_id": "new-lease-owner",
        "session_id": author.session,
        "epoch": sample["epoch"] + 1,
        "event_order": sample["event_order"] + 1,
        "at": "2026-11-20T01:11:00.000001+00:00",
        "finalized_at": "2026-11-20T01:11:00.000002+00:00",
        "observed_ns": f1a.ns("2026-11-20T01:11:00.000002+00:00"),
        "transaction_id": "12252",
    }
    transition_id = "61af2dfc-9a2a-4467-aa48-44d09103eda4"
    transition = {
        **acquisition,
        "kind": "history",
        "event_order": acquisition["event_order"] + 1,
        "transition_id": transition_id,
        "observed_ns": acquisition["observed_ns"] + 5 * 10**9,
        "transaction_id": "12253",
    }
    history = {
        **fixture["history"],
        "transition_id": transition_id,
        "reason": "RESULT_CONSUMED",
        "scheduler_epoch": acquisition["epoch"],
        "revision": 5,
    }
    after = {
        **before,
        "history": [*before["history"], history],
        "audit": [*before["audit"], acquisition, transition],
    }
    log = {
        **fixture["scheduler_log"],
        "session": author.session,
        "owner": acquisition["owner_id"],
        "epoch": acquisition["epoch"],
        "timestamp": "2026-11-20T01:11:00.023574896Z",
    }
    return killed, author, before, after, issued_ns, log


def test_scheduler_reconciliation_retains_restarted_container_and_wrong_session(
    tmp_path, monkeypatch
):
    killed, author, before, after, issued_ns, log = fenced_rows()
    smoke = offline_smoke(tmp_path, monkeypatch, author, after, [])
    smoke.role_log = lambda *_: role_log(log)
    smoke.reobserve = lambda t: t
    proof = smoke.reconcile_scheduler(before, after, issued_ns, killed)
    assert proof["process"]["uid"] == killed.uid
    assert proof["process"]["container_id"] != killed.container_id
    assert proof["acquisition_log"] == log
    log["session"] = killed.session
    with pytest.raises(SmokeFailure, match="F1A_FENCED_SCHEDULER_PROCESS"):
        smoke.reconcile_scheduler(before, after, issued_ns, killed)
    report = json.loads((tmp_path / "medium-1-fenced-reconciliation.json").read_bytes())
    assert report["final"] == after
    assert report["reason"].startswith("F1A_FENCED_SCHEDULER_PROCESS")


def test_backend_records_use_hosted_pod_names_and_container_name():
    worker, _, _ = hosted_worker("medium-4")
    scheduler, _, _ = hosted_scheduler()
    peers = ["worker-control-59ddfcb755-7cmvj", "worker-control-59ddfcb755-j8qcc"]
    cases = [
        ("medium-0", hosted_worker("medium-0")[0], [], ""),
        ("medium-1", scheduler, [], "/scheduler"),
        ("medium-4", worker, peers, ""),
    ]
    for cell, target, names, suffix in cases:
        obj = render_chaos(episode(cell), target, "trial", peers=names or None)
        records = [
            {
                "id": f"{target.namespace}/{target.pod}{suffix}",
                "selectorKey": ".",
                "phase": "Injected",
            }
        ]
        records.extend(
            {"id": f"{target.namespace}/{name}", "selectorKey": ".Target", "phase": "Injected"}
            for name in names
        )
        obj["status"] = {
            "conditions": [{"type": "AllInjected", "status": "True"}],
            "experiment": {"containerRecords": records},
        }
        assert backend_selected(obj, target, names)
        records[0]["id"] = f"{target.namespace}/{target.uid}{suffix}"
        with pytest.raises(SmokeFailure, match="F1A_BACKEND_SELECTION"):
            backend_selected(obj, target, names)


def result_witness(fixture, marker, *, status=200, code="OK"):
    wait = fixture["wait"]
    started = {
        **fixture["start"],
        "pod_uid": wait["pod_uid"],
        "session": wait["session"],
        "stream": "invocations",
    }
    released = {
        **wait,
        "event": "f1a_activity_release",
        "marker": marker["marker"],
        "gate_started_ns": fixture["claim_response"]["at_ns"] + 10**6,
        "at_ns": marker["started_ns"] + 10**6,
        "at": wait["at"] + 10,
    }
    finished = {**started, "event": "finish", "at": released["at"] + 0.001}
    result = {
        **fixture["claim_response"],
        "operation": "result",
        "status": status,
        "code": code,
        "path": fixture["claim_response"]["path"].removesuffix("claim") + "result",
        "sent_ns": released["at_ns"] + 10**6,
        "at_ns": released["at_ns"] + 2 * 10**6,
        "at": released["at"] + 0.002,
    }
    return [started, wait, fixture["claim_response"], released, finished, result]


def terminal_fixture(cell):
    fixture = run_44647473830(cell)
    worker = fixture["wait"]
    target = replace(
        TARGET,
        namespace=fixture["attempt"]["worker_id"].split(":")[2],
        pod=worker["worker"].rsplit("-", 1)[0],
        uid=worker["pod_uid"],
        session=worker["session"],
    )
    # The old medium-0 pod is gone. Use the replacement UID and a future claim
    # with the same worker/worker-control split seen in all three claim rows.
    now_ns = fixture["recovery_ns"]
    if cell == "medium-0":
        target = replace(Target(**fixture["replacement_target"]), session="replacement-session")
        now_ns = f1a.ns(fixture["attempt"]["heartbeat_deadline"]) + 3 * 10**9
    marker = {"started_ns": now_ns, "marker": RELEASE_MARKER}
    rows = result_witness(fixture, marker)
    attempt = {
        **fixture["attempt"],
        "state": "SUCCEEDED",
        "is_current": False,
        "result": {"echo": "fixture"},
        "outcome_disposition": "KNOWN_SUCCESS",
    }
    claim = fixture["claim"]
    if cell == "medium-0":
        request = f"{target.pod}-0:replacement-request"
        token_hash = "a1" * 32
        principal = (
            f"system:serviceaccount:{target.namespace}:worker:pod:{target.pod}:uid:{target.uid}"
        )
        keys = {
            "worker_id": principal,
            "worker_request_id": request,
            "attempt_number": 2,
            "token_hash": token_hash,
        }
        attempt.update(**keys, heartbeat_deadline="2026-11-20T01:09:50.000000+00:00")
        claim.update(keys)
        for row in rows:
            row.update(pod_uid=target.uid, session=target.session)
            if row["stream"] == "invocations":
                row["identity"] = {**row["identity"], "attempt": 2, "token_hash": token_hash}
            else:
                row.update(container_id=target.uid, token_hash=token_hash)
                if row["event"] == "f1a_http":
                    row.update(attempt_number=2, process=target.process)
                else:
                    row.update(attempt="2", request=request, worker=target.pod + "-0")
        rows[3]["gate_started_ns"] = now_ns - 10**6
    history = {
        **fixture["history"],
        "attempt_number": attempt["attempt_number"],
        "actor_kind": "worker",
        "actor_id": attempt["worker_id"],
        "scheduler_epoch": None,
        "reason": "ATTEMPT_RESULT_RECORDED",
        "created_at": datetime.fromtimestamp((now_ns + 2 * 10**6) / 10**9, UTC).isoformat(),
    }
    workflow = {**fixture["workflow"], "state": "SUCCEEDED"}
    cut = {"workflow": workflow, "attempts": [attempt], "claims": [claim], "history": [history]}
    return target, cut, rows, marker


def test_terminal_join_binds_winner_to_pod_session_and_claim():
    target, cut, rows, marker = terminal_fixture("medium-0")
    attempt = cut["attempts"][0]
    started, _, _, released, finished, result = rows
    assert cut["claims"][0]["pod_uid"] != started["pod_uid"]
    assert terminal_winner(cut, rows, [pod_for(target)], target.namespace, marker) == {
        "attempt": attempt,
        "claim": cut["claims"][0],
        "invocation": started,
        "result": result,
        "finish": finished,
        "release": released,
        "result_history": cut["history"][0],
    }
    finished["session"] = result["request"]
    with pytest.raises(SmokeFailure, match="F1A_WINNING_ACTIVITY_FINISHED"):
        terminal_winner(cut, rows, [pod_for(target)], target.namespace, marker)
    finished["session"] = started["session"]
    cut["claims"][0]["worker_id"] = rows[1]["worker"]
    with pytest.raises(SmokeFailure, match="F1A_WINNING_CLAIM"):
        terminal_winner(cut, rows, [pod_for(target)], target.namespace, marker)


def test_finish_retains_terminal_cut_rows_and_expired_lease(tmp_path, monkeypatch):
    target, cut, rows, marker = terminal_fixture("medium-1")
    smoke = offline_smoke(tmp_path, monkeypatch, target, cut, rows)
    smoke.release_activity = lambda *_args, **_kw: marker
    assert smoke.finish(cut["workflow"]["workflow_id"])[1]["result"] == rows[-1]
    cut["attempts"][0]["heartbeat_deadline"] = cut["claims"][0]["finalized_at"]
    with pytest.raises(SmokeFailure, match="F1A_WINNING_RESULT_WITHIN_LEASE"):
        smoke.finish(cut["workflow"]["workflow_id"])
    report = json.loads((tmp_path / "medium-1-terminal-timeout-witness.json").read_bytes())
    assert report["witness_rows"] == rows
    assert report["cut"] == cut
    assert report["candidates"][0]["pod_uid"] == target.uid


def test_release_retains_gate_clock_and_uses_observer(tmp_path, monkeypatch):
    fixture = run_44647473830("medium-4")
    target = Target(**fixture["target"])
    smoke = offline_smoke(tmp_path, monkeypatch, target, {}, [])
    wait = {**fixture["wait"], "gate_started_ns": fixture["claim_response"]["at_ns"]}
    smoke.exec_python = lambda *args: pytest.fail(f"unexpected release {args[:2]}")
    now_ns = wait["gate_started_ns"] + f1a.GATE_SECONDS * 10**9
    monkeypatch.setattr(f1a.time, "time_ns", lambda: now_ns)
    with pytest.raises(SmokeFailure, match="F1A_RELEASE_AFTER_GATE_BOUND"):
        smoke.release_activity(wait)
    path = tmp_path / f"medium-4-release-{wait['invocation']}.json"
    assert json.loads(path.read_bytes())["gate_remaining_ns"] == 0
    monkeypatch.setattr(f1a.time, "time_ns", lambda: fixture["recovery_ns"])

    def release(stage, pod, script, marker, invocation):
        assert (stage, pod, script) == ("RELEASE", "deployment/observer", RELEASE_ACTIVITY)
        assert (marker, invocation) == (RELEASE_MARKER, wait["invocation"])
        return {"released": True, "marker": f"{marker}-{invocation}"}

    smoke.exec_python = release
    assert smoke.release_activity(wait)["response"]["released"] is True
    assert json.loads(path.read_bytes())["gate_remaining_ns"] > 0


def test_medium4_replacement_joins_immediate_attempt_and_old_history():
    fixture = run_44647473830("medium-4")
    attempt, old, replacement, history = (
        fixture[key] for key in ("attempt", "old", "replacement", "replacement_history")
    )
    cut = {"attempts": [old, replacement], "history": [history]}
    assert partition_replacement(cut, attempt) == {
        "old": old,
        "replacement": replacement,
        "replacement_history": history,
    }
    replacement["iteration"] = 1
    with pytest.raises(SmokeFailure, match="F1A_REPLACEMENT_SAME_INVOCATION_KEY"):
        partition_replacement(cut, attempt)
    replacement["iteration"] = 0
    history["attempt_number"] = 2
    with pytest.raises(SmokeFailure, match="F1A_REPLACEMENT_COMMITTED_HISTORY"):
        partition_replacement(cut, attempt)
    history["attempt_number"] = 1
    history["created_at"] = fixture["claim"]["at"]
    with pytest.raises(SmokeFailure, match="F1A_REPLACEMENT_AFTER_OLD_LEASE"):
        partition_replacement(cut, attempt)


def test_stale_result_timeout_retains_hosted_shape_and_token_mismatch(tmp_path, monkeypatch):
    fixture = run_44647473830("medium-4")
    target = Target(**fixture["target"])
    invocation = fixture["start"]["id"]
    marker = {
        "started_ns": fixture["recovery_ns"] + 10**6,
        "marker": RELEASE_MARKER + "-" + invocation,
    }
    rows = result_witness(fixture, marker, status=409, code="STALE_ATTEMPT")
    result = rows[-1]
    rows.append(fixture["failure"])
    smoke = offline_smoke(tmp_path, monkeypatch, target, {}, rows)
    args = (
        target,
        fixture["attempt"],
        invocation,
        marker,
        fixture["replacement_ns"],
        fixture["recovery_ns"],
    )
    assert smoke.wait_stale(*args) == result
    result["token_hash"] = "af" * 32
    label = "F1A_CANDIDATE_STALE_TOKEN_HASH_MISMATCH"
    with pytest.raises(SmokeFailure, match=label):
        smoke.wait_stale(*args)
    evidence = json.loads(
        (tmp_path / "medium-4-typed-stale-rejection-timeout-witness.json").read_bytes()
    )
    assert evidence["witness_rows"] == rows
    assert evidence["candidates"] == [{"attempt_number": 1, "pod_uid": target.uid, "label": label}]


def topology_object(image=RUNTIME, namespace="episode"):
    return {
        "kind": "Deployment",
        "metadata": {"name": "scheduler", "namespace": namespace},
        "spec": {"template": {"spec": {"containers": [{"name": "scheduler", "image": image}]}}},
    }


def test_audit_rejects_an_unpushed_digest():
    audit_topology([topology_object()], {RUNTIME})
    try:
        audit_topology([topology_object(TAGGED_RUNTIME)], {RUNTIME})
    except SmokeFailure as error:
        assert "F1A_IMAGE_NOT_PUSHED" in str(error)
    else:
        pytest.fail("F1A_UNPUSHED_IMAGE_ACCEPTED")


def test_audit_rejects_an_unrewritten_namespace():
    audit_topology([topology_object()], {RUNTIME})
    try:
        audit_topology([topology_object(namespace="dur053-kind")], {RUNTIME})
    except SmokeFailure as error:
        assert "F1A_NAMESPACE_OR_PLACEHOLDER_UNREWRITTEN" in str(error)
    else:
        pytest.fail("F1A_UNREWRITTEN_NAMESPACE_ACCEPTED")


def render_real_objects():
    base = (ROOT / "deploy/k8s/base/resources.yaml").read_text(encoding="utf-8")
    data = json.loads(
        base.replace("dur053/runtime:UNBOUND", RUNTIME).replace("dur053/worker:UNBOUND", WORKER)
    )
    items = data["items"]
    for item in items:
        if item["kind"] in {"Deployment", "Service", "Job", "PersistentVolumeClaim"}:
            item["metadata"]["namespace"] = "dur053-kind"
    items = [row for row in items if row["kind"] != "NetworkPolicy"]
    return deployment_objects(
        items,
        "episode",
        {"runtime": TAGGED_RUNTIME, "worker": TAGGED_WORKER},
        {"runtime": RUNTIME, "worker": WORKER},
    )


def test_render_real_objects_preserves_pushed_image_names_and_two_by_two_topology():
    rendered = render_real_objects()
    roles = {
        row["metadata"]["name"]: row["spec"] for row in rendered if row["kind"] == "Deployment"
    }
    assert roles["worker"]["replicas"] == roles["scheduler"]["replicas"] == 2
    worker = roles["worker"]["template"]["spec"]["containers"][0]
    assert worker["image"] == TAGGED_WORKER
    assert {item["name"]: item.get("value") for item in worker["env"]}["WORKER_SLOTS"] == "2"
    assert not any(item["kind"] in {"ScaledObject", "HorizontalPodAutoscaler"} for item in rendered)
    assert topology_object(RUNTIME)["metadata"]["namespace"] == "episode"


def test_render_observer_witness_stays_read_only():
    try:
        objects = render_real_objects()
    except SmokeFailure as error:
        assert (
            str(error)
            == "F1A_WITNESS_WRITABLE_OUTSIDE_WORKER Deployment/observer/containers/observer"
        )
        pytest.fail("F1A_OBSERVER_WITNESS_WRITABLE")
    mounts = {}
    for item in objects:
        name = item["metadata"]["name"]
        if item["kind"] == "Deployment" and name in {"observer", "worker"}:
            pod = item["spec"]["template"]["spec"]
            sources = {v["name"]: v.get("persistentVolumeClaim") for v in pod["volumes"]}
            for mount in pod["containers"][0]["volumeMounts"]:
                if claim := sources[mount["name"]]:
                    mounts[name, mount["mountPath"]] = (
                        claim["claimName"],
                        mount.get("readOnly", False),
                    )
    assert mounts == {
        ("observer", "/witness"): ("witness", True),
        ("worker", "/witness"): ("witness", False),
        ("observer", "/f1a-control"): ("f1a-control", False),
        ("worker", "/f1a-control"): ("f1a-control", True),
    }, "F1A_OBSERVER_WITNESS_WRITABLE"


@pytest.mark.parametrize(
    ("change", "label"),
    [
        ("worker-control-writable", "F1A_CONTROL_WRITABLE_OUTSIDE_OBSERVER"),
        ("different-gate-path", "F1A_GATE_RELEASE_PATH_MISMATCH"),
        ("observer-control-readonly", "F1A_CONTROL_MOUNT_INVENTORY"),
        ("witness-alias", "F1A_WITNESS_WRITABLE_OUTSIDE_WORKER"),
    ],
)
def test_control_mount_audit_rejects_invalid_render(change, label):
    objects = render_real_objects()
    pods = {
        row["metadata"]["name"]: row["spec"]["template"]["spec"]
        for row in objects
        if row["kind"] == "Deployment"
    }
    worker = pods["worker"]["containers"][0]
    observer = pods["observer"]["containers"][0]
    if change == "different-gate-path":
        next(row for row in worker["env"] if row["name"] == f1a.RELEASE_ENV)["value"] = "/witness"
    elif change == "witness-alias":
        pod = pods["relay"]
        pod["volumes"].append({"name": "other", "persistentVolumeClaim": {"claimName": "witness"}})
        pod["containers"][0]["volumeMounts"].append({"name": "other", "mountPath": "/other"})
    else:
        container = observer if change == "observer-control-readonly" else worker
        next(row for row in container["volumeMounts"] if row["name"] == f1a.CONTROL_VOLUME)[
            "readOnly"
        ] = container is observer
    with pytest.raises(SmokeFailure, match=label):
        audit_control_mounts(objects, "episode")


def test_release_script_and_worker_gate_share_scoped_then_global_marker(
    tmp_path, monkeypatch, capsys
):
    monkeypatch.syspath_prepend(str(ROOT / "campaign/k6"))
    from dur053_k6_campaign import chaos_smoke

    marker = tmp_path / "f1a-release"
    observed = []
    monkeypatch.setenv(f1a.RELEASE_ENV, str(marker))
    monkeypatch.setattr(
        chaos_smoke,
        "campaign_witness",
        lambda: SimpleNamespace(trace=lambda event, fields: observed.append((event, fields))),
    )
    invocation = run_44647473830("medium-4")["start"]["id"]

    def release(scope):
        monkeypatch.setattr(sys, "argv", ["-c", str(marker), scope])
        exec(compile(RELEASE_ACTIVITY, "<release>", "exec"), {})
        return json.loads(capsys.readouterr().out)["marker"]

    scoped = release(invocation)
    assert not marker.exists()
    chaos_smoke.activity_gate("activity_start", {"invocation": invocation})
    assert observed[-1][0] == "f1a_activity_release"
    assert observed[-1][1]["marker"] == scoped
    assert release("") == str(marker)
    chaos_smoke.activity_gate("activity_start", {"invocation": "replacement"})
    assert observed[-1][1]["marker"] == str(marker)
    monkeypatch.setenv(f1a.RELEASE_ENV, str(tmp_path / "missing" / marker.name))
    with pytest.raises(ValueError, match="F1A_ACTIVITY_CONTROL_MISSING"):
        chaos_smoke.activity_gate("activity_start", {"invocation": invocation})
    assert observed[-1][0] == "f1a_activity_control_missing"


@pytest.mark.parametrize("lingers", [False, True])
def test_cleanup_reclaims_both_claims_and_continues_after_failure(tmp_path, monkeypatch, lingers):
    target = Target(**run_44647473830("medium-4")["target"])
    assert run_44647473830("cleanup")[-1]["step"] == "d-chaos-cleanup"
    smoke = offline_smoke(tmp_path, monkeypatch, target, {}, [])
    calls = []
    smoke.database = smoke.namespace.replace("-", "_")
    smoke.deadline = datetime(2026, 11, 19, tzinfo=UTC)
    smoke.namespace_started = smoke.database_started = True
    smoke.chaos_run = SimpleNamespace(possibly_created=True, remove=lambda: calls.append("chaos"))
    smoke.harness = SimpleNamespace(drop=lambda _: calls.append("db"))

    def k(stage, *args, **kwargs):
        calls.append(stage)
        assert smoke.deadline > datetime.now(UTC)
        if args[:2] == ("get", "pv"):
            items = (
                [
                    {
                        "metadata": {"name": "pvc-control"},
                        "spec": {"claimRef": {"name": "f1a-control", "namespace": smoke.namespace}},
                        "status": {"phase": "Released"},
                    }
                ]
                if lingers
                else []
            )
            return json.dumps({"items": items}).encode()
        return b""

    smoke.k = k
    if lingers:
        with pytest.raises(SmokeFailure, match="F1A_CLEANUP_PV_RECLAIM"):
            smoke.clear_episode()
    else:
        smoke.clear_episode()
    assert calls == [
        "chaos",
        "F1A_NAMESPACE_CLEANUP",
        "F1A_CLEANUP_NAMESPACE_READ",
        "F1A_CLEANUP_PV_READ",
        "F1A_RBAC_CLEANUP",
        "F1A_CLEANUP_RBAC_READ",
        "db",
    ]
    report = json.loads((tmp_path / "medium-4-cleanup.json").read_bytes())
    assert bool(report["remaining_volumes"]) == lingers
    if lingers:
        assert "F1A_CLEANUP_PV_STILL_BOUND" in report["steps"][2]["reason"]


def test_ci_handoff_is_a_sibling_and_always_uploads():
    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    assert "DUR053_K6_F1A_OUTPUT: ${{ runner.temp }}/dur053-k6-f1a" in workflow
    upload = workflow.split("Retain K6 Phase F1a evidence including failures", 1)[1]
    assert "if: always()" in upload and "path: ${{ runner.temp }}/dur053-k6-f1a/" in upload
    source = ast.parse((ROOT / "scripts/dur053_k3.py").read_text(encoding="utf-8"))
    handoff = next(
        node
        for node in ast.walk(source)
        if isinstance(node, ast.If) and ast.unparse(node.test) == "k6_f1a_output"
    )
    assert any(
        isinstance(node, ast.Call) and ast.unparse(node.func) == "dur053_k6_f1a_smoke.run"
        for node in ast.walk(handoff)
    )
    assert all(not isinstance(node, ast.If) for node in ast.walk(handoff) if node is not handoff)
