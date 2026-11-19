from __future__ import annotations

import ast
import json
import sys
from dataclasses import replace
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

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
    chain_counters,
    first_fenced_transition,
    require_drop_rise,
    require_stale,
    route_rows,
)
from dur053_k6_f1a_smoke import (  # noqa: E402
    CONTROL_PROBE,
    OBSERVER_SCRIPT,
    READ_SEALS,
    RELEASE_ACTIVITY,
    SUBMIT_TRACED,
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


def test_first_fenced_transition_joins_the_committed_lease():
    owner = {
        "partition_id": 3,
        "epoch": 4,
        "owner_id": "new-owner",
        "session_id": "new-session",
        "pod_uid": "peer",
    }
    acquisition = {**owner, "kind": "acquire", "event_order": 6}
    transition = {
        **owner,
        "kind": "history",
        "event_order": 7,
        "transition_id": "new-transition",
        "observed_ns": 55,
    }
    after = {"audit": [acquisition, {**transition, "epoch": 0}, transition]}
    before = {"history": []}
    assert first_fenced_transition(before, after, 50, TARGET, {"peer": {"new-session"}}) == {
        "acquire": acquisition,
        "first_transition": transition,
    }
    with pytest.raises(SmokeFailure, match="F1A_RECOVERY_COMMITTED_LEASE"):
        first_fenced_transition(
            before, {"audit": [transition]}, 50, TARGET, {"peer": {"new-session"}}
        )


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


def test_render_real_objects_preserves_pushed_image_names_and_two_by_two_topology():
    base = (ROOT / "deploy/k8s/base/resources.yaml").read_text(encoding="utf-8")
    data = json.loads(
        base.replace("dur053/runtime:UNBOUND", RUNTIME).replace("dur053/worker:UNBOUND", WORKER)
    )
    items = data["items"]
    for item in items:
        if item["kind"] in {"Deployment", "Service", "Job"}:
            item["metadata"]["namespace"] = "dur053-kind"
    items = [row for row in items if row["kind"] != "NetworkPolicy"]
    rendered = deployment_objects(
        items,
        "episode",
        {"runtime": TAGGED_RUNTIME, "worker": TAGGED_WORKER},
        {"runtime": RUNTIME, "worker": WORKER},
    )
    roles = {
        row["metadata"]["name"]: row["spec"] for row in rendered if row["kind"] == "Deployment"
    }
    assert roles["worker"]["replicas"] == roles["scheduler"]["replicas"] == 2
    worker = roles["worker"]["template"]["spec"]["containers"][0]
    assert worker["image"] == TAGGED_WORKER
    assert {item["name"]: item.get("value") for item in worker["env"]}["WORKER_SLOTS"] == "2"
    assert not any(item["kind"] in {"ScaledObject", "HorizontalPodAutoscaler"} for item in rendered)
    assert topology_object(RUNTIME)["metadata"]["namespace"] == "episode"


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
