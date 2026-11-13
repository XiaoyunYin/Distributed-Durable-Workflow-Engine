"""K4 live decisions exercised offline with explicit Kubernetes/process fakes."""

from __future__ import annotations

import base64
import copy
import hashlib
import io
import json
import sys
import tarfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import dur053_k4_collectors as collectors  # noqa: E402
import dur053_k4_driver as driver  # noqa: E402
import dur053_k4_interventions as interventions  # noqa: E402
import dur053_lock_check as locks  # noqa: E402
from dur053_k4_schema import CONFIRMATION, SESSION  # noqa: E402


def pod(uid="old", role="worker", restart=0, cid="containerd://old", ready=True):
    return dict(
        metadata=dict(name=uid, uid=uid, labels=dict(app=role), resourceVersion="1"),
        status=dict(
            conditions=[
                dict(
                    type="Ready",
                    status="True" if ready else "False",
                    lastTransitionTime="2026-01-01T00:00:00Z",
                )
            ],
            containerStatuses=[
                dict(
                    name=role,
                    containerID=cid,
                    restartCount=restart,
                    ready=ready,
                    state=dict(running=dict(startedAt="2026-01-01T00:00:00Z")),
                )
            ],
        ),
    )


def deployment(**status):
    return dict(
        metadata=dict(generation=3, annotations={"deployment.kubernetes.io/revision": "2"}),
        spec=dict(replicas=2, template=dict(spec=dict(containers=[dict(image="a-prime")]))),
        status=dict(
            observedGeneration=3,
            updatedReplicas=2,
            readyReplicas=2,
            availableReplicas=2,
            replicas=2,
        )
        | status,
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("observedGeneration", 2),
        ("updatedReplicas", 1),
        ("readyReplicas", 1),
        ("availableReplicas", 1),
        ("replicas", 3),
    ],
)
def test_rollout_status_requires_every_observation(field, value):
    assert not interventions.rollout_complete(deployment(**{field: value})), (
        "incomplete rollout accepted"
    )


def test_rollout_image_and_observed_status():
    assert interventions.rollout_complete(deployment(), "a-prime")
    assert not interventions.rollout_complete(deployment(), "a"), "wrong rollout image accepted"


def test_adapter_waits_for_observation_without_sleep(tmp_path):
    d = Mock()

    def until(predicate, seconds):
        assert not predicate(), "first incomplete rollout accepted"
        assert predicate(), "complete rollout rejected"

    d.until.side_effect = until
    a = interventions.Interventions(d, "ns", tmp_path, {}, {}, "a" * 40)
    a.get = Mock(side_effect=[deployment(readyReplicas=0), deployment(), deployment()])
    assert a.wait_rollout("worker", "a-prime") == "2"
    assert d.k.call_count == 0


def injected():
    return dict(
        status=dict(
            conditions=[dict(type="AllInjected", status="True")],
            experiment=dict(containerRecords=[dict(phase="Injected")]),
        )
    )


@pytest.mark.parametrize("mutation", ["condition", "records", "phase"])
def test_chaos_requires_backend_ack(mutation):
    obj = injected()
    if mutation == "condition":
        obj["status"]["conditions"][0]["status"] = "False"
    if mutation == "records":
        obj["status"]["experiment"]["containerRecords"] = []
    if mutation == "phase":
        obj["status"]["experiment"]["containerRecords"][0]["phase"] = "Not Injected"
    assert not interventions.chaos_ack(obj), "missing chaos acknowledgment accepted"


def test_kill_requires_exact_container_restart_death_and_ready():
    before = pod()
    after = pod(restart=1, cid="containerd://new")
    after["status"]["containerStatuses"][0]["lastState"] = dict(
        terminated=dict(exitCode=137, signal=9)
    )
    assert interventions.kill_proof(before, after, True)["exitCode"] == 137
    variants = [(False, after, "k4-chaos-ack-missing")]
    for mutation, guard in [
        ("uid", "wrong-uid"),
        ("restart", "restart-missing"),
        ("identity", "restart-missing"),
        ("exit", "death-missing"),
        ("signal", "death-missing"),
        ("ready", "replacement-not-ready"),
    ]:
        other = copy.deepcopy(after)
        status = other["status"]["containerStatuses"][0]
        if mutation == "uid":
            other["metadata"]["uid"] = "foreign"
        if mutation == "restart":
            status["restartCount"] = 0
        if mutation == "identity":
            status["containerID"] = "containerd://old"
        if mutation == "exit":
            status["lastState"]["terminated"]["exitCode"] = 143
        if mutation == "signal":
            status["lastState"]["terminated"]["signal"] = 15
        if mutation == "ready":
            other["status"]["conditions"][0]["status"] = "False"
        variants.append((True, other, "k4-chaos-" + guard))
    other = copy.deepcopy(after)
    other["status"]["containerStatuses"][0]["lastState"]["terminated"]["containerID"] = (
        "containerd://unrelated"
    )
    variants.append((True, other, "k4-chaos-death-container"))
    for ack, other, guard in variants:
        with pytest.raises(ValueError, match=guard):
            interventions.kill_proof(before, other, ack)


def test_eviction_singletons_once_no_harness_no_cordon(tmp_path):
    old = {role: [pod(role, role)] for role in interventions.ENGINE_ROLES}
    old["worker"] = [pod("w1"), pod("w2")]
    live = copy.deepcopy(old)
    calls = []

    def k(*args, **kwargs):
        body = json.loads(kwargs["data"])
        name = body["metadata"]["name"]
        calls.append(name)
        role = next(
            role for role, ps in old.items() if any(p["metadata"]["name"] == name for p in ps)
        )
        assert args[0] == "create" and "/eviction" in args[2]
        if role == "worker":
            live[role] = [p for p in live[role] if p["metadata"]["name"] != name] + [
                pod(name + "new")
            ]
            return SimpleNamespace(returncode=0, stderr="")
        return SimpleNamespace(returncode=1, stderr="TooManyRequests: disruption budget")

    a = interventions.Interventions(
        SimpleNamespace(k=k, until=lambda predicate, _: predicate()),
        "ns",
        tmp_path,
        {},
        {},
        "a" * 40,
    )
    a.pods = lambda role: live[role]
    a.get = lambda kind, role: dict(status=dict(disruptionsAllowed=1 if role == "worker" else 0))
    c = interventions.empty(CONFIRMATION)
    c.update(episode="e", intervention=10.0)
    targets = a.eviction(10.0, c)
    assert len(targets) == 2 and len(c["evictions"]) == len(calls) == 8
    assert c["no_cordon"] and not c["pdb_bypassed"] and not c["harness_targeted"]
    assert all(
        e["status"] == 429 and e["denial"] for e in c["evictions"] if e["role"] != "worker"
    ), "singleton denial not retained"
    assert all(calls.count(role) == 1 for role in interventions.ENGINE_ROLES - {"worker"}), (
        "singleton request retried"
    )
    assert all(e["scheduled"] == 10 for e in c["evictions"])


def test_pdb_missing_denial_fails(tmp_path):
    a = interventions.Interventions(Mock(), "ns", tmp_path, {}, {}, "a" * 40)
    a.pods = lambda role: [pod(role, role)]
    a.get = lambda *args: dict(status=dict(disruptionsAllowed=0))
    a.d.k.return_value = SimpleNamespace(returncode=0, stderr="")
    with pytest.raises(ValueError, match="singleton-denial-missing"):
        a.eviction(10, interventions.empty(CONFIRMATION))


def test_reconciliation_quiet_resets_and_deadline_only():
    assert (
        collectors.reconciled(
            40, all_terminal=True, witness_available=True, last_activity=10, deadline=600
        )
        == "complete"
    ), "reconciliation waited for deadline"
    assert (
        collectors.reconciled(
            40, all_terminal=True, witness_available=True, last_activity=20, deadline=600
        )
        == "waiting"
    ), "witness activity did not reset quiet"
    assert (
        collectors.reconciled(
            600, all_terminal=False, witness_available=True, last_activity=10, deadline=600
        )
        == "timeout"
    )
    with pytest.raises(ValueError, match="witness-unavailable"):
        collectors.reconciled(
            600, all_terminal=True, witness_available=False, last_activity=10, deadline=600
        )


@pytest.mark.parametrize("state", ["SUCCEEDED", "FAILED", "CANCELED", "WAITING_ACTIVITY"])
def test_empty_owner_is_never_authorized(state):
    kind = collectors.history_kind(
        dict(reason="SUBMITTED", owner="", new_state=state, node_id=None)
    )
    assert kind not in {"transition", "terminal"}, "empty owner authorized"


def test_history_receipt_is_not_result_consumption():
    assert collectors.history_kind(dict(reason="ATTEMPT_RESULT_RECORDED", owner="")) == "receipt"
    assert (
        collectors.history_kind(dict(reason="RESULT_CONSUMED", owner="scheduler")) == "transition"
    )


def test_streams_follow_every_engine_container_including_dead(tmp_path):
    streams = collectors.Streams(Mock(), "ns", tmp_path)
    streams.start = Mock()
    for event, p in [
        ("ADDED", pod()),
        ("DELETED", pod()),
        ("MODIFIED", pod(restart=1, cid="containerd://new")),
    ]:
        streams.observe_pod(json.dumps(dict(type=event, object=p)), 20)
    assert streams.start.call_count == 2, "restarted or deleted pod stream omitted"
    assert all(
        "logs" in call.args[1] and "-f" in call.args[1] and "--timestamps" in call.args[1]
        for call in streams.start.call_args_list
    )
    streams.observe_pod(json.dumps(dict(type="ADDED", object=pod("observer", "observer"))), 20)
    assert streams.start.call_count == 2


def test_log_stream_loss_fails_closed_even_after_death(tmp_path):
    s = collectors.Streams(Mock(), "ns", tmp_path)
    s.errors = ["log-old-abc: unexpected stream EOF exit 1"]
    s.pods = [dict(type="DELETED", object=pod())]
    with pytest.raises(ValueError, match="log-stream-loss"):
        s.check()
    s.errors = ["log-old-abc: unexpected stream EOF exit 0"]
    s.check()
    s.pods = []
    with pytest.raises(ValueError, match="log-stream-loss"):
        s.check()
    s.errors = ["pods: unexpected stream EOF exit 0"]
    with pytest.raises(ValueError, match="log-stream-loss"):
        s.check()


def test_deletion_does_not_invent_process_death():
    rows = collectors.session_rows([dict(type="DELETED", object=pod(), received=20)], [])
    assert rows[0]["death"] is None, "pod deletion fabricated process death"


def witness_export():
    s = interventions.empty(SESSION)
    s.update(
        session="cri",
        pod_uid="p",
        container_id="containerd://c",
        role="worker",
        start=1,
        last_alive=20,
    )
    ident = dict(namespace="n", workflow="w", node="a", iteration=0, attempt=1, token_hash="a" * 64)
    invocation = json.dumps(dict(event="start", id="v", identity=ident, at=10)) + "\n"
    trace = json.dumps(dict(event="session", at=1, timestamp="1970-01-01T00:00:02+00:00")) + "\n"
    seal = dict(
        session="s",
        pod_uid="p",
        container_id="p",
        acknowledged=["v"],
        complete=True,
        errors=0,
        streams={},
    )
    raw = {}
    for name, data in [("invocations", invocation), ("trace", trace)]:
        filename = name + ".jsonl"
        raw["/witness/" + filename] = base64.b64encode(data.encode()).decode()
        seal["streams"][name] = dict(
            file=filename, bytes=len(data), rows=1, sha256=hashlib.sha256(data.encode()).hexdigest()
        )
    raw["/witness/s.inventory.json"] = base64.b64encode(json.dumps(seal).encode()).decode()
    return raw, [s]


def test_witness_seals_cover_dead_sessions_and_acknowledged_prefix():
    raw, sessions = witness_export()
    w, vs, _ = collectors.export_seals(raw, sessions)
    assert vs[0]["start"] == 11 and w[0]["acknowledged"] == ["v"]
    sessions.append(dict(sessions[0], pod_uid="dead", container_id="old"))
    with pytest.raises(ValueError, match="dead-session-missing"):
        collectors.export_seals(raw, sessions)


@pytest.mark.parametrize("mutation", ["truncated", "hash", "ack", "missing"])
def test_witness_gaps_fail(mutation):
    raw, sessions = witness_export()
    if mutation == "truncated":
        raw["/witness/invocations.jsonl"] = base64.b64encode(b"{").decode()
    if mutation in {"hash", "ack"}:
        seal = json.loads(base64.b64decode(raw["/witness/s.inventory.json"]))
        if mutation == "hash":
            seal["streams"]["invocations"]["sha256"] = "f" * 64
        else:
            seal["acknowledged"] = []
        raw["/witness/s.inventory.json"] = base64.b64encode(json.dumps(seal).encode()).decode()
    if mutation == "missing":
        del raw["/witness/invocations.jsonl"]
    with pytest.raises(ValueError, match="k4-witness-"):
        collectors.export_seals(raw, sessions)


def test_arrival_journal_recovers_uncertain_prefix_and_retains_torn_tail():
    raw = json.dumps(dict(event="plan", plan=dict(rows=[dict(key="k")]))) + "\n"
    raw += json.dumps(dict(event="arrival", key="k", outcome="uncertain")) + "\n"
    plan, updates, torn = collectors.journal_rows(raw + '{"event":')
    assert plan["rows"][0]["key"] == "k" and updates["k"]["outcome"] == "uncertain" and torn
    with pytest.raises(ValueError, match="journal-corrupt"):
        collectors.journal_rows(raw + "bad\n")


def test_pinned_archives_verified_before_use():
    data = io.BytesIO()
    with tarfile.open(fileobj=data, mode="w:gz") as archive:
        info = tarfile.TarInfo("linux-amd64/helm")
        info.size = 3
        archive.addfile(info, io.BytesIO(b"bin"))
    blob = data.getvalue()
    item = dict(
        url="https://example.test/helm",
        sha256=hashlib.sha256(blob).hexdigest(),
        member="linux-amd64/helm",
    )

    def opener(*args, **kwargs):
        return io.BytesIO(blob)

    lock = dict(tools=dict(helm=item))
    assert locks.verify_archives(lock, opener)[0]["ok"]
    item["sha256"] = "0" * 64
    assert not locks.verify_archives(lock, opener)[0]["ok"], "bad archive checksum accepted"
    item["sha256"] = hashlib.sha256(blob).hexdigest()
    item["member"] = "absent"
    assert not locks.verify_archives(lock, opener)[0]["ok"], "missing archive member accepted"


def test_chaos_render_rejects_extra_images_and_pulls():
    values = driver.chaos_values()
    assert values["chaosDaemon"]["runtime"] == "containerd"
    assert values["chaosDaemon"]["socketPath"] == "/run/containerd/containerd.sock"
    assert not any(
        values[key]["create"] for key in ("dashboard", "dnsServer", "prometheus", "bpfki")
    )
    assert values["chaosDlv"]["enable"] is False
    images = [v for k, v in driver.LOCK["images"].items() if "chaos-" in k and "dashboard" not in k]
    objects = [
        dict(kind=kind, spec=dict(containers=[dict(image=image, imagePullPolicy="Never")]))
        for kind, image in zip(("Deployment", "DaemonSet"), images, strict=True)
    ]
    assert driver.validate_chaos_render(objects) == set(images)
    objects[0]["spec"]["containers"][0]["imagePullPolicy"] = "IfNotPresent"
    with pytest.raises(ValueError, match="unlocked-image"):
        driver.validate_chaos_render(objects)


def test_structured_logs_keep_scheduler_ack():
    data = dict(event="scheduler_acquisition", acquisition="a")
    text = "2026-01-01T00:00:00Z time=now level=INFO data=" + json.dumps(json.dumps(data))
    assert collectors.structured_log(text) == data
    assert collectors.structured_log("now " + json.dumps(dict(data=json.dumps(data)))) == data


def test_smoke_coverage_and_engineering_boundary():
    cells = [driver.episode_identity(name) for name in driver.SMOKE_CELLS]
    assert {
        "worker rollout",
        "scheduler rollout",
        "worker SIGKILL",
        "node drain/PDB eviction",
        "worker scale-down 4 to 2",
        "worker rollback",
    } <= {c["event"] for c in cells}
    assert any(c["duration"] == "long" for c in cells)
    assert [c["arm"] for c in cells[:2]] == ["baseline", "treatment"]
    assert driver.SMOKE_WINDOWS["long"] >= 45


def test_callback_without_durable_disposition_is_censored():
    from dur053_k4_evidence import assignment_stalls

    trace = [
        dict(
            event="revoke",
            at=1,
            generation=1,
            consumer_session="old",
            session="s",
            pod_uid="p",
            partitions=[],
        ),
        dict(
            event="assigned",
            at=2,
            generation=2,
            consumer_session="new",
            session="n",
            pod_uid="n",
            partitions=[],
        ),
    ]
    rows = collectors.callback_rows(trace, [], dict(history=[]), 1)
    result = assignment_stalls(rows, [], dict(invocations=[], history=[]), 30)
    assert result[0]["censored"], "callback-only assignment passed without sentinel"


def test_engineering_failure_retains_manifest_and_never_reads_ledger(tmp_path, monkeypatch):
    fake = Mock()
    fake.base.command.return_value = SimpleNamespace(stdout="a" * 40)
    fake.install.side_effect = RuntimeError("test setup failure")
    fake.cleanup.return_value = dict(status="PASS")
    monkeypatch.setattr(driver, "K4Driver", lambda *args: fake)
    monkeypatch.setattr(driver, "frozen", lambda *args: dict(sha256="a" * 64))
    monkeypatch.setattr(
        driver, "inventory", Mock(side_effect=AssertionError("engineering read ledger"))
    )
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_EVENT_NAME", "pull_request")
    out = tmp_path / "artifact"
    assert driver.run(out, tmp_path / "scratch", "lifecycle-smoke", engineering=True) == 1
    m = json.loads((out / "manifest.json").read_bytes())
    assert m["charged_seconds"] == 0 and m["measurement_claim"] is False
    assert m["budget_scope"] == "engineering-validation"
    assert "test setup failure" in (out / "driver-error.json").read_text()
    driver.inventory.assert_not_called()
    fake.build.assert_not_called()


def test_engineering_requires_pr_or_main_push(tmp_path, monkeypatch):
    fake = Mock()
    fake.base.command.return_value = SimpleNamespace(stdout="a" * 40)
    fake.cleanup.return_value = dict(status="PASS")
    fake.install.side_effect = RuntimeError("gate was bypassed")
    monkeypatch.setattr(driver, "K4Driver", lambda *args: fake)
    monkeypatch.setattr(driver, "frozen", lambda *args: dict(sha256="a" * 64))
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    out = tmp_path / "artifact"
    assert driver.run(out, tmp_path / "scratch", "lifecycle-smoke", engineering=True) == 1
    assert "k4-engineering-ci-only" in (out / "driver-error.json").read_text(), (
        "engineering dispatch admitted"
    )
    fake.install.assert_not_called()


def adapter_fixture(tmp_path, event="API rollout"):
    d = Mock()
    d.until.side_effect = lambda predicate, seconds: predicate()
    images = {
        lineage: {
            role: role + "-" + lineage + "@sha256:" + "a" * 64 for role in ("runtime", "worker")
        }
        for lineage in ("a", "a-prime")
    }
    a = interventions.Interventions(
        d,
        "ns",
        tmp_path,
        images,
        dict(
            runtime_a="a" * 64, runtime_a_prime="a" * 64, worker_a="b" * 64, worker_a_prime="b" * 64
        ),
        "a" * 40,
    )
    role = event.split()[0].lower()
    a.pods = Mock(return_value=[pod(role, role)])
    a.wait_rollout = Mock(return_value="1")
    a.set_image = Mock(return_value="2")
    return a, dict(episode="e", event=event), [dict(role=role, pod_uid=role)]


def test_api_rollout_targets_public_api_only(tmp_path):
    a, cell, overlap = adapter_fixture(tmp_path)
    record, targets = a.apply(cell, overlap)
    a.set_image.assert_called_once_with("api", "a-prime")
    assert record["target"] == "Deployment/api" and record["rollout_status"] == "complete"
    assert targets[0]["metadata"]["labels"]["app"] == "api"


def test_rollback_setup_precedes_undo_to_recorded_a(tmp_path):
    a, cell, overlap = adapter_fixture(tmp_path, "worker rollback")
    a.rollback_setup("worker")
    assert a.revisions == ("1", "2") and a.setup_completed is not None
    record, _ = a.apply(cell, overlap)
    a.d.k.assert_called_once_with(
        "-n", "ns", "rollout", "undo", "deployment/worker", "--to-revision=1"
    )
    assert record["setup_completed"] <= record["intervention"]
    a.wait_rollout.assert_called_with("worker", a.images["a"]["worker"])


def test_scale_down_observes_four_to_two_without_steering(tmp_path):
    a, cell, _ = adapter_fixture(tmp_path, "worker scale-down 4 to 2")
    old = [pod("w" + str(i)) for i in range(4)]
    a.pods.side_effect = [old, old[:2], old[:2]]
    a.release_after_deletion = Mock()
    c, removed = a.apply(cell, [dict(role="worker", pod_uid="w2")])
    assert {p["metadata"]["uid"] for p in removed} == {"w2", "w3"}
    assert c["overlap"][0]["pod_uid"] == "w2" and not c["deletion_cost_used"]
    a.d.k.assert_called_once_with("-n", "ns", "scale", "deployment/worker", "--replicas=2")
    a.wait_rollout.assert_called_once_with("worker")


def test_scale_down_refuses_incorrect_initial_replicas(tmp_path):
    a, cell, overlap = adapter_fixture(tmp_path, "worker scale-down 4 to 2")
    with pytest.raises(ValueError, match="scale-start-four"):
        a.apply(cell, overlap)
    a.d.k.assert_not_called()


def test_intervention_without_overlap_is_unexercised(tmp_path):
    a, cell, _ = adapter_fixture(tmp_path)
    with pytest.raises(ValueError, match="unexercised-overlap"):
        a.apply(cell, [])
    a.set_image.assert_not_called()


def test_intervention_refuses_other_role_ack(tmp_path):
    a, cell, _ = adapter_fixture(tmp_path)
    with pytest.raises(ValueError, match="overlap-wrong-target"):
        a.apply(cell, [dict(role="worker-control", pod_uid="worker-control")])
    a.set_image.assert_not_called()


def test_kill_uses_only_witnessed_container(tmp_path):
    a, cell, overlap = adapter_fixture(tmp_path, "worker SIGKILL")
    a.pods.return_value = [pod("idle"), pod("worker")]
    a.kill = Mock()
    record, targets = a.apply(cell, overlap)
    a.kill.assert_called_once_with(a.pods.return_value[1])
    assert record["injection_status"] == "injected" and len(targets) == 1


def test_expired_watch_fails_instead_of_resyncing_over_a_gap(tmp_path):
    s = collectors.Streams(Mock(), "ns", tmp_path)
    with pytest.raises(ValueError, match="pod-watch-error"):
        s.observe_pod(json.dumps(dict(type="ERROR", object=dict(code=410))), 10)


def test_lineages_differ_only_by_the_locked_label():
    configs = {
        lineage: {
            role: dict(
                Config=dict(Labels={"dur053.k4.lineage": lineage}, Env=["same"]),
                RootFS=dict(Layers=["same"]),
            )
            for role in ("runtime", "worker")
        }
        for lineage in ("a", "a-prime")
    }
    driver.lineage_identity(configs)
    for field in ("Config", "RootFS"):
        changed = copy.deepcopy(configs)
        if field == "Config":
            changed["a-prime"]["runtime"][field]["Env"] = ["changed"]
        else:
            changed["a-prime"]["worker"][field]["Layers"] = ["changed"]
        with pytest.raises(ValueError, match="label-only-images"):
            driver.lineage_identity(changed)
    changed = copy.deepcopy(configs)
    changed["a-prime"]["runtime"]["Config"]["Labels"]["dur053.k4.lineage"] = "a"
    with pytest.raises(ValueError, match="image-lineage-label"):
        driver.lineage_identity(changed)


def test_registered_progress_is_whole_unit_prefix(tmp_path, monkeypatch):
    fake = Mock()
    fake.fatal = False
    fake.base.command.return_value = SimpleNamespace(stdout="a" * 40)
    fake.cleanup.return_value = dict(status="PASS")
    fake.execute_episode.side_effect = [
        dict(valid=True, status="PASS"),
        dict(valid=True, status="PASS"),
    ]
    monkeypatch.setattr(driver, "K4Driver", lambda *args: fake)
    monkeypatch.setattr(driver, "frozen", lambda *args: dict(sha256="a" * 64))
    monkeypatch.setattr(driver, "inventory", lambda *args: ([], dict(entries=[])))
    monkeypatch.setattr(driver, "committed_ledger", lambda *args: {})
    monkeypatch.setattr(driver, "ledger_check", lambda *args: 0)
    monkeypatch.setattr(driver, "ci_proof", lambda *args: {})
    monkeypatch.setattr(driver, "github", lambda *args: {})
    monkeypatch.setattr(driver, "job_identity_and_history", lambda: ("200", set(), "a" * 40))
    unit = "short-core-0-pair-0"
    monkeypatch.setattr(driver, "assignment", lambda *args, **kwargs: [unit])
    monkeypatch.setattr(
        driver,
        "registry",
        lambda: [dict(episode=unit + "-" + arm, unit=unit) for arm in ("baseline", "treatment")],
    )
    monkeypatch.setattr(driver, "admit", lambda *args, **kwargs: 1000)
    monkeypatch.setattr(driver, "summaries", lambda *args: {})
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setenv("GITHUB_REPOSITORY", "example/repo")
    monkeypatch.setenv("GITHUB_RUN_ID", "100")
    out = tmp_path / "artifact"
    assert driver.run(out, tmp_path / "scratch", "k4-1") == 0
    progress = json.loads((out / "progress.json").read_bytes())
    assert progress["started"] == [unit], "progress split the admitted pair into episode IDs"
    assert fake.execute_episode.call_count == 2


def test_voluntary_interventions_require_observed_term():
    c = dict(kind="rollout", completed=20, targets=[dict(session="s")])
    sessions = [dict(session="s", signals=[])]
    with pytest.raises(ValueError, match="term-observation-missing"):
        interventions.require_term_evidence(c, sessions)
    sessions[0]["signals"] = [dict(signal=15)]
    interventions.require_term_evidence(c, sessions)
    c["kind"] = "kill"
    sessions[0]["signals"] = []
    interventions.require_term_evidence(c, sessions)


def test_one_second_observer_uses_bounded_light_export(tmp_path):
    d = driver.K4Driver.__new__(driver.K4Driver)
    d.base = Mock()
    d.episode_output = tmp_path
    d.base.sql.return_value = json.dumps(dict(clock=10, connections=[dict(count=2)]))
    d.snapshot("db", light=True)
    sql = d.base.sql.call_args.args[0]
    assert sql.startswith("/* K4 observer */ SET statement_timeout=")
    assert "BEGIN READ ONLY" in sql
    assert "k4_history_order" not in sql and "k4_lease_audit" not in sql
    assert "WHERE is_current AND state='CLAIMED'" in sql
    d.snapshot("db")
    sql = d.base.sql.call_args.args[0]
    assert "engine.k4_history_order" in sql and "engine.k4_lease_audit" in sql


def test_observer_connection_breach_fails_closed(tmp_path):
    d = driver.K4Driver.__new__(driver.K4Driver)
    d.base = Mock()
    d.episode_output = tmp_path
    d.base.sql.return_value = json.dumps(dict(clock=10, connections=[dict(count=93)]))
    with pytest.raises(ValueError, match="observed-connection-budget"):
        d.snapshot("db", light=True)


def test_claim_without_invocation_is_retained_without_inventing_execution():
    a = dict(
        workflow_id="w",
        node_id="sleep",
        iteration=0,
        attempt_number=1,
        worker_id="pod-0",
        worker_request_id="request",
        token_hash="a" * 64,
    )
    body = dict(worker_id="pod-0", request_id="request", attempt_lease_ms=60000, expected_attempt=1)
    digest = hashlib.sha256(json.dumps(body, separators=(",", ":")).encode()).hexdigest()
    traces = [
        dict(
            event="http",
            operation="claim",
            body_hash=digest,
            status=200,
            error="",
            session="s",
            at=10,
        )
    ]
    claims = collectors.durable_claims([a], traces, "ns")
    assert len(claims) == 1 and claims[0]["identity"]["workflow"] == "w", (
        "unexecuted claim was omitted"
    )
    assert claims[0]["at"] == 10 and claims[0]["session"] == "s"
    with pytest.raises(ValueError, match="claim-ack-clock-evidence-missing"):
        collectors.durable_claims([a], [], "ns")
