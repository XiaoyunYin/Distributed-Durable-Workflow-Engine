import hashlib
import json
import signal
import sys
import zipfile
from copy import deepcopy
from pathlib import Path
from subprocess import CompletedProcess
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))

import j2_harness as harness  # noqa: E402
import j2_harness_evidence as evidence  # noqa: E402

RUN = "125"
ATTEMPT = "2"
HEAD = "a" * 40


@pytest.fixture
def identity_env(monkeypatch):
    for key, value in {
        "GITHUB_RUN_ID": RUN,
        "GITHUB_RUN_ATTEMPT": ATTEMPT,
        "GITHUB_SHA": HEAD,
        "GITHUB_JOB": "harness-reset",
    }.items():
        monkeypatch.setenv(key, value)


def test_hosted_guard_rejects_private_and_nondispatch(tmp_path, monkeypatch):
    event = tmp_path / "event.json"
    monkeypatch.setenv("GITHUB_EVENT_PATH", str(event))
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("RUNNER_ENVIRONMENT", "github-hosted")
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    harness.write(event, {"repository": {"private": True}})
    with pytest.raises(ValueError, match="public GitHub-hosted"):
        harness.hosted_dispatch()
    harness.write(event, {"repository": {"private": False}})
    harness.hosted_dispatch()
    monkeypatch.setenv("GITHUB_EVENT_NAME", "pull_request")
    with pytest.raises(ValueError, match="workflow_dispatch"):
        harness.hosted_dispatch()


def topic_fixture():
    topics = dict(zip(harness.TOPICS.values(), ("id-a", "id-b"), strict=True))
    description = "\n".join(
        f"Topic: {topic}\tTopicId: {identity}\tPartitionCount: 8\tReplicationFactor: 1"
        for topic, identity in topics.items()
    )
    offsets = "".join(f"{topic}:{part}:0\n" for topic in topics for part in range(8))
    return topics, description, offsets


def test_topic_verification_rejects_missing_duplicate_and_wrong_partitions():
    topics, description, offsets = topic_fixture()
    assert harness.topic_state(description, offsets)[0] == topics
    with pytest.raises(ValueError, match="missing Kafka offsets"):
        harness.topic_state(description, "\n".join(offsets.splitlines()[:-1]))
    with pytest.raises(ValueError, match="offset row"):
        harness.topic_state(description, offsets + offsets.splitlines()[0])
    with pytest.raises(ValueError, match="partition count"):
        harness.topic_state(description.replace("PartitionCount: 8", "PartitionCount: 7"), offsets)


def test_assigned_group_offsets_reject_stale_commits_and_unassigned_partitions():
    group = "runtime-workers-v1"
    topic = harness.TOPICS[group]
    raw = "\n".join(
        f"{group} {topic} {i} - 0 - member-{i % 4} host client-{i % 4}" for i in range(8)
    )
    assert harness.group_state(raw, group, empty=True) == [0] * 8
    with pytest.raises(ValueError, match="stale Kafka group offset"):
        harness.group_state(raw.replace("0 - 0", "0 1 1"), group, empty=True)
    with pytest.raises(ValueError, match="invalid Kafka assignment"):
        harness.group_state(raw.replace("member-3", "-"), group, empty=True)


def witness_fixture():
    records = []
    files = [f"worker-{i}/session-{i}.jsonl" for i in range(2)]
    for i, path in enumerate(files):
        pod, file = path.split("/")
        uid, session = f"pod-uid-{i}", f"session-{i}"
        raw = json.dumps({"event": "session", "pod_uid": uid, "session": session}) + "\n"
        records.append(
            (
                uid,
                pod,
                {
                    "ledger": raw,
                    "files": list(files),
                    "inventory": {
                        "pod_uid": uid,
                        "pod_name": pod,
                        "session": session,
                        "file": file,
                        "complete": True,
                        "errors": 0,
                        "attempted": 1,
                        "written": 1,
                        "sha256": hashlib.sha256(raw.encode()).hexdigest(),
                    },
                },
            )
        )
    return records


def test_fresh_witness_allows_session_markers_and_rejects_stale_files():
    records = witness_fixture()
    assert harness.verify_witness(records, empty=True) == 0
    records[0][2]["files"].append("old-worker/stale.jsonl")
    with pytest.raises(ValueError, match="missing or stale files"):
        harness.verify_witness(records, empty=True)


def test_fresh_witness_rejects_real_invocation_even_when_attested():
    records = witness_fixture()
    uid, _, data = records[0]
    data["ledger"] += (
        json.dumps({"event": "start", "pod_uid": uid, "session": data["inventory"]["session"]})
        + "\n"
    )
    data["inventory"].update(
        attempted=2, written=2, sha256=hashlib.sha256(data["ledger"].encode()).hexdigest()
    )
    assert harness.verify_witness(records, empty=False) == 1
    with pytest.raises(ValueError, match="prior invocation"):
        harness.verify_witness(records, empty=True)


def test_fresh_identity_refuses_reused_namespace_database_topic_volume_or_pod():
    before = {
        "namespaces": {"app": "old-app", "monitor": "old-monitor"},
        "database_oid": 41,
        "witness_pvc": "old-claim",
        "witness_pv": "old-volume",
        "pods": {"worker-a": "old-worker"},
        "topics": {"tasks": "old-tasks", "events": "old-events"},
    }
    after = {
        "namespaces": {"app": "new-app", "monitor": "new-monitor"},
        "database_oid": 42,
        "witness_pvc": "new-claim",
        "witness_pv": "new-volume",
        "pods": {"worker-b": "new-worker"},
        "topics": {"tasks": "new-tasks", "events": "new-events"},
    }
    harness.verify_new_identity(before, after)
    for field, old in before.items():
        with pytest.raises(ValueError, match=f"reused identity: {field}"):
            harness.verify_new_identity(before, {**after, field: old})


def spec_fixture():
    digest = "sha256:" + "a" * 64
    container = {
        "name": "api",
        "image": f"runtime@{digest}",
        "resources": {
            "limits": {"cpu": "1000m", "memory": "256Mi"},
            "requests": {"cpu": "100m", "memory": "64Mi"},
        },
    }
    rendered = [
        {
            "kind": "Deployment",
            "metadata": {"namespace": harness.APP, "name": "api"},
            "spec": {"template": {"spec": {"containers": [container]}}},
        }
    ]
    live = deepcopy(container)
    live["resources"]["limits"]["cpu"] = "1"
    pod = {
        "metadata": {
            "namespace": harness.APP,
            "name": "api-a",
            "labels": {"app": "api"},
        },
        "spec": {"containers": [live]},
        "status": {
            "phase": "Running",
            "conditions": [{"type": "Ready", "status": "True"}],
            "containerStatuses": [
                {"name": "api", "imageID": f"runtime@{digest}", "ready": True, "restartCount": 0}
            ],
        },
    }
    return pod, rendered, digest


def test_live_spec_rejects_resource_and_image_drift():
    pod, rendered, _ = spec_fixture()
    harness.verify_container_specs([pod], rendered)
    pod["spec"]["containers"][0]["resources"]["limits"]["cpu"] = "900m"
    with pytest.raises(ValueError, match="resources differ"):
        harness.verify_container_specs([pod], rendered)
    pod["spec"]["containers"][0]["resources"]["limits"]["cpu"] = "1"
    pod["status"]["containerStatuses"][0]["imageID"] = "sha256:" + "b" * 64
    with pytest.raises(ValueError, match="image or resources"):
        harness.verify_container_specs([pod], rendered)


def test_readiness_fails_on_unready_and_missing_pods():
    pod, _, digest = spec_fixture()
    pod["status"]["containerStatuses"][0]["ready"] = False
    with pytest.raises(ValueError, match="not Ready"):
        harness.verify_pods([pod], {digest})
    pod["status"]["containerStatuses"][0]["ready"] = True
    with pytest.raises(ValueError, match="unexpected Ready replicas"):
        harness.verify_pods([pod], {digest})


def test_fresh_setup_quiesces_and_resets_transport_before_deploy(tmp_path, monkeypatch):
    calls = []
    driver = harness.Harness(tmp_path, tmp_path)
    (tmp_path / "package-kind").mkdir()
    previous = {"topics": {"old": "id"}}
    monkeypatch.setattr(driver, "remove_episode", lambda: calls.append("remove") or {})
    monkeypatch.setattr(driver, "wait_for_departed_consumers", lambda: calls.append("depart"))
    monkeypatch.setattr(driver, "reset_transport", lambda: calls.append("reset"))
    monkeypatch.setattr(
        harness, "apply_profile", lambda *args: pytest.fail("started pods before transport proof")
    )

    def transport(phase, *, empty):
        assert phase == "before-workloads" and empty
        calls.append("verify")
        harness.write(
            driver.output / "before-workloads-transport.json", {"groups": "runtime-workers-v1"}
        )
        return {}

    monkeypatch.setattr(driver, "transport", transport)
    with pytest.raises(ValueError, match="prior Kafka consumer state"):
        driver.fresh_episode("grid-reset", {}, "exporter", previous)
    assert calls == ["remove", "depart", "reset", "verify"]
    assert (tmp_path / "grid-reset/previous-cleanup.json").exists()


def test_partial_fixture_keeps_real_plan_and_supervisor_journal(
    tmp_path, identity_env, monkeypatch
):
    driver = harness.Harness(tmp_path, tmp_path)
    monkeypatch.setattr(driver, "pod", lambda *_: {"metadata": {"name": "observer"}})
    ids = [f"key-{i}" for i in range(harness.INTERRUPTED_COUNT)]
    run_id = f"j2-reset-{RUN}-{ATTEMPT}-partial-failure"
    plan = {
        "run_id": run_id,
        "offered": len(ids),
        "status": "IN_PROGRESS",
        "rows": [{"workflow_id": key} for key in ids],
    }
    events = [
        {"event": "started"},
        {"event": "committed", "ids": ids[:1]},
        {"event": "stopped", "exit_code": -signal.SIGTERM},
    ]
    raw = "\n".join(json.dumps(event) for event in events)

    def dummy_exec(*args, **kwargs):
        output = json.dumps(plan) if "cat" in args else raw
        return CompletedProcess(args, 0, output, "")

    monkeypatch.setattr(driver, "k", dummy_exec)
    monkeypatch.setattr(driver, "sql", lambda *_: ids[0])
    with pytest.raises(harness.InjectedStop, match=run_id):
        driver.load("partial-failure", partial=True)
    assert harness.read(tmp_path / "arrivals.json") == plan
    assert harness.read(tmp_path / "admissions-after-stop.json") == ids[:1]
    assert (tmp_path / "submission-events.jsonl").read_text() == raw
    events[1]["ids"] = ["outside-plan"]
    raw = "\n".join(json.dumps(event) for event in events)
    with pytest.raises(ValueError, match="durable partial submission"):
        driver.load("partial-failure", partial=True)


def test_closeout_attempts_cluster_cleanup_after_namespace_failure(tmp_path):
    events = []

    def remove():
        events.append("namespace")
        raise RuntimeError("namespace stuck")

    driver = SimpleNamespace(
        evidence=tmp_path,
        output=tmp_path,
        cluster_started=True,
        retain_stop=lambda: events.append("snapshot"),
        remove_episode=remove,
        cleanup=lambda: events.append("cluster") or {"status": "PASS", "owned_nodes": []},
    )
    assert harness.closeout(driver) is False
    assert events == ["snapshot", "namespace", "cluster"]
    record = harness.read(tmp_path / "cleanup.json")
    assert record == {
        "status": "FAIL",
        "owned_nodes": [],
        "errors": {"episode_cleanup": "namespace stuck"},
    }


def test_attempt_archive_verifies_identity_index_and_member_bytes(tmp_path, identity_env):
    folder = tmp_path / "evidence"
    folder.mkdir()
    raw = folder / "partial.json"
    raw.write_text("partial")
    evidence.seal(folder)
    before = harness.read(folder / evidence.INDEX)
    with pytest.raises(ValueError, match="already sealed"):
        evidence.seal(folder)
    assert before == harness.read(folder / evidence.INDEX)

    def bundled():
        archive = tmp_path / "evidence.zip"
        with zipfile.ZipFile(archive, "w") as zipped:
            for file in folder.iterdir():
                zipped.write(file, arcname=file.name)
        digest = harness.file_record(archive)["sha256"]
        metadata = {
            "id": 1,
            "expired": False,
            "expires_at": "2027-02-24T00:00:00Z",
            "workflow_run": {"id": int(RUN), "head_sha": HEAD},
            "digest": f"sha256:{digest}",
            "name": f"j2-harness-reset-{RUN}-{ATTEMPT}",
        }
        return archive, metadata, "1", digest

    args = bundled()
    evidence.verify_upload(*args)
    with pytest.raises(ValueError, match="identity, expiry or digest"):
        evidence.verify_upload(*args[:-1], "b" * 64)
    raw.write_text("changed")
    with pytest.raises(ValueError, match="ZIP digest"):
        evidence.verify_upload(*bundled())
    (folder / "unindexed.json").write_text("extra")
    with pytest.raises(ValueError, match="membership"):
        evidence.verify_upload(*bundled())


def test_archive_refuses_hidden_members_and_reused_paths(tmp_path, identity_env):
    (tmp_path / ".hidden").write_text("hidden")
    with pytest.raises(ValueError, match="unsafe evidence path"):
        evidence.seal(tmp_path)
    assert not evidence.safe_name("../escape")
    assert not evidence.safe_name("a//b")
    assert not evidence.safe_name(r"a\b")
