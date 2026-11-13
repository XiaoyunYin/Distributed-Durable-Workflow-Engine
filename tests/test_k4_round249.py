"""Exact Round 249 log/CRI lines, with trimmed credential-free pod watches."""

import copy
import json
import sys
import threading
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import Mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import dur053_k4_collectors as collectors  # noqa: E402
import dur053_k4_driver as driver  # noqa: E402
import dur053_k4_interventions as interventions  # noqa: E402
import dur053_record_k4 as recorder  # noqa: E402
from dur053_k3_policy import require  # noqa: E402


@pytest.fixture
def retained():
    return json.loads((Path(__file__).parent / "fixtures/k4-round249-process.json").read_bytes())


def test_retained_signal_matches_precise_init_exit(retained):
    sessions = collectors.session_rows(retained["pods"], [])
    assert all(s["death"] is None for s in sessions), "rounded finish used as exact death"
    collectors.apply_process_evidence(sessions, retained["logs"], retained["cri"])
    assert len(sessions) == 2 and all(len(s["signals"]) == 1 for s in sessions)
    for s in sessions:
        assert s["signals"][0]["at"] < s["death"] < s["signals"][0]["at"] + 1
        assert s["death_evidence"].startswith("containerd-task-exit:")
    exact = next(s for s in sessions if s["pod_uid"].startswith("6a9532cb"))
    assert exact["death"] == 1794589572 + 874221357 / 1e9


def test_retained_exec_exit_is_not_container_death(retained):
    assert retained["exec_exits"]
    for event in retained["exec_exits"]:
        raw = json.loads(event[2][event[2].index("{") :])
        assert raw["id"] != raw["container_id"]
        sessions = [dict(container_id="containerd://" + raw["container_id"])]
        collectors.apply_process_evidence(sessions, [], [event])
        assert "death" not in sessions[0], "exec exit counted as container death"


def test_cri_exit_cross_checks_rounded_finish(retained):
    for delta in (-1, 1):
        events = copy.deepcopy(retained["pods"])
        term = events[0]["object"]["status"]["containerStatuses"][0]["state"]["terminated"]
        term["finishedAt"] = datetime.fromtimestamp(
            collectors.utc(term["finishedAt"]) + delta, UTC
        ).isoformat()
        with pytest.raises(ValueError, match="k4-cri-finished-bound"):
            collectors.apply_process_evidence(
                collectors.session_rows(events, []), [], retained["cri"]
            )


def test_log_eof_waits_for_retained_init_exit(tmp_path, monkeypatch, retained):
    now = [100.0]
    monkeypatch.setattr(collectors.time, "monotonic", lambda: now[0])
    received = retained["treatment_cri"][0][1]
    monkeypatch.setattr(collectors.time, "time", lambda: received - 14.9 + now[0] - 100)
    s = collectors.Streams(Mock(), "ns", tmp_path)
    s.errors = [retained["treatment_error"]["error"].split("k4-log-stream-loss: ")[1]]
    s.check()  # No pod watch death yet, and no CRI exit delivered yet.
    now[0] += 14.9
    for _, at, text in retained["treatment_cri"]:
        s.observe_cri(text, at)
    assert s.cri_exits
    s.check()
    now[0] += 100
    s.check()


def test_log_eof_without_exact_death_expires(tmp_path, monkeypatch, retained):
    now = [100.0]
    monkeypatch.setattr(collectors.time, "monotonic", lambda: now[0])
    s = collectors.Streams(Mock(), "ns", tmp_path)
    s.errors = [retained["treatment_error"]["error"].split("k4-log-stream-loss: ")[1]]
    s.check()
    # Unrelated container death, even in the same pod, cannot authorize EOF.
    label = s.errors[0].split(":", 1)[0]
    uid = label[4:].rsplit("-", 1)[0]
    s.pods = [
        dict(
            type="MODIFIED",
            object=dict(
                metadata=dict(uid=uid),
                status=dict(
                    containerStatuses=[
                        dict(containerID="containerd://other", state=dict(terminated={}))
                    ]
                ),
            ),
        )
    ]
    s.cri_exits = [dict(container_id="other", received=0)]
    now[0] += 15
    with pytest.raises(ValueError, match="k4-log-stream-loss"):
        s.check()


def test_log_eof_late_evidence_does_not_reset_grace(tmp_path, monkeypatch, retained):
    monkeypatch.setattr(collectors.time, "monotonic", lambda: 100)
    s = collectors.Streams(Mock(), "ns", tmp_path)
    s.errors = [retained["treatment_error"]["error"].split("k4-log-stream-loss: ")[1]]
    s.check()
    cutoff = next(iter(s.eof_received.values())) + 15
    for _, _, text in retained["treatment_cri"]:
        s.observe_cri(text, cutoff + 0.1)
    monkeypatch.setattr(collectors.time, "monotonic", lambda: 116)
    with pytest.raises(ValueError, match="k4-log-stream-loss"):
        s.check()


def test_apply_safety_propagates_to_infrastructure(tmp_path, monkeypatch, retained):
    d = driver.K4Driver.__new__(driver.K4Driver)
    d.output = tmp_path
    d.engineering = False
    d.images = {"a": dict(runtime="runtime-image", worker="worker-image")}
    d.proof, d.source, d.namespaces = {}, "a" * 40, {"ns"}
    d.setup = Mock(return_value=("ns", "db"))
    d.start_samples = Mock()
    d.check_observation = Mock()
    d.hold_state = Mock(
        return_value=dict(
            host_start=0, host_end=0, observer=0, path="hold", existed=False, exists=True, mtime=0
        )
    )
    d.overlap = Mock(return_value=[dict(role="worker")])
    d.load = Mock(return_value=Mock(returncode=0))
    d.base = Mock()
    d.observer_stop = threading.Event()
    d.sampler = Mock()
    d.collector = Mock()
    adapter = Mock()
    adapter.intervention_issued = 110
    error = retained["treatment_error"]["error"].split("k4-log-stream-loss: ")[1]
    adapter.apply.side_effect = lambda *a: require(False, "k4-log-stream-loss: " + error)
    monkeypatch.setattr(driver, "Interventions", lambda *a: adapter)
    monkeypatch.setattr(
        driver, "parameters", lambda *a: dict(rate=1, L=1, W=1, warmup=0, confirmation=20)
    )
    clock = iter(range(100, 10000))
    monkeypatch.setattr(driver.time, "time", lambda: next(clock))
    monkeypatch.setattr(driver.time, "sleep", lambda _: None)
    cell = dict(
        episode="short-core-0-pair-0-treatment",
        duration="short",
        arm="treatment",
        seed=1,
        event="worker rollout",
    )
    job = dict(
        job="j",
        run_id="1",
        job_id="2",
        head_sha="a" * 40,
        tree_sha="b" * 40,
        budget_scope="engineering-validation",
        measurement_claim=False,
    )
    with pytest.raises(ValueError, match="k4-log-stream-loss"):
        d.execute_episode(cell, {}, dict(sha256="c" * 64), job, None)
    assert adapter.apply.call_count == 1
    assert not (d.episode_output / "intervention-error.json").exists()
    assert not (d.episode_output / "unexercised.json").exists()
    # Exercise the same content check used by the outer harness-incident path.
    name = cell["episode"]
    source = dict(component="collector", errors=["k4-log-stream-loss: " + error], missing=[])
    driver.save(d.episode_output / "harness-source.json", source)
    driver.locked_document(
        d.episode_output / "harness-incident.json",
        dict(
            schema="dur053.k4-harness.v1",
            episode=name,
            reason="collection-failure",
            component="collector",
            at=500,
            warmup_start=103,
            source=name + "/harness-source.json",
        ),
    )
    recorder.validate_disposition(
        tmp_path,
        dict(
            episode=name,
            classification="infrastructure-invalid",
            reason="collection-failure",
            evidence=[name + "/harness-incident.json"],
        ),
    )


def test_safety_outcome_guard_list():
    assert not interventions.SAFETY_GUARDS & interventions.OUTCOME_GUARDS
    for guard in interventions.SAFETY_GUARDS | {"k4-watch-new", "k4-stream-new", "unknown"}:
        assert not interventions.intervention_outcome(ValueError("K3 guard: " + guard))
    for guard in interventions.OUTCOME_GUARDS:
        assert interventions.intervention_outcome(ValueError("K3 guard: " + guard))
    assert not interventions.intervention_outcome(RuntimeError("k4-rollout-incomplete"))
    assert not interventions.intervention_outcome(
        ValueError("wrapper K3 guard: k4-rollout-incomplete")
    )


def test_completion_deadline_preserves_predicate_safety_errors():
    d = Mock()
    a = interventions.Interventions(d, "ns", None, {}, {}, "a" * 40)
    d.until.side_effect = TimeoutError("deadline")
    with pytest.raises(ValueError, match="k4-rollout-incomplete") as caught:
        a.wait(lambda: False, 180, "k4-rollout-incomplete")
    assert interventions.intervention_outcome(caught.value)

    def until(predicate, seconds):
        predicate()

    d.until.side_effect = until

    def safety():
        raise TimeoutError("collector command deadline")

    with pytest.raises(TimeoutError, match="collector command deadline") as caught:
        a.wait(safety, 180, "k4-rollout-incomplete")
    assert not interventions.intervention_outcome(caught.value)
