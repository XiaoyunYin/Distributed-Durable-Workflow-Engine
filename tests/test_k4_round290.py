"""Engineering-only long-drain coverage, independent of rollout victim/latency."""

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import dur053_k4_driver as driver  # noqa: E402


def clock(monkeypatch):
    now = [100.0]
    monkeypatch.setattr(driver.time, "time", lambda: now[0])
    monkeypatch.setattr(driver.time, "sleep", lambda seconds: now.__setitem__(0, now[0] + seconds))
    return now


def held(uid, at=100, node="dur048.sleep"):
    return dict(
        session=uid,
        pod_uid=uid,
        at=at,
        claim=dict(node=node),
        acknowledgment="durable-witness:" + uid,
    )


def harness(tmp_path):
    d = object.__new__(driver.K4Driver)
    d.episode_output = tmp_path
    d.check_observation = lambda: None
    pods = [
        dict(metadata=dict(uid=uid), status=dict(conditions=[dict(type="Ready", status="True")]))
        for uid in ("a", "b")
    ]
    adapter = SimpleNamespace(pods=lambda role: pods)
    hold = dict(host_start=99, host_end=100, observer=100)
    return d, adapter, hold


def test_selection_excludes_foreign_pods_and_fast_or_prehold_work(tmp_path, monkeypatch):
    now = clock(monkeypatch)
    d, adapter, hold = harness(tmp_path)
    reads = []

    def overlap(*args):
        reads.append(now[0])
        return [held("a"), held("b", 99), held("b", node="pure.echo"), held("foreign")]

    d.overlap = overlap
    result = d.engineering_drain_overlap("ns", adapter, hold, 200)
    assert result == [held("a")], "R317 selected session is not an old worker held dur048.sleep"
    assert reads == [100.0] and now[0] == 100.0
    proof = json.loads((tmp_path / "engineering-long-drain.json").read_bytes())
    assert proof["targets"] == ["a", "b"] and proof["deadline"] == 159


@pytest.mark.parametrize("first_deleted", ["a", "b"])
def test_arrival_zero_only_issues_at_once_and_waits_for_held_worker_signal(
    tmp_path, monkeypatch, first_deleted
):
    now = clock(monkeypatch)
    d, adapter, hold = harness(tmp_path)
    # Arrival 0 has entered its held sleep on a; the other old worker is idle.
    d.overlap = lambda *args: [held("a")]
    selected = d.engineering_drain_overlap("ns", adapter, hold, 200)
    assert selected == [held("a")] and now[0] == 100.0, (
        "R317 arrival-0-only rollout waited for idle old worker"
    )
    d.collector = SimpleNamespace(live=SimpleNamespace(read=lambda: []))
    d.witness_raw = lambda ns: {}
    signal_at = 101 if first_deleted == "a" else 104

    def seals(*args):
        traces = []
        if now[0] >= 101:
            traces.append(dict(event="signal", signal=15, session=first_deleted, at=101))
        if now[0] >= signal_at:
            traces.append(dict(event="signal", signal=15, session="a", at=signal_at))
        return [], [dict(id="a", session="a", finish=None)], traces

    monkeypatch.setattr(driver, "export_seals", seals)
    released = []
    # The adapter calls release on the first observed deletion of either pod.
    d.release_engineering_drain("ns", selected, hold, 200, lambda: released.append(now[0]))
    assert len(released) == 1 and signal_at <= released[0] < signal_at + 0.051, (
        "R317 arrival-0 hold released before selected worker SIGTERM"
    )
    proof = json.loads((tmp_path / "engineering-long-drain-signal.json").read_bytes())
    assert {t["session"] for t in proof["signals"]} == {"a"}
    assert proof["observed_at"] - selected[0]["at"] == pytest.approx(released[0] - 100)


def test_unready_old_worker_does_not_qualify(tmp_path, monkeypatch):
    clock(monkeypatch)
    d, adapter, hold = harness(tmp_path)
    adapter.pods("worker")[0]["status"]["conditions"][0]["status"] = "False"
    d.overlap = lambda *args: [held("a")]
    with pytest.raises(ValueError, match="k4-engineering-long-drain-target-timeout"):
        d.engineering_drain_overlap("ns", adapter, hold, 100.12)


@pytest.mark.parametrize("deadline,expected", [(100.12, 100.12), (200, 159)])
def test_target_wait_timeout_is_distinct_and_bounded(tmp_path, monkeypatch, deadline, expected):
    now = clock(monkeypatch)
    d, adapter, hold = harness(tmp_path)
    d.overlap = lambda *args: [held("foreign"), held("a", node="pure.echo"), held("b", 99)]
    with pytest.raises(ValueError, match="k4-engineering-long-drain-target-timeout"):
        d.engineering_drain_overlap("ns", adapter, hold, deadline)
    assert expected <= now[0] < expected + 0.051
    assert not (tmp_path / "engineering-long-drain.json").exists()


def test_slow_target_read_cannot_pass_after_deadline(tmp_path, monkeypatch):
    now = clock(monkeypatch)
    d, adapter, hold = harness(tmp_path)

    def overlap(*args):
        now[0] = 101
        return [held("a"), held("b")]

    d.overlap = overlap
    with pytest.raises(ValueError, match="k4-engineering-long-drain-target-timeout"):
        d.engineering_drain_overlap("ns", adapter, hold, 100.5)


@pytest.mark.parametrize("victim", ["a", "b"])
def test_release_waits_for_one_actual_target_term(tmp_path, monkeypatch, victim):
    now = clock(monkeypatch)
    d, _, hold = harness(tmp_path)
    d.collector = SimpleNamespace(live=SimpleNamespace(read=lambda: []))
    d.witness_raw = lambda ns: {}
    invocations = [dict(id=uid, session=uid, finish=None) for uid in ("a", "b")]

    def seals(*args):
        # Deletion happened at t=100; SIGTERM is delayed by 16.18 s. Pure
        # elapsed-age selection would be insufficient; held work has not begun.
        traces = [
            dict(event="signal", signal=9, session=victim, at=100),
            dict(event="signal", signal=15, session="foreign", at=100),
            dict(event="signal", signal=15, session=victim, at=99),
        ]
        if now[0] >= 116.18:
            traces.append(dict(event="signal", signal=15, session=victim, at=116.18))
        return [], invocations, traces

    monkeypatch.setattr(driver, "export_seals", seals)
    released = []
    d.release_engineering_drain(
        "ns", [held("a"), held("b")], hold, 200, lambda: released.append(now[0])
    )
    assert len(released) == 1 and 116.18 <= released[0] < 116.231, (
        "R317 hold released before qualifying target SIGTERM"
    )
    assert released[0] + 45 > 116.18 + 30 + 5
    proof = json.loads((tmp_path / "engineering-long-drain-signal.json").read_bytes())
    assert len(proof["signals"]) == 1 and proof["signals"][0]["session"] == victim


@pytest.mark.parametrize("finish", [None, 100.01])
def test_signal_wait_timeout_is_distinct_and_bounded(tmp_path, monkeypatch, finish):
    now = clock(monkeypatch)
    d, _, hold = harness(tmp_path)
    d.collector = SimpleNamespace(live=SimpleNamespace(read=lambda: []))
    d.witness_raw = lambda ns: {}
    monkeypatch.setattr(
        driver,
        "export_seals",
        lambda *args: (
            [],
            [dict(id="a", session="a", finish=finish)],
            [] if finish is None else [dict(event="signal", signal=15, session="a", at=100.02)],
        ),
    )
    released = []
    with pytest.raises(ValueError, match="k4-engineering-long-drain-signal-timeout"):
        d.release_engineering_drain("ns", [held("a")], hold, 100.12, lambda: released.append(1))
    assert not released and 100.12 <= now[0] < 100.171


def test_signal_read_crossing_hold_expiry_refuses(tmp_path, monkeypatch):
    now = clock(monkeypatch)
    d, _, hold = harness(tmp_path)
    d.collector = SimpleNamespace(live=SimpleNamespace(read=lambda: []))
    d.witness_raw = lambda ns: {}

    def seals(*args):
        now[0] = 159
        return (
            [],
            [dict(id="a", session="a", finish=None)],
            [dict(event="signal", signal=15, session="a", at=158.9)],
        )

    monkeypatch.setattr(driver, "export_seals", seals)
    with pytest.raises(ValueError, match="k4-engineering-long-drain-signal-timeout"):
        d.release_engineering_drain("ns", [held("a")], hold, 200, lambda: pytest.fail("released"))


def test_signal_wait_ignores_uninitialized_surge_witness(tmp_path, monkeypatch):
    import base64

    from workers.k4_witness import CampaignWitness

    clock(monkeypatch)
    monkeypatch.setenv("RUNTIME_NAMESPACE", "ns")
    monkeypatch.delenv("DUR053_K4_HOLD_FILE", raising=False)
    witness = CampaignWitness(str(tmp_path), "old-worker", "old", "old")
    invocation = witness.begin(
        dict(
            workflow_id="wf", node_id="dur048.sleep", iteration=0, attempt_number=1, claim_token="t"
        )
    )
    witness.trace("signal", dict(signal=15))
    d, _, hold = harness(tmp_path)
    old = dict(
        role="worker",
        session=witness.session,
        pod_uid="old",
        container_id="containerd://old",
        start=0,
        death=None,
        last_alive=100,
    )
    surge = dict(old, session="surge", pod_uid="surge", container_id="containerd://surge")
    d.collector = SimpleNamespace(live=SimpleNamespace(read=lambda: [old, surge]))
    raw = {p.as_posix(): base64.b64encode(p.read_bytes()).decode() for p in tmp_path.iterdir()}
    d.witness_raw = lambda ns: raw
    candidate = dict(held("old"), session=witness.session)
    candidate["acknowledgment"] = "durable-witness:" + invocation["id"]
    released = []
    d.release_engineering_drain("ns", [candidate], hold, 200, lambda: released.append(True))
    assert released == [True], "R317 surge witness prevented selected target confirmation"


@pytest.mark.parametrize(
    "engineering,scope,episode,enabled",
    [
        (True, "engineering-validation", "long-core-0-pair-0-treatment", True),
        (False, "registered-campaign", "long-core-0-pair-0-treatment", False),
        (True, "registered-campaign", "long-core-0-pair-0-treatment", False),
        (False, "engineering-validation", "long-core-0-pair-0-treatment", False),
        (True, "engineering-validation", driver.REGISTERED_SMOKE, False),
        (True, "engineering-validation", "long-core-0-pair-0-baseline", False),
    ],
)
def test_early_hold_precedes_arrivals_only_in_engineering_long_smoke(
    tmp_path, monkeypatch, engineering, scope, episode, enabled
):
    clock(monkeypatch)
    d = object.__new__(driver.K4Driver)
    d.output, d.engineering = tmp_path, engineering
    d.base = SimpleNamespace()
    d.images, d.proof, d.source = dict(a={}), {}, "source"
    d.setup = lambda *args: ("ns", "db")
    d.start_samples = lambda *args: None
    d.check_observation = lambda: None
    calls = []
    d.hold_state = lambda *args: calls.append("hold") or dict(observer=100)
    monkeypatch.setattr(driver, "locked_document", lambda *args: None)
    monkeypatch.setattr(driver, "episode_parameters", lambda *args, **kw: dict(rate=1, L=1, W=50))
    monkeypatch.setattr(driver, "Interventions", lambda *args: SimpleNamespace())

    class LoadStarted(Exception):
        pass

    class Pool:
        def __init__(self, **kw):
            pass

        def submit(self, *args):
            calls.append("load")
            raise LoadStarted

    monkeypatch.setattr(driver, "ThreadPoolExecutor", Pool)
    job = dict.fromkeys(["job", "run_id", "job_id", "head_sha", "tree_sha"], "test")
    job.update(budget_scope=scope, measurement_claim=scope == "registered-campaign")
    with pytest.raises(LoadStarted):
        d.execute_episode(driver.episode_identity(episode), {}, dict(sha256="hash"), job, None)
    assert calls == (["hold", "load"] if enabled else ["load"]), (
        "R317 engineering hold missing before arrivals or registered timing changed"
    )


@pytest.mark.parametrize("engineering", [True, False])
def test_episode_connects_selection_and_signal_release(tmp_path, monkeypatch, engineering):
    import threading

    clock(monkeypatch)
    d = object.__new__(driver.K4Driver)
    d.output, d.engineering = tmp_path, engineering
    d.images, d.proof, d.source = dict(a={}), {}, "source"
    d.setup = lambda *args: ("ns", "db")
    d.start_samples = lambda *args: None
    d.check_observation = lambda: None
    d.collector = SimpleNamespace(live=SimpleNamespace(read=lambda: []), close=lambda: None)
    d.observer_stop = threading.Event()
    d.sampler = SimpleNamespace(join=lambda **kw: None)
    d.namespaces = {"ns"}
    d.base = SimpleNamespace(reset_episode=lambda *args: None)
    d.hold_state = lambda *args: dict(observer=100, host_start=99, mtime=100)
    d.start_target_signal_diagnostics = lambda *args: None
    d.stop_target_signal_diagnostics = lambda *args: None
    calls = []
    overlap = [held("a"), held("b")]
    d.confirmed_overlap = lambda *args: calls.append("ordinary-overlap") or overlap
    d.engineering_drain_overlap = lambda *args: calls.append("held-overlap") or overlap

    def release_signal(ns, selected, hold, deadline, release):
        assert selected == overlap and deadline == 308
        calls.append("wait-term")
        release()

    d.release_engineering_drain = release_signal

    class Applied(Exception):
        pass

    adapter = SimpleNamespace(intervention_issued=None)

    def apply(cell, selected):
        assert selected == overlap
        adapter.release()  # The adapter invokes this on observed deletion.
        raise Applied

    adapter.apply = apply
    monkeypatch.setattr(driver, "Interventions", lambda *args: adapter)
    monkeypatch.setattr(driver, "locked_document", lambda *args: None)
    monkeypatch.setattr(
        driver,
        "episode_parameters",
        lambda *args, **kw: dict(rate=1, L=100, W=50, warmup=5, confirmation=200, T=300),
    )

    class Pool:
        def __init__(self, **kw):
            pass

        def submit(self, *args):
            return SimpleNamespace(done=lambda: True)

        def shutdown(self, **kw):
            pass

    monkeypatch.setattr(driver, "ThreadPoolExecutor", Pool)
    job = dict.fromkeys(["job", "run_id", "job_id", "head_sha", "tree_sha"], "test")
    job.update(
        budget_scope="engineering-validation" if engineering else "registered-campaign",
        measurement_claim=not engineering,
    )
    with pytest.raises(Applied):
        d.execute_episode(
            driver.episode_identity("long-core-0-pair-0-treatment"),
            {},
            dict(sha256="hash"),
            job,
            None,
        )
    assert calls == (["held-overlap", "wait-term"] if engineering else ["ordinary-overlap"]), (
        "R317 episode bypassed engineering selection or signal wait, or changed registered path"
    )
