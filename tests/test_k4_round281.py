"""Retain discriminating termination evidence without changing the TERM guard."""

import json
import sys
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import dur053_k4_driver as driver  # noqa: E402
from dur053_k4_interventions import require_term_evidence  # noqa: E402


def pod(uid, cid, deadline=None):
    metadata = dict(uid=uid, name="worker-" + uid)
    if deadline is not None:
        metadata["deletionTimestamp"] = driver.stamp(deadline)
    return dict(
        metadata=metadata,
        spec=dict(terminationGracePeriodSeconds=40),
        status=dict(containerStatuses=[dict(containerID="containerd://" + cid)]),
    )


def test_no_targets_needs_no_termination_collector(tmp_path):
    d = object.__new__(driver.K4Driver)
    d.episode_output = tmp_path
    d.retain_target_termination([], dict(kind="rollout", intervention=10), 100)
    assert json.loads((tmp_path / "target-termination.json").read_bytes()) == dict(targets=[])


@pytest.mark.parametrize("episode_deadline,expected", [(100, 50), (46, 46)])
def test_wait_retains_cri_status_and_target_killing_lines(
    tmp_path, monkeypatch, episode_deadline, expected
):
    d = object.__new__(driver.K4Driver)
    d.episode_output = tmp_path
    now = [45.0]
    targets = [pod("uid-a", "cid-a"), pod("uid-b", "cid-b"), pod("uid-c", "cid-c")]
    d.collector = SimpleNamespace(
        lock=threading.Lock(),
        pods=[
            dict(object=pod("uid-a", "cid-a", 49)),
            dict(object=pod("uid-b", "cid-b", 50)),
            dict(object=pod("uid-c", "cid-c", 48)),
            # A later grace-0 rewrite must not replace the first bound.
            dict(object=pod("uid-b", "cid-b", 44)),
        ],
        cri_exits=[],
    )
    commands = []
    line = (
        'full prefix "Killing container with a grace period" '
        'podUID="uid-b" containerID="cid-b" gracePeriod=40'
    )

    def command(args, **kw):
        commands.append(args)
        return SimpleNamespace(
            returncode=0, stdout=line + '\n"Killing container" unrelated\n', stderr=""
        )

    d.base = SimpleNamespace(command=command)
    d.check_observation = lambda: None
    monkeypatch.setattr(driver.time, "time", lambda: now[0])

    def sleep(seconds):
        assert 0 < seconds <= 0.1
        now[0] = round(now[0] + seconds, 6)
        for cid, at, status in [("cid-a", 45.2, 0), ("cid-c", 45.4, 137)]:
            if now[0] >= at and not any(e["container_id"] == cid for e in d.collector.cri_exits):
                d.collector.cri_exits.append(dict(container_id=cid, at=at, exit_status=status))

    monkeypatch.setattr(driver.time, "sleep", sleep)
    d.retain_target_termination(targets, dict(kind="rollback", intervention=10), episode_deadline)
    rows = json.loads((tmp_path / "target-termination.json").read_bytes())["targets"]
    assert now[0] == expected, (
        "target death/grace evidence collection ended early or exceeded bound"
    )
    assert rows[0]["cri_exit"] == dict(container_id="cid-a", at=45.2, exit_status=0)
    assert rows[2]["cri_exit"] == dict(container_id="cid-c", at=45.4, exit_status=137)
    assert rows[1]["cri_exit"] is None
    assert rows[1]["grace_deadline"] == 50, "grace bound ignored first deletion timestamp"
    assert (tmp_path / "diagnostics/target-kubelet.txt").read_text() == line + "\n"
    assert commands == [
        [
            "docker",
            "exec",
            "dur053-k3a-control-plane",
            "journalctl",
            "-u",
            "kubelet",
            "--no-pager",
            "--since",
            "@9",
        ]
    ], "kubelet query did not use integer epoch argv"
    with pytest.raises(ValueError, match="k4-term-observation-missing"):
        require_term_evidence(
            dict(kind="rollback", completed=50, targets=[dict(session="s")]),
            [dict(session="s", signals=[], exit_code=137, death=50)],
        )


def test_k4_cluster_enables_killing_records_only_in_generated_config(tmp_path, monkeypatch):
    commands = []
    monkeypatch.setattr(
        driver,
        "Driver",
        lambda *args: SimpleNamespace(
            live_reservation=0,
            password="password",
            demand_token="token",
            command=lambda args, **kw: commands.append(args) or SimpleNamespace(returncode=0),
        ),
    )
    d = driver.K4Driver(tmp_path, tmp_path, engineering=True)
    config = tmp_path / "original.json"
    original = b'{"nodes": [{"role": "control-plane"}]}'
    config.write_bytes(original)
    d.base.command(["kind", "create", "cluster", "--config", str(config)])
    generated = json.loads(Path(commands[0][-1]).read_bytes())
    assert "v: '2'" in generated["nodes"][0]["kubeadmConfigPatches"][0]
    assert config.read_bytes() == original


@pytest.mark.parametrize("token", ["worker-uid-a", "uid-a", "cid-a"])
def test_kubelet_v2_target_tokens(tmp_path, token):
    d = object.__new__(driver.K4Driver)
    d.episode_output = tmp_path
    d.collector = SimpleNamespace(lock=threading.Lock(), pods=[], cri_exits=[])
    line = f'prefix "Killing container with a grace period" pod="{token}" gracePeriod=40'
    d.base = SimpleNamespace(
        command=lambda *a, **kw: SimpleNamespace(
            returncode=0, stdout=line + '\n"Killing container" unrelated\n', stderr=""
        )
    )
    d.retain_target_termination([pod("uid-a", "cid-a")], dict(kind="rollout", intervention=1), 0)
    assert (tmp_path / "diagnostics/target-kubelet.txt").read_text() == line + "\n"


@pytest.mark.parametrize("failure", ["exit", "timeout", "none"])
@pytest.mark.parametrize("deadline", [12, 7.4])
def test_live_signal_diagnostics_repeat_and_bound(tmp_path, monkeypatch, failure, deadline):
    d = object.__new__(driver.K4Driver)
    d.episode_output = tmp_path
    target = pod("uid-a", "cid-a")
    target["metadata"]["namespace"] = "test"
    target["status"]["containerStatuses"][0]["name"] = "worker"
    d.collector = SimpleNamespace(lock=threading.Lock(), pods=[])
    now = [2.0]
    calls = []

    class Stopped:
        def is_set(self):
            return False

        def wait(self, seconds):
            assert 0 < seconds <= 0.1
            now[0] = round(now[0] + seconds, 6)
            if now[0] >= 3 and not d.collector.pods:
                d.collector.pods.append(dict(object=pod("uid-a", "cid-a", 11)))

    def command(*args, **kw):
        calls.append((now[0], args, kw))
        assert args[:9] == (
            "-n",
            "test",
            "exec",
            "worker-uid-a",
            "-c",
            "worker",
            "--",
            "python",
            "-c",
        )
        compile(args[-1], "proc-diagnostic", "exec")
        for field in [
            "Name",
            "State",
            "SigPnd",
            "ShdPnd",
            "SigBlk",
            "SigIgn",
            "SigCgt",
            "/proc/1/status",
            "/proc/1/task",
            "*/status",
            "*/wchan",
        ]:
            assert field in args[-1]
        assert kw["allowed"] is True and 0 < kw["timeout"] <= 1
        assert now[0] + kw["timeout"] <= min(deadline, 11)
        if failure == "timeout":
            raise TimeoutError("exec timed out; partial stderr")
        return SimpleNamespace(
            returncode=1 if failure == "exit" else 0, stdout="proc output", stderr="exec stderr"
        )

    d.base = SimpleNamespace(k=command)
    monkeypatch.setattr(driver.time, "time", lambda: now[0])
    d.retain_target_termination([target], {}, deadline, signals_only=True, stopped=Stopped())
    expected = [3, 8] if deadline == 12 else [3]
    assert [call[0] for call in calls] == expected, "live signal samples missed deletion or repeat"
    assert now[0] <= deadline
    rows = [
        json.loads(path.read_bytes()) for path in sorted((tmp_path / "diagnostics").glob("*.json"))
    ]
    assert len(rows) == len(expected)
    for i, row in enumerate(rows):
        assert row["attempt"] == i and row["deletion_observed_at"] == 3
        if failure == "timeout":
            assert row["exit_code"] is None and "exec timed out" in row["stderr"]
        else:
            assert row["exit_code"] == (1 if failure == "exit" else 0)
            assert row["stdout"] == "proc output" and row["stderr"] == "exec stderr"
    assert not (tmp_path / "target-termination.json").exists()


def test_live_monitors_armed_at_issue_before_rollout(tmp_path, monkeypatch):
    d = object.__new__(driver.K4Driver)
    d.episode_output = tmp_path
    order = []
    adapter = SimpleNamespace(issue=lambda targets: order.append("issued"))
    threads = []

    class Thread:
        def __init__(self, **kw):
            threads.append(kw)

        def start(self):
            order.append("monitor-started")

    monkeypatch.setattr(driver.threading, "Thread", Thread)
    stopped, _ = d.start_target_signal_diagnostics(adapter, 50)
    targets = [pod("uid-a", "cid-a"), pod("uid-b", "cid-b")]
    adapter.issue(targets)
    assert order == ["issued", "monitor-started", "monitor-started"]
    assert all(t["target"] == d.retain_target_termination for t in threads)
    assert [t["args"] for t in threads] == [([p], {}, 50) for p in targets]
    assert all(t["kwargs"] == dict(signals_only=True, stopped=stopped) for t in threads)
    adapter.issue(targets)
    assert len(threads) == 2
