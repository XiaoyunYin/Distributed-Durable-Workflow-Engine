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
    assert (tmp_path / "diagnostics/target-kubelet.txt").read_text() == line + "\n"
    assert "-n" not in commands[0] and "--since" in commands[0]
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
