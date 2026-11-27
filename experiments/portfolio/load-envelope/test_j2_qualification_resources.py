import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))

import j2_qualification_resources as resources  # noqa: E402
from j2_qualification_evidence import CapacityFailure, classify, read  # noqa: E402


@pytest.fixture
def collect(tmp_path, monkeypatch):
    uid = "3c-d44"
    group = f"/sys/fs/cgroup/kubelet.slice/kubelet-burstable-pod{uid.replace('-', '_')}.slice"
    assert resources.pod_cgroup(group + "/cri-containerd-scope", uid) == group
    now = 0

    def sleep(seconds):
        nonlocal now
        now += seconds

    monkeypatch.setattr(
        resources,
        "time",
        SimpleNamespace(monotonic=lambda: now, time_ns=lambda: now * 10**9, sleep=sleep),
    )

    def run(statuses, kills=0, *, trigger="missing PID", kernel_hit=False):
        calls = iter(statuses)

        def probe(args, **kwargs):
            assert kwargs["timeout"] > 0
            if args[0] == "kubectl":
                stdout = json.dumps(
                    {
                        "metadata": {"uid": uid},
                        "status": {
                            "containerStatuses": [
                                next(calls, {"restartCount": 0, "state": {"running": {}}})
                            ]
                        },
                    }
                )
            elif args[-1] == "dmesg":
                group_path = group.removeprefix("/sys/fs/cgroup")
                scope = (
                    "/docker-kind.scope" + group_path + "/cri.scope" if kernel_hit else "/unrelated"
                )
                stdout = f"oom-kill:constraint=CONSTRAINT_MEMCG,oom_memcg={scope}\n"
            else:
                assert args[-1] == group
                stdout = (
                    f"memory.events\noom 1\noom_kill {kills}\n"
                    "memory.peak\n2147483648\nmemory.events.local\noom_kill 0\n"
                )
            return SimpleNamespace(stdout=stdout, stderr="", returncode=0)

        resources.diagnose_failure(
            tmp_path,
            ["docker", "exec", "node"],
            "kind-test",
            "app/observer",
            uid,
            group,
            probe,
            trigger=trigger,
        )

    return run


def test_polls_until_kubelet_reveals_oom_killed_last_state(collect, tmp_path):
    running = {"restartCount": 0, "state": {"running": {}}}
    restarted = {
        "restartCount": 1,
        "state": {"running": {}},
        "lastState": {
            "terminated": {"reason": "OOMKilled", "exitCode": 137, "finishedAt": "2026-11-25Z"}
        },
    }
    with pytest.raises(CapacityFailure) as failure:
        collect([running, restarted])
    assert failure.value.criterion == "oom"
    record = read(tmp_path / "capacity-observer.json")
    assert len(record["polls"]) == 2
    assert record["polls"][-1]["status"]["containerStatuses"] == [restarted]


def test_poll_classifies_restart_without_oom(collect):
    restart = {"restartCount": 1, "lastState": {"terminated": {"reason": "Error", "exitCode": 1}}}
    with pytest.raises(CapacityFailure) as failure:
        collect([restart])
    assert failure.value.criterion == "restart"


def test_vanished_pid_and_no_kubelet_update_stays_invalid_after_60_seconds(collect, tmp_path):
    collect([])
    record = read(tmp_path / "capacity-observer.json")
    assert len(record["polls"]) == 30
    assert resources.time.monotonic() == 60
    assert record["criterion"] is None
    assert record["kernel"]["lines"]
    assert (
        classify(
            "failure", {"status": "FAIL", "population_started": True, "error": "missing PID"}, None
        )[0]
        == "invalid"
    )


def test_exit_137_uses_pod_oom_kill_counter_before_kubelet_update(collect, tmp_path):
    with pytest.raises(CapacityFailure) as failure:
        collect([], kills=1, trigger="live checker exit 137")
    assert failure.value.criterion == "oom"
    record = read(tmp_path / "capacity-observer.json")
    assert record["trigger"] == "live checker exit 137"
    assert record["polls"][0]["files"] == {
        "memory.events": "oom 1\noom_kill 1\n",
        "memory.peak": "2147483648\n",
        "memory.events.local": "oom_kill 0\n",
    }


def test_kernel_oom_path_can_include_host_prefix(collect):
    with pytest.raises(CapacityFailure) as failure:
        collect([], kernel_hit=True)
    assert failure.value.criterion == "oom"
