import copy
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))

import j3_episode as episode  # noqa: E402
from j3_evidence import (  # noqa: E402
    SECOND,
    foreign_isolation,
    foreign_positive,
    notifications,
    slot_occupancy,
    target_only_attainment,
)
from j3_prometheus import evaluation_rows  # noqa: E402
from j3_resources import cgroup_paths, checker_failure, checker_outcome  # noqa: E402
from j3_slots import ready_slots, sample_ready_slots  # noqa: E402

UID = "abc-def"
GROUP = "/sys/fs/cgroup/kubepods.slice/kubepods-burstable-podabc_def.slice"
HOST_GROUP = "/system.slice/docker-kind-node.scope" + GROUP.removeprefix("/sys/fs/cgroup")


def test_observer_resolves_pod_group_from_container_membership():
    group = GROUP.removeprefix("/sys/fs/cgroup")
    relative = group + "/cri-containerd-xyz.scope"
    assert cgroup_paths(f"0::{relative}\n", UID) == ("/sys/fs/cgroup" + relative, GROUP)
    with pytest.raises(ValueError, match="not resolved"):
        cgroup_paths(f"0::{relative}\n", "different-pod")


@pytest.mark.parametrize(
    ("uid", "last_state", "restarts", "oom_kills", "kernel", "verdict"),
    [
        (UID, "OOMKilled", 1, 0, [], "oom"),
        (UID, "Error", 1, 0, [], "restart"),
        (UID, "Error", 0, 2, [], "oom"),
        (UID, "Error", 0, 0, [], "unexplained_checker_exit"),
        ("new-pod", "OOMKilled", 1, 0, [], "unexplained_checker_exit"),
        (UID, "Error", 0, 1, [], "unexplained_checker_exit"),
        (
            UID,
            "Error",
            0,
            0,
            ["oom-kill: task_memcg=" + GROUP.removeprefix("/sys/fs/cgroup") + "/runtime"],
            "oom",
        ),
        (
            UID,
            "Error",
            0,
            0,
            [
                "[725.77] oom-kill:constraint=CONSTRAINT_MEMCG,nodemask=(null),"
                f"oom_memcg={HOST_GROUP},task_memcg={HOST_GROUP}/cri-containerd-xyz.scope,"
                "task=checker,pid=30191,uid=65532"
            ],
            "oom",
        ),
        (
            UID,
            "Error",
            0,
            0,
            [f"oom-kill: task_memcg={HOST_GROUP}-sibling/runtime,task=checker"],
            "unexplained_checker_exit",
        ),
    ],
)
def test_checker_failure_uses_pod_identity_and_positive_oom_evidence(
    uid, last_state, restarts, oom_kills, kernel, verdict
):
    baseline = {"memory.events": "oom_kill 1\n"}
    polls = [
        {
            "uid": uid,
            "status": {
                "containerStatuses": [
                    {"restartCount": restarts, "lastState": {"terminated": {"reason": last_state}}}
                ]
            },
            "files": {"memory.events": f"oom_kill {oom_kills}\n"},
        }
    ]
    assert checker_failure(UID, GROUP, baseline, polls, kernel) == verdict


def test_missing_checker_status_and_cgroup_evidence_is_unexplained():
    assert checker_failure(UID, GROUP, {}, [{"files": {}}], []) == "unexplained_checker_exit"


@pytest.mark.parametrize(
    ("criterion", "outcome"),
    [("oom", "capacity"), ("restart", "capacity"), ("unexplained_checker_exit", "invalid")],
)
def test_runner_classifies_only_the_recorded_checker_criterion(tmp_path, criterion, outcome):
    assert checker_outcome(tmp_path) == "invalid"
    (tmp_path / "checker-failure.json").write_text(
        json.dumps({"criterion": criterion, "untrusted_text": "oom restart"})
    )
    assert checker_outcome(tmp_path) == outcome


@pytest.mark.parametrize("accepted", [True, False])
def test_sender_guard_stops_on_a_terminal_miss(tmp_path, accepted):
    journal, abort, armed = (tmp_path / name for name in ("journal", "abort", "armed"))
    exit_record = tmp_path / "exit.json"
    first = {"event": "arrival", "key": "one", "outcome": "uncertain", "status": 0, "error": ""}
    last = {"event": "closed"} if accepted else {**first, "error": "transport-error"}
    records = [
        {"event": "plan"},
        first,
        *([{**first, "outcome": "accepted", "status": 200}] if accepted else []),
        last,
    ]
    content = "".join(json.dumps(row) + "\n" for row in records)
    child = (
        f"from pathlib import Path; Path({str(journal)!r}).write_bytes({content.encode()!r});"
        + ("" if accepted else "import time; time.sleep(20)")
    )
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            episode.GUARD_SENDER,
            str(journal),
            str(abort),
            str(armed),
            str(exit_record),
            sys.executable,
            "-c",
            child,
        ],
        capture_output=True,
        timeout=10,
        check=False,
    )
    assert armed.is_file() and result.returncode == (0 if accepted else 1)
    assert not (tmp_path / "exit.json.tmp").exists()
    status = json.loads(exit_record.read_bytes())
    assert status["returncode"] == result.returncode
    assert status["closed"] is accepted and status["exited_ns"] > 0
    if not accepted:
        assert json.loads(result.stderr)["error"] == "transport-error"


def test_sender_guard_records_a_launch_failure(tmp_path):
    journal, abort, armed, exit_record, missing_binary = (
        tmp_path / name for name in ("journal", "abort", "armed", "exit.json", "missing")
    )
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            episode.GUARD_SENDER,
            *map(str, (journal, abort, armed, exit_record, missing_binary)),
        ],
        capture_output=True,
        timeout=10,
        check=False,
    )
    assert result.returncode != 0
    status = json.loads(exit_record.read_bytes())
    assert status["returncode"] == result.returncode
    assert status["closed"] is False and status["exited_ns"] > 0
    assert not (tmp_path / "exit.json.tmp").exists()


def test_observer_sender_exit_read_checks_the_last_complete_journal_line(tmp_path):
    record, journal = tmp_path / "record", tmp_path / "journal"

    def read():
        result = subprocess.run(
            [sys.executable, "-c", episode.READ_SENDER_EXIT, str(record), str(journal)],
            capture_output=True,
            timeout=10,
            check=True,
        )
        return json.loads(result.stdout)

    (tmp_path / "record.tmp").write_text('{"returncode":0}', encoding="utf-8")
    assert read() == {"record": None, "journal_last_line": None}
    record.write_text('{"returncode":0}', encoding="utf-8")
    journal.write_bytes(b"{}\n" * 3000 + b'{"event":"closed"}')
    assert read()["journal_last_line"] is None
    with journal.open("ab") as stream:
        stream.write(b"\n")
    assert read() == {"record": '{"returncode":0}', "journal_last_line": '{"event":"closed"}'}


@pytest.mark.parametrize(
    ("case", "error"),
    [
        ("lingering", None),
        ("nonzero", "sender exited with status 7"),
        ("missing", "sender exit evidence missing at the drain deadline"),
        ("local_only", "sender exited without an exit record"),
        ("truncated", None),
        ("partial", None),
        ("open_journal", "sender exit record lacks a closed journal"),
        ("unquiet", "drain did not reach 30 seconds quiet within 600 seconds"),
    ],
)
def test_drain_uses_the_observer_sender_exit_record(tmp_path, monkeypatch, case, error):
    stop = 100 * SECOND
    local_exit = stop - 40 * SECOND
    now = local_exit - 5 * SECOND if case == "local_only" else stop
    closed = '{"event":"closed"}'
    reads = []
    valid = {"returncode": 7 if case == "nonzero" else 0, "closed": True, "exited_ns": stop}
    observer_processes = [{"pid": 12, "state": "S (sleeping)", "cmdline": ["python", "guard"]}]

    def sleep(seconds):
        nonlocal now
        now += seconds * SECOND

    class Client:
        pid = 42
        returncode = None

        def poll(self):
            if case == "local_only" and now >= local_exit and self.returncode is None:
                self.returncode = 0
            return self.returncode

        def wait(self, timeout):
            if self.returncode is None:
                raise subprocess.TimeoutExpired("kubectl", timeout)
            return self.returncode

        def terminate(self):
            self.returncode = -15

    def kubectl(*args, **kwargs):
        if episode.READ_SENDER_EXIT in args:
            assert kwargs["timeout"] == 5
            raw = json.dumps(valid)
            if case in ("missing", "local_only"):
                raw = None
            elif not reads and case in ("truncated", "partial"):
                raw = '{"returncode":' if case == "truncated" else '{"returncode":0,"closed":true}'
            reads.append((now, raw))
            last = "{}" if case == "open_journal" else closed
            stdout = json.dumps({"record": raw, "journal_last_line": last})
        elif episode.OBSERVER_PROCESSES in args:
            stdout = json.dumps(observer_processes)
        elif args[-2:] == ("cat", episode.JOURNAL):
            stdout = closed + "\n"
        else:
            assert "-i" in args
            stdout = ""
        return SimpleNamespace(returncode=0, stdout=stdout, stderr="")

    client = Client()
    monkeypatch.setattr(episode.subprocess, "Popen", lambda *args, **kwargs: client)
    monkeypatch.setattr(
        episode, "time", SimpleNamespace(time_ns=lambda: now, monotonic_ns=lambda: now, sleep=sleep)
    )
    monkeypatch.setattr(
        episode,
        "duration_witness",
        lambda _driver: {
            "at_ns": now,
            "failed": 0,
            "admitted": 1,
            "succeeded": 1,
            "active_claims": 0,
            "unpublished": int(case == "unquiet"),
            "revision_hash": "stable",
        },
    )
    driver = SimpleNamespace(output=tmp_path, k=kubectl, env={})
    samples = SimpleNamespace(latest={"witness": {}, "slots": list(range(4)), "sample_ns": stop})
    rows = [{"namespace": "target", "scheduled_ns": stop - SECOND}]
    sender = episode.Sender(driver, "observer", "sender", rows, "test", stop - SECOND, stop)
    deadline = stop + (2 if error else 40) * SECOND

    def run():
        with sender.run():
            while now < stop:
                assert not sender.check_running()
                sleep(1)
            return episode.drain(driver, sender, samples, deadline, sender.check_running)

    if error:
        with pytest.raises(ValueError, match=error):
            run()
        evidence = json.loads((tmp_path / "sender-process-evidence.jsonl").read_bytes())
        assert evidence["reason"] == error
        assert evidence["local_client"]["pid"] == 42
        assert evidence["local_client"]["alive"] is (case != "local_only")
        assert evidence["observer_processes"] == observer_processes
        assert not (tmp_path / "quiet-drain.json").exists()
        if case == "missing":
            assert evidence["at_ns"] == deadline
            assert not (tmp_path / "sender-exit.json").exists()
        if case == "local_only":
            assert reads[0][0] == local_exit
            assert evidence["at_ns"] == local_exit + 30 * SECOND < stop
            assert not (tmp_path / "sender-exit.json").exists()
        return

    result = run()
    ignored_first = case in ("truncated", "partial")
    assert len(reads) == (2 if ignored_first else 1)
    assert result["quiet_start_ns"] == stop + int(ignored_first) * SECOND
    assert result["quiet_seconds"] == 30
    evidence = json.loads((tmp_path / "exec-stream-lingering.json").read_bytes())
    assert evidence["event"] == "exec_stream_lingering"
    assert evidence["termination_started_ns"] == evidence["exit_observed_ns"] + 30 * SECOND
    assert evidence["client_before"]["alive"] and not evidence["client_after"]["alive"]
    assert client.returncode == -15


def test_drain_starts_quiet_when_outbox_empties_with_the_same_fingerprint(tmp_path, monkeypatch):
    stop = 100 * SECOND
    now = stop
    unpublished = iter((3, 2, 1))
    slots = {"witness": {"worker": "stable"}, "slots": list(range(4)), "sample_ns": stop}

    def sleep(seconds):
        nonlocal now
        now += seconds * SECOND

    def durable_read(_driver):
        return {
            "at_ns": now,
            "failed": 0,
            "admitted": 1,
            "succeeded": 1,
            "active_claims": 0,
            "unpublished": next(unpublished, 0),
            "revision_hash": "stable",
        }

    monkeypatch.setattr(episode, "time", SimpleNamespace(time_ns=lambda: now, sleep=sleep))
    monkeypatch.setattr(episode, "duration_witness", durable_read)
    sender = SimpleNamespace(rows=[{}], exit_record={"returncode": 0}, check_running=lambda: True)
    result = episode.drain(
        SimpleNamespace(output=tmp_path),
        sender,
        SimpleNamespace(latest=slots),
        stop + 40 * SECOND,
        lambda: None,
    )

    assert result["quiet_start_ns"] == stop + 3 * SECOND
    assert result["quiet_end_ns"] == stop + 33 * SECOND
    assert result["quiet_seconds"] == 30
    reads = list(episode.jsonl(tmp_path / "quiet-reads.jsonl"))
    assert [read["unpublished"] for read in reads[:4]] == [3, 2, 1, 0]
    assert [read["quiet_start_ns"] for read in reads[:4]] == [None, None, None, stop + 3 * SECOND]


def test_ready_slots_counts_only_live_ready_pods_with_matching_witnesses():
    pod = {
        "metadata": {"name": "worker-a", "uid": "uid-a"},
        "status": {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]},
        "spec": {
            "containers": [{"name": "worker", "env": [{"name": "WORKER_SLOTS", "value": "2"}]}]
        },
    }
    ready = {"pod_name": "worker-a", "pod_uid": "uid-a", "session": "a", "complete": True}
    assert ready_slots([pod], {"uid-a": ready}) == ["worker-a-0", "worker-a-1"]
    assert ready_slots([], {}) == []
    with pytest.raises(ValueError, match="missing or mismatched"):
        ready_slots([pod], {})
    pod["metadata"]["deletionTimestamp"] = "2026-01-01T00:00:00Z"
    assert ready_slots([pod], {}) == []


@pytest.mark.parametrize("batch_timeout", [False, True])
def test_ready_sample_retains_failed_and_terminating_pods_with_zero_slots(
    tmp_path, monkeypatch, batch_timeout
):
    clock = SimpleNamespace(time_ns=lambda: 100, monotonic_ns=iter((30, 50)).__next__)
    monkeypatch.setattr("j3_slots.time", clock)

    def pod(name, ip, terminating=False):
        metadata = {"name": name, "uid": name}
        if terminating:
            metadata["deletionTimestamp"] = "2026-01-01T00:00:00Z"
        return {
            "metadata": metadata,
            "status": {
                "phase": "Running",
                "podIP": ip,
                "conditions": [{"type": "Ready", "status": "True"}],
            },
            "spec": {
                "containers": [{"name": "worker", "env": [{"name": "WORKER_SLOTS", "value": "2"}]}]
            },
        }

    class Driver:
        output = tmp_path
        metric = None
        reads = 0

        def k(self, _flag, _app, _verb, name, *_args, **_kwargs):
            self.reads += 1
            assert name == "observer"
            assert json.loads(_args[-1]) == {"live": "10.0.0.1", "missing": "10.0.0.2"}
            assert "connection.request('GET','/witness')" in _args[-2]
            if batch_timeout:
                raise TimeoutError("observer exec timed out")
            return SimpleNamespace(
                stdout=json.dumps(
                    {
                        "live": {
                            "witness": {
                                "complete": True,
                                "pod_name": "live",
                                "pod_uid": "live",
                                "session": "live",
                            }
                        },
                        "missing": {"error": "worker witness timed out"},
                    }
                )
            )

        def python(self, _ns, _receiver, code, **_kwargs):
            self.metric = code

    driver = Driver()
    sample = sample_ready_slots(
        driver,
        "app",
        "telemetry",
        "receiver",
        [pod("live", "10.0.0.1"), pod("missing", "10.0.0.2"), pod("terminating", "10.0.0.3", True)],
        observer_pod="observer",
    )
    assert driver.reads == 1
    assert sample["witness_transport"] == "observer_http"
    assert sample["witness_read_ns"] == 20
    assert sample["slots"] == ([] if batch_timeout else ["live-0", "live-1"])
    assert set(sample["errors"]) == ({"live", "missing"} if batch_timeout else {"missing"})
    assert sample["pods"][2]["terminating"]
    assert json.loads((tmp_path / "ready-slots.jsonl").read_bytes()) == sample
    assert f'"ready_slots": {len(sample["slots"])}' in driver.metric


def test_foreign_positive_requires_both_hot_windows_at_the_quiet_target_sample():
    t = 100 * SECOND
    keys = [f"foreign-{i}" for i in range(100)]
    seeds = [{"workflow_id": key, "commit_seen_ns": t - 1} for key in keys]
    target = [{"valid": True, "sample_ns": t, "firing": []}]
    witness = {
        "namespace": "foreign",
        "sample_ns": t,
        "admission_ids": keys,
        "claimable_attempts": 100,
        "oldest_claimable_age_ns": 28_540_000_001,
        "windows": {w: {"eligible": 100, "bad": 100} for w in ("2m", "10m")},
    }
    assert foreign_positive(witness, target, seeds, "foreign")["foreign"] == witness
    with pytest.raises(ValueError, match="simultaneous"):
        foreign_positive(witness, [{**target[0], "sample_ns": t + 1}], seeds, "foreign")
    witness["windows"]["10m"]["eligible"] = 99
    with pytest.raises(ValueError, match="10m"):
        foreign_positive(witness, target, seeds, "foreign")


def test_foreign_receipts_isolate_published_counts_and_each_slot():
    foreign = {
        "workflow_id": "f",
        "namespace": "foreign",
        "revision": 1,
        "state": "WAITING_ACTIVITY",
        "submission_key": "fk",
    }
    target = {
        "workflow_id": "t",
        "namespace": "target",
        "revision": 2,
        "state": "SUCCEEDED",
        "submission_key": "tk",
    }
    claim = {
        "workflow_id": "t",
        "node_id": "echo",
        "iteration": 0,
        "attempt_number": 1,
        "token_hash": "hash",
        "worker_request_id": "worker-a-0:request",
    }
    start = {
        **claim,
        "worker_id": "worker-a-0",
        "invocation_id": "invocation",
        "event": "start",
        "timestamp": "1970-01-01T00:00:00.1Z",
    }
    end = {**start, "event": "end", "timestamp": "1970-01-01T00:00:00.2Z"}
    data = {
        "plan": [{**row, "scheduled_ns": 0} for row in (foreign, target)],
        "workflows": [foreign, target],
        "history": [
            {**foreign, "reason": "J3_FOREIGN_SEED"},
            {**target, "new_state": "SUCCEEDED", "created_ns": SECOND},
        ],
        "attempts": [
            {
                **foreign,
                "state": "DISPATCHABLE",
                "is_current": True,
                "token_hash": None,
                "heartbeat_deadline": None,
                "worker_id": None,
            },
            claim,
        ],
        "outbox": [],
        "slot_events": [start, end],
    }
    assert foreign_isolation(data, [foreign], "foreign") == {
        "seeded": 1,
        "claims": 0,
        "revision_changes": 0,
        "outbox_rows": 0,
    }
    slots = ["worker-a-0", "worker-a-1", "worker-b-0", "worker-b-1"]
    inclusive = slot_occupancy(data, slots, "target", exclude_foreign=False)
    assert inclusive == slot_occupancy(data, slots, "target", exclude_foreign=True)
    assert list(inclusive["occupied_invocations"].values()) == [1, 0, 0, 0]
    assert target_only_attainment(data, "target", 0, 3 * SECOND) == {
        "eligible": 1,
        "on_time": 1,
        "incomplete": 0,
        "attainment": 1,
    }
    data["outbox"].append({"workflow_id": "f", "published_at": "already-published"})
    with pytest.raises(ValueError, match="foreign outbox"):
        foreign_isolation(data, [foreign], "foreign")


def test_receiver_requires_resolution_matching_a_real_prometheus_transition():
    target = {"namespace": "target", "campaign": "run"}
    alert = "OldestClaimable"
    transitions = [
        {"alert": alert, "firing": True, "at_ns": 10 * SECOND},
        {"alert": alert, "firing": False, "at_ns": 20 * SECOND},
    ]
    firing = {
        "received_ns": 11 * SECOND,
        "http_status": 204,
        "status": "firing",
        "alerts": [
            {
                "status": "firing",
                "labels": {**target, "alertname": alert},
                "fingerprint": "id",
                "startsAt": "1970-01-01T00:00:10Z",
                "endsAt": "0001-01-01T00:00:00Z",
            }
        ],
    }
    resolved = copy.deepcopy(firing)
    resolved.update(received_ns=21 * SECOND, status="resolved")
    resolved["alerts"][0].update(status="resolved", endsAt="1970-01-01T00:00:20Z")
    rows = [
        {"heartbeat": "start", "received_ns": 0},
        firing,
        resolved,
        {"heartbeat": "end", "received_ns": 30 * SECOND},
    ]
    assert len(notifications(rows, transitions, target, SECOND, 25 * SECOND)) == 2
    with pytest.raises(ValueError, match="unexpected or unpaired"):
        notifications(rows, transitions[:1], target, SECOND, 25 * SECOND)
    resolved["alerts"][0]["endsAt"] = "1970-01-01T00:00:19Z"
    with pytest.raises(ValueError, match="missing resolved"):
        notifications(rows, transitions, target, SECOND, 25 * SECOND)


def test_prometheus_joins_original_visibility_ns_and_rejects_missing_scrape_fields():
    target = {"namespace": "target", "campaign": "run"}
    t = 1_767_226_800 * SECOND
    at, scrape, sample = t + SECOND, t, t - 12345
    float_timestamp = sample / SECOND

    def series(name, when, value, **labels):
        return {
            "metric": {"__name__": name, **target, **labels},
            "values": [[when / SECOND, str(value)]],
        }

    rows = [
        series("up", scrape, 1),
        series("j3:expected", at, 1),
        series("j3:valid", at, 1),
        series("j3_sample_timestamp_seconds", scrape, float_timestamp),
        series("j3_demand_timestamp_seconds", scrape, t / SECOND),
        series("j3_oldest_claimable_age_seconds", scrape, 0),
        series("j3_claimable_attempts", scrape, 0),
        *(
            series(f"j3_{field}", scrape, 0, window=window)
            for window in ("2m", "10m", "all")
            for field in ("bad", "eligible")
        ),
    ]
    stream = [{"sample_ns": sample, "demand": {"sampled_at": "2026-01-01T00:20:00Z"}}]
    health = [
        {"name": name, "health": "ok"}
        for name in (
            "j3:expected",
            "j3:valid",
            "j3:oldest_fire",
            "j3:oldest_clear",
            "j3:burn_fire",
            "j3:burn_clear",
            "OldestClaimable",
            "DeadlineBudgetBurn",
            "ScrapeAbsent",
        )
    ]
    (event,) = evaluation_rows(rows, stream, target, t, t + 2 * SECOND, health)
    assert event["sample_ns"] == sample
    assert event["scrape_ns"] == scrape
    assert event["at_ns"] == at
    with pytest.raises(ValueError, match="missing visibility evidence"):
        evaluation_rows(rows, [], target, t, t + 2 * SECOND, health)
    with pytest.raises(ValueError, match="required metric missing"):
        evaluation_rows(rows[:-1], stream, target, t, t + 2 * SECOND, health)
