import hashlib
import json
import sys
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))

import j2_drain as drain  # noqa: E402
import j2_step_rehearsal as step  # noqa: E402
from test_j2_offline import episode, ts  # noqa: E402, F401
from test_j2_schedule import arrival, encoded, stock_plan  # noqa: E402

S = drain.SECOND


def expected(keys):
    return {
        key: {"hash": key + "-hash", "namespace": "j2", "scheduled_ns": 2 * S, "result": 5}
        for key in keys
    }


def snapshot(ns, *, admitted="a", succeeded="a", revision=4, unpublished=0, claimed=0):
    return {
        "lower_mono_ns": ns,
        "upper_mono_ns": ns + S // 10,
        "lower_unix_ns": ns,
        "upper_unix_ns": ns + S // 10,
        "db": {
            "submissions": [
                {"id": key, "key": key, "namespace": "j2", "hash": key + "-hash"}
                for key in admitted
            ],
            "succeeded": list(succeeded),
            "unpublished": unpublished,
            "claimed": claimed,
            "terminal_failures": 0,
            "terminal_revisions": revision,
        },
        "witness": [{"session": "stable"}],
    }


class DrainClock:
    def __init__(self):
        self.ns = 10 * S

    def now(self):
        return self.ns

    def sleep(self, seconds):
        self.ns += round(seconds * S)

    def read(self, **kwargs):
        state = snapshot(self.ns, **kwargs)
        self.ns = state["upper_mono_ns"]
        return state


def drive(clock, observe, check, retained, keys="a", acknowledged="a", unsent=()):
    return drain.quiet_drain(
        stop_ns=10 * S,
        observe=observe,
        check=check,
        expected=expected(keys),
        sender={"acknowledged": list(acknowledged), "explicitly_unsent": list(unsent)},
        retain=lambda label, data: retained.append((label, data)),
        now=clock.now,
        sleep=clock.sleep,
    )


def test_quiet_starts_after_checker_and_uses_opposite_bracket_edges():
    clock = DrainClock()
    retained = []
    checked = []

    def run_checker(state):
        checked.append(state)
        clock.sleep(5)

    result = drive(clock, clock.read, run_checker, retained)
    assert len(checked) == 1
    assert result["checked_export_seconds"] == pytest.approx(5.1)
    assert result["quiet_start_upper_mono_ns"] > 15 * S
    assert result["quiet_end_lower_mono_ns"] - result["quiet_start_upper_mono_ns"] >= 30 * S
    assert result["drain_seconds"] == (retained[-1][1]["upper_mono_ns"] - 10 * S) / S
    assert retained[0][0] == "initial" and retained[-1][0] == "final"


@pytest.mark.parametrize("disruption", ["revision", "witness", "read_error", "remaining"])
def test_quiet_restarts_on_evidence_change_or_unavailable_read(disruption):
    clock = DrainClock()
    retained = []
    checked = []
    failed_read = False

    def observe():
        nonlocal failed_read
        if disruption == "read_error" and clock.ns >= 17 * S and not failed_read:
            failed_read = True
            raise TimeoutError("temporary read timeout")
        state = clock.read()
        if clock.ns >= 17 * S:
            if disruption == "revision":
                state["db"]["terminal_revisions"] = 5
            if disruption == "witness":
                state["witness"][0]["session"] = "changed"
            if disruption == "remaining" and clock.ns < 19 * S:
                state["db"]["unpublished"] = 1
        return state

    result = drive(clock, observe, checked.append, retained)
    assert len(checked) == 2
    assert result["quiet_start_upper_mono_ns"] >= 17 * S
    assert result["drain_seconds"] >= 37


def test_pending_and_missing_identities_are_retained_at_deadline():
    clock = DrainClock()
    retained = []
    with pytest.raises(drain.DrainExpired, match="2 missing IDs"):
        drive(
            clock,
            lambda: clock.read(succeeded=""),
            lambda *_: pytest.fail("checker ran"),
            retained,
            keys="abc",
            acknowledged="ab",
            unsent="c",
        )
    label, evidence = retained[-1]
    assert label == "deadline"
    assert evidence["missing"] == ["a", "b"]
    assert evidence["missing_acknowledged"] == ["b"]
    assert not evidence["ready"]
    assert evidence["pending_lower_bound_ns"]["a"] > 600 * S


def test_incomplete_export_before_deadline_cannot_start_quiet():
    clock = DrainClock()
    retained = []

    def slow_checker(_):
        clock.sleep(601)
        raise TimeoutError("checker timed out")

    with pytest.raises(drain.DrainExpired, match="600 s"):
        drive(clock, clock.read, slow_checker, retained)
    assert "quiet-start" not in {label for label, _ in retained}
    assert retained[-1][0] == "deadline"


def test_zero_counts_do_not_mask_wrong_hash_duplicate_success_or_unsent_admission():
    state = snapshot(10 * S, succeeded="aa")
    sender = {"explicitly_unsent": [], "acknowledged": ["a"]}
    with pytest.raises(drain.IntegrityError, match="duplicated.*success"):
        drain.readiness(state, expected("a"), sender)
    state["db"]["succeeded"] = ["a"]
    state["db"]["submissions"][0]["hash"] = "wrong"
    with pytest.raises(drain.IntegrityError, match="hash differs"):
        drain.readiness(state, expected("a"), sender)
    sender["explicitly_unsent"] = ["a"]
    with pytest.raises(drain.IntegrityError, match="explicitly unsent"):
        drain.readiness(state, expected("a"), sender)


@pytest.mark.parametrize("change", ["none", "receipt", "terminal", "witness"])
def test_checked_results_reconcile_receipts_and_final_witness(episode, monkeypatch, change):  # noqa: F811
    _planned, trace, starts, polls, uncertainty, observed = episode
    trace["Submissions"][0].update(Key="a", Hash="a-hash", Namespace="j2")
    events = []
    for i, start in enumerate(starts):
        start.update(
            invocation_id=str(i), pod_uid="p", session="s", sequence=i * 2, monotonic_ns=i * 2
        )
        finish = {
            **start,
            "event": "end",
            "sequence": start["sequence"] + 1,
            "monotonic_ns": start["monotonic_ns"] + 1,
            "outcome": "SUCCEEDED",
        }
        events.extend((start, finish))
    if change == "receipt":
        trace["Snapshot"]["engine.outbox"]["rows"][0]["payload"] = 6
    if change == "terminal":
        trace["Snapshot"]["engine.transition_history"]["rows"].pop(2)
    if change == "witness":
        events.pop(1)
    monkeypatch.setattr(drain, "witness_events", lambda _: events)
    report = {
        "status": "PASS",
        "valid": True,
        "non_terminal": [],
        "workflow_ids": ["a"],
        "trace": trace,
    }
    target = expected("a")
    target["a"]["scheduled_ns"] = S + 1
    sender = {"acknowledged": ["a"], "explicitly_unsent": []}
    if change != "none":
        with pytest.raises(ValueError, match="invalid final revision|missing invocation"):
            drain.checked_results(report, target, sender, {}, polls, uncertainty, observed)
    else:
        (row,) = drain.checked_results(report, target, sender, {}, polls, uncertainty, observed)
        assert row["workflow_id"] == "a" and row["terminal_ns"] == 8 * S
        assert row["winning_attempt"] == ["a", "x", 0, 2]
        assert json.loads(json.dumps(row)) == row


def test_full_clock_schedule_stays_below_pinned_observer_volume(monkeypatch):
    monkeypatch.setenv("GITHUB_RUN_ID", "12")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    grid = step.episode_schedule("grid-drain", stepped=False)
    full = step.episode_schedule("step-drain", stepped=True)
    assert grid["count"] == 720
    assert full["warmup_end_ns"] - full["origin_ns"] == 120 * S
    assert full["hold_end_ns"] - full["origin_ns"] == 1920 * S
    assert full["stop_ns"] - full["hold_end_ns"] == 300 * S
    assert [period["count"] for period in full["periods"]] == [1920, 7300]
    assert full["count"] == 9220 < step.POPULATION_LIMIT
    assert full["worker_sleep_ms"] != grid["worker_sleep_ms"]
    boundary = full["hold_end_ns"]
    cohorts = drain.cohort_counts(
        full,
        [
            {"scheduled_ns": boundary - S, "terminal_ns": boundary + S},
            {"scheduled_ns": boundary, "terminal_ns": boundary + S},
        ],
    )
    assert cohorts["hold"] == {"offered": 1800, "checked": 1, "successful_in_window": 0}
    assert cohorts["step"] == {"offered": 7300, "checked": 1, "successful_in_window": 1}


def test_durable_request_hash_is_derived_from_the_immutable_plan(monkeypatch):
    monkeypatch.setenv("GITHUB_RUN_ID", "12")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    schedule = step.stock_schedule("eng-12-1", "job-1", "grid", "run", "j2", S, "1", 0, 1)
    head = stock_plan(schedule)
    row = head["plan"]["rows"][0]
    key = row["workflow_id"]
    payload = {"run_id": "run", "sequence": 0}
    row.update(
        request={
            "definition_id": "dur053-run-dur048.sleep",
            "definition_version": 1,
            "initial_node_id": "dur048.sleep",
            "initial_input": payload,
            "payload": payload,
            "workflow_id": key,
            "submission_key": key,
            "namespace": "j2",
            "actor_id": "dur053-loadgen",
        },
        activity="dur048.sleep",
        expected=payload,
    )
    sent = arrival(row, sent_ns=S)
    sent["request_hash"] = hashlib.sha256(drain.json_bytes(row["request"])).hexdigest()
    arrivals, report = drain.expected_arrivals(encoded(head, sent), schedule)
    assert arrivals[key]["hash"].startswith("sub-v1:")
    assert report["unresolved"] == [key]
    sent["request_hash"] = "0" * 64
    with pytest.raises(ValueError, match="journal request hash differs"):
        drain.expected_arrivals(encoded(head, sent), schedule)


def test_carry_probe_resumes_broker_when_pending_read_fails(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(step, "wait_until", lambda *_: True)

    def command(args, **_):
        calls.append(args[1])
        return SimpleNamespace(returncode=0)

    def fail(_):
        raise TimeoutError("pending read failed")

    driver = SimpleNamespace(
        command=command,
        read_database=fail,
        worker_uids=lambda: ["a", "b"],
        output=tmp_path,
        worker_sleep_ms=150,
    )
    with pytest.raises(TimeoutError, match="pending read failed"):
        step.carry_probe(driver, {}, 0, None)
    assert calls == ["pause", "unpause"]
    assert drain.read(tmp_path / "step-transition.json")["worker_uids_before"] == ["a", "b"]


def test_archive_rejects_a_changed_two_period_schedule_before_replay(tmp_path, monkeypatch):
    episode_dir = tmp_path / "evidence"
    episode_dir.mkdir()
    for member in drain.REPLAY_FILES:
        (episode_dir / member).write_text("{}", encoding="utf-8")
    good = {"periods": [{"rate": "1"}, {"rate": "24.33102566649034"}]}
    drain.write(episode_dir / "schedule.json", good)
    index = {f"step-drain/{p.name}": drain.file_record(p) for p in episode_dir.iterdir()}
    archive = tmp_path / "artifact.zip"
    receipt = tmp_path / "receipt.json"

    def bundle():
        with zipfile.ZipFile(archive, "w") as zipped:
            zipped.writestr("freeze.json", json.dumps({"files": index}))
            for p in episode_dir.iterdir():
                zipped.write(p, f"step-drain/{p.name}")

    def replay(root):
        assert drain.read(root / "schedule.json") == good
        return {}, 123

    monkeypatch.setattr(drain, "replay_episode", replay)
    monkeypatch.setattr(drain, "cohort_counts", lambda *_: {})
    bundle()
    drain.replay_archive(archive, receipt)
    assert drain.read(receipt)["step-drain"]["status"] == "PASS"
    drain.write(episode_dir / "schedule.json", {"periods": [{"rate": "24.33102566649034"}]})
    bundle()
    with pytest.raises(ValueError, match="replay member differs: step-drain/schedule.json"):
        drain.replay_archive(archive, receipt)


def test_verifies_exact_snapshot_row_bytes(tmp_path):
    path = tmp_path / "checker.json"
    rows = [{"id": "a"}]
    raw = drain.json_bytes(rows)
    record = {
        "trace": {
            "Snapshot": {
                "engine.test": {
                    "rows": rows,
                    "count": 1,
                    "sha256": hashlib.sha256(raw).hexdigest(),
                }
            }
        }
    }
    path.write_bytes(drain.json_bytes(record))
    assert drain.indexed_report(path, verify_tables=True)[1]["engine.test"]["count"] == 1
    rows[0]["id"] = ts(1)
    path.write_bytes(drain.json_bytes(record))
    with pytest.raises(ValueError, match="snapshot table hash differs"):
        drain.indexed_report(path, verify_tables=True)
