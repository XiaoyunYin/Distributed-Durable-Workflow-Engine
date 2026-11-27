import hashlib
import json
import sys
import zipfile
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))

import j2_drain as drain  # noqa: E402
import j2_step_rehearsal as step  # noqa: E402
from j2_checker_report import ResourceEvidenceError  # noqa: E402
from j2_qualification_resources import require_checker_resources  # noqa: E402
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
                {
                    "id": key,
                    "key": key,
                    "namespace": "j2",
                    "hash": key + "-hash",
                    "revision": revision,
                }
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


def drive(clock, observe, check, retained, keys="a", acknowledged="a", unsent=(), *, expiry=None):
    return drain.quiet_drain(
        stop_ns=10 * S,
        observe=observe,
        check=check,
        check_expiry=observe if expiry is None else expiry,
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


def test_clean_expiry_retains_unacknowledged_pending_and_missing_identities(tmp_path):
    clock = DrainClock()
    retained = []

    def expiry():
        assert clock.now() == 610 * S
        drain.write(tmp_path / "live-checker.json", {"status": "PASS", "non_terminal": ["a"]})
        return clock.read(succeeded="", unpublished=8, claimed=1)

    with pytest.raises(drain.DrainExpired, match="2 missing IDs") as stopped:
        drive(
            clock,
            lambda: clock.read(succeeded=""),
            lambda *_: pytest.fail("checker ran"),
            retained,
            keys="abc",
            acknowledged="",
            unsent="c",
            expiry=expiry,
        )
    label, evidence = retained[-1]
    assert label == "deadline"
    assert evidence["missing"] == ["a", "b"]
    assert evidence["missing_acknowledged"] == []
    assert evidence["pending"] == ["a"]
    assert evidence["unpublished"] == 8 and evidence["claimed"] == 1
    assert evidence["check_status"] == "PASS" and stopped.value.result == evidence
    assert evidence["classification"] == "drain_failure"
    assert not evidence["ready"]
    assert evidence["pending_lower_bound_ns"]["a"] > 600 * S
    assert retained[-2][0] == "final"
    assert drain.read(tmp_path / "live-checker.json")["status"] == "PASS"


def test_acknowledged_identity_absent_at_expiry_is_an_integrity_stop():
    clock = DrainClock()
    retained = []

    with pytest.raises(drain.IntegrityError, match=r"acknowledged.*\['b'\]") as stopped:
        drive(
            clock,
            lambda: clock.read(succeeded="", unpublished=8),
            lambda *_: pytest.fail("checker ran before expiry"),
            retained,
            keys="ab",
            acknowledged="ab",
        )

    assert stopped.value.classification == "integrity_stop"
    label, evidence = retained[-1]
    assert label == "deadline"
    assert evidence["classification"] == "integrity_stop"
    assert evidence["check_status"] == "failed"
    assert evidence["pending"] == ["a"]
    assert evidence["missing_acknowledged"] == ["b"]


@pytest.mark.parametrize("failure", ["duplicate", "unexpected", "checker_error"])
def test_integrity_stop_takes_precedence_at_expiry(failure):
    clock = DrainClock()
    retained = []

    def expiry():
        if failure == "checker_error":
            raise TimeoutError("checker could not export snapshot")
        if failure == "duplicate":
            return clock.read(succeeded="aa")
        return clock.read(admitted="ax", succeeded="a")

    with pytest.raises(drain.IntegrityError, match="expiry check failed"):
        drive(clock, lambda: clock.read(unpublished=8), lambda _: None, retained, expiry=expiry)
    label, evidence = retained[-1]
    assert label == "deadline"
    assert evidence["classification"] == "integrity_stop"
    assert evidence["check_status"] == "failed"
    assert evidence["last_verified_counts"]["unpublished"] == 8


@pytest.mark.parametrize("expiry", [False, True])
@pytest.mark.parametrize(
    "outcome",
    [
        "fast",
        "corroborated",
        "missing_exit",
        "missing_cgroup",
        "over_limit",
        "nonzero",
        "violation",
    ],
)
def test_checker_exit_peak_and_failure_classes(tmp_path, monkeypatch, expiry, outcome):
    clock = DrainClock()
    retained = []
    pod = f"{step.APP}/observer"
    # A single cgroup sample is sufficient even if the checker PID was missed.
    vmhwm = 47_000 if outcome in {"corroborated", "missing_exit"} else None
    (tmp_path / "live-checker-resources.tsv").write_text(
        "" if outcome == "missing_cgroup" else f"1\t50000000\t60000000\t{vmhwm or ''}\n",
        encoding="utf-8",
    )

    def run_checker(*_, **__):
        status = 1 if outcome == "nonzero" else 0
        peak = step.OBSERVER_LIMIT // 1024 + 1 if outcome == "over_limit" else 47_704
        marker = {"exit_status": status, "peak_rss_kib": peak}
        (tmp_path / "live-checker.stderr").write_text(
            ""
            if outcome in {"missing_exit", "nonzero", "violation"}
            else "J2_CHECKER " + json.dumps(marker) + "\n",
            encoding="utf-8",
        )
        drain.write(
            tmp_path / "live-checker.json",
            {
                "status": "PASS",
                "valid": True,
                "trace": {"Snapshot": {}, "Submissions": []},
                "violations": ["duplicate success"] if outcome == "violation" else [],
            },
        )
        return status

    driver = SimpleNamespace(
        observer="observer",
        container_groups={(pod, "uid"): "/observer"},
        output=tmp_path,
        node_exec=["node"],
        env={},
        checker_args=lambda *args: args,
        run_stage=run_checker,
    )
    monkeypatch.setattr(
        step, "sample_checker", lambda *_: nullcontext(SimpleNamespace(returncode=0))
    )

    def observe():
        return clock.read(unpublished=int(expiry))

    def check(state):
        _, _, sampling = step.capture_checker(
            driver, {pod: {"uid": "uid"}}, [], terminal=not expiry
        )
        require_checker_resources(sampling)
        return state

    def run():
        return drive(clock, observe, check, retained, expiry=lambda: check(observe()))

    if outcome in {"fast", "corroborated"}:
        if expiry:
            with pytest.raises(drain.DrainExpired):
                run()
        else:
            assert run()["status"] == "PASS"
        summary = drain.read(tmp_path / "checker-resource-summary.json")
        assert summary["checker_peak_rss_kib"] == 47_704
        assert summary["checker_vmhwm_peak_kib"] == vmhwm
        assert summary["samples"] == 1
    elif outcome in {"missing_exit", "missing_cgroup", "over_limit"}:
        with pytest.raises(ResourceEvidenceError) as stopped:
            run()
        assert stopped.value.classification == "invalid"
        assert not isinstance(stopped.value, drain.IntegrityError)
        if expiry:
            assert retained[-1][0] == "deadline"
            assert retained[-1][1]["classification"] == "invalid"
        else:
            assert retained[-1][0] == "check-invalid"
    else:
        with pytest.raises(drain.IntegrityError) as stopped:
            run()
        assert stopped.value.classification == "integrity_stop"


@pytest.mark.parametrize("integrity", [False, True])
def test_only_checked_grid_drain_failure_continues_to_fresh_episode(
    tmp_path, monkeypatch, integrity
):
    episodes = []
    failed = (
        drain.IntegrityError("duplicate success")
        if integrity
        else drain.DrainExpired("grid expiry", {"check_status": "PASS"})
    )

    def fresh_episode(name, _addresses, _exporter, previous):
        assert previous == (episodes[-1] if episodes else None)
        episodes.append(name)
        return name

    def exercise(_driver, name, **_):
        if name == "grid-drain":
            raise failed

    driver = SimpleNamespace(
        stage=lambda _: nullcontext(),
        fresh_episode=fresh_episode,
        pod=lambda *_: {"metadata": {"name": "observer"}},
        kube=lambda *_: {"items": []},
        install_checker=lambda: None,
        evidence=tmp_path,
    )
    monkeypatch.setattr(step, "start_profile", lambda *_: (None, None))
    monkeypatch.setattr(step, "install_sender", lambda *_, **__: None)
    monkeypatch.setattr(step, "exercise", exercise)
    with pytest.raises(type(failed)) as error:
        step.rehearse(driver, None, None, {False: (None, "stock"), True: (None, "two-period")}, "1")
    assert error.value is failed
    assert episodes == (["grid-drain"] if integrity else ["grid-drain", "step-drain"])


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
    assert retained[-1][1]["check_status"] == "PASS"


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


@pytest.mark.parametrize(
    "defect", [None, "violation", "pending_set", "duplicate_success", "missing_acknowledged"]
)
def test_expiry_reconciles_completed_work_and_preserves_pending(
    episode,  # noqa: F811
    tmp_path,
    monkeypatch,
    defect,
):
    _, trace, starts, polls, uncertainty, observed = episode
    trace["Submissions"] = [
        {"WorkflowID": key, "Key": key, "Hash": key + "-hash", "Namespace": "j2"} for key in "ab"
    ]
    workflow = trace["Snapshot"]["engine.workflow_executions"]["rows"][0]
    workflow.update(submission_key="a", submission_payload_hash="a-hash", namespace="j2")
    trace["Snapshot"]["engine.workflow_executions"]["rows"].append(
        {
            "workflow_id": "b",
            "submission_key": "b",
            "submission_payload_hash": "b-hash",
            "namespace": "j2",
            "revision": 1,
            "state": "RUNNING",
        }
    )
    events = []
    for i, start in enumerate(starts):
        start.update(
            invocation_id=str(i), pod_uid="p", session="s", sequence=i * 2, monotonic_ns=i * 2
        )
        events.extend(
            (start, {**start, "event": "end", "sequence": i * 2 + 1, "outcome": "SUCCEEDED"})
        )
    events.append({**starts[0], "workflow_id": "b", "invocation_id": "pending"})
    monkeypatch.setattr(drain, "witness_events", lambda _: events)
    for row in trace["Snapshot"]["engine.outbox"]["rows"]:
        row["published_at"] = None
    if defect == "duplicate_success":
        history = trace["Snapshot"]["engine.transition_history"]["rows"]
        history.append(next(row for row in history if row["new_state"] == "SUCCEEDED").copy())
    for table in trace["Snapshot"].values():
        rows = table["rows"]
        table.update(count=len(rows), sha256=hashlib.sha256(drain.json_bytes(rows)).hexdigest())
    path = tmp_path / "live-checker.json"
    path.write_bytes(
        drain.json_bytes(
            {
                "status": "PASS",
                "valid": True,
                "non_terminal": [] if defect == "pending_set" else ["b"],
                "violations": ["structural violation"] if defect == "violation" else [],
                "workflow_ids": ["a", "b"],
                "trace": trace,
            }
        )
    )
    report, _ = drain.indexed_report(
        path,
        snapshot=tmp_path / "snapshot.json",
        join_trace=True,
        verify_tables=True,
        drain_counts=True,
    )
    expiry = drain.checked_cut(report, {"upper_unix_ns": observed}, [])
    target = expected("abc")
    target["a"]["scheduled_ns"] = S + 1
    acknowledged = ["a", "c"] if defect == "missing_acknowledged" else ["a"]
    sender = {"acknowledged": acknowledged, "explicitly_unsent": []}
    args = report, target, sender, {}, polls, uncertainty, observed
    if defect == "missing_acknowledged":
        with pytest.raises(ValueError, match=r"checked identity mismatch: .*'missing': \['c'\]"):
            drain.checked_results(*args, expiry=expiry)
    elif defect:
        with pytest.raises(ValueError, match="structural|pending set|duplicated.*success"):
            drain.checked_results(*args, expiry=expiry)
    else:
        (row,) = drain.checked_results(*args, expiry=expiry)
        assert row["workflow_id"] == "a"
        assert expiry["db"]["unpublished"] == 1
        residuals = drain.readiness(expiry, target, sender)
        assert residuals["pending"] == ["b"]
        assert residuals["missing"] == ["b", "c"]
        assert residuals["missing_acknowledged"] == []
        assert (tmp_path / "snapshot.json").is_file()


def test_full_clock_schedule_stays_below_pinned_observer_volume(monkeypatch):
    monkeypatch.setenv("GITHUB_RUN_ID", "12")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    grid = step.episode_schedule("grid-drain", stepped=False, hold_rate="4")
    full = step.episode_schedule("step-drain", stepped=True)
    assert grid["count"] == 720
    assert full["warmup_end_ns"] - full["origin_ns"] == 120 * S
    assert full["hold_end_ns"] - full["origin_ns"] == 1920 * S
    assert full["stop_ns"] - full["hold_end_ns"] == 300 * S
    assert [period["count"] for period in full["periods"]] == [1920, 7300]
    assert full["periods"][1]["rate"] == step.RATES["C"]
    assert full["count"] == 9220 < step.POPULATION_LIMIT
    retry = step.episode_schedule("step-drain", stepped=True, hold_rate="4")
    assert full["worker_sleep_ms"] == retry["worker_sleep_ms"] == grid["worker_sleep_ms"] == 100
    assert retry["hold_rate_scope"] == "non-grid engineering hold"
    assert retry["periods"] == [{"rate": "4", "count": 7680, "seconds": 1920}, full["periods"][1]]
    assert retry["count"] == 14980 < step.POPULATION_LIMIT
    assert "intervention" not in full
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


def test_eight_per_second_hold_fits_observer_limit_and_preserves_c_period(monkeypatch):
    monkeypatch.setenv("GITHUB_RUN_ID", "12")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    schedule = step.episode_schedule("step-drain", stepped=True, hold_rate="8")
    assert schedule["periods"] == [
        {"rate": "8", "count": 15360, "seconds": 1920},
        {"rate": step.RATES["C"], "count": 7300, "seconds": 300},
    ]
    assert schedule["count"] == 22660 <= step.POPULATION_LIMIT


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


@pytest.mark.parametrize("same_identity", [True, False])
def test_carry_probe_reads_natural_pending_work_on_both_sides(tmp_path, monkeypatch, same_identity):
    boundary = 10 * S
    identity = "dur053-run-12-000001"
    brackets = iter(
        [
            (-800, -770, []),
            (-400, -380, ["dur053-run-12-000002"]),
            (-150, -100, [identity]),
            (-60, 60, ["dur053-run-12-000002"]),
            (80, 130, [identity if same_identity else "dur053-run-12-000000"]),
            (200, 250, []),
            (400, 450, []),
        ]
    )
    monkeypatch.setattr(step, "wait_until", lambda *_: True)

    def bracketed(probe):
        lower, upper, ids = next(brackets)
        assert probe() == []
        return ids, {
            "lower_mono_ns": boundary + lower * 1_000_000,
            "upper_mono_ns": boundary + upper * 1_000_000,
        }

    monkeypatch.setattr(step, "bracketed", bracketed)
    driver = SimpleNamespace(
        read_database=lambda _: "",
        worker_uids=lambda: ["a", "b"],
        output=tmp_path,
        worker_sleep_ms=100,
    )
    schedule = dict(run_id="run", seed=12, periods=[{"count": 2}], rate="4", hold_end_ns=boundary)
    step.carry_probe(driver, schedule, boundary, None)
    record = drain.read(tmp_path / "step-transition.json")
    assert record["observed_carry_ids"] == ([identity] if same_identity else [])
    assert record["status"] == ("observed" if same_identity else "unobserved")
    assert record["worker_uids_before"] == record["worker_uids_after"] == ["a", "b"]
    assert record["valid_before_reads"] == record["valid_after_reads"] == 3


def test_a_straddling_or_stale_read_cannot_establish_carry_in():
    reads = [
        {"lower_mono_ns": 1, "upper_mono_ns": S - 1, "pending_hold_ids": ["old"]},
        {"lower_mono_ns": 2 * S - 2, "upper_mono_ns": 2 * S + 2, "pending_hold_ids": ["old"]},
        {"lower_mono_ns": 2 * S, "upper_mono_ns": 2 * S + 2, "pending_hold_ids": ["old"]},
    ]
    assert step.observed_carry(reads, 2 * S) == {
        "valid_before_reads": 0,
        "valid_after_reads": 1,
        "observed_carry_ids": [],
    }


@pytest.mark.parametrize("expired", [False, True])
def test_archive_rejects_a_changed_two_period_schedule_before_replay(
    tmp_path, monkeypatch, expired
):
    episode_dir = tmp_path / "evidence"
    episode_dir.mkdir()
    for member in drain.REPLAY_FILES:
        (episode_dir / member).write_text("{}", encoding="utf-8")
    result = (
        {"status": "FAIL", "classification": "drain_failure"} if expired else {"status": "PASS"}
    )
    drain.write(episode_dir / "drain-result.json", result)
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
    summary = drain.read(receipt)["step-drain"]
    assert summary["status"] == result["status"]
    if expired:
        assert summary["classification"] == "drain_failure" and summary["check_status"] == "PASS"
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
