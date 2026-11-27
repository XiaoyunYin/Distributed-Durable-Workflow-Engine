import hashlib
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))

import j2_attribution as attribution  # noqa: E402
import j2_checker_report as checker  # noqa: E402
import j2_control_evidence as control  # noqa: E402
import j2_schedule as schedule  # noqa: E402
from j2_drain import expected_arrivals  # noqa: E402
from j2_measure_authorization import cell_traffic  # noqa: E402
from j2_offline import join  # noqa: E402
from j2_qualification_evidence import file_record, write  # noqa: E402
from test_j2_offline import episode as offline_episode  # noqa: E402, F401
from test_j2_schedule import ORIGIN, arrival, encoded, stock_plan  # noqa: E402

S = schedule.SECOND


def hash_bytes(raw):
    return hashlib.sha256(raw).hexdigest()


def fresh_schedule(*, step=False, origin=ORIGIN):
    common = ("eng-r422", "job-1", "soak" if step else "grid", "run", "j2-test", origin)
    if step:
        raw = schedule.continuous_schedule(*common, ("1", 2), ("2", 1), warmup=1)
    else:
        raw = schedule.stock_schedule(*common, "1", 0, 1)
    return {**raw, "dispatch_journal": schedule.DISPATCH_JOURNAL}


def dispatch_fixture(plan_schedule, sha, *, changed=None, crash=False, torn=False):
    head = stock_plan(plan_schedule)
    head.update(
        schema=schedule.DISPATCH_JOURNAL, origin_ns=plan_schedule["origin_ns"], schedule_sha256=sha
    )
    intents, outcomes = [], []
    for i, (row, (at, cutoff)) in enumerate(
        zip(head["plan"]["rows"], schedule.schedule_slots(plan_schedule), strict=True)
    ):
        key = row["key"]
        activity = {8: "pure.echo", 9: "pure.add"}.get(i % 10, "dur048.sleep")
        payload = (
            [2, 3] if activity == "pure.add" else {"run_id": plan_schedule["run_id"], "sequence": i}
        )
        body = dict(
            workflow_id=key,
            submission_key=key,
            namespace=plan_schedule["namespace"],
            payload=payload,
            actor_id="dur053-loadgen",
            definition_id=f"dur053-{plan_schedule['run_id']}-{activity}",
            definition_version=1,
            initial_node_id=activity,
            initial_input=payload,
        )
        relative = at - plan_schedule["origin_ns"]
        end = cutoff - plan_schedule["origin_ns"]
        row.update(
            request=body,
            expected=5 if activity == "pure.add" else payload,
            activity=activity,
            scheduled_ns=relative,
            scheduled_at=schedule.timestamp(at),
        )
        request_hash = hash_bytes(json.dumps(body, sort_keys=True, separators=(",", ":")).encode())
        intents.append(
            dict(
                event="intent",
                sequence=i,
                key=key,
                workflow_id=key,
                at_ns=relative,
                cutoff_ns=end,
                request_sha256=request_hash,
                prepared_ns=-4 * S,
            )
        )
        outcome = dict(
            event="outcome",
            sequence=i,
            key=key,
            dispatch_intent_ns=relative,
            transport_entry_ns=relative,
            request_written_ns=relative,
            refusal_ns=None,
            completed_ns=relative + 5,
            write_callbacks=1,
            status=201,
            outcome="accepted",
            error="",
            in_flight_at_release=1,
            acknowledged_workflow_id=key,
            batch=1,
        )
        if changed:
            changed(i, relative, end, outcome)
        outcomes.append(outcome)
    prefix = encoded(head, *intents)
    precommit = prefix + encoded(
        dict(event="precommit", count=len(intents), sha256=hash_bytes(prefix))
    )
    if crash:
        return head, precommit + (b'{"event":"outco' if torn else b"")
    record = encoded(
        dict(
            event="flush",
            batch=0,
            lock_acquired_ns=None,
            sync_entry_ns=-4 * S,
            sync_return_ns=-3 * S,
            directory_sync_return_ns=-2 * S,
        ),
        *outcomes,
        dict(
            event="flush", batch=1, lock_acquired_ns=None, sync_entry_ns=3 * S, sync_return_ns=3 * S
        ),
    )
    return head, precommit + record + encoded(
        dict(event="closed", count=len(outcomes), sha256=hash_bytes(record))
    )


def test_r422_go_python_cutoff_vectors():
    def offsets(cell, rate):
        plan = schedule.registered_schedule(
            schedule.MEASURED_CAMPAIGN, "job-1", cell, "run", "j2-test", ORIGIN, rate
        )
        plan["dispatch_journal"] = schedule.DISPATCH_JOURNAL
        return [(at - ORIGIN, end - ORIGIN) for at, end in schedule.schedule_slots(plan)]

    assert offsets("BC", "BC")[-1] == (719995417835, 720049080756)
    stepped = offsets("soak", "AB")
    assert stepped[19376:19378] == [(1919942251058, 1920000000000), (1920000000000, 1920041099788)]
    assert stepped[-1] == (2219987353597, 2220028453385)


@pytest.mark.parametrize("step", [False, True])
@pytest.mark.parametrize(
    "when", ["first", "near-cutoff", "early", "at-cutoff", "late", "missing", "duplicate"]
)
def test_request_write_defines_delivery_and_retains_timing(step, when):
    plan = fresh_schedule(step=step)
    sha = "c" * 64

    def move(i, at, cutoff, row):
        if i != 0:
            return
        written = {
            "first": at,
            "near-cutoff": cutoff - 1,
            "early": at - 1,
            "at-cutoff": cutoff,
            "late": cutoff + 1,
            "missing": None,
            "duplicate": at,
        }[when]
        row["request_written_ns"] = written
        row["write_callbacks"] = 2 if when == "duplicate" else int(written is not None)
        if written is not None:
            row["dispatch_intent_ns"] = row["transport_entry_ns"] = min(at, written)
            row["completed_ns"] = max(row["completed_ns"], written)

    head, raw = dispatch_fixture(plan, sha, changed=move)
    ids = [row["key"] for row in head["plan"]["rows"]]
    result = schedule.sender_report(raw, plan, ids, schedule_sha256=sha)
    assert result["all_accepted_verdict"] == "PASS" and result["stock_verdict"] is None
    if when in {"first", "near-cutoff"}:
        assert not result["timing_exceptions"] and result["sent_keys"] == ids
    else:
        (problem,) = result["timing_exceptions"]
        assert problem["key"] == ids[0] and problem["cutoff_ns"] > problem["at_ns"]
        assert problem["precommit_flush"]["directory_sync_return_ns"] == -2 * S
        assert problem["outcome_flush"]["sync_return_ns"] == 3 * S
        assert result["outside_slot"] == (
            [ids[0]] if when in {"early", "at-cutoff", "late"} else []
        )


@pytest.mark.parametrize(
    "mode",
    [
        "unresolved",
        "preflight",
        "missing-journal",
        "committed",
        "wrong-admission",
        "torn",
        "written",
        "response",
        "timeout",
        "refused",
        "contradiction",
    ],
)
def test_authorization_uses_positive_evidence_and_clean_refusal(tmp_path, monkeypatch, mode):
    plan = fresh_schedule()
    write(tmp_path / "schedule.json", plan)
    sha = file_record(tmp_path / "schedule.json")["sha256"]

    def outcome(_i, _at, _end, row):
        if mode in {"timeout", "response"}:
            row.pop("acknowledged_workflow_id")
            row.update(
                status=0 if mode == "timeout" else 503, outcome="uncertain", error="request-timeout"
            )
            if mode == "response":
                row.update(request_written_ns=None, write_callbacks=0)
        elif mode in {"refused", "contradiction"}:
            row.pop("acknowledged_workflow_id")
            row.update(
                transport_entry_ns=None,
                request_written_ns=None,
                refusal_ns=0,
                write_callbacks=0,
                status=0,
                outcome="unsent",
                error="capacity-missed",
                in_flight_at_release=16,
            )

    head, raw = dispatch_fixture(
        plan,
        sha,
        changed=outcome,
        crash=mode
        in {"unresolved", "preflight", "missing-journal", "committed", "wrong-admission", "torn"},
        torn=mode == "torn",
    )
    key = head["plan"]["rows"][0]["key"]
    write(tmp_path / "sender-release.json", {"run_id": plan["run_id"]})
    if mode != "missing-journal":
        (tmp_path / "sender-journal.jsonl").write_bytes(raw)
    write(tmp_path / "journal-read.json", {"exit": 0})
    if mode in {"preflight", "missing-journal"}:
        write(tmp_path / "sender-exit.json", {"exec_exit": 2})
        write(
            tmp_path / "sender-events.jsonl",
            {
                "schema": schedule.DISPATCH_JOURNAL,
                "run_id": plan["run_id"],
                "schedule_sha256": sha,
                "dispatch_enabled": False,
                "accepted": 0,
            },
        )
    if mode in {"committed", "wrong-admission", "contradiction"}:
        expected, _ = expected_arrivals(raw, plan, schedule_sha256=sha)
        write(tmp_path / "live-checker-read.json", {"exit": 0})
        (tmp_path / "live-checker.json").touch()
        admitted = dict(
            WorkflowID=key, Key=key, Namespace=plan["namespace"], Hash=expected[key]["hash"]
        )
        if mode == "wrong-admission":
            admitted["Hash"] = "wrong"
        monkeypatch.setattr(
            checker,
            "indexed_report",
            lambda *_a, **_kw: (
                {"status": "PASS", "valid": True, "trace": {"Submissions": [admitted]}},
                {},
            ),
        )
    result = cell_traffic(tmp_path)
    expected = {
        "preflight": "no_requests",
        "missing-journal": "no_requests",
        "refused": "no_requests",
        "committed": "consumed",
        "written": "consumed",
        "response": "consumed",
        "timeout": "consumed",
    }
    assert result["disposition"] == expected.get(mode, "unresolved")
    if mode == "committed":
        report = schedule.sender_report(raw, plan, [key], schedule_sha256=sha)
        assert (
            report["durable_admissions"] == report["remaining_intents"] == [key]
            and not report["sent_keys"]
        )
    if mode == "contradiction":
        with pytest.raises(checker.IntegrityError, match="refused"):
            schedule.sender_report(raw, plan, [key], schedule_sha256=sha)


@pytest.mark.parametrize("damage", ["duplicate", "binding", "seal", "legacy"])
def test_journal_fails_closed_on_schema_binding_or_integrity_drift(damage):
    plan = fresh_schedule()
    sha = "c" * 64
    _, raw = dispatch_fixture(plan, sha)
    if damage == "duplicate":
        lines = raw.splitlines(keepends=True)
        raw = b"".join(lines[:5] + [lines[4]] + lines[5:])
    elif damage == "binding":
        sha = "a" * 64
    elif damage == "seal":
        raw = raw.replace(b'"prepared_ns": -4000000000', b'"prepared_ns": -5000000000')
    else:
        del plan["dispatch_journal"]
    with pytest.raises(ValueError, match="duplicate|binding|digest|versioned sender"):
        schedule.sender_report(raw, plan, [], schedule_sha256=sha)


def test_attribution_joins_new_healthy_journal_to_the_same_winning_phases(
    offline_episode,  # noqa: F811
    tmp_path,
    monkeypatch,
):
    planned, snapshot, events, first_observed, width, observed = offline_episode
    plan = fresh_schedule(origin=planned[0][1])
    sha = "a" * 64
    head, actual = dispatch_fixture(plan, sha)
    row = head["plan"]["rows"][0]
    key = row["key"]

    def use_equivalent_key(value):
        return json.loads(json.dumps(value).replace('"a"', json.dumps(key)))

    snapshot, events, first_observed = map(use_equivalent_key, (snapshot, events, first_observed))
    legacy = {k: v for k, v in plan.items() if k != "dispatch_journal"}
    request_hash = hash_bytes(
        json.dumps(row["request"], sort_keys=True, separators=(",", ":")).encode()
    )
    old = encoded(
        {"event": "plan", "plan": head["plan"]},
        {**arrival(row, sent_ns=plan["origin_ns"]), "request_hash": request_hash},
        {
            **arrival(row, sent_ns=plan["origin_ns"], outcome="accepted", status=201),
            "request_hash": request_hash,
        },
        {"event": "closed"},
    )
    expected_old, _ = expected_arrivals(old, legacy)
    expected_new, _ = expected_arrivals(actual, plan, schedule_sha256=sha)
    assert expected_old == expected_new
    got = list(
        join(
            [(key, expected_new[key]["scheduled_ns"])],
            snapshot,
            events,
            first_observed,
            width,
            observed,
        )
    )
    want = list(
        join(
            [(key, expected_old[key]["scheduled_ns"])],
            snapshot,
            events,
            first_observed,
            width,
            observed,
        )
    )
    assert got == want and got[0]["status"] == "observed"
    outbox = snapshot["Snapshot"]["engine.outbox"]["rows"][0]
    outbox.update(event_id="result-id", published_at=schedule.timestamp(got[0]["terminal_ns"]))
    snapshot["Snapshot"]["engine.outbox_publications"] = {
        "rows": [{"event_id": "result-id", "published_at": outbox["published_at"]}]
    }
    upper = first_observed[key]
    polls = [
        {
            "lower_mono_ns": upper,
            "upper_mono_ns": upper,
            "lower_unix_ns": upper,
            "upper_unix_ns": upper,
            "terminal_ids": [key],
        }
    ]
    monkeypatch.setattr(control, "witness_events", lambda value: value)

    def delay(raw, expected):
        rows, _ = control.journal_arrivals(raw)
        return control.delay_fixture(
            {"plan": rows, "expected": expected, "witness": events, "width": width},
            snapshot,
            polls,
            [{"observed_at": schedule.timestamp(planned[0][1])}],
        )["verdict"]

    assert delay(actual, expected_new) == delay(old, expected_old)
    write(
        tmp_path / "sender-report.json",
        schedule.sender_report(actual, plan, [key], schedule_sha256=sha),
    )
    (samples,) = attribution._sender_samples(tmp_path, plan, [plan["origin_ns"], plan["stop_ns"]])
    assert samples["peak_intent_lateness_ns"] == samples["peak_request_write_lateness_ns"] == 0
