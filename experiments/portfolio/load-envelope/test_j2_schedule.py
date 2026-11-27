import ast
import base64
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))

import j2_harness as harness  # noqa: E402
import j2_schedule as schedule  # noqa: E402
import j2_schedule_rehearsal as hosted  # noqa: E402

ORIGIN = 1770000000000000123


def stock_plan(period):
    rows = []
    for i in range(period["count"]):
        key = f"dur053-{period['run_id']}-{period['seed']}-{i:06d}"
        offset = int(i * 1_000_000_000 / float(period["rate"]))
        rows.append(
            {
                "key": key,
                "workflow_id": key,
                "sequence": i,
                "scheduled_ns": offset,
                "scheduled_at": schedule.timestamp(period["origin_ns"] + offset),
                "outcome": "NOT_SENT",
            }
        )
    return {
        "event": "plan",
        "plan": {
            "schema": "dur053.arrivals.v1",
            "status": "IN_PROGRESS",
            "arm": "short",
            "namespace": period["namespace"],
            "run_id": period["run_id"],
            "seed": period["seed"],
            "rate": float(period["rate"]),
            "concurrency": period["cap"],
            "offered": period["count"],
            "rows": rows,
        },
    }


def arrival(row, *, outcome="uncertain", status=0, sent_ns=None, error=""):
    key = row["key"]
    return {
        "event": "arrival",
        "id": key,
        "workflow": key,
        "key": key,
        "scheduled": row["scheduled_at"],
        "outcome": outcome,
        "sent": schedule.timestamp(sent_ns) if sent_ns is not None else None,
        "status": status,
        "error": error,
        "attempts": int(sent_ns is not None),
    }


def encoded(*records):
    return b"".join(json.dumps(row).encode() + b"\n" for row in records)


def period(rate="1", *, warmup=1, measured=2):
    return schedule.stock_schedule(
        "eng-1", "job-1", "grid", "run-1", "j2-test", ORIGIN, rate, warmup, measured
    )


def test_registered_orders_rates_and_half_open_schedule():
    assert schedule.ORDERS == (
        ("A", "AB", "B", "BC", "C"),
        ("B", "BC", "C", "A", "AB"),
        ("C", "A", "AB", "B", "BC"),
    )
    for key, expected in {
        "A": 5138,
        "AB": 7267,
        "B": 10276,
        "BC": 13418,
        "C": 17519,
    }.items():
        p = period(schedule.RATES[key], warmup=120, measured=600)
        assert p["count"] == expected
        assert p["warmup_end_ns"] == ORIGIN + 120 * schedule.SECOND
        assert p["stop_ns"] == ORIGIN + 720 * schedule.SECOND
        tail = int(expected * schedule.SECOND / float(p["rate"]))
        last = int((expected - 1) * schedule.SECOND / float(p["rate"]))
        assert last < 720 * schedule.SECOND <= tail
    p = period()
    assert p["seed"] == int(hashlib.sha256(b"eng-1/job-1/grid").hexdigest()[:8], 16) >> 1


def test_interrupted_journal_keeps_durable_plan_rows_and_unresolved_separate():
    p = period()
    head = stock_plan(p)
    first, second, third = head["plan"]["rows"]
    intent = arrival(second, sent_ns=ORIGIN + schedule.SECOND + 9)
    data = encoded(head, intent) + b'{"event":"arrival"'
    report = schedule.k4_report(data, p, [first["key"], second["key"]])
    assert report["durable_admissions"] == [first["key"], second["key"]]
    assert report["admitted_without_acknowledgment"] == [first["key"], second["key"]]
    assert report["unresolved"] == [third["key"]]
    assert report["torn_suffix"] and not report["closed"]
    assert report["stock_verdict"] == "FAIL"
    assert report["remaining_intents"] == [second["key"]]
    assert report["sender_samples"][0]["recorded_intent_lateness_ns"] == 9


def test_journal_cap_lateness_and_acknowledgment_are_independent():
    p = period()
    head = stock_plan(p)
    first, second, third = head["plan"]["rows"]
    first_intent = arrival(first, sent_ns=ORIGIN + 1)
    second_intent = arrival(second, sent_ns=ORIGIN + schedule.SECOND + 4)
    first_final = {**first_intent, "outcome": "accepted", "status": 201}
    second_final = {**second_intent, "error": "transport-error"}
    third_unsent = arrival(third, outcome="unsent", error="capacity-missed")
    report = schedule.k4_report(
        encoded(
            head,
            first_intent,
            second_intent,
            third_unsent,
            first_final,
            second_final,
            {"event": "closed"},
        ),
        p,
        [second["key"]],
    )
    assert report["max_in_flight_from_journal"] == 2
    assert report["missing_acknowledged"] == [first["key"]]
    assert report["admitted_without_acknowledgment"] == [second["key"]]
    assert report["explicitly_unsent"] == [third["key"]]
    assert report["stock_verdict"] == "FAIL"


def test_completed_journal_records_acceptance_and_rejects_tail_at_stop():
    p = period(warmup=1, measured=1)
    head = stock_plan(p)
    first, second = head["plan"]["rows"]
    first_intent = arrival(first, sent_ns=ORIGIN)
    second_intent = arrival(second, sent_ns=p["stop_ns"])
    completed = [
        head,
        first_intent,
        {**first_intent, "outcome": "accepted", "status": 201},
        second_intent,
        {**second_intent, "outcome": "accepted", "status": 201},
        {"event": "closed"},
    ]
    durable = [first["key"], second["key"]]
    report = schedule.k4_report(encoded(*completed), p, durable)
    assert report["warmup_offered"] == 1
    assert report["stock_verdict"] == "PASS"
    assert report["outside_slot"] == [second["key"]]
    with pytest.raises(ValueError, match="after close"):
        schedule.k4_report(encoded(*completed, first_intent), p, durable)
    with pytest.raises(ValueError, match="unexpected or duplicate"):
        schedule.k4_report(encoded(*completed), p, durable + ["someone-else"])


def test_explicit_unsent_cannot_have_durable_commit():
    p = period()
    head = stock_plan(p)
    first = head["plan"]["rows"][0]
    with pytest.raises(ValueError, match="explicit unsent"):
        schedule.k4_report(
            encoded(head, arrival(first, outcome="unsent", error="capacity-missed")),
            p,
            [first["key"]],
        )


def test_continuous_journal_uses_period_cutoffs_and_consecutive_identities():
    p = schedule.continuous_schedule(
        "eng-1", "job-1", "step", "run-1", "j2-test", ORIGIN, ("1.5", 1), ("2", 1)
    )
    head = stock_plan(p)
    first, tail, step, last = head["plan"]["rows"]
    offsets = (0, 666_666_666, 1_000_000_000, 1_500_000_000)
    for row, value in zip((first, tail, step, last), offsets, strict=True):
        row["scheduled_ns"] = value
        row["scheduled_at"] = schedule.timestamp(ORIGIN + value)
    accepted = [first, step, last]
    records = [head]
    for row in (first, tail, step, last):
        intent = arrival(row, sent_ns=ORIGIN + row["scheduled_ns"])
        records.append(intent)
        if row is tail:
            records.append(arrival(tail, outcome="unsent", error="cutoff-after-intent"))
        else:
            records.append({**intent, "outcome": "accepted", "status": 201})
    records.append({"event": "closed"})
    durable = [row["key"] for row in accepted]
    report = schedule.k4_report(encoded(*records), p, durable)
    assert report["explicitly_unsent"] == [tail["key"]]
    assert report["unresolved"] == report["remaining_intents"] == report["outside_slot"] == []
    assert report["stock_verdict"] == "FAIL"
    records[3]["sent"] = schedule.timestamp(p["hold_end_ns"])
    report = schedule.k4_report(encoded(*records), p, durable)
    assert report["outside_slot"] == [tail["key"]]


def test_observer_sender_install_probe_matches_embedded_stdlib_imports():
    scripts = (
        hosted.INSTALL,
        harness.IMPORT_PROBE,
        harness.WITNESS,
        harness.PUBLISH_STOP,
        harness.INTERRUPT,
    )
    imports = set()
    for script in scripts:
        for node in ast.walk(ast.parse(script)):
            if isinstance(node, ast.Import):
                imports.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                assert node.module is not None and node.level == 0
                imports.add(node.module)
    assert imports == set(hosted.SCHEDULE_POD_IMPORTS)
    assert {name.split(".")[0] for name in imports} <= sys.stdlib_module_names


@pytest.mark.parametrize("verified", [True, False])
def test_observer_install_rehashes_bytes_before_execution(tmp_path, verified):
    raw = b"test stock binary"
    binary = tmp_path / "loadgen"
    observed = hashlib.sha256(raw).hexdigest()
    wanted = observed if verified else "0" * 64
    result = subprocess.run(
        [sys.executable, "-B", "-S", "-c", hosted.INSTALL, str(binary), wanted],
        input=base64.b64encode(raw),
        capture_output=True,
        check=False,
        timeout=10,
    )
    if verified:
        assert result.returncode == 0 and result.stdout.decode().strip() == observed
        assert binary.read_bytes() == raw
    else:
        assert result.returncode != 0 and not binary.exists()


def test_stock_install_checks_pinned_head_and_k4_help(tmp_path, monkeypatch):
    head = "a" * 40
    monkeypatch.setenv("GITHUB_SHA", head)
    path = tmp_path / "sender"
    path.write_bytes(b"sender")
    files = hosted.file_record(path)
    manifest = tmp_path / "sender-build.json"
    hosted.write(
        manifest,
        {
            "schema": "j2.stock-k4-build.v1",
            "source": head,
            "package": hosted.PACKAGE,
            "binary": files,
        },
    )
    help_text = "\t-k4\n\t-k4-start string\n\t-k4-stop string\n"

    def exec_in_pod(*args, **kwargs):
        if hosted.INSTALL in args:
            assert args[-1] == files["sha256"]
            assert base64.b64decode(kwargs["data"]) == b"sender"
            return SimpleNamespace(stdout=files["sha256"])
        assert args[-2:] == (hosted.STOCK, "-help")
        return SimpleNamespace(stdout="", stderr=help_text)

    driver = SimpleNamespace(k=exec_in_pod, output=tmp_path, observer="obs")
    hosted.install_sender(driver, path, manifest)
    assert hosted.read(tmp_path / "sender-install.json")["sha256"] == files["sha256"]
    help_text = "\t-k4-start string\n\t-k4-stop string\n"
    with pytest.raises(ValueError, match="missing K4 mode"):
        hosted.install_sender(driver, path, manifest)
    monkeypatch.setenv("GITHUB_SHA", "b" * 40)
    with pytest.raises(ValueError, match="build identity or hash mismatch"):
        hosted.install_sender(driver, path, manifest)


def test_run_stock_writes_stop_before_launch_and_copies_journal_after_exit(tmp_path, monkeypatch):
    p = period()
    calls = []

    def exec_in_pod(*args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(stdout="raw journal" if "cat" in args else "", returncode=0)

    monkeypatch.setattr(hosted.time, "time_ns", lambda: ORIGIN - 1)
    driver = SimpleNamespace(
        k=exec_in_pod, output=tmp_path, observer="obs", journal_name="sender-journal.jsonl"
    )
    assert hosted.run_stock(driver, p, partial=False).returncode == 0
    assert calls[0][0][-1] == hosted.STOP
    assert calls[0][1]["data"] == schedule.timestamp(p["stop_ns"])
    assert "-k4" in calls[1][0] and schedule.timestamp(p["origin_ns"]) in calls[1][0]
    assert calls[2][0][-2:] == ("cat", hosted.JOURNAL)
    assert (tmp_path / driver.journal_name).read_bytes() == b"raw journal"


def test_continuous_launch_uses_one_process_and_one_output(tmp_path):
    p = schedule.continuous_schedule(
        "eng-1", "job-1", "step", "run-1", "j2-test", ORIGIN, ("1", 8), ("4", 3)
    )
    calls = []

    def exec_in_pod(*args, **kwargs):
        calls.append(args)
        return SimpleNamespace(stdout="raw journal" if "cat" in args else "", returncode=0)

    driver = SimpleNamespace(
        k=exec_in_pod, output=tmp_path, observer="obs", journal_name="sender-journal.jsonl"
    )
    assert hosted.run_continuous(driver, p, partial=False).returncode == 0
    launch, readback = calls
    assert launch[5] == hosted.CONTINUOUS
    assert launch[launch.index("-hold-duration") + 1] == "8s"
    assert launch[launch.index("-step-duration") + 1] == "3s"
    assert launch[launch.index("-step-rate") + 1] == "4"
    assert launch.count("-output") == 1
    assert readback[-2:] == ("cat", launch[-1])
    assert hosted.read(tmp_path / "schedule.json")["count"] == 20


def test_observer_resource_summary_keeps_empty_io_and_missing_samples_distinct():
    entries = [f"100\t{field}\tok\t" for field in hosted.RESOURCE_FILES]
    entries.extend(f"200\t{field}\tok\t" for field in hosted.RESOURCE_FILES)
    result = hosted.resource_summary("\n".join(entries) + "\n")
    assert result["samples"] == 2 and result["complete"] and result["max_gap_ns"] == 100
    assert not hosted.resource_summary("\n".join(entries[:-1]))["complete"]
    entries[-1] = "200\tio.pressure\terror\tunreadable"
    assert hosted.resource_summary("\n".join(entries))["errors"][0]["counter"] == "io.pressure"
