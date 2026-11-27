import base64
import hashlib
import http.client
import json
import sys
import threading
import zipfile
from copy import deepcopy
from http.server import HTTPServer
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))

import j2_control_evidence as control  # noqa: E402
import j2_control_proxy as proxy  # noqa: E402
import j2_controls as hosted  # noqa: E402
from j2_drain import fingerprint  # noqa: E402
from j2_offline import utc_ns  # noqa: E402
from j2_qualification_evidence import file_record, write  # noqa: E402
from j2_schedule import schedule_slots, timestamp  # noqa: E402
from test_j2_offline import episode, ts  # noqa: E402, F401

S = control.SECOND
CAMPAIGN = "eng-controls-12-1"
ORIGIN = 1_770_000_000 * S


def stock(name):
    return control.control_schedule(name, CAMPAIGN, "j2-test", ORIGIN)


def test_control_geometry_and_paired_delays():
    runs = {name: stock(name) for name in control.EPISODES}
    assert len({run["run_id"] for run in runs.values()}) == 4
    assert all(run["campaign"] == CAMPAIGN for run in runs.values())
    a, b, healthy, capped = runs.values()
    assert [p["count"] for p in runs.values()] == [30, 30, 720, 720]
    assert [p["cap"] for p in runs.values()] == [16, 16, 16, 1]
    assert [p["worker_sleep_ms"] for p in runs.values()] == [100, 2100, 100, 100]
    assert {p["request_timeout"] for p in runs.values()} == {"10s"}
    assert [x - ORIGIN for x, _ in schedule_slots(a)] == [i * 10 * S for i in range(30)]
    assert b["warmup_end_ns"] == ORIGIN
    assert healthy["warmup_end_ns"] == capped["warmup_end_ns"] == ORIGIN + 120 * S
    assert healthy["stop_ns"] == capped["stop_ns"] == ORIGIN + 720 * S

    def rows(label, phase, other):
        plan = []
        joined = {}
        for i in range(30):
            key = f"{label}-{i}"
            activity = "dur048.sleep" if i % 10 < 8 else "pure"
            plan.append(dict(key=key, sequence=i, activity=activity))
            run = phase if activity == "dur048.sleep" else other
            joined[key] = dict(workflow_id=key, phase_ns=[0, 0, run, 0], raw_end_to_end_ns=run)
        return {"plan": plan, "rows": joined}

    base = rows("base", 100, 0)
    delayed = rows("delayed", 100 + 2 * S, 100 * S)
    check = control.paired_delays(base, delayed)
    assert [row["sequence"] for row in check["paired"]] == [i for i in range(30) if i % 10 < 8]
    assert check["median_phase_ns"] == [0, 0, 2 * S, 0]
    with pytest.raises(ValueError, match="paired phase"):
        control.paired_delays(base, rows("bad", 100 + S, 0))


def test_only_the_worker_sleep_setting_can_differ():
    containers = [
        {
            "image": "same-pin",
            "env": [{"name": "DUR048_ACTIVITY_DELAY_MS", "value": "100"}],
        }
    ]
    before = {"template": {"spec": {"containers": containers}}}
    after = deepcopy(before)
    worker = after["template"]["spec"]["containers"][0]
    worker["env"][0]["value"] = "2100"
    assert control.worker_comparison(before, after)["after"] == "2100"
    worker["image"] = "different"
    with pytest.raises(ValueError, match="worker specs drifted"):
        control.worker_comparison(before, after)


def test_raw_delay_shift_moves_winner_terminal_receipt_and_poll(episode, monkeypatch):  # noqa: F811
    planned, snapshot, events, _, width, _ = episode
    outbox = snapshot["Snapshot"]["engine.outbox"]["rows"][0]
    outbox.update(event_id="result-id", published_at=ts(7, 5))
    snapshot["Snapshot"]["engine.outbox_publications"] = {
        "rows": [
            {
                "event_id": "result-id",
                "published_at": ts(7, 5),
            }
        ]
    }
    bounds = dict(
        lower_mono_ns=9 * S, upper_mono_ns=9 * S, lower_unix_ns=9 * S, upper_unix_ns=9 * S
    )
    polls = [{**bounds, "terminal_ids": ["a"]}]
    monkeypatch.setattr(control, "witness_events", lambda value: value)
    source = {
        "plan": [{"key": "a", "activity": "dur048.sleep"}],
        "expected": {"a": {"scheduled_ns": planned[0][1]}},
        "witness": events,
        "width": width,
    }
    result = control.delay_fixture(source, snapshot, polls, [{"observed_at": ts(2)}])
    assert result["verdict"]["phase_delta_ns"] == [0, 0, 2 * S, 0]
    assert result["verdict"]["raw_end_to_end_delta_ns"] == 2 * S
    after = result["delayed-input"]
    assert after["polls"][0]["upper_unix_ns"] == 11 * S
    publication = after["snapshot"]["Snapshot"]["engine.outbox_publications"]["rows"][0]
    assert utc_ns(publication["published_at"]) == 9 * S + 5
    assert outbox["published_at"] == ts(7, 5) and polls[0]["upper_unix_ns"] == 9 * S


def test_withheld_downstream_terminals_and_checked_expiry():
    executions = [
        dict(
            workflow_id=k,
            state="SUCCEEDED",
            revision=2,
            namespace="j2",
            submission_key=k,
            submission_payload_hash=k,
        )
        for k in "ab"
    ]
    history = [dict(workflow_id=k, revision=2, new_state="SUCCEEDED") for k in "ab"]
    report = {
        "workflow_ids": list("ab"),
        "trace": {
            "UnpublishedOutbox": 0,
            "Snapshot": {
                "engine.transition_history": {"rows": history},
                "engine.workflow_executions": {"rows": executions},
                "engine.activity_attempts": {"rows": []},
            },
        },
    }
    cut = dict(lower_mono_ns=0, upper_mono_ns=1, lower_unix_ns=0, upper_unix_ns=1)
    source = {
        "state": control.checked_cut(report, cut, []),
        "sender": {"acknowledged": list("ab"), "explicitly_unsent": []},
        "expected": {k: {"hash": k, "namespace": "j2", "scheduled_ns": 0} for k in "ab"},
        "report": report,
        "cut": cut,
    }
    check = control.withheld_terminal(source)
    assert check["baseline"] == history
    assert len(check["duplicated"]) == len(history) == 2
    assert (
        check["verdicts"]["removed"]["missing"]
        == check["verdicts"]["duplicated"]["missing"]
        == ["a"]
    )
    assert check["verdicts"]["removed"]["classification"] == "drain_failure"
    assert check["verdicts"]["duplicated"]["classification"] == "integrity_stop"
    assert check["verdicts"]["duplicated"]["duplicated"] == ["b"]
    expiry = control.checked_expiry(source)["verdict"]
    assert expiry["classification"] == "drain_failure"
    assert expiry["check_status"] == "PASS" and expiry["drain_seconds"] == 600
    assert expiry["succeeded"] == 2 and expiry["missing"] == []


def test_control_samples_use_exporter_timestamp_and_reject_conflicting_reads():
    raw = {
        name: [{"values": values}]
        for name, values in {
            "j2_outstanding": [[2, "1"], [3, "1"], [4, "999"]],
            "j2_unpublished": [[2, "5"], [3, "5"], [4, "999"]],
            "j2_sample_timestamp_seconds": [[2, "1.999999999"], [3, "1.999999999"], [4, "3"]],
            "j2_query_ok": [[2, "1"], [3, "1"], [4, "0"]],
            "up": [[2, "1"], [3, "1"], [4, "1"]],
        }.items()
    }
    assert control.gates_samples(raw) == [
        {"observed_at": timestamp(2 * S - 1), "outstanding": 1, "unpublished": 5},
    ]
    raw["j2_unpublished"][0]["values"][1][1] = "6"
    with pytest.raises(control.ResourceEvidenceError, match="conflicting"):
        control.gates_samples(raw)


def test_capped_sender_cannot_mask_a_required_sample_gap(tmp_path):
    schedule = stock(control.CAPPED)
    empty_series = {
        name: [{"values": []}]
        for name in (
            "up",
            "j2_query_ok",
            "j2_sample_timestamp_seconds",
            "j2_outstanding",
            "j2_unpublished",
        )
    }
    write(tmp_path / "prometheus-samples.json", empty_series)
    run = {
        "schedule": schedule,
        "sender": {"stock_verdict": "FAIL"},
        "expected": {},
        "rows": {},
    }
    with pytest.raises(control.ResourceEvidenceError, match="incomplete measured gate inputs"):
        control.gate_control(tmp_path, run)


def test_cohorts_keep_warmup_carry_and_post_window_success_separate():
    schedule = stock(control.HEALTHY)
    middle, end = schedule["warmup_end_ns"], schedule["stop_ns"]
    times = {"warm": (middle - 1, middle), "last": (end - 1, end)}
    rows = {
        key: {
            "scheduled_ns": scheduled,
            "terminal_ns": terminal,
            "workflow_id": key,
            "phase_ns": [0, 0, 1, 0],
            "end_to_end_ns": 2,
            "admitted": True,
            "attempts": 1,
            "status": "observed",
            "terminal_state": "SUCCEEDED",
        }
        for key, (scheduled, terminal) in times.items()
    }
    cohorts = control.cohort_delivery(
        {
            "schedule": schedule,
            "expected": rows,
            "rows": rows,
            "sent": set(rows),
            "sender": {"durable_admissions": list(rows)},
        }
    )
    assert cohorts["warmup"]["post_window_success"] == ["warm"]
    assert cohorts["measured"]["post_window_success"] == ["last"]
    assert cohorts["measured"]["carry_in_success"] == ["warm"]
    assert not cohorts["measured"]["in_window_success"]
    phases = cohorts["measured"]["phases"]
    assert phases["end_to_end_quantiles"]["p95_ns"] == 2
    assert "p99_ns" not in phases["end_to_end_quantiles"]

    rows = {
        str(i): {
            **rows["last"],
            "workflow_id": str(i),
            "scheduled_ns": middle + i,
            "terminal_ns": middle + i * 2,
            "phase_ns": [0, 0, i, 0],
            "end_to_end_ns": i + 1,
        }
        for i in range(200)
    }
    measured = control.cohort_delivery(
        {
            "schedule": schedule,
            "expected": rows,
            "rows": rows,
            "sent": set(rows),
            "sender": {"durable_admissions": list(rows)},
        }
    )["measured"]
    assert measured["phases"]["end_to_end_quantiles"]["p99_ns"] == 198
    assert measured["end_to_end_p99_phase_ns"] == [0, 0, 197, 0]


def cap_fixture():
    schedule = stock(control.CAPPED)
    plan = [{"key": k, "scheduled_at": timestamp(ORIGIN + i * S)} for i, k in enumerate("abcde")]

    def acknowledged(key, interval):
        return dict(
            key=key,
            error="",
            outcome="accepted",
            sent=timestamp(ORIGIN + interval * S),
            attempts=1,
            status=201,
        )

    def forward(key, interval, hold=0):
        return dict(
            key=key,
            request_sha256=key,
            upstream_status=201,
            downstream_status=201,
            upstream_body_base64=base64.b64encode(response_body(key)).decode(),
            downstream_body_sha256=hashlib.sha256(response_body(key)).hexdigest(),
            entered_unix_ns=ORIGIN + interval * S + 10,
            upstream_end_unix_ns=ORIGIN + interval * S + 20,
            release_unix_ns=ORIGIN + interval * S + hold + 20,
            entered_mono_ns=interval * S + 10,
            upstream_end_mono_ns=interval * S + 20,
            release_mono_ns=interval * S + hold + 20,
            released_mono_ns=interval * S + hold + 30,
        )

    return {
        "schedule": schedule,
        "sender": {
            "max_in_flight_from_journal": 1,
            "stock_verdict": "FAIL",
            "explicitly_unsent": list("bcd"),
            "durable_admissions": list("ae"),
        },
        "plan": plan,
        "journal": [
            acknowledged("a", 0),
            *(
                dict(
                    key=k,
                    error="capacity-missed",
                    outcome="unsent",
                    sent=None,
                    attempts=0,
                    status=0,
                )
                for k in "bcd"
            ),
            acknowledged("e", 4),
        ],
        "expected": {k: {"request_hash": k} for k in "abcde"},
        "rows": dict.fromkeys("ae"),
    }, [forward("a", 0, 3 * S), forward("e", 4)]


def response_body(key):
    return json.dumps({"workflow": {"workflow_id": key}}).encode()


@pytest.mark.parametrize("damage", [None, "hold", "wrong_miss", "replayed", "response"])
def test_positive_and_negative_capped_proof(damage):
    run, rows = cap_fixture()
    if damage == "hold":
        rows[0]["release_mono_ns"] -= 1
    elif damage == "wrong_miss":
        run["journal"][1]["error"] = "missed-deadline"
    elif damage == "replayed":
        rows.append(rows[0])
    elif damage == "response":
        rows[0]["downstream_body_sha256"] = "changed"
    if damage:
        with pytest.raises(ValueError):
            control.capped_control(run, rows)
    else:
        result = control.capped_control(run, rows)
        assert result["status"] == "sender_invalid" and result["stock_verdict"] == "FAIL"
        assert result["capacity_missed"] == list("bcd")
        assert result["deadlines_elapsed_during_hold"] == list("bc")
        assert result["durable_admissions"] == result["checked"] == list("ae")
        assert result["shedding_count"] == 0 and result["saturation_attribution"] is None


def test_proxy_forwards_each_body_once_and_holds_only_the_first(monkeypatch, capsys):
    wire = []
    mono = 0
    clock = type("Clock", (), {})()
    clock.time_ns = clock.monotonic_ns = lambda: mono

    def sleep(seconds):
        nonlocal mono
        mono += round(seconds * S)
        assert len(wire) == 1

    class Response:
        status = 201
        reason = "Created"

        def getheaders(self):
            return [("X-Upstream", "kept"), ("Connection", "keep-alive")]

        def read(self, _):
            return response_body(wire[-1][2]["X-Dur053-K4-Request"])

    class Connection:
        def __init__(self, hostname, port, timeout):
            assert (hostname, port, timeout) == ("api", 8080, 10)

        def request(self, method, path, payload, headers):
            assert method == "POST" and path == proxy.PATH
            wire.append((path, payload, headers))

        def getresponse(self):
            return Response()

        def close(self):
            pass

    clock.sleep = sleep
    monkeypatch.setattr(proxy, "time", clock)
    monkeypatch.setattr(proxy, "HTTPConnection", Connection)
    with HTTPServer(("127.0.0.1", 0), proxy.Proxy) as server:
        server.seen = set()
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            for key, code in (("a", 201), ("a", 409), ("b", 201)):
                client = http.client.HTTPConnection(*server.server_address, timeout=2)
                body = json.dumps({"workflow_id": key}).encode()
                client.request(
                    "POST",
                    proxy.PATH,
                    body,
                    {
                        "Authorization": "Bearer fake",
                        "Content-Type": "application/json",
                        "X-Dur053-K4-Request": key,
                    },
                )
                reply = client.getresponse()
                assert reply.status == code
                if code == 201:
                    assert reply.read() == response_body(key)
                    assert reply.getheader("X-Upstream") == "kept"
                client.close()
        finally:
            server.shutdown()
            thread.join()
    output = capsys.readouterr().out
    assert "Bearer" not in output
    first, second = map(json.loads, output.splitlines())
    assert [r[2]["X-Dur053-K4-Request"] for r in wire] == ["a", "b"]
    assert first["release_mono_ns"] - first["upstream_end_mono_ns"] == 3 * S
    assert second["release_mono_ns"] == second["upstream_end_mono_ns"]


def test_quiet_replay_rejects_a_gap_after_the_quiet_start(tmp_path):
    def state(at):
        return {
            "db": {
                "submissions": [],
                "succeeded": [],
                "unpublished": 0,
                "claimed": 0,
                "terminal_failures": 0,
            },
            "witness": [],
            "lower_mono_ns": at * S,
            "upper_mono_ns": at * S + 1,
        }

    start = state(2)
    final = state(33)
    digest = fingerprint(final)
    cut = {"lower_mono_ns": 0, "upper_mono_ns": S}
    sender = {"acknowledged": [], "explicitly_unsent": []}
    write(tmp_path / "deadline-anchor.json", {"stop_mono_ns": 0})
    log = tmp_path / "drain-polls.jsonl"
    entries = [
        {"event": "quiet-start", **start},
        {
            "event": "poll",
            "ready": True,
            "fingerprint": digest,
            "lower_mono_ns": final["lower_mono_ns"],
            "upper_mono_ns": final["upper_mono_ns"],
        },
    ]
    log.write_text("\n".join(map(json.dumps, entries)), encoding="utf-8")
    assert control.quiet_evidence(tmp_path, final, {}, sender, cut, start)["status"] == "PASS"
    entries.insert(1, {"event": "read-error"})
    log.write_text("\n".join(map(json.dumps, entries)), encoding="utf-8")
    with pytest.raises(ValueError, match="continuous bracketed quiet"):
        control.quiet_evidence(tmp_path, final, {}, sender, cut, start)


def test_archive_derives_before_comparing_runner_verdict(tmp_path, monkeypatch):
    monkeypatch.setattr(hosted, "EPISODES", (hosted.CAPPED,))
    monkeypatch.setattr(hosted, "REPLAY_INPUTS", ("raw.json",))
    source = tmp_path / "raw.json"
    write(source, {"actual": "sender_invalid"})
    members = {
        f"{hosted.CAPPED}/raw.json": file_record(source),
        f"{hosted.CAPPED}/proxy.jsonl": file_record(source),
        "control-verdicts.json": {},
    }
    archive = tmp_path / "archive.zip"

    def derive(root, identifier):
        assert identifier == CAMPAIGN
        return json.loads((root / hosted.CAPPED / "raw.json").read_bytes()), {}

    monkeypatch.setattr(hosted, "derive", derive)
    for actual in ("sender_invalid", "forged"):
        with zipfile.ZipFile(archive, "w") as bundle:
            bundle.writestr(
                "freeze.json", json.dumps({"run_id": "12", "run_attempt": "1", "files": members})
            )
            bundle.write(source, f"{hosted.CAPPED}/raw.json")
            bundle.write(source, f"{hosted.CAPPED}/proxy.jsonl")
            bundle.writestr("control-verdicts.json", json.dumps({"actual": actual}))
        output = tmp_path / "receipt.json"
        if actual == "forged":
            with pytest.raises(ValueError, match="independent control replay differs"):
                hosted.replay_archive(archive, output)
        else:
            hosted.replay_archive(archive, output)
            assert json.loads(output.read_bytes()) == {"actual": actual}
