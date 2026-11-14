"""Round 239 regressions: independently retained evidence and failing controls."""

from __future__ import annotations

import io
import json
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest
from test_k4_driver import pod
from test_k4_offline import episode_fixture, seal_witness, write

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import dur053_k4 as k4  # noqa: E402
import dur053_k4_collectors as collectors  # noqa: E402
import dur053_k4_driver as driver  # noqa: E402
import dur053_k4_evidence as evidence  # noqa: E402
import dur053_k4_schema as schema  # noqa: E402
import dur053_record_k4 as recorder  # noqa: E402

pytest_plugins = ["test_k4_offline"]

EXIT = (
    "2026-11-13 11:44:29.617655479 +0000 UTC k8s.io /tasks/exit "
    '{"container_id":"7c2249c013be566bfbaa721cb61738514df6a7b3cc7088a8deb697905d8242fc",'
    '"id":"7c2249c013be566bfbaa721cb61738514df6a7b3cc7088a8deb697905d8242fc",'
    '"pid":4324,"exited_at":{"seconds":1794570269,"nanos":617577213}}'
)
RETAINED_START = (
    "2026-11-13 12:52:40.884437304 +0000 UTC k8s.io /tasks/start "
    '{"container_id":"965f0f7fb899de104b2553c8015e2fae0662a3d35edd44fd07ef47a8f6b3424e","pid":4334}'
)
RETAINED_EXIT = (
    "2026-11-13 12:52:41.013449142 +0000 UTC k8s.io /tasks/exit "
    '{"container_id":"4ccd6cc203b6bf1708e560d161a06661722d265a272eb4f011e0c826cbcb9e47",'
    '"id":"4ccd6cc203b6bf1708e560d161a06661722d265a272eb4f011e0c826cbcb9e47",'
    '"pid":4362,"exited_at":{"seconds":1794574361,"nanos":13353454}}'
)


def test_raw_watch_resume_bookmark_and_error(tmp_path):
    d = Mock()
    d.k.return_value.stdout = json.dumps(dict(metadata=dict(resourceVersion="123"), items=[]))
    s = collectors.Streams(d, "scratch", tmp_path)
    s.start = Mock()
    s.start_watch("pods")
    args = s.start.call_args.args[1]
    assert args[:5] == ["kubectl", "--context", "kind-dur053-k3a", "get", "--raw"]
    assert args[5] == (
        "/api/v1/namespaces/scratch/pods?watch=true&resourceVersion=123"
        "&allowWatchBookmarks=true&timeoutSeconds=3600"
    )
    bookmark = json.dumps(dict(type="BOOKMARK", object=dict(metadata=dict(resourceVersion="124"))))
    s.observe_pod(bookmark, 10)
    s.observe_endpoint(bookmark, 10)
    assert s.versions == {"pods": "124", "endpoints": "124"} and not s.pods and not s.endpoints
    for kind in ("pods", "endpoints"):
        callback = s.observe_pod if kind == "pods" else s.observe_endpoint
        with pytest.raises(ValueError, match="k4-watch-expired"):
            callback(json.dumps(dict(type="ERROR", object=dict(code=410, reason="Gone"))), 11)


def test_stream_line_framing_and_fail_closed(tmp_path, monkeypatch):
    lines = [
        dict(type="BOOKMARK", object=dict(metadata=dict(resourceVersion="2"))),
        dict(type="MODIFIED", object=pod("new", role="scratch")),
    ]
    proc = Mock(stdout=io.BytesIO(("\n".join(json.dumps(r) for r in lines) + "\n").encode()))
    proc.wait.return_value = 0
    proc.poll.return_value = 0
    monkeypatch.setattr(collectors.subprocess, "Popen", lambda *a, **k: proc)
    s = collectors.Streams(Mock(env={}), "ns", tmp_path)
    s.start("pods", ["raw-watch"], s.observe_pod)
    s.threads[0].join(2)
    assert len(s.pods) == 1 and s.versions["pods"] == "1"
    assert len(s.log_text("pods.jsonl")) == 2
    with pytest.raises(ValueError, match="unexpected stream EOF"):
        s.check()
    # Malformed exits are retained before parsing and have a separate failure record.
    proc.stdout = io.BytesIO(b"bad /tasks/exit line\n")
    s.start("cri-exits", ["ctr"], s.observe_cri)
    s.threads[-1].join(2)
    assert "k4-cri-exit-unparseable" in (tmp_path / "cri-exits-parse-failures.jsonl").read_text()


def test_ctr_real_text_selection_and_identity():
    e = collectors.cri_event(EXIT)
    assert e["container_id"] == "7c2249c013be566bfbaa721cb61738514df6a7b3cc7088a8deb697905d8242fc"
    assert e["exit_status"] == 0 and e["exit_status_source"] == "omitted-zero"
    assert collectors.cri_event(EXIT.replace("/tasks/exit", "/tasks/start")) is None
    status_137 = EXIT.replace('"pid":4324,', '"pid":4324,"exit_status":137,')
    explicit = collectors.cri_event(status_137)
    assert explicit["exit_status"] == 137 and explicit["exit_status_source"] == "explicit"
    malformed_timestamp = EXIT.replace("11:44:29", "not-a-time")
    for line in (
        "/tasks/exit bad",
        status_137.replace('"exit_status":137', '"exit_status":"bad"'),
        malformed_timestamp,
        EXIT[:-2],
    ):
        with pytest.raises(ValueError, match="k4-cri-exit-unparseable"):
            collectors.cri_event(line)
    sessions = [
        dict(container_id=f"containerd://{explicit['container_id']}"),
        dict(container_id="containerd://live"),
    ]
    collectors.apply_process_evidence(sessions, [], [("cri", 1, status_137)])
    assert sessions[0]["exit_code"] == 137 and "death" not in sessions[1]


def test_observe_cri_accepts_retained_ctr_lines():
    streams = collectors.Streams(Mock(), "ns", Path("."))
    assert streams.observe_cri(RETAINED_START, 1794574360.8852007)
    assert streams.observe_cri(RETAINED_EXIT, 1794574361.0145087)
    assert [event["source"] for event in streams.cri_exits] == [RETAINED_EXIT]
    assert streams.cri_exits[0]["exit_status"] == 0


def test_stream_self_check_requires_server_events(tmp_path):
    s = collectors.Streams(Mock(), "ns", tmp_path)
    s.observe_pod(json.dumps(dict(type="ADDED", object=pod(role="scratch"))), 1)
    with pytest.raises(ValueError, match="self-check-timeout"):
        s.self_check(timeout=0)
    s.first_events = dict(pods=1, endpoints=2, **{"cri-exits": 3})
    assert s.self_check()["first_events"] == s.first_events
    s.start = Mock()
    s.start_cri()
    assert s.start.call_args.args[1] == [
        "docker",
        "exec",
        "dur053-k3a-control-plane",
        "ctr",
        "--namespace",
        "k8s.io",
        "events",
    ]


def test_ownerless_scheduler_reaches_checker(tmp_path, freeze, checker):
    m, docs = episode_fixture(tmp_path, freeze)
    row = docs["timeline"]["history"][-1]
    raw = dict(
        reason="RESULT_CONSUMED", owner="", epoch=2, actor_kind="scheduler", new_state="SUCCEEDED"
    )
    row.update(
        kind=collectors.history_kind(raw),
        owner="",
        epoch=2,
        scheduler_epoch=2,
        actor_kind="scheduler",
    )
    write(tmp_path / "timeline.json", docs["timeline"])
    result = evidence.run_checker(checker, tmp_path)
    assert not result["valid"] and any("lacks owner" in r for r in result["violations"]), (
        "ownerless scheduler hidden"
    )
    for actor in ("client", "worker"):
        kind = collectors.history_kind(dict(raw, epoch=0, actor_kind=actor))
        assert kind == "claim"
        row.update(kind=kind, owner="", epoch=0, scheduler_epoch=None, actor_kind=actor)
        write(tmp_path / "timeline.json", docs["timeline"])
        assert evidence.run_checker(checker, tmp_path)["valid"], (
            "client/worker ownerless history rejected"
        )
    assert collectors.history_kind(dict(raw, epoch=0, actor_kind="scheduler")) == "terminal"


def test_ownerless_unknown_actor_keeps_authorization():
    for actor in (None, "unknown", "scheduler"):
        for epoch in (None, 0):
            for reason in ("ATTEMPT_RESULT_RECORDED", "CANCELED", "TIMEOUT", "RESULT_CONSUMED"):
                for state, expected in (("SUCCEEDED", "terminal"), ("RUNNING", "transition")):
                    assert (
                        collectors.history_kind(
                            dict(
                                reason=reason,
                                owner="",
                                epoch=epoch,
                                actor_kind=actor,
                                new_state=state,
                            )
                        )
                        == expected
                    ), "ownerless unknown actor alias hidden"


def journal_fixture(root, m, docs):
    import hashlib

    arrivals = docs["arrivals"]["rows"]
    shifts = {}
    for i, a in enumerate(arrivals):
        scheduled = collectors.utc(driver.stamp(m["warmup_start"] + i / m["rate"]))
        shifts[a["workflow"]] = scheduled - a["scheduled"]
        a["sent"] += shifts[a["workflow"]]
        a["scheduled"] = scheduled
        a["phase"] = "warmup" if a["scheduled"] < m["intervention"] else "measured"
    # The Phase A fixture restarts its grid at intervention. Moving to the live
    # continuous grid must move each workflow's evidence too, preserving latency
    # and lifecycle ordering rather than leaving terminal times before arrival.
    timeline = docs["timeline"]
    for kind in ("claims", "invocations", "receipts", "history"):
        for row in timeline[kind]:
            shift = shifts[row["identity"]["workflow"]]
            for key in ("at", "start", "finish", "last_alive", "abort"):
                if row.get(key) is not None:
                    row[key] += shift
    for row in timeline["workflows"]:
        for key in ("terminal", "first_observed"):
            row[key] += shifts[row["workflow"]]
    for row in timeline["http"]:
        row["at"] += shifts[row["request"].rsplit("-", 1)[0]]
    seal_witness(timeline)
    docs["witness"]["rows"] = timeline["witness"]
    for key in ("timeline", "witness"):
        path = root / evidence.FILES[key]
        write(path, docs[key])
        m["files"][key] = hashlib.sha256(path.read_bytes()).hexdigest()
    arrivals = [a for a in arrivals if a["scheduled"] < m["window_end"]]
    docs["arrivals"]["rows"] = arrivals
    write(root / "arrivals.json", docs["arrivals"])
    m["files"]["arrivals"] = hashlib.sha256((root / "arrivals.json").read_bytes()).hexdigest()
    write(root / "episode.json", m)
    plan = dict(
        rate=m["rate"],
        rows=[
            dict(
                key=a["key"],
                workflow_id=a["workflow"],
                sequence=i,
                scheduled_at=driver.stamp(a["scheduled"]),
            )
            for i, a in enumerate(arrivals)
        ],
    )
    journal = [
        dict(event="plan", plan=plan),
        *[dict(event="arrival", **a) for a in arrivals],
        dict(
            event="stop",
            reason="stop-file",
            at=driver.stamp(m["window_end"]),
            stop=driver.stamp(m["window_end"]),
        ),
    ]
    write_journal(root, journal)
    return journal


def write_journal(root, rows):
    (root / "arrivals-journal.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))


def test_schedule_requires_window_coverage_and_stop(tmp_path, freeze, checker):
    m, docs = episode_fixture(tmp_path, freeze)
    rows = journal_fixture(tmp_path, m, docs)
    assert evidence.schedule_stopped(tmp_path, m)
    assert "k4-arrival-schedule" not in evidence.episode(tmp_path, freeze, checker)["issues"]
    # Remove the tail from both plan and exported arrivals: old verifier passed.
    cutoff = m["window_end"] - 10
    rows[0]["plan"]["rows"] = [
        r for r in rows[0]["plan"]["rows"] if collectors.utc(r["scheduled_at"]) < cutoff
    ]
    kept = {r["key"] for r in rows[0]["plan"]["rows"]}
    rows = [r for r in rows if r.get("event") != "arrival" or r["key"] in kept]
    docs["arrivals"]["rows"] = [a for a in docs["arrivals"]["rows"] if a["key"] in kept]
    write_journal(tmp_path, rows)
    write(tmp_path / "arrivals.json", docs["arrivals"])
    import hashlib

    m["files"]["arrivals"] = hashlib.sha256((tmp_path / "arrivals.json").read_bytes()).hexdigest()
    write(tmp_path / "episode.json", m)
    assert "k4-arrival-schedule" in evidence.episode(tmp_path, freeze, checker)["issues"], (
        "truncated plan accepted"
    )
    write_journal(tmp_path, rows[:-1])
    assert not evidence.schedule_stopped(tmp_path, m)


def test_late_stop_file_observation_has_specific_issue(tmp_path, freeze, checker):
    m, docs = episode_fixture(tmp_path, freeze)
    rows = journal_fixture(tmp_path, m, docs)
    stop = next(row for row in rows if row.get("event") == "stop")
    stop["at"] = driver.stamp(m["window_end"] + 2 / m["rate"])
    write_journal(tmp_path, rows)

    assert evidence.schedule_stopped(tmp_path, m)
    assert not evidence.schedule_stop_timely(tmp_path, m, m["rate"])
    assert "k4-arrival-stop-late" in evidence.episode(tmp_path, freeze, checker)["issues"]


def test_schedule_uses_wire_start_precision(tmp_path, freeze, checker):
    m, docs = episode_fixture(tmp_path, freeze)
    # The host retains a sub-microsecond fraction which stamp() cannot send.
    m["warmup_start"] = 0.0000001
    rows = journal_fixture(tmp_path, m, docs)
    assert rows[0]["plan"]["rows"][0]["scheduled_at"] == driver.stamp(0)
    assert "k4-arrival-schedule" not in evidence.episode(tmp_path, freeze, checker)["issues"], (
        "wire start lost the first arrival"
    )
    # Real fifth-artifact RFC3339Nano value; retain its sub-microsecond digits.
    assert collectors.utc("2026-11-13T15:33:10.399884391Z") == 1794583990 + 0.399884391, (
        "nanosecond fraction truncated"
    )


def test_latency_probes_enclose_interval(tmp_path, freeze, checker):
    m, _ = episode_fixture(tmp_path, freeze)
    value = evidence.episode(tmp_path, freeze, checker)
    assert value["valid"], value["issues"]
    assert value["latency"]["missing"] == 0, "terminal required inside query round trip"
    m["clock_brackets"] = m["clock_brackets"][:1]
    write(tmp_path / "episode.json", m)
    assert "k4-latency-evidence-missing" in evidence.episode(tmp_path, freeze, checker)["issues"]


def test_assignment_setup_censor_is_retained(tmp_path, freeze, checker):
    import hashlib

    m, docs = episode_fixture(tmp_path, freeze)
    row = dict(
        change="startup",
        at=1,
        consumer="one",
        session="s",
        generation=1,
        partitions=[],
        survivors=["one"],
        trigger_uid="uid",
        arrival="",
        disposition_order=None,
        event="revoke",
    )
    docs["kafka"]["rows"] = [row, dict(row, event="stable", at=2)]

    def derive():
        write(tmp_path / "kafka.json", docs["kafka"])
        m["files"]["kafka"] = hashlib.sha256((tmp_path / "kafka.json").read_bytes()).hexdigest()
        write(tmp_path / "episode.json", m)
        return evidence.episode(tmp_path, freeze, checker)

    value = derive()
    assert value["valid"], value["issues"]
    assert value["assignment_stalls"][0]["censored"], "startup censoring hidden"
    row["at"] = m["intervention"]
    docs["kafka"]["rows"][1]["at"] = m["intervention"] + 1
    assert "k4-assignment-disposition-censored" not in derive()["issues"], (
        "idle intervention censoring rejected"
    )


def test_hold_evidence_requires_release_and_reports_expiry():
    state = dict(
        host_start=1, host_end=2, observer=1.5, path="/hold", existed=False, exists=True, mtime=1.5
    )
    released = dict(
        state, host_start=65, host_end=66, observer=65.5, existed=True, exists=False, mtime=0
    )
    h = dict(
        role="worker",
        hold_set=state,
        hold_released=released,
        bound_expiry=61.5,
        bound_expired=True,
        measured_arrivals=1,
    )
    m = dict(hold=h)
    arrivals = [dict(phase="measured", scheduled=10), dict(phase="measured", scheduled=62)]
    schema.validate(h, schema.HOLD)
    assert evidence.hold_evidence(m, arrivals)
    h["hold_released"] = None
    assert not evidence.hold_evidence(m, arrivals), "hold release instant omitted"
    with pytest.raises(ValueError, match="k4-schema"):
        schema.validate({k: v for k, v in h.items() if k != "hold_released"}, schema.HOLD)


def test_response_less_claim_uses_commit_audit():
    a = dict(
        workflow_id="w",
        node_id="n",
        iteration=0,
        attempt_number=1,
        worker_id="pod",
        worker_request_id="req",
        token_hash="a" * 64,
    )
    body = dict(worker_id="pod", request_id="req", attempt_lease_ms=60000, expected_attempt=1)
    import hashlib

    trace = dict(
        event="http_start",
        operation="claim",
        session="worker-session",
        at=9,
        body_hash=hashlib.sha256(json.dumps(body, separators=(",", ":")).encode()).hexdigest(),
        request_id=body["request_id"],
        worker_id=body["worker_id"],
    )
    audit = dict(
        a,
        at=driver.stamp(10),
        finalized_at=driver.stamp(11),
        order=7,
        session="api-session",
        pod_uid="api-pod",
        container_id="api-cid",
        transaction="tx",
    )
    claims = collectors.durable_claims([a], [trace], "ns", [audit])
    assert len(claims) == 1 and claims[0]["at"] == 10 and claims[0]["order"] == 7, (
        "response-less claim lost"
    )
    assert claims[0]["commit_bracket"]["finalized"] == 11
    with pytest.raises(ValueError, match="claim-ack-clock-evidence-missing"):
        collectors.durable_claims([a], [trace], "ns", [])
    with pytest.raises(ValueError, match="claim-session-evidence-missing"):
        collectors.durable_claims([a], [], "ns", [audit])


def stopped_fixture(root, freeze):
    eid = "short-core-0-pair-0-baseline"
    epdir = root / eid
    m, docs = episode_fixture(epdir, freeze, episode=eid)
    docs["timeline"]["workflows"][0]["state"] = "FAILED"
    write(epdir / "timeline.json", docs["timeline"])
    (epdir / "witness.json").unlink()
    m.update(status="FAIL", metrics="not-derivable", missing_exports=["witness.json"])
    write(epdir / "episode.json", m)
    write(
        epdir / "intake-stopped.json",
        dict(error="k4-engine-outcome-with-export-loss", missing_exports=["witness.json"]),
    )
    return m


def test_stopped_intake_charges_failure_and_prevents_go(tmp_path, freeze):
    m = stopped_fixture(tmp_path, freeze)
    d = dict(
        episode=m["episode"], replacement_for="", classification="valid", reason="", evidence=[]
    )
    recorder.validate_disposition(tmp_path, d)
    value = recorder.stopped_intake(tmp_path, m)
    assert value["metrics"] == "not-derivable" and value["status"] == "FAIL"
    summary = evidence.summaries([value])
    assert summary["k7_decision"] == "NO-GO"
    d["classification"] = "infrastructure-invalid"
    d.update(reason="collection-failure", evidence=["incident"])
    with pytest.raises(ValueError, match="engine-outcome-not-infrastructure"):
        recorder.validate_disposition(tmp_path, d)
    (tmp_path / m["episode"] / "intake-stopped.json").unlink()
    with pytest.raises(ValueError, match="evidence-path"):
        recorder.stopped_intake(tmp_path, m)


def test_stopped_intake_requires_observed_engine_failure(tmp_path, freeze):
    m = stopped_fixture(tmp_path, freeze)
    path = tmp_path / m["episode"] / "timeline.json"
    t = json.loads(path.read_bytes())
    t["workflows"][0]["state"] = "SUCCEEDED"
    write(path, t)
    with pytest.raises(ValueError, match="stopped-intake-engine-evidence"):
        recorder.stopped_intake(tmp_path, m)


def test_continuation_needs_claude_acknowledgement(freeze):
    prior = dict(
        job="k4-1",
        job_id="1",
        pending=["suffix"],
        status="FAIL",
        replacement_for="",
        intake_stopped=["short-core-0-pair-0-baseline"],
        dispositions=[],
    )
    with pytest.raises(ValueError, match="claude-acknowledgement-required"):
        k4.assignment(freeze, "k4-1-c1", {"1": prior})
    assert not k4.replacement_ids(prior)
    prior["claude_acknowledgement"] = True
    assert k4.assignment(freeze, "k4-1-c1", {"1": prior}) == ["suffix"]


def test_overhead_end_to_end_and_commit_path_fields():
    plan = dict(rows=[dict(workflow_id="w", scheduled_at=driver.stamp(1))])
    snap = dict(workflows=[dict(workflow_id="w", state="SUCCEEDED", updated_at=driver.stamp(3))])
    value = driver.overhead_sensitivity(plan, snap, {"w"}, [dict(seconds=0.03)])
    assert value["end_to_end_latency"] == dict(n=1, p50=2, p99=2)
    assert value["db_commit_path"]["p50"] == 0.03 and value["db_commit_path"]["p99"] == 0.03


def test_overhead_artifact_contains_both_mode_timings(tmp_path, freeze, monkeypatch):
    monkeypatch.setattr(driver.time, "time", lambda: 100)
    d = driver.K4Driver.__new__(driver.K4Driver)
    d.output = tmp_path
    d.base = Mock()
    d.namespaces = set()
    d.load = Mock()
    journal = [
        dict(
            event="plan",
            plan=dict(rows=[dict(key="w", workflow_id="w", scheduled_at=driver.stamp(110))]),
        ),
        dict(event="arrival", key="w", outcome="accepted"),
    ]
    witness = [
        dict(event="start", id="i", workflow_id="w", at=1),
        dict(event="finish", id="i", at=2),
    ]

    def setup(cell, addresses, *, campaign):
        d.collector = Mock()
        d.collector.log_text.return_value = [
            (
                "log",
                1,
                "2026-01-01T00:00:00Z "
                + json.dumps(dict(data=dict(event="k4_commit_timing", seconds=0.01, success=True))),
            )
        ]
        return "ns", "db"

    d.setup = setup

    def snapshot(*args, **kwargs):
        value = dict(
            workflows=[dict(workflow_id="w", state="SUCCEEDED", updated_at=driver.stamp(115))]
        )
        write(d.episode_output / "database.json", value)
        return value

    d.snapshot = snapshot

    def python(ns, role, code, **kwargs):
        if "k4-arrivals" in code:
            return "".join(json.dumps(r) + "\n" for r in journal)
        if "for p in pathlib.Path" in code:
            return json.dumps(witness)
        return ""

    d.base.python.side_effect = python
    d.overhead_probe({}, freeze)
    result = json.loads((tmp_path / "instrumentation-overhead.json").read_bytes())
    assert set(result["modes"]) == {"off", "on"}
    for value in result["modes"].values():
        assert value["end_to_end_latency"] == dict(n=1, p50=5, p99=5)
        assert value["db_commit_path"]["p99"] == 0.01
    assert driver.verify_overhead(tmp_path, freeze) == result


def test_stopped_intake_record_and_committed_ack_gate(tmp_path, checker, monkeypatch):
    import hashlib
    import lzma

    import dur053_payload
    from test_k4_offline import empty_job, git_commit, local_repository

    monkeypatch.setattr(
        dur053_payload, "FILTERS", [dict(id=lzma.FILTER_LZMA2, preset=0, dict_size=1 << 20)]
    )
    repo = local_repository(tmp_path / "repo")
    f = k4.frozen(repo)
    source = tmp_path / "raw"
    ep = stopped_fixture(source, f)
    ep["job"] = "k4-1"
    write(source / ep["episode"] / "episode.json", ep)
    value = recorder.stopped_intake(source, ep)
    write(source / ep["episode"] / "metrics.json", value)
    m = empty_job(source, f)
    m.update(
        status="FAIL",
        elapsed_live_seconds=400,
        charged_seconds=400,
        live_start=0,
        live_end=400,
        cleanup="PASS",
        episodes=[ep["episode"]],
        intake_stopped=[ep["episode"]],
        dispositions=[
            dict(
                episode=ep["episode"],
                replacement_for="",
                classification="valid",
                reason="",
                evidence=[],
            )
        ],
    )
    write(
        source / "summary.json",
        evidence.summaries(
            [value], safety_rows=[evidence.safety_evidence(source / ep["episode"], checker)]
        ),
    )
    m["artifact_sha256"] = recorder.artifact_hashes(source)
    write(source / "manifest.json", m)
    ack_path = repo / "experiments/portfolio/k8s-recovery/k4-continuation-acknowledgements.json"
    write(ack_path, dict(schema="dur053.k4-continuation-acks.v1", acknowledgements=[]))
    git_commit(repo)
    target = recorder.record(source, repo, checker)
    git_commit(repo)
    manifests, ledger = recorder.inventory(repo)
    assert ledger["entries"][-1]["charged_seconds"] == 400
    prior = manifests[0][1]
    with pytest.raises(ValueError, match="claude-acknowledgement-required"):
        k4.assignment(f, "k4-1-c1", {prior["job_id"]: prior})
    ack = dict(
        schema="dur053.k4-continuation-acks.v1",
        acknowledgements=[
            dict(
                job_id=m["job_id"],
                manifest_sha256=hashlib.sha256((target / "manifest.json").read_bytes()).hexdigest(),
                reviewer="Claude",
                acknowledged=True,
                review_round=241,
            )
        ],
    )
    write(ack_path, ack)
    with pytest.raises(ValueError):
        recorder.inventory(repo)  # Dirty acknowledgement bytes cannot unblock history.
    git_commit(repo)
    manifests, _ = recorder.inventory(repo)
    prior = manifests[0][1]
    assert k4.assignment(f, "k4-1-c1", {prior["job_id"]: prior}) == m["pending"]
    assert evidence.verify_evidence(repo, checker)["k7_decision"] == "NO-GO"


def test_backend_requires_collected_scratch_exit(tmp_path, monkeypatch):
    d = driver.K4Driver.__new__(driver.K4Driver)
    d.output, d.base = tmp_path, Mock()
    d.namespaces, d.proof, d.source = set(), {}, "a" * 40
    d.images = {"a": {"worker": "worker-image"}}

    def k(*args, **kwargs):
        return Mock(
            stdout=(
                json.dumps(dict(clientVersion=dict(gitVersion="v1.32.2")))
                if args[0] == "version"
                else "--raw"
            )
        )

    d.base.k.side_effect = k
    d.base.command.return_value.stdout = "events --help"
    streams = Mock(cri_exits=[])
    streams.self_check.return_value = dict(first_events=dict(pods=1, endpoints=2, cri=3))
    monkeypatch.setattr(driver, "Streams", lambda *args: streams)
    adapter = Mock()
    before = pod("scratch", role="scratch", cid="containerd://dead")
    adapter.get.return_value = before
    adapter.kill.return_value = pod("scratch", role="scratch", restart=1, cid="containerd://new")
    monkeypatch.setattr(driver, "Interventions", lambda *args: adapter)
    with pytest.raises(ValueError, match="backend-cri-exit-missing"):
        d.validate_backend()
    streams.cri_exits = [
        dict(container_id="dead", exit_status=0, exit_status_source="omitted-zero", source=EXIT)
    ]
    with pytest.raises(ValueError, match="backend-cri-exit-missing"):
        d.validate_backend()
    streams.cri_exits = [
        dict(container_id="dead", exit_status=137, exit_status_source="explicit", source=EXIT)
    ]
    d.validate_backend()
    recorded = json.loads((tmp_path / "chaos-validation.json").read_bytes())
    assert recorded["cri_task_exits"][0]["source"] == EXIT
    assert recorded["stream_self_check"]["first_events"]
