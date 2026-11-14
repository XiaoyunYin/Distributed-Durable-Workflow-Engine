"""R301-R303 regressions; retained source is reduced and credential scanned."""

import copy
import hashlib
import json
import re
import shutil
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import dur053_k4 as k4
import dur053_k4_collectors as collectors
import dur053_k4_driver as driver
import dur053_k4_evidence as evidence
import dur053_k4_schema as schema
import dur053_record_k4 as recorder
from test_k4_offline import checker, episode_fixture, freeze, write  # noqa: F401


def stream(tmp_path):
    return collectors.Streams(SimpleNamespace(), "ns", tmp_path)


def append_log(path, data):
    with path.open("ab") as handle:
        handle.write(
            (
                json.dumps(dict(received=10, source="now " + json.dumps(dict(data=data)))) + "\n"
            ).encode()
        )


def boundary(identity="one", boundary="enter"):
    return dict(
        event="k4_boundary",
        role="api",
        identity=identity,
        boundary=boundary,
        timestamp="1970-01-01T00:00:10Z",
        pod_uid="pod",
        session="session",
    )


def test_each_line_decoded_once_across_polls(tmp_path, monkeypatch):
    col = stream(tmp_path)
    path = tmp_path / "log-pod-cid.jsonl"
    append_log(path, boundary())
    original = collectors.structured_log
    calls = []

    def counted(text):
        calls.append(text)
        return original(text)

    monkeypatch.setattr(collectors, "structured_log", counted)
    col.live.read()
    append_log(path, boundary("two"))
    col.live.read()
    col.live.read()
    assert len(calls) == 2, "structured log line was decoded more than once"
    assert len(col.live.open) == 2
    append_log(path, boundary(boundary="exit"))
    col.live.read()
    assert list(col.live.open) == [("api", "two")]


def test_partial_lines_and_file_order(tmp_path):
    col = stream(tmp_path)
    first, last = (tmp_path / ("log-" + n + ".jsonl") for n in ("a", "z"))
    append_log(first, boundary())
    append_log(last, boundary(boundary="exit"))
    col.live.read()
    assert not col.live.open
    append_log(first, boundary())  # Older file cannot supersede the last file.
    with last.open("ab") as handle:
        handle.write(b'{"received":10,')
    col.live.read()
    assert not col.live.open
    with last.open("ab") as handle:
        handle.write(
            ('"source":' + json.dumps("now " + json.dumps(dict(data=boundary()))) + "}\n").encode()
        )
    col.live.read()
    assert len(col.live.open) == 1


def test_live_end_stamped_after_read(tmp_path, monkeypatch):
    col = stream(tmp_path)
    col.live.sessions["session"] = dict(pod_uid="pod")
    clock = [20]

    def read():
        clock[0] = 30
        col.live.open[("api", "one")] = boundary()
        return list(col.live.sessions.values())

    col.live.read = read
    monkeypatch.setattr(driver.time, "time", lambda: clock[0])
    drv = SimpleNamespace(collector=col)
    result = driver.K4Driver.overlap(drv, "ns", "api", 20)
    assert result[0]["end"] == 30, "live end precedes evidence read"


def test_exit_between_detection_and_issue_repolls(monkeypatch):
    drv = driver.K4Driver.__new__(driver.K4Driver)
    exited = dict(session="old", acknowledgment="old-boundary")
    current = dict(session="new", acknowledgment="new-boundary")
    polls = iter([[exited], [], [current], [current]])
    drv.overlap = lambda *args: next(polls)
    drv.check_observation = lambda: None
    monkeypatch.setattr(driver.time, "time", lambda: 1)
    monkeypatch.setattr(driver.time, "sleep", lambda _: None)
    selected = drv.confirmed_overlap("ns", "scheduler", 10)
    assert selected == [current], "issued against an exited boundary"


def test_confirmed_overlap_past_deadline_is_driver_incident(monkeypatch):
    drv = driver.K4Driver.__new__(driver.K4Driver)
    clock = [1]

    def read(*args):
        clock[0] += 5
        return [dict(session="old", acknowledgment="ack")]

    drv.overlap = read
    drv.check_observation = lambda: None
    monkeypatch.setattr(driver.time, "time", lambda: clock[0])
    with pytest.raises(ValueError, match="k4-harness-confirmation-deadline"):
        drv.confirmed_overlap("ns", "scheduler", 10)


@pytest.fixture
def retained():
    raw = (Path(__file__).parent / "fixtures/k4-round262-intake.json").read_bytes()
    assert not re.search(
        rb'(?i)(bearer\s+|access[_-]?token|refresh[_-]?token|api[_-]?key|password|secret|"(?:claim_)?token"\s*:)',
        raw,
    )
    for match in re.finditer(rb'"token_hash"\s*:\s*"([^"]*)"', raw):
        assert re.fullmatch(rb"[0-9a-f]{64}", match[1]), "fixture contains a raw claim token"
    return json.loads(raw)


def materialize_retained(tmp_path, row):
    name = row["episode"]["episode"]
    write(tmp_path / name / "episode.json", row["episode"])
    if "confirmation" in row:
        write(tmp_path / name / "confirmation.json", row["confirmation"])
        write(tmp_path / name / "metrics.json", row["metrics"])
    else:
        ref = row["disposition"]["evidence"][0]
        write(tmp_path / ref, row["incident"])
        write(tmp_path / row["incident"]["source"], row["source"])
    return name


def test_k4_1_shaped_intake_all_22_dispositions(tmp_path, retained):
    source = tmp_path / "downloaded"
    output = tmp_path / "raw"
    for row in retained["episodes"]:
        materialize_retained(source, row)
    shutil.copytree(source, output)

    def hashes():
        return {
            p.relative_to(source).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in source.rglob("*")
            if p.is_file()
        }

    before = hashes()
    derived = []
    safety = []
    for row in retained["episodes"]:
        name = row["episode"]["episode"]
        if "metrics" in row:
            metrics = copy.deepcopy(row["metrics"])
            if name == "short-core-2-pair-0-baseline":
                metrics["issues"] = sorted(
                    set(
                        evidence.confirmation(
                            row["confirmation"], row["timeline"], "scheduler rollout", row["leases"]
                        )
                    )
                )
                assert metrics["issues"] == ["k4-confirmation-boundary"]
            d = recorder.derive_disposition(source, name, metrics)
            recorder.save_disposition_evidence(output, d)
            recorder.validate_disposition(output, d, metrics=metrics)
            safety.append(row["safety"])
        else:
            d = row["disposition"]
            recorder.validate_disposition(output, d)
        derived.append(d)
    assert len(derived) == 22
    assert [d["classification"] for d in derived[:20]] == ["valid"] * 20
    assert derived[20]["reason"] == "harness-confirmation-miss", (
        "retained miss contributes as engine failure"
    )
    assert derived[21]["reason"] == "collection-failure"
    assert len(recorder.contributions(dict(dispositions=derived))) == 20
    aggregate = evidence.safety_aggregate(safety)
    assert aggregate["passed"] and len(aggregate["episodes"]) == 21
    assert "short-core-2-pair-0-baseline" in aggregate["episodes"], (
        "excluded baseline safety was discarded"
    )
    assert retained["charged_seconds"] == 10874
    assert hashes() == before, "disposition derivation modified downloaded source bytes"
    assert (output / derived[20]["evidence"][0]).is_file()
    assert not (source / derived[20]["evidence"][0]).exists()


def test_hand_label_inside_overlap_refused(tmp_path, retained):
    row = copy.deepcopy(retained["episodes"][20])
    c = row["confirmation"]
    c["overlap"][0]["end"] = c["intervention"] + 1
    name = materialize_retained(tmp_path, row)
    # Generate the incident using the original outside-window bytes first.
    original = retained["episodes"][20]
    write(tmp_path / name / "confirmation.json", original["confirmation"])
    d = recorder.derive_disposition(tmp_path, name, row["metrics"])
    recorder.save_disposition_evidence(tmp_path, d)
    write(tmp_path / name / "confirmation.json", c)
    with pytest.raises(ValueError, match="k4-harness-confirmation-miss-content"):
        recorder.validate_disposition(tmp_path, d, metrics=row["metrics"])


def test_valid_disposition_confirmation_miss_refused(tmp_path, retained):
    row = retained["episodes"][20]
    materialize_retained(tmp_path, row)
    with pytest.raises(ValueError, match="k4-valid-disposition-confirmation-miss"):
        recorder.validate_disposition(tmp_path, row["disposition"], metrics=row["metrics"])


def fault_episode(tmp_path, freeze, checker, *, miss):  # noqa: F811
    ep, docs = episode_fixture(tmp_path, freeze)
    ep["status"] = "FAIL"
    docs["timeline"]["workflows"][0]["state"] = "FAILED"
    if miss:
        for overlap in docs["confirmation"]["overlap"]:
            overlap["end"] = docs["confirmation"]["intervention"] - 0.1
    write(tmp_path / "timeline.json", docs["timeline"])
    write(tmp_path / "confirmation.json", docs["confirmation"])
    for key, filename in evidence.FILES.items():
        ep["files"][key] = hashlib.sha256((tmp_path / filename).read_bytes()).hexdigest()
    write(tmp_path / "episode.json", ep)
    metrics = evidence.episode(tmp_path, freeze, checker)
    write(tmp_path / "metrics.json", metrics)
    assert "k4-accepted-incomplete-or-failed" in metrics["issues"]
    assert not metrics["valid"]
    return ep, metrics


def test_timing_miss_with_failed_workflow(tmp_path, freeze, checker, monkeypatch):  # noqa: F811
    name = "short-coverage-0-0-treatment"
    _, metrics = fault_episode(tmp_path / name, freeze, checker, miss=True)
    assert "k4-confirmation-boundary" in metrics["issues"]
    d = recorder.derive_disposition(tmp_path, name, metrics)
    assert d["reason"] == "harness-confirmation-miss", "timing miss with engine failure retained"
    recorder.save_disposition_evidence(tmp_path, d)
    recorder.validate_disposition(tmp_path, d, metrics=metrics)
    assert recorder.contributions(dict(dispositions=[d])) == []
    # Isolate the all-attempts safety gate from missing contributing cells.
    monkeypatch.setattr(evidence, "registry", lambda: [])
    result = verify_campaign(tmp_path, freeze, checker, monkeypatch, name, d)
    assert result["safety"]["failed_workflows"] == 1
    assert result["complete"]
    assert not result["safety"]["passed"] and not result["valid"]
    assert not any(g["safety"] for g in result["k7"].values())


def test_nontiming_fault_fail_accepted(tmp_path, freeze, checker):  # noqa: F811
    name = "short-coverage-0-0-treatment"
    _, metrics = fault_episode(tmp_path / name, freeze, checker, miss=False)
    assert not set(metrics["issues"]) & recorder.TIMING_ISSUES
    d = recorder.derive_disposition(tmp_path, name, metrics)
    assert d["classification"] == "valid" and not d["reason"]
    assert metrics["status"] == "FAIL"
    recorder.validate_disposition(tmp_path, d, metrics=metrics)
    assert recorder.contributions(dict(dispositions=[d])) == [name]


def test_timing_issue_inside_overlap_stays_valid(tmp_path, retained):
    row = copy.deepcopy(retained["episodes"][20])
    row["confirmation"]["overlap"][0]["end"] = row["confirmation"]["intervention"] + 1
    assert row["confirmation"]["intervention"] <= row["episode"]["confirmation_deadline"]
    name = materialize_retained(tmp_path, row)
    assert set(row["metrics"]["issues"]) & recorder.TIMING_ISSUES
    d = recorder.derive_disposition(tmp_path, name, row["metrics"])
    assert d["classification"] == "valid", "timing label overrode retained inside-overlap times"
    assert row["episode"]["status"] == "FAIL"
    recorder.validate_disposition(tmp_path, d, metrics=row["metrics"])
    assert recorder.contributions(dict(dispositions=[d])) == [name]


def test_safety_aggregate_includes_excluded_and_unexercised(tmp_path, freeze, checker):  # noqa: F811
    rows = []
    for name, status in [
        ("short-core-0-pair-0-baseline", "invalid"),
        ("short-core-0-pair-0-treatment", "unexercised"),
    ]:
        ep, docs = episode_fixture(tmp_path / name, freeze, episode=name)
        ep["status"] = status
        docs["timeline"]["workflows"][0]["state"] = "FAILED"
        docs["timeline"]["http"].append(
            dict(
                role="api",
                request="r",
                body_hash="a" * 64,
                attempt=1,
                operation="submit",
                at=120,
                status=503,
                error="",
            )
        )
        write(tmp_path / name / "timeline.json", docs["timeline"])
        rows.append(evidence.safety_evidence(tmp_path / name, checker))
    aggregate = evidence.safety_aggregate(rows)
    summary = evidence.summaries([], safety_rows=rows)
    assert aggregate["failed_workflows"] == 2 and aggregate["http_5xx"] == 2
    assert not summary["safety"]["passed"], "excluded engine failure hidden from safety aggregate"
    assert not any(g["safety"] for g in summary["k7"].values())


def test_registered_scheduler_smoke_keeps_registered_parameters(freeze):  # noqa: F811
    assert driver.REGISTERED_SMOKE in driver.SMOKE_CELLS
    cell = k4.episode_identity(driver.REGISTERED_SMOKE)
    assert cell["event"] == "scheduler rollout" and cell["duration"] == "short"
    params = k4.parameters(freeze, "short")
    assert params["warmup"] == k4.WARMUP and params["W"] == 180
    assert params["confirmation"] == 2 * params["L"]
    smoke = driver.episode_parameters(
        freeze, dict(cell, episode=driver.REGISTERED_SMOKE), engineering=True
    )
    assert smoke == params, "registered scheduler smoke timing was shortened"
    short = driver.episode_parameters(
        freeze, dict(cell, episode=driver.SMOKE_CELLS[0]), engineering=True
    )
    assert short["warmup"] == driver.SMOKE_WARMUP and short["W"] == driver.SMOKE_WINDOWS["short"]


def test_schema_reference_regenerated():
    import dur053_k4_schema_docs as docs

    path = Path(__file__).resolve().parents[1] / "experiments/portfolio/k8s-recovery/k4-schema.md"
    assert path.read_text(encoding="utf-8") == docs.document(path.read_text(encoding="utf-8"))
    assert "harness-confirmation-miss" in schema.INFRA_REASONS
    assert "reaction" in schema.CONFIRMATION and "failed_workflows" in schema.SAFETY


def test_reaction_recorded_by_intervention(tmp_path):
    from dur053_k4_interventions import Interventions

    adapter = Interventions(
        SimpleNamespace(k=lambda *args, **kwargs: {}),
        "ns",
        tmp_path,
        {"a": {"runtime": "a", "worker": "a"}, "a-prime": {"runtime": "b", "worker": "b"}},
        {},
        "source",
    )
    adapter.pods = lambda role: [{"metadata": {"uid": "pod-1", "labels": {"app": role}}}]
    adapter.wait_rollout = lambda role: "1"
    adapter.set_image = lambda role, lineage: "2"
    record, _ = adapter.apply(
        {"event": "worker rollout", "episode": "test"},
        [{"role": "worker", "pod_uid": "pod-1", "at": 10}],
    )
    assert record["reaction"] == record["intervention"] - 10, "reaction time was not recorded"


@pytest.mark.parametrize("kind", ["scale-down", "kill", "empty-scale-down"])
def test_reaction_binds_retained_overlap(tmp_path, freeze, checker, monkeypatch, kind):  # noqa: F811
    from dur053_k4_interventions import Interventions

    event = "worker SIGKILL" if kind == "kill" else "worker scale-down 4 to 2"
    cell = next(r for r in k4.registry() if r["event"] == event)
    manifest, docs = episode_fixture(tmp_path, freeze, episode=cell["episode"])
    pods = [{"metadata": {"uid": f"pod-{i}"}} for i in range(4)]
    current = list(pods)

    def command(*args):
        assert args == ("-n", "ns", "scale", "deployment/worker", "--replicas=2")
        current[:] = pods[:2]

    adapter = Interventions(
        SimpleNamespace(k=command),
        "ns",
        tmp_path,
        {"a": {"worker": "a"}, "a-prime": {"worker": "b"}},
        docs["confirmation"]["binary_proof"],
        "a" * 40,
    )
    adapter.pods = lambda role: list(current)
    adapter.release_after_deletion = lambda role: None
    adapter.wait_rollout = lambda role: "1"
    adapter.wait = lambda predicate, *args: predicate()
    killed = []
    adapter.kill = lambda pod: killed.append(pod)
    monkeypatch.setattr("dur053_k4_interventions.time.time", lambda: 120.0)
    # The earliest observed overlap is on a survivor. Both removed pods overlap later.
    indices = [2, 3] if kind == "kill" else [0] if kind == "empty-scale-down" else [0, 2, 3]
    overlap = [
        dict(docs["confirmation"]["overlap"][0], role="worker", pod_uid=f"pod-{i}", at=117 + i)
        for i in indices
    ]
    record, targets = adapter.apply(cell, overlap)
    assert record["observed_overlap_first_at"] == min(o["at"] for o in overlap)
    if kind == "kill":
        assert targets == killed == [pods[2]]
        assert record["overlap"] == overlap
    else:
        assert targets == pods[2:]
        assert record["overlap"] == [o for o in overlap if o["pod_uid"] in {"pod-2", "pod-3"}]
    expected = (
        record["intervention"] - min(o["at"] for o in record["overlap"])
        if record["overlap"]
        else None
    )
    assert record["reaction"] == expected, "reaction was computed before overlap filtering"
    # Exercise the real verifier's unchanged reaction equality, including legacy-null handling.
    write(tmp_path / "confirmation.json", record)
    manifest["files"]["confirmation"] = hashlib.sha256(
        (tmp_path / "confirmation.json").read_bytes()
    ).hexdigest()
    write(tmp_path / "episode.json", manifest)
    assert evidence.episode(tmp_path, freeze, checker)["reaction"] == expected
    # Disclosure alone never changes acceptance predicates or metrics.
    record["observed_overlap_first_at"] = 0
    write(tmp_path / "confirmation.json", record)
    manifest["files"]["confirmation"] = hashlib.sha256(
        (tmp_path / "confirmation.json").read_bytes()
    ).hexdigest()
    write(tmp_path / "episode.json", manifest)
    assert evidence.episode(tmp_path, freeze, checker)["reaction"] == expected


def test_verification_aggregates_excluded_safety(tmp_path, freeze, checker, monkeypatch):  # noqa: F811
    name = "short-core-2-pair-0-baseline"
    _, docs = episode_fixture(tmp_path / name, freeze, episode=name)
    docs["timeline"]["workflows"][0]["state"] = "FAILED"
    write(tmp_path / name / "timeline.json", docs["timeline"])
    d = dict(episode=name, classification="infrastructure-invalid", reason="collection-failure")
    result = verify_campaign(tmp_path, freeze, checker, monkeypatch, name, d)
    assert result["safety"]["failed_workflows"] == 1, (
        "campaign verification discarded excluded safety"
    )
    assert not result["safety"]["passed"]


def verify_campaign(tmp_path, freeze, checker, monkeypatch, name, disposition):  # noqa: F811
    from contextlib import contextmanager

    rows = [evidence.safety_evidence(tmp_path / name, checker)]
    values = (
        [evidence.episode(tmp_path / name, freeze, checker)]
        if (tmp_path / name / "metrics.json").exists()
        else []
    )
    write(tmp_path / "summary.json", evidence.summaries(values, safety_rows=rows))
    manifest = dict(
        job="k4-1",
        run_id="1",
        job_id="2",
        status="FAIL",
        charged_seconds=400,
        pending=[],
        episodes=[name],
        dispositions=[disposition],
    )

    @contextmanager
    def materialize(*args, **kwargs):
        yield tmp_path

    monkeypatch.setattr(recorder, "materialize_job", materialize)
    monkeypatch.setattr(evidence, "frozen", lambda _: freeze)
    monkeypatch.setattr(
        recorder, "inventory", lambda _: ([(tmp_path / "manifest.json", manifest)], {"entries": []})
    )
    monkeypatch.setattr(recorder, "validate_job", lambda *args, **kwargs: manifest)
    return evidence.verify_evidence(tmp_path, checker)


def test_preissue_deadline_incident_is_not_collection_failure(tmp_path, retained):
    row = copy.deepcopy(retained["episodes"][21])
    name = materialize_retained(tmp_path, row)
    ep = row["episode"]
    ep["confirmation_deadline"] = 20
    write(tmp_path / name / "episode.json", ep)
    incident = row["incident"]
    incident.update(reason="harness-confirmation-miss", component="driver")
    write(tmp_path / row["disposition"]["evidence"][0], incident)
    write(
        tmp_path / incident["source"],
        dict(
            component="driver",
            errors=["k4-harness-confirmation-deadline"],
            observed_overlap=True,
            deadline=20,
            observed_at=21,
        ),
    )
    d = dict(row["disposition"], reason="harness-confirmation-miss")
    recorder.validate_disposition(tmp_path, d)
    assert recorder.metric_episodes(dict(dispositions=[d]), tmp_path) == []
