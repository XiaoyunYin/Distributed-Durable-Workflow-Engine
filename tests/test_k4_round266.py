"""R304-R306 regressions, including the pinned retained k4-1 intake."""

import copy
import hashlib
import json
import lzma
import re
import sys
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from test_k4_offline import record_directory

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import dur053_k4 as k4
import dur053_k4_driver as driver
import dur053_k4_evidence as evidence
import dur053_payload_v2 as payload
import dur053_record_k4 as recorder
from test_k4_offline import freeze, git_commit, local_repository, write  # noqa: F401
from test_k4_round262 import append_log, boundary, materialize_retained, stream


def test_glob_reordering_cannot_discard_exit(tmp_path, monkeypatch):
    col = stream(tmp_path)
    old = tmp_path / "log-z.jsonl"
    new = tmp_path / "log-a.jsonl"
    earlier = [tmp_path / "log-b.jsonl", tmp_path / "log-c.jsonl"]
    for path in earlier:
        path.write_bytes(b"")
    append_log(old, boundary())
    order = [*earlier, old]
    original = Path.glob
    monkeypatch.setattr(
        Path,
        "glob",
        lambda self, pattern: iter(order) if self == tmp_path else original(self, pattern),
    )
    col.live.read()
    assert col.live.open
    append_log(new, boundary("other"))
    order[:] = [new, old, *earlier]  # Old file's per-call index drops from 2 to 1.
    append_log(old, boundary(boundary="exit"))
    col.live.read()
    assert ("api", "one") not in col.live.open, "glob reorder discarded boundary exit"
    assert ("api", "other") in col.live.open


def test_exited_overlap_is_unexercised_and_next_episode_continues(tmp_path, freeze, monkeypatch):  # noqa: F811
    drv = driver.K4Driver.__new__(driver.K4Driver)
    drv.output, drv.engineering, drv.fatal = tmp_path, False, False
    drv.images = {
        "a": {"runtime": "image@sha256:" + "a" * 64, "worker": "image@sha256:" + "b" * 64}
    }
    drv.proof, drv.source = {}, "source"
    drv.base = Mock()
    drv.namespaces = {"ns"}
    drv.setup = lambda *args: ("ns", "db")
    drv.start_samples = drv.stop_samples = drv.check_observation = lambda *args: None
    drv.hold_state = lambda *args: dict(
        mtime=100,
        observer=100,
        host_start=100,
        host_end=100,
        path="/hold",
        existed=False,
        exists=True,
    )
    drv.load = lambda *args: SimpleNamespace(returncode=0)
    drv.arrival_journal = drv.assemble = lambda *args: None
    drv.observer_stop, drv.sampler = threading.Event(), Mock()
    clock = [100.0]
    monkeypatch.setattr(driver.time, "time", lambda: clock[0])
    monkeypatch.setattr(
        driver.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds)
    )
    monkeypatch.setattr(
        driver,
        "reconciliation_round",
        lambda *args: ({"workflows": []}, 100, None, 400, 400, False),
    )
    monkeypatch.setattr(driver, "reconciled", lambda *args, **kwargs: "complete")
    monkeypatch.setattr(
        driver, "derive_episode", lambda path, *args: dict(valid=True, episode=path.name)
    )
    adapters = []

    def adapter(*args):
        result = Mock(intervention_issued=None)
        adapters.append(result)
        return result

    monkeypatch.setattr(driver, "Interventions", adapter)
    reads = []

    def overlap(*args):
        reads.append(1)
        return [dict(session="old", acknowledgment="ack")] if len(reads) == 1 else []

    drv.overlap = overlap
    job = dict(
        job="k4-1",
        run_id="1",
        job_id="2",
        head_sha="a" * 40,
        tree_sha="b" * 40,
        budget_scope="registered-campaign",
        measurement_claim=True,
    )
    cells = k4.registry()[:2]
    for cell in cells:
        drv.collector = SimpleNamespace(live=SimpleNamespace(read=lambda: None), close=lambda: None)
        result = drv.execute_episode(cell, {}, freeze, job, Path("unused"))
        epdir = tmp_path / cell["episode"]
        ep = json.loads((epdir / "episode.json").read_bytes())
        assert ep["status"] == "unexercised", "expired observation aborted as harness miss"
        assert result["episode"] == cell["episode"] and not drv.fatal
    disclosure = json.loads((tmp_path / cells[0]["episode"] / "unexercised.json").read_bytes())
    assert disclosure == dict(reason="k4-unexercised-overlap", observed_exited_overlaps=1)
    assert len(adapters) == 2 and not any(a.apply.called for a in adapters)


def test_confirmed_overlap_late_issue_is_harness_miss(monkeypatch):
    drv = driver.K4Driver.__new__(driver.K4Driver)
    clock = [1]
    drv.check_observation = lambda: None

    def overlap(*args):
        clock[0] += 5
        return [dict(session="still-open", acknowledgment="ack")]

    drv.overlap = overlap
    monkeypatch.setattr(driver.time, "time", lambda: clock[0])
    with pytest.raises(ValueError, match="k4-harness-confirmation-deadline"):
        drv.confirmed_overlap("ns", "scheduler", 10)


@pytest.fixture
def legacy_download(tmp_path, monkeypatch):
    raw = (Path(__file__).parent / "fixtures/k4-round266-intake.json").read_bytes()
    assert not re.search(
        rb'(?i)(bearer\s+|access[_-]?token|refresh[_-]?token|api[_-]?key|password|secret|"(?:claim_)?token"\s*:)',
        raw,
    )
    for match in re.finditer(rb'"token_hash"\s*:\s*"([^"]*)"', raw):
        assert re.fullmatch(rb"[0-9a-f]{64}", match[1])
    fixture = json.loads(raw)
    source = tmp_path / "downloaded"
    rows = {r["episode"]["episode"]: r for r in fixture["episodes"]}
    for row in rows.values():
        materialize_retained(source, row)
    for name, value in fixture["actions"].items():
        write(source / name, value)
    write(source / "summary.json", fixture["summary"])
    m = fixture["manifest"]
    m["artifact_sha256"] = recorder.artifact_hashes(source)
    write(source / "manifest.json", m)

    # The fixture trims bulk diagnostics/exports. Isolate metric recomputation
    # at this seam; the real retained download exercises the unmocked recorder.
    def recompute(path, *args):
        row = rows[path.name]
        result = dict(
            copy.deepcopy(row["metrics"]),
            reaction=None,
            budget_scope=row["episode"].get("budget_scope", m["budget_scope"]),
            safety=row.get("safety"),
        )
        if path.name == "short-core-2-pair-0-baseline":
            result["issues"] = evidence.confirmation(
                row["confirmation"], row["timeline"], "scheduler rollout", row["leases"]
            )
        return result

    monkeypatch.setattr(evidence, "episode", recompute)
    monkeypatch.setattr(evidence, "safety_evidence", lambda path, _: rows[path.name].get("safety"))
    monkeypatch.setattr(
        payload, "FILTERS", [dict(id=lzma.FILTER_LZMA2, preset=0, dict_size=1 << 20)]
    )
    return source, m


def file_hashes(source):
    return {
        p.relative_to(source).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in source.rglob("*")
        if p.is_file()
    }


def test_pinned_legacy_record_roundtrip(tmp_path, legacy_download):
    source, original = legacy_download
    before = file_hashes(source)
    repo = local_repository(tmp_path / "repo")
    target = record_directory(source, repo, Path("unused"))
    document = json.loads((target / "reclassification.json").read_bytes())
    derived = document["dispositions"]
    assert len(derived) == 22 and len(recorder.contributions(dict(dispositions=derived))) == 20
    assert derived[20]["reason"] == "harness-confirmation-miss"
    assert derived[21] == original["dispositions"][21]
    assert document["safety"]["passed"] and len(document["safety"]["episodes"]) == 21
    assert document["summary"]["disposition_counts"] == {"PASS": 20}
    assert (target / "manifest.json").read_bytes() == (source / "manifest.json").read_bytes()
    ledger = json.loads((repo / k4.LEDGER).read_bytes())
    assert ledger["entries"][-1]["charged_seconds"] == 10874
    assert len(ledger["entries"][-1]["k4_episodes"]) == 20
    git_commit(repo)
    inventory, _ = recorder.inventory(repo)
    assert inventory[0][1]["job"] == "k4-1"
    assert inventory[0][1]["dispositions"] == derived
    verified = evidence.verify_evidence(repo, Path("unused"), store=repo.parent / "store")
    assert verified["schedule"]["charged_seconds"] == 10874
    assert verified["retained_jobs"][0]["dispositions"] == derived
    assert len(verified["safety"]["episodes"]) == 21
    assert file_hashes(source) == before, "legacy intake modified retained bytes"
    sidecar = target / "reclassification.json"
    sidecar.write_bytes(sidecar.read_bytes() + b"\n")
    with pytest.raises(ValueError, match="pilot-committed-bytes"):
        recorder.inventory(repo)


@pytest.mark.parametrize("key", list(recorder.LEGACY_PIN))
def test_legacy_pin_refuses_other_identity(legacy_download, freeze, key):  # noqa: F811
    source, m = legacy_download
    changed = dict(m, **{key: "other"})
    with pytest.raises(ValueError, match="k4-legacy-pin"):
        recorder.legacy_reclassification(source, freeze, Path("unused"), changed)


def test_legacy_refuses_other_disposition_difference(legacy_download, freeze):  # noqa: F811
    source, m = legacy_download
    m["dispositions"][0]["classification"] = "unexercised"
    with pytest.raises(ValueError, match="k4-legacy-disposition-difference"):
        recorder.legacy_reclassification(source, freeze, Path("unused"), m)


@pytest.mark.parametrize("field,value", [("accepted", -1), ("invented", 1), ("reaction", 0)])
def test_legacy_refuses_retained_metric_difference(legacy_download, freeze, field, value):  # noqa: F811
    source, m = legacy_download
    path = source / m["episodes"][0] / "metrics.json"
    metrics = json.loads(path.read_bytes())
    metrics[field] = value
    write(path, metrics)
    m["artifact_sha256"] = recorder.artifact_hashes(source)
    with pytest.raises(ValueError, match="k4-legacy-metric-fields"):
        recorder.legacy_reclassification(source, freeze, Path("unused"), m)


def refuse_added_metric(legacy_download, monkeypatch, field, value):
    source, m = legacy_download
    episode = evidence.episode

    def recompute(path, *args):
        result = episode(path, *args)
        result[field] = value
        return result

    monkeypatch.setattr(evidence, "episode", recompute)
    with pytest.raises(ValueError, match="k4-legacy-metric-fields"):
        recorder.legacy_reclassification(source, freeze, Path("unused"), m)


def test_legacy_refuses_unexpected_added_metric(legacy_download, freeze, monkeypatch):  # noqa: F811
    refuse_added_metric(legacy_download, monkeypatch, "unexpected", True)


def test_legacy_refuses_budget_scope_mismatch(legacy_download, freeze, monkeypatch):  # noqa: F811
    refuse_added_metric(legacy_download, monkeypatch, "budget_scope", "engineering-validation")


def test_legacy_refuses_safety_mismatch(legacy_download, freeze, monkeypatch):  # noqa: F811
    source, manifest = legacy_download
    value = copy.deepcopy(
        evidence.safety_evidence(source / manifest["episodes"][0], Path("unused"))
    )
    value["lost_accepted"] += 1
    refuse_added_metric(legacy_download, monkeypatch, "safety", value)


def test_legacy_refuses_changed_summary(legacy_download, freeze):  # noqa: F811
    source, m = legacy_download
    path = source / "summary.json"
    summary = json.loads(path.read_bytes())
    summary["k7_decision"] = "GO"
    write(path, summary)
    m["artifact_sha256"] = recorder.artifact_hashes(source)
    with pytest.raises(ValueError, match="k4-legacy-summary"):
        recorder.legacy_reclassification(source, freeze, Path("unused"), m)
