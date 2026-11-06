from __future__ import annotations

import hashlib
import io
import json
import shutil
import sys
import tarfile
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import dur053_payload as payload  # noqa: E402
import dur053_pilot as pilot  # noqa: E402
import dur053_pilot_evidence as evidence  # noqa: E402
import dur053_record_pilot as recorder  # noqa: E402
from dur053_k3 import save  # noqa: E402
from dur053_k3_policy import artifact_hashes  # noqa: E402
from test_k3b_admission import commit, initialize  # noqa: E402
from test_k3b_pilot import fixture_artifact, fixture_calibration  # noqa: E402


def repository(root: Path) -> Path:
    root.mkdir()
    initialize(root)
    (root / ".gitattributes").write_bytes(b"* -text\n*.tar.xz binary\n")
    (root / "experiments/portfolio/k8s-recovery").mkdir(parents=True)
    save(
        root / "experiments/portfolio/k8s-recovery/live-time-ledger.json",
        {"schema": "dur053.live-ledger.v1", "limit_seconds": 259200, "entries": []},
    )
    return root


def test_round_trip_and_determinism(tmp_path: Path) -> None:
    source = tmp_path / "source"
    manifest = fixture_artifact(source, cells=[("long", 0)], job="k3-pilot-2")
    # Every extension and CRLF byte is part of the preserved evidence.
    (source / "bytes.bin").write_bytes(bytes(range(256)) + b"\r\n")
    manifest["artifact_sha256"] = artifact_hashes(source)
    save(source / "manifest.json", manifest)
    with payload.materialize(source) as raw_copy:
        assert not raw_copy.is_relative_to(source)
        assert artifact_hashes(raw_copy) == artifact_hashes(source)
    assert not raw_copy.exists()
    a, b = tmp_path / "a", tmp_path / "b"
    a.mkdir()
    b.mkdir()
    payload.pack(source, a)
    payload.pack(source, b)
    assert artifact_hashes(a) == artifact_hashes(b), "payload determinism bypassed"
    repo = repository(tmp_path / "repo")
    target = recorder.record(source, repo)
    commit(repo)
    with payload.materialize(target, repo) as output:
        assert not output.is_relative_to(repo)
        assert {
            p.relative_to(output): p.read_bytes() for p in output.rglob("*") if p.is_file()
        } == {p.relative_to(source): p.read_bytes() for p in source.rglob("*") if p.is_file()}, (
            "payload byte preservation bypassed"
        )
    assert not output.exists()
    assert pilot.read_artifact(target, repo) == manifest
    assert evidence.dependency_plan(repo, "k3-pilot-3")[1]["long"]["r"] == 40
    # The freeze consumes this archived cell, then refuses only absent campaign cells.
    with (
        patch.object(evidence, "recheck"),
        pytest.raises(ValueError, match="pilot-freeze-all-cells"),
    ):
        evidence.freeze(repo, Path("unused"))
    index_path = target / "payload-index.json"
    index_path.write_bytes(index_path.read_bytes() + b" ")
    commit(repo)
    with pytest.raises(ValueError, match="pilot-payload-ledger-index"):
        pilot.read_artifact(target, repo)


def packed(tmp_path: Path) -> tuple[Path, Path]:
    source, target = tmp_path / "source", tmp_path / "packed"
    fixture_artifact(source)
    target.mkdir()
    payload.pack(source, target)
    return source, target


def replace_archive(target: Path, transform: str) -> None:
    index = json.loads((target / "payload-index.json").read_bytes())
    entry = index["archives"][0]
    path = target / entry["path"]
    with tarfile.open(path, "r:xz") as archive:
        files = [(m, archive.extractfile(m).read()) for m in archive]  # type: ignore[union-attr]
    with tarfile.open(path, "w:xz") as archive:
        for i, (member, data) in enumerate(files):
            if transform == "missing" and i == 0:
                continue
            if transform == "hash" and i == 0:
                data = b"X" * len(data)
            if transform == "metadata" and i == 0:
                member.mtime = 1
            archive.addfile(member, io.BytesIO(data))
        if transform in {"extra", "traversal", "link", "duplicate"}:
            name = {
                "extra": "extra.txt",
                "traversal": "../escape",
                "link": "link",
                "duplicate": files[0][0].name,
            }[transform]
            member = tarfile.TarInfo(name)
            if transform == "link":
                member.type, member.linkname = tarfile.SYMTYPE, "outside"
            archive.addfile(member, io.BytesIO(b""))
    entry.update(size=path.stat().st_size, sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    save(target / "payload-index.json", index)


@pytest.mark.parametrize(
    "attack",
    [
        "tamper",
        "extra",
        "missing",
        "traversal",
        "link",
        "duplicate",
        "hash",
        "dual",
        "extra_archive",
        "index",
    ],
)
def test_refusals(tmp_path: Path, attack: str) -> None:
    source, target = packed(tmp_path)
    if attack == "tamper":
        index = json.loads((target / "payload-index.json").read_bytes())
        old_hash = index["archives"][0]["sha256"]
        replace_archive(target, "metadata")
        index = json.loads((target / "payload-index.json").read_bytes())
        index["archives"][0]["sha256"] = old_hash
        save(target / "payload-index.json", index)
    elif attack == "dual":
        shutil.copyfile(source / "images.json", target / "images.json")
    elif attack == "extra_archive":
        (target / "payload/extra.tar.xz").write_bytes(b"extra")
    elif attack == "index":
        index = json.loads((target / "payload-index.json").read_bytes())
        index["archives"][0]["members"].pop()
        save(target / "payload-index.json", index)
    else:
        replace_archive(target, attack)
    with pytest.raises(ValueError, match="pilot-.*"), payload.materialize(target):
        pytest.fail("payload integrity bypassed")


@pytest.mark.parametrize("bound", ["ARCHIVE_LIMIT", "PAYLOAD_LIMIT"])
def test_bounds_before_repository_write(tmp_path: Path, bound: str) -> None:
    source = tmp_path / "source"
    fixture_artifact(source)
    repo = repository(tmp_path / "repo")
    before = (repo / "experiments/portfolio/k8s-recovery/live-time-ledger.json").read_bytes()
    with patch.object(payload, bound, 1), pytest.raises(ValueError, match="pilot-payload-.*size"):
        recorder.record(source, repo)
    assert not (repo / pilot.EVIDENCE).exists(), "payload prewrite bound bypassed"
    assert (
        repo / "experiments/portfolio/k8s-recovery/live-time-ledger.json"
    ).read_bytes() == before


def test_deterministic_split(tmp_path: Path) -> None:
    source, target = packed(tmp_path)
    # Root files split at their sorted midpoint when the group exceeds the limit.
    single = tmp_path / "single.tar.xz"
    payload.compress(source, ["images.json"], single)
    limit = single.stat().st_size
    a, b = tmp_path / "split-a", tmp_path / "split-b"
    a.mkdir()
    b.mkdir()
    with patch.object(payload, "ARCHIVE_LIMIT", limit):
        first = payload.pack(source, a)
        payload.pack(source, b)
        assert len(first["archives"]) == 2
        assert artifact_hashes(a) == artifact_hashes(b)
        with payload.materialize(a) as output:
            assert artifact_hashes(output) == artifact_hashes(source)


def test_empty_source_directory_is_not_erased(tmp_path: Path) -> None:
    source, target = packed(tmp_path)
    (source / "unknown-empty-directory").mkdir()
    staging = tmp_path / "new-staging"
    staging.mkdir()
    with pytest.raises(ValueError, match="pilot-payload-source-layout"):
        payload.pack(source, staging)
    assert not (staging / "payload").exists()


def test_complete_archived_freeze_and_arm_disclosure(tmp_path: Path) -> None:
    repo = repository(tmp_path / "repo")
    targets = {}
    for n, (job, cells) in enumerate(pilot.JOBS.items()):
        source = tmp_path / job
        manifest = fixture_artifact(
            source, cells=cells, job=job, run=str(n + 10), job_id=str(n + 20)
        )
        if job == "k3-pilot-1b":
            fixture_calibration(source)
        if job == "k3-pilot-3":
            manifest["dependencies"]["long"] = {
                "manifest_sha256": hashlib.sha256(
                    (targets["k3-pilot-2"] / "manifest.json").read_bytes()
                ).hexdigest(),
                "b": 0.1,
                "r": 40,
            }
        manifest["artifact_sha256"] = artifact_hashes(source)
        save(source / "manifest.json", manifest)
        targets[job] = recorder.record(source, repo)
    commit(repo)
    with patch.object(evidence, "recheck"):
        frozen = evidence.freeze(repo, Path("fixture-checker"))
    assert all(not arm["engine_path_saturation"] for arm in frozen["arms"].values())
    assert frozen["arms"]["short"]["initial_engine_path_metrics"]["offered_rate"] == 1
    assert frozen["arms"]["short"]["L"] == 2.004
    from dur053_saturation import engine_path_metrics

    def overloaded(cell: Path) -> dict:
        result = engine_path_metrics(cell)
        if cell.name == "short-1":
            result["triggers"] = ["throughput"]
        return result

    with (
        patch.object(evidence, "recheck"),
        patch("dur053_saturation.engine_path_metrics", side_effect=overloaded),
    ):
        flagged = evidence.freeze(repo, Path("fixture-checker"))
    assert flagged["arms"]["short"]["engine_path_saturation"], "engine arm flag bypassed"
    assert not flagged["arms"]["medium"]["engine_path_saturation"]
    assert any(
        "short: L for this arm is derived under engine-path overload" in text
        for text in flagged["limitations"]
    ), "engine limitation text bypassed"
    assert all(frozen["arms"][arm]["L"] == flagged["arms"][arm]["L"] for arm in pilot.ARMS)
