"""K4 exercises the existing B.4 envelope and ledger/index/member bindings."""

import hashlib
import json
import lzma
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import dur053_payload as payload  # noqa: E402
from test_k4_offline import git_commit, local_repository, write  # noqa: E402


def small_payload(tmp_path, monkeypatch):
    monkeypatch.setattr(
        payload, "FILTERS", [dict(id=lzma.FILTER_LZMA2, preset=0, dict_size=1 << 20)]
    )
    source = tmp_path / "raw"
    source.mkdir()
    (source / "raw.log").write_bytes(b"original evidence bytes\n")
    write(source / "manifest.json", dict(artifact_sha256=payload.artifact_hashes(source)))
    target = tmp_path / "packed"
    target.mkdir()
    index = payload.pack(source, target)
    return source, target, index


def reject_materialization(target, expected, repository=None):
    try:
        with payload.materialize(target, repository):
            pass
    except ValueError:
        return
    pytest.fail(expected)


def test_archive_identity(tmp_path, monkeypatch):
    _, target, index = small_payload(tmp_path, monkeypatch)
    archive = target / index["archives"][0]["path"]
    previous = archive.read_bytes()
    # Legal XZ stream padding preserves all decoded bytes while changing the
    # committed archive identity. A decoder error cannot kill this control.
    archive.write_bytes(previous + b"\0" * 4)
    assert archive.read_bytes() != previous
    index["archives"][0]["size"] = archive.stat().st_size
    write(target / "payload-index.json", index)
    reject_materialization(target, "changed archive identity accepted")


def test_raw_member_identity(tmp_path, monkeypatch):
    source, target, index = small_payload(tmp_path, monkeypatch)
    (source / "raw.log").write_bytes(b"altered evidence bytes\n")
    archive = target / index["archives"][0]["path"]
    payload.compress(source, ["raw.log"], archive)
    index["archives"][0].update(
        size=archive.stat().st_size, sha256=hashlib.sha256(archive.read_bytes()).hexdigest()
    )
    write(target / "payload-index.json", index)
    reject_materialization(target, "changed raw member accepted")


def test_committed_index_identity(tmp_path, monkeypatch):
    _, target, _ = small_payload(tmp_path, monkeypatch)
    repo = local_repository(tmp_path / "repo")
    import shutil

    dest = repo / "evidence"
    shutil.copytree(target, dest)
    ledger = json.loads(
        (repo / "experiments/portfolio/k8s-recovery/live-time-ledger.json").read_bytes()
    )
    ledger["entries"].append(
        dict(
            artifact="evidence/manifest.json",
            artifact_sha256=hashlib.sha256((dest / "manifest.json").read_bytes()).hexdigest(),
            payload_index_sha256=hashlib.sha256(
                (dest / "payload-index.json").read_bytes()
            ).hexdigest(),
        )
    )
    write(repo / "experiments/portfolio/k8s-recovery/live-time-ledger.json", ledger)
    git_commit(repo)
    with payload.materialize(dest, repo):
        pass
    (dest / "payload-index.json").write_bytes((dest / "payload-index.json").read_bytes() + b"\n")
    git_commit(repo)
    reject_materialization(dest, "changed committed index accepted", repo)
