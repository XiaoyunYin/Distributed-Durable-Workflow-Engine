import json
import subprocess
import sys
import zipfile
from datetime import UTC, datetime
from functools import partial
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))

import j2_artifact_fetch as artifact  # noqa: E402
import j2_replay_storage as storage  # noqa: E402
from j2_barrier import canonical, digest  # noqa: E402

ENDPOINT = "repos/owner/repo/actions/artifacts/42"


def responses(monkeypatch, script):
    calls, sleeps = [], []
    clock = 0

    def sleep(seconds):
        nonlocal clock
        sleeps.append(seconds)
        clock += seconds

    def run(argv, *, stdout, stderr, timeout, check):
        nonlocal clock
        calls.append((argv[-1], timeout))
        status, body, elapsed = next(script)
        if status is not None:
            stdout.write(f"HTTP/2.0 {status} status\r\nContent-Type: test\r\n\r\n".encode())
            stdout.write(body)
        if elapsed >= timeout:
            clock += timeout
            raise subprocess.TimeoutExpired(argv, timeout)
        clock += elapsed
        return SimpleNamespace(returncode=int(status != 200))

    monkeypatch.setattr(artifact, "time", SimpleNamespace(monotonic=lambda: clock, sleep=sleep))
    monkeypatch.setattr(artifact.subprocess, "run", run)
    return calls, sleeps


def read_attempts(receipt):
    path = receipt / "artifact-api-attempts.jsonl"
    entries = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert all(datetime.fromisoformat(entry["time"]).tzinfo == UTC for entry in entries)
    return [(entry["endpoint"], entry["http_status"]) for entry in entries]


def test_retry_metadata_404_and_zip_503_preserves_response_bytes(tmp_path, monkeypatch):
    metadata, archive = b'{"id":42}\n', b"PK\x03\x04\x00\r\n\r\n\xff\n"
    calls, sleeps = responses(
        monkeypatch,
        iter(
            [(404, b"not found", 0), (200, metadata, 0), (503, b"not ready", 0), (200, archive, 0)]
        ),
    )
    zip_path = tmp_path / "attempt.zip"
    artifact.fetch_artifact(ENDPOINT, tmp_path, zip_path)
    assert (tmp_path / "artifact.json").read_bytes() == metadata
    assert zip_path.read_bytes() == archive
    assert calls == [
        (ENDPOINT, 120),
        (ENDPOINT, 115),
        (ENDPOINT + "/zip", 115),
        (ENDPOINT + "/zip", 110),
    ]
    assert sleeps == [5, 5]
    assert read_attempts(tmp_path) == [
        (ENDPOINT, 404),
        (ENDPOINT, 200),
        (ENDPOINT + "/zip", 503),
        (ENDPOINT + "/zip", 200),
    ]


@pytest.mark.parametrize("status", [401, 429, None])
def test_nonretryable_response_fails_at_once(tmp_path, monkeypatch, status):
    calls, sleeps = responses(monkeypatch, iter([(status, b"error", 0)]))
    zip_path = tmp_path / "attempt.zip"
    with pytest.raises(ValueError, match=str(status or "unknown")):
        artifact.fetch_artifact(ENDPOINT, tmp_path, zip_path)
    assert calls == [(ENDPOINT, 120)] and not sleeps
    assert not zip_path.exists() and not (tmp_path / "artifact.json").exists()
    assert read_attempts(tmp_path) == [(ENDPOINT, status)]


def test_persistent_404_exhausts_bound_at_fixed_intervals(tmp_path, monkeypatch):
    calls, sleeps = responses(monkeypatch, iter([(404, b"not found", 0)] * 24))
    with pytest.raises(TimeoutError, match="deadline reached"):
        artifact.fetch_artifact(ENDPOINT, tmp_path, tmp_path / "attempt.zip")
    assert calls == [(ENDPOINT, remaining) for remaining in range(120, 0, -5)]
    assert sleeps == [5] * 23
    assert read_attempts(tmp_path) == [(ENDPOINT, 404)] * 24


def test_zip_request_shares_remaining_budget_and_logs_timeout(tmp_path, monkeypatch):
    calls, sleeps = responses(monkeypatch, iter([(200, b"metadata", 118), (200, b"partial", 2)]))
    archive = tmp_path / "attempt.zip"
    with pytest.raises(TimeoutError, match="/zip"):
        artifact.fetch_artifact(ENDPOINT, tmp_path, archive)
    assert calls == [(ENDPOINT, 120), (ENDPOINT + "/zip", 2)] and not sleeps
    assert (tmp_path / "artifact.json").read_bytes() == b"metadata" and not archive.exists()
    assert read_attempts(tmp_path) == [(ENDPOINT, 200), (ENDPOINT + "/zip", 200)]


def frozen_zip(path, *, extra=None):
    content = b"frozen payload"
    with zipfile.ZipFile(path, "w") as bundle:
        bundle.writestr("data.txt", content)
        bundle.writestr(
            "freeze.json",
            canonical({"files": {"data.txt": {"bytes": len(content), "sha256": digest(content)}}}),
        )
        if extra:
            bundle.writestr(extra, b"unlisted")
    return path.read_bytes()


def test_release_only_transport_verifies_the_original_zip_and_frozen_members(tmp_path, monkeypatch):
    path = tmp_path / "original.zip"
    body = frozen_zip(path)
    meta = {
        "source_run": 10,
        "source_attempt": 1,
        "job": "job-1",
        "kind": "grid-A",
        "id": 20,
        "size_in_bytes": len(body),
        "digest": "sha256:" + digest(body),
    }
    release = {"campaign": "eng-grid", "repository": "owner/repo", "tag": "eng-replay-j2-evidence"}
    name = storage.asset_name(release["campaign"], meta)
    assert name == "eng-grid--r10--a1--job-1--grid-A--artifact-20.zip"
    url = storage.public_url("owner/repo", release["tag"], name)
    original = storage.archive_members(path, meta["kind"])
    meta["release"] = {
        "id": 99,
        "name": name,
        "url": url,
        "inventory": original,
        "bytes": len(body),
        "sha256": digest(body),
    }

    class Response(BytesIO):
        status = 200

    def open_public(actual, *, timeout):
        assert actual == url and timeout > 0
        return Response(body)

    monkeypatch.setattr(
        storage, "public_download", partial(storage.public_download, opener=open_public)
    )
    monkeypatch.setattr(
        storage, "download", lambda *_: pytest.fail("Actions source is unavailable")
    )
    downloaded = tmp_path / "published.zip"
    storage.pull_archive(None, meta, downloaded, release=release, remaining=lambda: 60)
    assert storage.release_member_record(downloaded, meta, lambda: 60) == original
    meta["release"]["inventory"] = {}
    with pytest.raises(ValueError, match="member hashes differ"):
        storage.release_member_record(downloaded, meta, lambda: 60)
    meta["digest"] = "sha256:" + "0" * 64
    with pytest.raises(ValueError, match="size or digest differs"):
        storage.pull_archive(None, meta, downloaded, release=release, remaining=lambda: 60)


@pytest.mark.parametrize("entry", ["../escape", "sub/ok"])
def test_release_refuses_unsafe_or_unlisted_members(tmp_path, entry):
    path = tmp_path / "unsafe.zip"
    frozen_zip(path, extra=entry)
    with pytest.raises(ValueError, match="unsafe|frozen index"):
        storage.archive_members(path, "grid-A")


def test_release_download_refuses_a_missing_public_asset(tmp_path):
    def missing(url, *, timeout):
        raise FileNotFoundError(url)

    with pytest.raises(FileNotFoundError):
        storage.public_download(
            "https://example.com/missing.zip",
            tmp_path / "never.zip",
            {"bytes": 1, "sha256": "0" * 64},
            lambda: 1,
            opener=missing,
        )
    assert not (tmp_path / "never.zip").exists()
