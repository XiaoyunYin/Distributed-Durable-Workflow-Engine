import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))

import j2_artifact_fetch as artifact  # noqa: E402

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
