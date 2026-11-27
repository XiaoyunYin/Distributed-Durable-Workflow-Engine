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
import j2_publish_evidence as publisher  # noqa: E402
import j2_replay_storage as storage  # noqa: E402
from j2_barrier import canonical, digest  # noqa: E402
from j2_qualification_evidence import file_record  # noqa: E402

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


@pytest.mark.parametrize("extra", [0, 1])
def test_publisher_splits_only_above_64_mib(tmp_path, monkeypatch, extra):
    limit = 64 << 20
    source = tmp_path / "source.zip"
    with source.open("wb") as stream:
        stream.truncate(limit)
        if extra:
            stream.seek(limit)
            stream.write(b"X")
    published = []

    def capture(repository, tag, release_id, path, existing, budget, *, retries=0):
        record = {"name": path.name, **file_record(path)}
        published.append((record, retries))
        return record

    monkeypatch.setattr(publisher, "publish_asset", capture)
    archive = publisher.publish_archive("owner/repo", "tag", 1, source, {}, None)
    assert archive["bytes"] == limit + extra and archive["name"] == source.name
    if extra:
        first = {
            "name": "source.zip.part-001-of-002",
            "bytes": limit,
            "sha256": published[0][0]["sha256"],
        }
        last = {"name": "source.zip.part-002-of-002", "bytes": 1, "sha256": digest(b"X")}
        assert archive["parts"] == [first, last]
        assert published == [(first, 3), (last, 3)]
        assert list(tmp_path.iterdir()) == [source]
    else:
        assert "parts" not in archive
        assert published == [(archive, 0)]


@pytest.mark.parametrize("damage", [None, "part", "whole", "order"])
def test_split_release_index_round_trip(tmp_path, monkeypatch, damage):
    release = {
        "schema": storage.INDEX_SCHEMA,
        "campaign": "eng-grid",
        "repository": "owner/repo",
        "tag": "eng-replay-j2-evidence",
        "run": 10,
        "attempt": 1,
    }
    meta = {"source_run": 10, "source_attempt": 1, "job": "job-1", "kind": "grid-A", "id": 20}
    source = tmp_path / storage.asset_name(release["campaign"], meta)
    body = frozen_zip(source)
    meta.update(size_in_bytes=len(body), digest=f"sha256:{digest(body)}")
    monkeypatch.setattr(storage, "RELEASE_PART_BYTES", len(body) // 2 + 1)
    assets = {}

    def capture(repository, tag, release_id, path, existing, budget, *, retries):
        url = storage.public_url(repository, tag, path.name)
        assets[url] = path.read_bytes()
        return {"name": path.name, "url": url, "id": len(assets), **file_record(path)}

    monkeypatch.setattr(publisher, "publish_asset", capture)
    monkeypatch.setattr(storage, "download", lambda *_: pytest.fail("Actions transport used"))
    index_name = "eng-grid--r10--a1--index.json"
    published = publisher.publish_archive(
        release["repository"], release["tag"], 1, source, {}, None
    )
    if damage in {"part", "whole"}:
        part = published["parts"][-1]
        last = assets[part["url"]]
        assets[part["url"]] = last[:-1] + bytes([last[-1] ^ 1])
        if damage == "whole":
            part["sha256"] = digest(assets[part["url"]])
    elif damage == "order":
        published["parts"].reverse()
    published["inventory"] = storage.archive_members(source, meta["kind"])
    meta["release"] = published
    release["artifacts"] = {"source": meta}
    raw = canonical(release)
    assets[storage.public_url(release["repository"], release["tag"], index_name)] = raw

    class Response(BytesIO):
        status = 200

    def open_public(url, *, timeout):
        assert timeout > 0
        return Response(assets[url])

    monkeypatch.setattr(storage, "urlopen", open_public)
    loaded = storage.load_release_index(
        release["repository"],
        release["campaign"],
        release["run"],
        release["tag"],
        index_name,
        digest(raw),
        opener=open_public,
    )
    assert loaded == release
    target = tmp_path / "reassembled.zip"
    if damage:
        reason = {
            "part": "asset size or digest differs",
            "whole": "reassembled release ZIP digest differs",
            "order": "part identity, order, size or digest differs",
        }[damage]
        with pytest.raises(ValueError, match=reason):
            storage.pull_archive(
                None, loaded["artifacts"]["source"], target, release=loaded, remaining=lambda: 60
            )
        assert not target.exists()
    else:
        storage.pull_archive(
            None, loaded["artifacts"]["source"], target, release=loaded, remaining=lambda: 60
        )
        assert target.read_bytes() == body
        assert storage.release_member_record(target, meta, lambda: 60) == published["inventory"]


def test_part_retry_cleans_only_its_own_starter_and_surfaces_stderr(tmp_path, monkeypatch, capsys):
    repo, tag = "owner/repo", "eng-replay-j2-evidence"
    path = tmp_path / "source.zip.part-001-of-002"
    path.write_bytes(b"part")
    remote = {
        "unrelated.zip": {"id": 7, "state": "starter"},
        "existing.zip": {"id": 8, "state": "uploaded"},
    }
    failed = {"id": 9, "name": path.name, "state": "starter"}
    uploaded = {
        "id": 10,
        "name": path.name,
        "state": "uploaded",
        "size": 4,
        "browser_download_url": storage.public_url(repo, tag, path.name),
    }
    calls = []
    endpoint = f"repos/{repo}/releases/assets/9"

    def get(request):
        assert request == endpoint
        return remote[path.name]

    def run(argv, **options):
        if argv[1] == "api":
            assert argv == ["gh", "api", "--method", "DELETE", endpoint]
            assert remote.pop(path.name) == failed
            calls.append("delete")
        else:
            assert argv == ["gh", "release", "upload", tag, str(path), "--repo", repo]
            assert options["capture_output"] and options["check"]
            calls.append("upload")
            if calls == ["upload"]:
                remote[path.name] = failed
                raise subprocess.CalledProcessError(
                    1, argv, stderr="remote error: tls: bad record MAC\n"
                )
            remote[path.name] = uploaded

    monkeypatch.setattr(publisher, "assets", lambda *_: remote.copy())
    monkeypatch.setattr(publisher, "gh_json", get)
    monkeypatch.setattr(publisher.subprocess, "run", run)
    actual = publisher.publish_asset(
        repo, tag, 1, path, remote.copy(), SimpleNamespace(remaining=lambda: 60), retries=3
    )
    assert calls == ["upload", "delete", "upload"] and actual["id"] == 10
    assert remote["unrelated.zip"]["state"] == "starter"
    assert remote["existing.zip"]["state"] == "uploaded"
    assert "remote error: tls: bad record MAC" in capsys.readouterr().err


def test_starter_cleanup_preserves_an_asset_that_completed(monkeypatch):
    item = {"id": 9, "name": "part", "state": "starter"}
    complete = {**item, "state": "uploaded"}
    monkeypatch.setattr(publisher, "gh_json", lambda _: complete)
    monkeypatch.setattr(
        publisher.subprocess, "run", lambda *_a, **_k: pytest.fail("asset deletion")
    )
    assert publisher.discard_starter("owner/repo", "part", item) == complete


def test_part_upload_retries_at_most_three_times(tmp_path, monkeypatch):
    calls = []

    def failed(*args):
        calls.append(args)
        raise subprocess.TimeoutExpired("gh", 1)

    monkeypatch.setattr(publisher, "upload", failed)
    monkeypatch.setattr(publisher, "assets", lambda *_: {})
    budget = SimpleNamespace(remaining=lambda: 60)
    with pytest.raises(subprocess.TimeoutExpired):
        publisher.publish_asset("owner/repo", "tag", 1, tmp_path / "part", {}, budget, retries=3)
    assert len(calls) == 4
