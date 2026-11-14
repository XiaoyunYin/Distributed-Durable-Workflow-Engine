"""Amendment B.4.1: offline release integrity and a payload-free history gate."""

import hashlib
import io
import json
import lzma
import sys
import tarfile
from pathlib import Path
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import dur053_k4 as k4  # noqa: E402
import dur053_k4_evidence as evidence  # noqa: E402
import dur053_k4_schema as schema  # noqa: E402
import dur053_payload as v1  # noqa: E402
import dur053_payload_v2 as payload  # noqa: E402
import dur053_record_k4 as recorder  # noqa: E402
import pytest
from test_k4_offline import (  # noqa: E402
    default,
    git_commit,
    local_repository,
    upload_directory,
    write,
)
from test_k4_round266 import legacy_download  # noqa: E402, F401


@pytest.fixture
def packed(tmp_path, monkeypatch):
    monkeypatch.setattr(
        payload, "FILTERS", [dict(id=lzma.FILTER_LZMA2, preset=0, dict_size=1 << 20)]
    )
    source, staging = tmp_path / "raw", tmp_path / "stage"
    source.mkdir()
    staging.mkdir()
    (source / "raw.log").write_bytes(b"every retained byte\r\n")
    write(
        source / "manifest.json",
        dict(job="k4-1", run_id="1", artifact_sha256=payload.artifact_hashes(source)),
    )
    index = payload.pack(source, staging)
    store = upload_directory(staging, tmp_path / "repo")
    return source, staging, store, index


def refuses(staging, store, expected):
    try:
        with payload.materialize(staging, store=store):
            pass
    except ValueError as error:
        return str(error)
    pytest.fail(expected)


def test_long_basename_roundtrip(tmp_path, monkeypatch):
    monkeypatch.setattr(
        payload, "FILTERS", [dict(id=lzma.FILTER_LZMA2, preset=0, dict_size=1 << 20)]
    )
    source, staging = tmp_path / "raw", tmp_path / "stage"
    source.mkdir()
    staging.mkdir()
    name = "log-" + "u" * 36 + "-" + "a" * 64 + ".jsonl"
    assert len(name.encode()) == 111
    (source / name).write_bytes(b"retained\n")
    write(
        source / "manifest.json",
        dict(job="k4-1", run_id="1", artifact_sha256=payload.artifact_hashes(source)),
    )
    index = payload.pack(source, staging)
    store = upload_directory(staging, tmp_path / "repo")
    with payload.materialize(staging, store=store) as raw:
        assert payload.artifact_hashes(raw) == payload.artifact_hashes(source)
    with tarfile.open(staging / "assets" / index["archives"][0]["asset"]) as archive:
        assert archive.getmembers()[0].pax_headers == {"path": name}


def test_short_names_byte_identical(tmp_path):
    # Production compression settings; include the USTAR 100-byte boundary.
    source = tmp_path / "raw"
    source.mkdir()
    names = ["a", "b" * 100]
    for name in names:
        (source / name).write_bytes(b"identical evidence\r\n")
    v1.compress(source, names, tmp_path / "v1.xz")
    payload.compress(source, names, tmp_path / "v2.xz")
    assert (tmp_path / "v1.xz").read_bytes() == (tmp_path / "v2.xz").read_bytes()


def replace_archive(packed, data):
    _, staging, store, index = packed
    entry = index["archives"][0]
    path = store / index["store"]["tag"] / entry["asset"]
    path.write_bytes(data)
    entry.update(size=len(data), sha256=hashlib.sha256(data).hexdigest())
    write(staging / "payload-index.json", index)


@pytest.mark.parametrize("global_header", [False, True])
def test_pax_headers_refuse(packed, global_header):
    source, staging, store, _ = packed
    output = io.BytesIO()
    with tarfile.open(
        fileobj=output,
        mode="w",
        format=tarfile.PAX_FORMAT,
        pax_headers={"comment": "global"} if global_header else None,
    ) as archive:
        member = tarfile.TarInfo("raw.log")
        data = (source / "raw.log").read_bytes()
        member.size, member.mode = len(data), 0o644
        if not global_header:
            member.pax_headers = {"comment": "forbidden"}
        archive.addfile(member, io.BytesIO(data))
    replace_archive(packed, lzma.compress(output.getvalue()))
    error = refuses(staging, store, "non-path PAX key accepted")
    assert ("k4-payload-pax-global" if global_header else "k4-payload-pax-key") in error


def test_store_hash_refuses(packed):
    _, staging, store, index = packed
    entry = index["archives"][0]
    path = store / index["store"]["tag"] / entry["asset"]
    path.write_bytes(path.read_bytes() + b"\0" * 4)  # Valid XZ padding, unchanged raw bytes.
    entry["size"] = path.stat().st_size
    write(staging / "payload-index.json", index)
    assert "pilot-payload-archive-hash" in refuses(staging, store, "store hash mismatch accepted")


def test_store_size_refuses(packed):
    _, staging, store, index = packed
    path = store / index["store"]["tag"] / index["archives"][0]["asset"]
    path.write_bytes(path.read_bytes() + b"\0" * 4)
    assert "pilot-payload-archive-size" in refuses(staging, store, "store size mismatch accepted")


def test_store_missing_refuses(packed):
    _, staging, store, index = packed
    (store / index["store"]["tag"] / index["archives"][0]["asset"]).unlink()
    assert "k4-store-assets" in refuses(staging, store, "missing release asset accepted")


def test_release_extra_asset_refuses(packed):
    _, staging, store, index = packed
    (store / index["store"]["tag"] / "extra.tar.xz").write_bytes(b"extra")
    assert "k4-store-assets" in refuses(staging, store, "extra release asset accepted")


def test_total_cap_refuses(packed, monkeypatch, tmp_path):
    source, staging, store, index = packed
    assert payload.PAYLOAD_LIMIT == 1_440_000_000
    total = sum(e["size"] for e in index["archives"])
    monkeypatch.setattr(payload, "PAYLOAD_LIMIT", total - 1)
    destination = tmp_path / "too-large"
    destination.mkdir()
    with pytest.raises(ValueError, match="pilot-payload-total-size"):
        payload.pack(source, destination)
    assert "pilot-payload-total-size" in refuses(staging, store, "total cap ignored")


def test_mixed_layout_refuses(packed):
    _, staging, store, _ = packed
    (staging / "payload").mkdir()
    assert "k4-payload-mixed-layout" in refuses(staging, store, "mixed K4 layout accepted")


def test_legacy_stage_commit_requires_upload(tmp_path, legacy_download):  # noqa: F811
    source, _ = legacy_download
    repo = local_repository(tmp_path / "repo")
    staging, store = tmp_path / "stage", tmp_path / "store"
    recorder.stage(source, staging, repo, Path("unused"))
    before = (repo / k4.LEDGER).read_bytes()
    with pytest.raises(ValueError, match="k4-store-missing"):
        recorder.record(staging, repo, Path("unused"), store=store)
    assert not (repo / k4.EVIDENCE_ROOT).exists()
    assert (repo / k4.LEDGER).read_bytes() == before
    upload_directory(staging, repo)
    target = recorder.record(staging, repo, Path("unused"), store=store)
    assert not (target / "payload").exists() and not (target / "assets").exists()
    git_commit(repo)
    result = evidence.verify_evidence(repo, Path("unused"), store=store)
    assert len(result["safety"]["episodes"]) == 21


def two_job_history(tmp_path):
    """Committed synthetic intake envelopes; never a network or decompression fixture."""
    repo = local_repository(tmp_path / "repo")
    ledger = json.loads((repo / k4.LEDGER).read_bytes())
    fixture = json.loads((Path(__file__).parent / "fixtures/k4-round266-intake.json").read_bytes())
    for number in (1, 2):
        retained = fixture["episodes"][number - 1]
        metrics = retained["metrics"]
        name = metrics["episode"]
        m = default(schema.JOB)
        m.update(
            schema="dur053.k4-job.v1",
            job=f"k4-{number}",
            run_id=str(number),
            job_id=str(number),
            status="FAIL",
            charged_seconds=1,
        )
        m.update(
            episodes=[name],
            dispositions=[
                dict(
                    episode=name, replacement_for="", classification="valid", reason="", evidence=[]
                )
            ],
        )
        target = repo / k4.EVIDENCE_ROOT / m["job"] / m["run_id"]
        write(target / "manifest.json", m)
        write(
            target / "payload-index.json",
            dict(
                schema="dur053.payload.v2",
                archives=[],
                store=dict(
                    kind="github-release",
                    repository=payload.REPOSITORY,
                    tag=f"dur053-evidence-{m['job']}-{number}",
                ),
            ),
        )
        write(
            target / "intake.json",
            dict(
                metrics=[metrics],
                safety_rows=[retained["safety"]],
                manifest_sha256=hashlib.sha256((target / "manifest.json").read_bytes()).hexdigest(),
            ),
        )
        row = {
            k: m[k]
            for k in (
                "run_id",
                "job_id",
                "head_sha",
                "tree_sha",
                "merge_ref_sha",
                "status",
                "charged_seconds",
            )
        }
        row.update(
            category=m["job"],
            k4_episodes=[name],
            artifact=(target / "manifest.json").relative_to(repo).as_posix(),
        )
        for name, key in [
            ("manifest.json", "artifact_sha256"),
            ("payload-index.json", "payload_index_sha256"),
            ("intake.json", "intake_sha256"),
        ]:
            row[key] = hashlib.sha256((target / name).read_bytes()).hexdigest()
        ledger["entries"].append(row)
    write(repo / k4.LEDGER, ledger)
    git_commit(repo)
    return repo


def test_history_gate_never_materializes(tmp_path, monkeypatch):
    repo = two_job_history(tmp_path)
    no_payload = Mock(side_effect=AssertionError("history gate touched raw payload"))
    monkeypatch.setattr(recorder, "materialize_job", no_payload)
    monkeypatch.setattr(recorder, "materialize", no_payload)
    monkeypatch.setattr(payload, "materialize", no_payload)
    monkeypatch.setattr(payload, "release_assets", no_payload)
    monkeypatch.setattr(payload, "fetch_asset", no_payload)
    found, _ = recorder.inventory(repo)
    assert len(found) == 2
    view = evidence.history_view(repo)
    assert len(view["retained_jobs"]) == 2
    assert len(view["safety"]["episodes"]) == 2
    assert view["disposition_counts"] == {"PASS": 2}
    no_payload.assert_not_called()


@pytest.mark.parametrize("name", ["manifest.json", "payload-index.json", "intake.json"])
def test_history_bound_bytes_refuse(tmp_path, name):
    repo = two_job_history(tmp_path)
    path = repo / k4.EVIDENCE_ROOT / "k4-1/1" / name
    path.write_bytes(path.read_bytes() + b"\n")
    git_commit(repo)
    with pytest.raises(ValueError, match="k4-.*binding"):
        recorder.inventory(repo)
