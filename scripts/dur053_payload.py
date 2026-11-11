"""B.4 deterministic storage and fail-closed, external materialisation."""

from __future__ import annotations

import hashlib
import io
import json
import lzma
import shutil
import tarfile
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import Any

from dur053_k3_policy import artifact_hashes, require

# Decimal MB; measured pilot plus conservative headroom, registered in B.4.
ARCHIVE_LIMIT = 45_000_000
PAYLOAD_LIMIT = 90_000_000
FILTERS = [{"id": lzma.FILTER_LZMA2, "preset": 6, "dict_size": 128 * 1024 * 1024}]


def safe_name(name: str) -> bool:
    path = PurePosixPath(name)
    return (
        bool(name)
        and not path.is_absolute()
        and path.as_posix() == name
        and all(part not in {"", ".", ".."} and ":" not in part for part in name.split("/"))
        and "\\" not in name
    )


@contextmanager
def scratch(repository: Path | None = None) -> Iterator[Path]:
    with tempfile.TemporaryDirectory(prefix="dur053-payload-") as tmp:
        root = Path(tmp).resolve()
        if repository is not None:
            require(not root.is_relative_to(repository.resolve()), "pilot-payload-external-temp")
        yield root


def compress(source: Path, members: list[str], target: Path) -> None:
    with (
        lzma.open(target, "wb", format=lzma.FORMAT_XZ, filters=FILTERS) as stream,
        tarfile.open(fileobj=stream, mode="w|", format=tarfile.USTAR_FORMAT) as archive,
    ):
        for name in members:
            require(safe_name(name), "pilot-payload-path")
            path = source / name
            require(path.is_file() and not path.is_symlink(), "pilot-payload-regular")
            member = tarfile.TarInfo(name)
            member.size = path.stat().st_size
            member.mtime, member.uid, member.gid, member.mode = 0, 0, 0, 0o644
            with path.open("rb") as data:
                archive.addfile(member, data)


def pack(source: Path, staging: Path) -> dict[str, Any]:
    """Stage everything externally; no repository writes until all bounds pass."""
    manifest = json.loads((source / "manifest.json").read_bytes())
    require(manifest["artifact_sha256"] == artifact_hashes(source), "pilot-artifact-hashes")
    require(
        all(
            not p.is_symlink()
            and (p.is_file() or (p.is_dir() and any(f.is_file() for f in p.rglob("*"))))
            for p in source.rglob("*")
        ),
        "pilot-payload-source-layout",
    )
    groups: dict[str, list[str]] = {}
    for name in sorted(manifest["artifact_sha256"]):
        require(safe_name(name), "pilot-payload-path")
        require(
            name.split("/")[0] not in {"payload", "payload-index.json"}, "pilot-payload-reserved"
        )
        group = name.split("/")[0] if "/" in name else ""
        groups.setdefault(group, []).append(name)
    payload = staging / "payload"
    payload.mkdir()
    entries: list[dict[str, Any]] = []

    def emit(members: list[str]) -> None:
        path = payload / f"{len(entries):04d}.tar.xz"
        compress(source, members, path)
        if path.stat().st_size > ARCHIVE_LIMIT and len(members) > 1:
            path.unlink()
            middle = len(members) // 2
            emit(members[:middle])
            emit(members[middle:])
            return
        require(path.stat().st_size <= ARCHIVE_LIMIT, "pilot-payload-archive-size")
        entries.append(
            {
                "path": "payload/" + path.name,
                "size": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "members": members,
            }
        )
        require(sum(e["size"] for e in entries) <= PAYLOAD_LIMIT, "pilot-payload-total-size")

    for members in groups.values():
        emit(members)
    index = {"schema": "dur053.payload.v1", "archives": entries}
    (staging / "payload-index.json").write_bytes(
        (json.dumps(index, sort_keys=True, indent=2) + "\n").encode()
    )
    shutil.copyfile(source / "manifest.json", staging / "manifest.json")
    return index


@contextmanager
def materialize(directory: Path, repository: Path | None = None) -> Iterator[Path]:
    """Validate the entire representation before yielding any evidence to readers."""
    from dur053_pilot import committed_bytes

    require(
        directory.is_dir()
        and not directory.is_symlink()
        and all(not p.is_symlink() for p in directory.rglob("*")),
        "pilot-artifact-symlinks",
    )
    raw = (directory / "manifest.json").read_bytes()
    manifest = json.loads(raw)
    archived = (directory / "payload-index.json").exists() or (directory / "payload").exists()
    if repository is not None:
        committed_bytes(repository, directory / "manifest.json")
    if not archived:
        if repository is not None:
            for name in manifest["artifact_sha256"]:
                committed_bytes(repository, directory / name)
        with scratch(repository) as output:
            shutil.copytree(directory, output, dirs_exist_ok=True)
            require(artifact_hashes(output) == manifest["artifact_sha256"], "pilot-artifact-hashes")
            yield output
        return
    index_raw = (directory / "payload-index.json").read_bytes()
    if repository is not None:
        committed_bytes(repository, directory / "payload-index.json")
        ledger_path = repository / "experiments/portfolio/k8s-recovery/live-time-ledger.json"
        ledger = json.loads(committed_bytes(repository, ledger_path))
        rows = [
            r
            for r in ledger["entries"]
            if r["artifact"] == (directory / "manifest.json").relative_to(repository).as_posix()
        ]
        require(
            len(rows) == 1
            and rows[0]["artifact_sha256"] == hashlib.sha256(raw).hexdigest()
            and rows[0].get("payload_index_sha256") == hashlib.sha256(index_raw).hexdigest(),
            "pilot-payload-ledger-index",
        )
    index = json.loads(index_raw)
    require(index["schema"] == "dur053.payload.v1", "pilot-payload-schema")
    entries = index["archives"]
    paths = [e["path"] for e in entries]
    require(
        len(set(paths)) == len(paths)
        and paths == [f"payload/{i:04d}.tar.xz" for i in range(len(entries))],
        "pilot-payload-archives",
    )
    require(
        {p.relative_to(directory).as_posix() for p in directory.rglob("*") if p.is_file()}
        == {"manifest.json", "payload-index.json", *paths},
        "pilot-payload-exact-layout",
    )
    require(
        {p.relative_to(directory).as_posix() for p in directory.rglob("*") if p.is_dir()}
        == {"payload"},
        "pilot-payload-exact-layout",
    )
    require(sum(e["size"] for e in entries) <= PAYLOAD_LIMIT, "pilot-payload-total-size")
    with scratch(repository) as output:
        (output / "manifest.json").write_bytes(raw)
        seen: set[str] = set()
        for entry in entries:
            path = directory / entry["path"]
            require(
                path.stat().st_size == entry["size"] <= ARCHIVE_LIMIT, "pilot-payload-archive-size"
            )
            data = path.read_bytes()
            require(
                hashlib.sha256(data).hexdigest() == entry["sha256"], "pilot-payload-archive-hash"
            )
            if repository is not None:
                committed_bytes(repository, path)
            members = []
            with tarfile.open(fileobj=io.BytesIO(data), mode="r:xz") as archive:
                for member in archive:
                    name = member.name
                    require(
                        safe_name(name)
                        and name != "manifest.json"
                        and member.isreg()
                        and not member.issparse()
                        and name not in seen,
                        "pilot-payload-member",
                    )
                    require(name in manifest["artifact_sha256"], "pilot-payload-member-set")
                    seen.add(name)
                    members.append(name)
                    destination = output / name
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    stream = archive.extractfile(member)
                    if stream is None:
                        raise ValueError("K3 guard: pilot-payload-member")
                    with destination.open("wb") as result:
                        shutil.copyfileobj(stream, result)
            require(
                members == entry["members"] and members == sorted(members),
                "pilot-payload-index-members",
            )
        require(seen == set(manifest["artifact_sha256"]), "pilot-payload-member-set")
        require(artifact_hashes(output) == manifest["artifact_sha256"], "pilot-artifact-hashes")
        yield output
