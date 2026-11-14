"""K4 Amendment B.4.1 external release assets; K3 stays on payload v1."""

from __future__ import annotations

import hashlib
import json
import lzma
import shutil
import tarfile
import tempfile
import urllib.parse
import urllib.request
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import Any

from dur053_k3_policy import artifact_hashes, require

REPOSITORY = "XiaoyunYin/Distributed-Durable-Workflow-Engine"

# Decimal MB; measured pilot plus conservative headroom, registered in B.4.
ARCHIVE_LIMIT = 45_000_000
PAYLOAD_LIMIT = 1_440_000_000
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


def fits_ustar(name: str) -> bool:
    try:
        tarfile.TarInfo(name).tobuf(format=tarfile.USTAR_FORMAT)
        return True
    except (ValueError, UnicodeError):
        return False


class UstarMember(tarfile.TarInfo):
    def tobuf(self, format=tarfile.DEFAULT_FORMAT, encoding="utf-8", errors="strict"):
        return super().tobuf(tarfile.USTAR_FORMAT, encoding, errors)


def compress(source: Path, members: list[str], target: Path) -> None:
    with (
        lzma.open(target, "wb", format=lzma.FORMAT_XZ, filters=FILTERS) as stream,
        tarfile.open(fileobj=stream, mode="w|", format=tarfile.PAX_FORMAT) as archive,
    ):
        for name in members:
            require(safe_name(name), "pilot-payload-path")
            path = source / name
            require(path.is_file() and not path.is_symlink(), "pilot-payload-regular")
            member = tarfile.TarInfo(name)
            member.size = path.stat().st_size
            member.mtime, member.uid, member.gid, member.mode = 0, 0, 0, 0o644
            with path.open("rb") as data:
                if fits_ustar(name):
                    member = UstarMember.frombuf(
                        member.tobuf(format=tarfile.USTAR_FORMAT), "utf-8", "strict"
                    )
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
    payload = staging / "assets"
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
                "asset": path.name,
                "size": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "members": members,
            }
        )
        require(sum(e["size"] for e in entries) <= PAYLOAD_LIMIT, "pilot-payload-total-size")

    for members in groups.values():
        emit(members)
    index = {
        "schema": "dur053.payload.v2",
        "archives": entries,
        "store": {
            "kind": "github-release",
            "repository": REPOSITORY,
            "tag": f"dur053-evidence-{manifest['job']}-{manifest['run_id']}",
        },
    }
    (staging / "payload-index.json").write_bytes(
        (json.dumps(index, sort_keys=True, indent=2) + "\n").encode()
    )
    shutil.copyfile(source / "manifest.json", staging / "manifest.json")
    return index


class StrictMember(tarfile.TarInfo):
    def _proc_member(self, archive):
        require(self.type in {tarfile.REGTYPE, tarfile.AREGTYPE, tarfile.XHDTYPE, tarfile.XGLTYPE},
            "pilot-payload-member")
        return super()._proc_member(archive)

    def _proc_pax(self, archive):
        require(self.type == tarfile.XHDTYPE, "k4-payload-pax-global")
        offset = archive.fileobj.tell()
        data = archive.fileobj.read(self.size)
        # Inspect before tarfile interprets size, sparse, charset or other keys.
        pos, keys = 0, []
        while pos < len(data):
            length_text, space, record = data[pos:].partition(b" ")
            require(bool(space) and length_text.isdigit(), "k4-payload-pax-record")
            length = int(length_text)
            require(5 <= length <= len(data) - pos, "k4-payload-pax-record")
            record = data[pos + len(length_text) + 1 : pos + length]
            key, equals, value = record[:-1].partition(b"=")
            require(record.endswith(b"\n") and bool(equals), "k4-payload-pax-record")
            require(key == b"path", "k4-payload-pax-key")
            keys.append(key)
            if key == b"path":
                require(not fits_ustar(value.decode("utf-8")), "k4-payload-pax-unnecessary")
            pos += length
        require(len(keys) == len(set(keys)) == 1, "k4-payload-pax-record")
        archive.fileobj.seek(offset)
        return super()._proc_pax(archive)


def validate_index(directory: Path) -> dict[str, Any]:
    require(
        directory.is_dir()
        and not directory.is_symlink()
        and all(not p.is_symlink() for p in directory.rglob("*")),
        "k4-payload-symlinks",
    )
    require(not (directory / "payload").exists(), "k4-payload-mixed-layout")
    index = json.loads((directory / "payload-index.json").read_bytes())
    manifest = json.loads((directory / "manifest.json").read_bytes())
    require(index["schema"] == "dur053.payload.v2", "k4-payload-schema")
    require(
        index["store"]
        == dict(
            kind="github-release",
            repository=REPOSITORY,
            tag=f"dur053-evidence-{manifest['job']}-{manifest['run_id']}",
        ),
        "k4-payload-store",
    )
    entries = index["archives"]
    require(
        [e["asset"] for e in entries] == [f"{i:04d}.tar.xz" for i in range(len(entries))]
        and all(e["path"] == "payload/" + e["asset"] for e in entries),
        "k4-payload-archives",
    )
    require(
        all(type(e["size"]) is int and 0 < e["size"] <= ARCHIVE_LIMIT for e in entries),
        "pilot-payload-archive-size",
    )
    require(sum(e["size"] for e in entries) <= PAYLOAD_LIMIT, "pilot-payload-total-size")
    return index


def release_assets(index: dict, store: str | Path) -> dict[str, Any]:
    """List exactly one release. Directory stores represent that release's assets."""
    if str(store) != "github":
        root = Path(store) / index["store"]["tag"]
        require(root.is_dir() and not root.is_symlink(), "k4-store-missing")
        require(all(p.is_file() and not p.is_symlink() for p in root.iterdir()), "k4-store-layout")
        return {p.name: p for p in root.iterdir()}
    spec = index["store"]
    tag = urllib.parse.quote(spec["tag"], safe="")
    url = f"https://api.github.com/repos/{spec['repository']}/releases/tags/{tag}"
    request = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(request, timeout=60) as response:
        release = json.load(response)
    require(
        release["tag_name"] == spec["tag"]
        and release["name"] == spec["tag"]
        and release["prerelease"] is True
        and not release["draft"],
        "k4-store-release",
    )
    assets = release["assets"]
    require(len({a["name"] for a in assets}) == len(assets), "k4-store-assets")
    result = {}
    for asset in assets:
        expected = (
            f"https://github.com/{spec['repository']}/releases/download/{tag}/{asset['name']}"
        )
        require(asset["browser_download_url"] == expected, "k4-store-asset-url")
        result[asset["name"]] = expected
    return result


def fetch_asset(location: Any, destination: Path) -> None:
    if isinstance(location, Path):
        shutil.copyfile(location, destination)
    else:
        with (
            urllib.request.urlopen(location, timeout=60) as response,
            destination.open("wb") as out,
        ):
            shutil.copyfileobj(response, out)


@contextmanager
def materialize(
    directory: Path, repository: Path | None = None, *, store="github"
) -> Iterator[Path]:
    from dur053_pilot import committed_bytes

    index = validate_index(directory)
    raw = (directory / "manifest.json").read_bytes()
    manifest = json.loads(raw)
    if repository is not None:
        index_raw = committed_bytes(repository, directory / "payload-index.json")
        committed_bytes(repository, directory / "manifest.json")
        ledger = json.loads(
            committed_bytes(
                repository, repository / "experiments/portfolio/k8s-recovery/live-time-ledger.json"
            )
        )
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
    assets = release_assets(index, store)
    require(set(assets) == {e["asset"] for e in index["archives"]}, "k4-store-assets")
    with scratch(repository) as output, scratch(repository) as downloads:
        (output / "manifest.json").write_bytes(raw)
        seen: set[str] = set()
        for entry in index["archives"]:
            path = downloads / entry["asset"]
            fetch_asset(assets[entry["asset"]], path)
            require(
                path.stat().st_size == entry["size"] <= ARCHIVE_LIMIT, "pilot-payload-archive-size"
            )
            with path.open("rb") as handle:
                digest = hashlib.file_digest(handle, "sha256").hexdigest()
            require(digest == entry["sha256"], "pilot-payload-archive-hash")
            members = []
            with tarfile.open(path, mode="r:xz", tarinfo=StrictMember) as archive:
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
                    require(
                        member.mtime == member.uid == member.gid == 0
                        and member.mode == 0o644
                        and member.uname == member.gname == "",
                        "k4-payload-member-metadata",
                    )
                    seen.add(name)
                    members.append(name)
                    destination = output / name
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    stream = archive.extractfile(member)
                    require(stream is not None, "pilot-payload-member")
                    with destination.open("wb") as result:
                        shutil.copyfileobj(stream, result)
            require(
                members == entry["members"] and members == sorted(members),
                "pilot-payload-index-members",
            )
        require(seen == set(manifest["artifact_sha256"]), "pilot-payload-member-set")
        require(artifact_hashes(output) == manifest["artifact_sha256"], "pilot-artifact-hashes")
        yield output
