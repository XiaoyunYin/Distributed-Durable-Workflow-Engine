#!/usr/bin/env python3
"""Verify and safely extract one DUR-050 tar.gz without leaving partial output."""

from __future__ import annotations

import argparse
import hashlib
import tarfile
import tempfile
from pathlib import Path, PurePosixPath


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def safe_extract(
    archive: Path, destination: Path, expected_size: int, expected_sha256: str
) -> None:
    if destination.exists() or destination.is_symlink():
        raise ValueError(f"refusing to overwrite extraction destination: {destination}")
    if archive.stat().st_size != expected_size:
        raise ValueError("archive byte size does not match the remote manifest")
    if sha256(archive) != expected_sha256.lower():
        raise ValueError("archive SHA-256 does not match the remote manifest")

    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".dur050-extract-", dir=destination.parent
    ) as temp_name:
        staging = Path(temp_name) / "tree"
        staging.mkdir()
        total_size = 0
        members_seen: set[str] = set()
        try:
            with tarfile.open(archive, mode="r:gz") as tar:
                members = tar.getmembers()
                if not members or len(members) > 200_000:
                    raise ValueError("archive has no members or exceeds the member safety limit")
                for member in members:
                    path = PurePosixPath(member.name)
                    if (
                        path.is_absolute()
                        or not path.parts
                        or any(part in ("", ".", "..") for part in path.parts)
                    ):
                        raise ValueError(f"unsafe archive member path: {member.name!r}")
                    if member.name in members_seen and not member.isdir():
                        raise ValueError(f"duplicate non-directory archive member: {member.name!r}")
                    members_seen.add(member.name)
                    if not (member.isdir() or member.isfile()):
                        raise ValueError(f"unsupported archive member type: {member.name!r}")
                    target = staging.joinpath(*path.parts)
                    if member.isdir():
                        target.mkdir(parents=True, exist_ok=True)
                        continue
                    total_size += member.size
                    if total_size > 2 * 1024 * 1024 * 1024:
                        raise ValueError("archive expands beyond the 2 GiB safety limit")
                    target.parent.mkdir(parents=True, exist_ok=True)
                    source = tar.extractfile(member)
                    if source is None:
                        raise ValueError(f"could not read archive member: {member.name!r}")
                    with source, target.open("xb") as output:
                        while block := source.read(1024 * 1024):
                            output.write(block)
            top_level = {name.split("/", 1)[0] for name in members_seen}
            if len(top_level) != 1:
                raise ValueError("archive must contain exactly one top-level output directory")
            root = staging / next(iter(top_level))
            if not root.is_dir():
                raise ValueError("archive top-level member is not a directory")
            try:
                root.rename(destination)
            except FileExistsError as exc:
                raise ValueError(
                    f"refusing to overwrite extraction destination: {destination}"
                ) from exc
        except BaseException:
            # The temporary directory context removes every partial file on failure.
            raise


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", required=True, type=Path)
    parser.add_argument("--destination", required=True, type=Path)
    parser.add_argument("--expected-size", required=True, type=int)
    parser.add_argument("--expected-sha256", required=True)
    args = parser.parse_args()
    if (
        args.expected_size <= 0
        or len(args.expected_sha256) != 64
        or any(c not in "0123456789abcdefABCDEF" for c in args.expected_sha256)
    ):
        parser.error("expected size and SHA-256 are malformed")
    try:
        safe_extract(args.archive, args.destination, args.expected_size, args.expected_sha256)
    except (OSError, tarfile.TarError, ValueError) as exc:
        parser.exit(1, f"dur050-safe-extract: {exc}\n")
    print(f"PASS: verified and extracted {args.expected_size} bytes to {args.destination}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
