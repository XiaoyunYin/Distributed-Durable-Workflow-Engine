"""Seal and verify attempt-scoped J2 harness evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import zipfile
from pathlib import Path, PurePosixPath

from j2_qualification_evidence import file_record, require, write

INDEX = "freeze.json"


def safe_name(name):
    path = PurePosixPath(name)
    return (
        not path.is_absolute()
        and path.as_posix() == name
        and bool(path.parts)
        and not any(part.startswith(".") or "\\" in part or ":" in part for part in path.parts)
    )


def seal(directory):
    require(not (directory / INDEX).exists(), "harness evidence already sealed")
    files = {}
    for path in sorted(directory.rglob("*")):
        name = path.relative_to(directory).as_posix()
        require(safe_name(name) and not path.is_symlink(), f"unsafe evidence path: {name}")
        if path.is_dir():
            continue
        require(path.is_file(), f"non-regular evidence: {name}")
        files[name] = file_record(path)
    require(files, "empty harness evidence")
    write(
        directory / INDEX,
        {
            "schema": "j2.harness.freeze.v1",
            "run_id": os.environ["GITHUB_RUN_ID"],
            "run_attempt": os.environ["GITHUB_RUN_ATTEMPT"],
            "head": os.environ["GITHUB_SHA"],
            "files": files,
        },
    )


def verify_upload(
    archive, metadata, artifact_id, artifact_digest, *, mode="harness-reset", job=None
):
    require(
        mode
        in {
            "harness-reset",
            "harness-schedule",
            "harness-sender",
            "harness-step",
            "controls",
            "controls-r422",
            "measure-replay",
            "r422-full-clock",
            "r422-full-clock-replay",
        },
        "unexpected J2 artifact mode",
    )
    require(
        (job in (1, 2, 3) if mode == "r422-full-clock" else job is None),
        "J2 artifact job does not match mode",
    )
    suffix = f"-job-{job}" if job is not None else ""
    digest = file_record(archive)["sha256"]
    require(
        metadata["id"] == int(artifact_id)
        and metadata["expired"] is False
        and metadata["expires_at"]
        and str(metadata["workflow_run"]["id"]) == os.environ["GITHUB_RUN_ID"]
        and metadata["workflow_run"]["head_sha"] == os.environ["GITHUB_SHA"]
        and metadata["name"]
        == (f"j2-{mode}-{os.environ['GITHUB_RUN_ID']}-{os.environ['GITHUB_RUN_ATTEMPT']}{suffix}")
        and metadata["digest"] == f"sha256:{digest}"
        and digest == artifact_digest,
        "Actions evidence artifact identity, expiry or digest mismatch",
    )
    with zipfile.ZipFile(archive) as bundle:
        entries = [info for info in bundle.infolist() if not info.is_dir()]
        names = {info.filename for info in entries}
        require(
            len(entries) == len(names)
            and all(
                safe_name(info.filename) and not stat.S_ISLNK(info.external_attr >> 16)
                for info in entries
            ),
            "unsafe or duplicate ZIP member",
        )
        require(INDEX in names, "ZIP hash index missing")
        index = json.loads(bundle.read(INDEX))
        require(
            index["schema"] == "j2.harness.freeze.v1"
            and index["run_id"] == os.environ["GITHUB_RUN_ID"]
            and index["run_attempt"] == os.environ["GITHUB_RUN_ATTEMPT"]
            and index["head"] == os.environ["GITHUB_SHA"]
            and names == set(index["files"]) | {INDEX},
            "ZIP index identity or membership mismatch",
        )
        for name, expected in index["files"].items():
            require(bundle.getinfo(name).file_size == expected["bytes"], f"ZIP size: {name}")
            with bundle.open(name) as stream:
                require(
                    hashlib.file_digest(stream, "sha256").hexdigest() == expected["sha256"],
                    f"ZIP digest: {name}",
                )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("seal").add_argument("directory", type=Path)
    verify = commands.add_parser("verify-upload")
    verify.add_argument("archive", type=Path)
    verify.add_argument("metadata", type=Path)
    verify.add_argument("artifact_id")
    verify.add_argument("artifact_digest")
    verify.add_argument("--mode", default="harness-reset")
    verify.add_argument("--job", type=int)
    args = parser.parse_args()
    if args.command == "seal":
        seal(args.directory)
    else:
        verify_upload(
            args.archive,
            json.loads(args.metadata.read_bytes()),
            args.artifact_id,
            args.artifact_digest,
            mode=args.mode,
            job=args.job,
        )


if __name__ == "__main__":
    main()
