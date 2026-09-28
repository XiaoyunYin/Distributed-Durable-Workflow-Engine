#!/usr/bin/env python3
"""Verify committed evidence hashes recorded by the DUR-050 cost manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterator


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "experiments/m8/dur050-capacity-overload/pilot-20260924-e3d780f/cost-manifest.json"
HEX_SHA256 = re.compile(r"^[0-9A-Fa-f]{64}$")


def walk_pairs(value: Any, at: str = "$", parent: dict[str, Any] | None = None) -> Iterator[tuple[str, str, str, str]]:
    if isinstance(value, dict):
        for key, child in value.items():
            if not isinstance(child, str):
                continue
            if key == "path":
                hash_key = "sha256"
            elif key.endswith("_path"):
                hash_key = key[:-5] + "_sha256"
            else:
                continue
            digest = value.get(hash_key)
            if isinstance(digest, str):
                yield f"{at}.{key}", child, hash_key, digest
        for key, child in value.items():
            yield from walk_pairs(child, f"{at}.{key}", value)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from walk_pairs(child, f"{at}[{index}]")


def repository_path(raw: str) -> str:
    # Some historical state entries add an explanatory parenthetical after the path.
    return raw.split(" (", 1)[0].replace("\\", "/")


def is_external_pin(path: str) -> str | None:
    if re.match(r"^[A-Za-z]:/", path) or path.startswith("/"):
        return "external absolute path (for example the local Terraform executable)"
    if path.endswith(".tfplan") or path.startswith("deploy/aws/.terraform/"):
        return "ignored saved Terraform plan; its hash is a safety pin, not a committed evidence blob"
    if path == "deploy/aws/terraform.tfstate" or path.endswith(".tfstate"):
        return "local Terraform state is intentionally not committed"
    return None


def git_blob(repo: Path, revision: str, path: str) -> bytes:
    return subprocess.check_output(
        ["git", "-C", str(repo), "show", f"{revision}:{path}"],
        stderr=subprocess.PIPE,
    )


def verify(manifest_path: Path, revision: str) -> tuple[int, list[str], list[str]]:
    document = json.loads(manifest_path.read_text(encoding="utf-8"))
    errors: list[str] = []
    exclusions: list[str] = []
    checked = 0

    for where, raw_path, hash_key, recorded in walk_pairs(document):
        path = repository_path(raw_path)
        exclusion = is_external_pin(path)
        if exclusion:
            exclusions.append(f"{where}: {path} ({exclusion})")
            continue
        if not HEX_SHA256.fullmatch(recorded):
            errors.append(f"{where}.{hash_key}: expected a 64-character SHA-256, got {recorded!r}")
            continue
        try:
            blob = git_blob(ROOT, revision, path)
        except subprocess.CalledProcessError:
            errors.append(f"{where}: evidence path is not present in {revision}: {path}")
            continue
        actual = hashlib.sha256(blob).hexdigest().upper()
        checked += 1
        if actual != recorded.upper():
            errors.append(
                f"{where}.{hash_key}: SHA-256 mismatch for committed blob {path}: "
                f"recorded {recorded.upper()}, actual {actual}"
            )

    for where, node in walk_objects(document):
        for key in ("sha256_working_file", "sha256_committed_blob_lf"):
            if key in node:
                errors.append(f"{where}.{key}: use canonical sha256 plus optional sha256_windows_crlf")
        for key, value in node.items():
            if (key == "sha256_windows_crlf" or key.endswith("_sha256_windows_crlf")) and not HEX_SHA256.fullmatch(str(value)):
                errors.append(f"{where}.{key}: historical Windows hash is not a 64-character SHA-256")

    return checked, exclusions, errors


def walk_objects(value: Any, at: str = "$") -> Iterator[tuple[str, dict[str, Any]]]:
    if isinstance(value, dict):
        yield at, value
        for key, child in value.items():
            yield from walk_objects(child, f"{at}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from walk_objects(child, f"{at}[{index}]")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, default=MANIFEST)
    parser.add_argument("--revision", default="HEAD", help="Git commit or tree to read committed blobs from")
    args = parser.parse_args()
    try:
        checked, exclusions, errors = verify(args.manifest, args.revision)
    except (OSError, json.JSONDecodeError, subprocess.CalledProcessError) as exc:
        print(f"FAIL: could not verify DUR-050 manifest hashes: {exc}", file=sys.stderr)
        return 1
    for line in exclusions:
        print(f"SKIP non-repository pin: {line}")
    if errors:
        for error in errors:
            print(f"FAIL: {error}", file=sys.stderr)
        return 1
    print(f"PASS: verified {checked} committed evidence path/hash pairs; {len(exclusions)} explicit local pins excluded.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
