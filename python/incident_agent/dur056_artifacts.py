"""Write-once study artifacts and atomic marker updates for DUR-056."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from incident_agent.dur056_retrieval import fingerprint


class Dur056ArtifactError(RuntimeError):
    """Raised when a versioned artifact or scorer marker is invalid."""


def write_once(directory: Path, stem: str, value: Any) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    path = directory / f"{stem}-{stamp}.json"
    payload = (json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n").encode(
        "utf-8"
    )
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError as error:
        raise Dur056ArtifactError(
            f"refusing to overwrite versioned artifact: {path.name}"
        ) from error
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        path.unlink(missing_ok=True)
        raise
    return path


def write_unit_once(directory: Path, unit_id: str, value: Any) -> Path:
    if not unit_id or any(
        char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
        for char in unit_id
    ):
        raise Dur056ArtifactError("DUR-056 unit ID contains unsafe filename characters")
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{unit_id}.json"
    payload = (json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n").encode(
        "utf-8"
    )
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError as error:
        raise Dur056ArtifactError(f"DUR-056 unit is write-once: {unit_id}") from error
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        path.unlink(missing_ok=True)
        raise
    return path


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise Dur056ArtifactError(f"cannot read study artifact {path.name}: {error}") from error
    if not isinstance(value, dict):
        raise Dur056ArtifactError(f"study artifact must be a JSON object: {path.name}")
    return value


def atomic_write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n").encode(
        "utf-8"
    )
    descriptor, name = tempfile.mkstemp(prefix="dur056-marker-", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def create_exclusive_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n").encode(
        "utf-8"
    )
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError as error:
        raise Dur056ArtifactError(f"DUR-056 one-shot marker already exists: {path.name}") from error
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


def current_git_head(root: Path) -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        raise Dur056ArtifactError(f"cannot determine DUR-056 scorer HEAD: {error}") from error
    return result.stdout.strip()


def canonical_config_fingerprint(config: dict[str, Any], key: str) -> str:
    core = dict(config)
    embedded = core.pop(key, None)
    calculated = fingerprint(core)
    if embedded != calculated:
        raise Dur056ArtifactError(f"DUR-056 config fingerprint mismatch for {key}")
    return calculated
