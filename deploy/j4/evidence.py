"""Frozen cell schedule, remaining-time admission and portable evidence index."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

ARMS = (*(f"C{i}" for i in range(5)), *(f"B{i}" for i in range(6)))
SECOND = 1_000_000_000
CAP = 350 * 60
BOOTSTRAP = CLOSEOUT = 25 * 60
CELL = 5 * 60
RESET = 2 * 60


def schedule(mode: str) -> list[dict]:
    if mode not in {"qualification", "measurement"}:
        raise ValueError("invalid J4 campaign mode")
    repeats = 1 if mode == "qualification" else 3
    return [
        {"cell": f"j4-{arm.lower()}-{repeat}", "arm": arm, "repeat": repeat}
        for repeat in range(1, repeats + 1)
        for arm in ARMS
    ]


def forecast(
    remaining: list[dict],
    allowances: dict[str, float],
    *,
    bootstrap: bool,
    reset_first: bool = False,
) -> float:
    # Preserve the frozen allowances as floors even on a fast qualification.
    runs = sum(max(CELL, allowances.get(case["arm"], 0)) for case in remaining)
    gap_count = max(0, len(remaining) - 1 + int(reset_first)) if remaining else 0
    return (
        (max(BOOTSTRAP, allowances.get("bootstrap", 0)) if bootstrap else 0)
        + runs
        + gap_count * max(RESET, allowances.get("reset", 0))
        + max(CLOSEOUT, allowances.get("closeout", 0))
    )


def admit(remaining: list[dict], allowances: dict[str, float], elapsed: float, **phase) -> float:
    needed = forecast(remaining, allowances, **phase)
    if needed > CAP - elapsed:
        raise ValueError("J4 remaining forecast exceeds the 350-minute job cap")
    return needed


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pending = path.with_suffix(path.suffix + ".pending")
    pending.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    pending.replace(path)


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    # A producer can be appending during a live poll. Complete lines are the
    # only usable witnesses. The final independent check requires all joins.
    raw = path.read_bytes()
    if not raw.endswith(b"\n"):
        raw = raw.rpartition(b"\n")[0]
    return [json.loads(line) for line in raw.splitlines()]


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def index(root: Path) -> dict[str, str]:
    files = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ValueError("unexpected symlink in J4 evidence")
        if path.is_file() and path != root / "sha256.json":
            files[path.relative_to(root).as_posix()] = digest(path)
    return files


def seal(root: Path) -> None:
    write_json(root / "sha256.json", index(root))


def verify_index(root: Path) -> None:
    expected = json.loads((root / "sha256.json").read_bytes())
    if not expected or expected != index(root):
        raise ValueError("J4 evidence index is incomplete or mismatched")
