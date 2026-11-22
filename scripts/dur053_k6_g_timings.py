"""Summarize worker persistence timings against the acquired ledgers."""

from __future__ import annotations

from math import ceil
from pathlib import Path

from dur053_k6_b_smoke import require
from dur053_k6_g_bundle import Json, read_lines


def distribution(samples: list[int]) -> Json:
    ordered = sorted(samples)
    count = len(ordered)
    return {
        "count": count,
        "p50_ns": ordered[ceil(count * 0.50) - 1] if count else None,
        "p99_ns": ordered[ceil(count * 0.99) - 1] if count else None,
        "max_ns": ordered[-1] if count else None,
    }


def ledger_cost(bundle: Path, timing_documents: list[Json], *, effect: bool) -> Json:
    operations: dict[str, list[int]] = {
        name: [] for name in ("append", "checkpoint", "terminal", "activity")
    }
    per_kind: dict[str, list[int]] = {}
    sessions = []
    for timing in timing_documents:
        session = timing["session"]
        path = bundle / "witness" / f"{session}.jsonl"
        _, *recorded, seal = read_lines(path)
        rows = timing["rows"]
        appends = [r for r in rows if r["operation"] == "append"]
        require(
            [(r["sequence"], r["kind"]) for r in appends]
            == [(r["position"]["sequence"], r["data"]["kind"]) for r in recorded],
            "G_LEDGER_APPEND_TIMINGS",
        )
        require(
            sorted(r["invocation"] for r in rows if r["operation"] == "activity")
            == sorted(r["data"]["invocation"] for r in recorded if r["data"]["kind"] == "finish"),
            "G_LEDGER_ACTIVITY_TIMINGS",
        )
        require(
            any(r["operation"] == "checkpoint" for r in rows)
            and sum(r["operation"] == "terminal" for r in rows) == (seal["mode"] == "terminal"),
            "G_LEDGER_CHECKPOINT_TIMINGS",
        )
        for row in rows:
            operations[row["operation"]].append(row["elapsed_ns"])
        for row in appends:
            per_kind.setdefault(row["kind"], []).append(row["elapsed_ns"])
        sessions.append(
            {"session": session, "bytes": path.stat().st_size, "seal_mode": seal["mode"]}
        )
    return {
        "ledger_bytes": sum(row["bytes"] for row in sessions),
        "sessions": sessions,
        "activity_interval": "effect_runner_including_result_rpc" if effect else "pure_payload",
        "distributions": {name: distribution(values) for name, values in operations.items()},
        "append_by_kind": {kind: distribution(values) for kind, values in per_kind.items()},
    }
