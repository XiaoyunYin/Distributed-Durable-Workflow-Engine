#!/usr/bin/env python3
"""K3b registered arithmetic and evidence contracts (no live operations)."""

from __future__ import annotations

import base64
import hashlib
import json
import math
import re
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any

from dur053_k3_policy import artifact_hashes, require

ARMS = {"short": 0.1, "medium": 5.0, "long": 45.0, "cpu": 1.0}
PLANNING_RATES = {"short": 50.0, "medium": 1.0, "long": 1 / 9, "cpu": 4.0}
CANDIDATES = (10000, 100000, 1000000)
CALIBRATION_RESERVATION = 3 * 22 * 120
CEILING = 18900
CONCURRENCY = 16
SATURATION = {"MISSED_DEADLINE", "CAPACITY_MISSED", "MISSED_SEND_DEADLINE"}
EVIDENCE = Path("experiments/portfolio/k8s-recovery/k3-pilot-v1")
JOBS = {
    "k3-pilot-1a": [(a, i) for a in ("short", "medium") for i in range(4)],
    "k3-pilot-1b": [("cpu", i) for i in range(4)],
    "k3-pilot-2": [("long", 0)],
    "k3-pilot-3": [("long", i) for i in range(1, 4)],
}


def finite(value: Any, label: str, *, zero: bool = False) -> float:
    require(
        type(value) in (int, float)
        and math.isfinite(value)
        and (value >= 0 if zero else value > 0),
        label,
    )
    return float(value)


def job_base(name: str) -> str:
    require(bool(re.fullmatch(r"k3-pilot-(1a|1b|2|3)(-c[1-9][0-9]*)?", name)), "pilot-job-name")
    return name.split("-c")[0]


def nearest_rank(values: list[float], q: float) -> float:
    require(bool(values) and 0 < q <= 1, "pilot-quantile-population")
    clean = [finite(x, "pilot-quantile-finite", zero=True) for x in values]
    return sorted(clean)[math.ceil(q * len(clean)) - 1]


def reference(busy: list[float], count: int) -> dict[str, float]:
    require(type(count) is int and count > 0 and bool(busy), "pilot-b-population")
    b = math.fsum(finite(x, "pilot-b-finite", zero=True) for x in busy) / count
    return {"b": b, "r": 4 / max(b, 0.01)}


def slo(repeats: list[list[float]]) -> float:
    require(len(repeats) == 3 and all(len(x) == 200 for x in repeats), "pilot-L-population")
    return math.ceil(1000 * 2 * max(nearest_rank(x, 0.99) for x in repeats)) / 1000


def rates(arm: str, index: int, initial: dict[str, Any] | None = None) -> tuple[float, float]:
    require(arm in ARMS and type(index) is int and 0 <= index <= 3, "pilot-cell")
    if index == 0:
        u = 1 / max(1, ARMS[arm])
        return u, u
    r = finite(initial["r"] if initial is not None else PLANNING_RATES[arm], "pilot-r")
    return 0.25 * r, 0.5 * r


def run_reservation(arm: str, index: int, initial: dict[str, Any] | None = None) -> float:
    _, measured = rates(arm, index, initial)
    return 120 + 200 / measured + 600 + 600 + 60


def full_reservation(
    cells: list[tuple[str, int]],
    initials: dict[str, Any] | None = None,
    *,
    calibration: bool = False,
    setup: bool = True,
) -> int:
    initials = initials or {}
    return math.ceil(
        (300 if setup else 0)
        + 600
        + (CALIBRATION_RESERVATION if calibration else 0)
        + math.fsum(run_reservation(a, i, initials.get(a)) for a, i in cells)
    )


def admit(
    cells: list[tuple[str, int]],
    initials: dict[str, Any] | None = None,
    *,
    calibration: bool = False,
    elapsed: float = 0,
    setup: bool = True,
    reservation: int = CEILING,
    ledger_remaining: int = 259200,
) -> int:
    elapsed = finite(elapsed, "pilot-elapsed", zero=True)
    needed = full_reservation(cells, initials, calibration=calibration, setup=setup)
    require(elapsed + needed <= min(CEILING, reservation), "pilot-job-fit")
    require(elapsed + needed <= ledger_remaining, "pilot-ledger-fit")
    return needed


def carry_over(
    name: str, pending: list[tuple[str, int]], reason: str, started: bool
) -> dict[str, Any]:
    job_base(name)
    require(bool(pending) and reason == "pilot-job-fit" and not started, "pilot-carry-time-only")
    base, _, number = name.partition("-c")
    return {
        "job": base + "-c" + str(int(number or 0) + 1),
        "cells": [list(x) for x in pending],
        "reason": reason,
        "started": False,
    }


def select_cpu(candidates: list[dict[str, Any]], *, engineering: bool = False) -> dict[str, Any]:
    for i, candidate in enumerate(candidates):
        require(i < 3 and candidate["units"] == CANDIDATES[i], "pilot-calibration-order")
        samples = candidate["measured_busy_seconds"]
        require(
            candidate["status"] == "PASS"
            and candidate["discarded"] == 1
            and candidate["concurrency"] == 1
            and candidate["timeout_seconds"] == 120
            and len(samples) == (3 if engineering else 21),
            "pilot-calibration-complete",
        )
        median = nearest_rank(samples, 0.5)
        if median >= 1 or i == 2:
            require(len(candidates) == i + 1, "pilot-calibration-first-candidate")
            return {
                "units": CANDIDATES[i],
                "median_busy_seconds": median,
                "limitation": "no candidate median reached 1 s" if median < 1 else None,
            }
    raise ValueError("K3 guard: pilot-calibration-incomplete")


def committed_bytes(repository: Path, path: Path) -> bytes:
    require(not path.is_symlink(), "pilot-committed-symlink")
    relative = path.resolve().relative_to(repository.resolve()).as_posix()
    normalized = subprocess.run(
        ["git", "hash-object", "--path=" + relative, "--", str(path.resolve())],
        cwd=repository,
        capture_output=True,
        check=False,
    )
    head = subprocess.run(
        ["git", "rev-parse", "HEAD:" + relative],
        cwd=repository,
        capture_output=True,
        check=False,
    )
    require(
        normalized.returncode == head.returncode == 0 and normalized.stdout == head.stdout,
        "pilot-committed-bytes",
    )
    result = subprocess.run(
        ["git", "show", "HEAD:" + relative], cwd=repository, capture_output=True, check=False
    )
    require(result.returncode == 0, "pilot-committed-bytes")
    return result.stdout


def read_artifact(directory: Path, repository: Path | None = None) -> dict[str, Any]:
    require(
        directory.is_dir()
        and not directory.is_symlink()
        and all(not p.is_symlink() for p in directory.rglob("*")),
        "pilot-artifact-symlinks",
    )
    raw = (directory / "manifest.json").read_bytes()
    manifest = json.loads(raw)
    require(
        manifest["schema"] == "dur053.k3b.v1"
        and manifest["budget_scope"] == "registered-campaign"
        and manifest["measurement_claim"] is True,
        "pilot-registered-only",
    )
    require(manifest["artifact_sha256"] == artifact_hashes(directory), "pilot-artifact-hashes")
    require(
        all(str(manifest[k]).isdigit() for k in ("run_id", "job_id")), "pilot-artifact-identity"
    )
    job_base(manifest["job"])
    if repository is not None:
        committed_bytes(repository, directory / "manifest.json")
        for relative in manifest["artifact_sha256"]:
            committed_bytes(repository, directory / relative)
    return manifest


def witness_events(export: dict[str, Any]) -> list[dict[str, Any]]:
    events = []
    expected = export["expected_pods"]
    require(len(expected) == 2 and len(set(expected)) == 2, "pilot-four-slot-witness")
    require(
        {x["inventory"]["pod_uid"] for x in export["ledgers"]} == set(expected)
        and len(export["ledgers"]) == 2,
        "pilot-witness-pods",
    )
    for ledger in export["ledgers"]:
        inv = ledger["inventory"]
        data = base64.b64decode(ledger["data"], validate=True)
        lines = data.splitlines()
        require(
            inv["complete"] is True
            and inv["errors"] == 0
            and inv["attempted"] == inv["written"] == len(lines)
            and data.endswith(b"\n")
            and hashlib.sha256(data).hexdigest() == inv["sha256"],
            "pilot-witness-integrity",
        )
        rows = [json.loads(line) for line in lines]
        require(
            rows[0]["event"] == "session"
            and all(
                r["sequence"] == i + 1
                and r["session"] == inv["session"]
                and r["pod_uid"] == inv["pod_uid"]
                for i, r in enumerate(rows)
            ),
            "pilot-witness-sequence",
        )
        events.extend(rows[1:])
    return events


def busy_times(export: dict[str, Any], workflow_ids: set[str]) -> list[float]:
    events = witness_events(export)
    starts = {e["invocation_id"]: e for e in events if e["event"] == "start"}
    ends = {e["invocation_id"]: e for e in events if e["event"] == "end"}
    require(
        len(starts) + len(ends) == len(events) and starts.keys() == ends.keys(),
        "pilot-witness-pairs",
    )
    durations, seen = [], set()
    for key, start in starts.items():
        end = ends[key]
        require(
            all(
                start[k] == end[k]
                for k in (
                    "workflow_id",
                    "node_id",
                    "iteration",
                    "attempt_number",
                    "claim_token",
                    "session",
                    "pod_uid",
                    "worker_id",
                )
            )
            and end["outcome"] == "SUCCEEDED",
            "pilot-witness-end",
        )
        seconds = finite(
            (end["monotonic_ns"] - start["monotonic_ns"]) / 1e9, "pilot-witness-duration", zero=True
        )
        if start["workflow_id"] in workflow_ids:
            durations.append(seconds)
            seen.add(start["workflow_id"])
    require(seen == workflow_ids, "pilot-witness-coverage")
    return durations


def combine(windows: list[dict[str, Any]]) -> dict[str, Any]:
    require(bool(windows), "pilot-window-population")
    result = dict(windows[0])
    result["rows"] = [row for w in windows for row in w["rows"]]
    # Campaign checker validates index/rate on a single absolute schedule. Keep
    # windows separate in evidence, and reconcile their union using the checker
    # window metadata extension rather than inventing scheduled timestamps.
    result["windows"] = windows
    result["offered"] = len(result["rows"])
    result["status"] = "PASS" if all(w["status"] == "PASS" for w in windows) else "FAIL"
    return result


def window_ok(arrivals: dict[str, Any], count: int) -> set[str]:
    rows = arrivals["rows"]
    require(not any(x["outcome"] in SATURATION for x in rows), "pilot-generator-saturation")
    require(
        arrivals["status"] == "PASS"
        and arrivals["offered"] == count == len(rows)
        and all(x["outcome"] == "ACCEPTED" for x in rows),
        "pilot-arrival-completeness",
    )
    ids = {x["workflow_id"] for x in rows}
    require(len(ids) == count, "pilot-arrival-identities")
    return ids


def summarize(directory: Path, *, engineering: bool = False) -> dict[str, Any]:
    record = json.loads((directory / "record.json").read_bytes())
    require(record["status"] == "PASS" and not record["timed_out"], "pilot-run-accepted")
    require(record["engineering"] is engineering, "pilot-run-scope")
    require(
        record["faults"] == []
        and record["slots"] == 4
        and record["autoscaling"] == "paused-at-two-replicas",
        "pilot-fixed-slots",
    )
    count = 10 if engineering else 200
    warmup = json.loads((directory / "warmup-arrivals.json").read_bytes())
    measured = json.loads((directory / "measured-arrivals.json").read_bytes())
    warm_ids = window_ok(warmup, math.ceil((20 if engineering else 120) * record["warmup_rate"]))
    ids = window_ok(measured, count)
    require(
        not warm_ids.intersection(ids)
        and record["measured_start_ns"]
        >= record["warmup_reconciled_ns"]
        >= record["warmup_end_ns"]
        >= record["warmup_start_ns"] + (20 if engineering else 120) * 1e9,
        "pilot-window-separation",
    )
    require(
        measured["rate"] == record["measured_rate"]
        and warmup["rate"] == record["warmup_rate"]
        and measured["concurrency"] == warmup["concurrency"] == CONCURRENCY,
        "pilot-window-rates",
    )
    for name in ("warmup-checker.json", "checker.json", "offline-checker.json"):
        report = json.loads((directory / name).read_bytes())
        require(
            report["valid"] is True
            and report["campaign"]["valid"] is True
            and not report["non_terminal"]
            and not report["campaign"]["non_terminal"],
            "pilot-checker-safety",
        )
    require(
        record["warmup_quiet_seconds"] >= 30
        and record["measured_quiet_seconds"] >= 30
        and record["reconciliation_timeout_seconds"] == 600,
        "pilot-reconciliation",
    )
    witness = json.loads((directory / "witness.json").read_bytes())
    busy = busy_times(witness, ids)
    trace = json.loads((directory / "snapshot.json").read_bytes())
    workflows = trace["Snapshot"]["engine.workflow_executions"]["rows"]
    terminal = {w["workflow_id"]: w for w in workflows}
    require(
        set(terminal) == ids | warm_ids
        and len(workflows) == len(terminal)
        and all(w["state"] == "SUCCEEDED" for w in workflows),
        "pilot-terminal-completeness",
    )
    samples = [json.loads(line) for line in (directory / "samples.jsonl").read_text().splitlines()]
    require(bool(samples), "pilot-samples-required")
    observed = {}
    previous = -1
    pod_ids = set(witness["expected_pods"])
    for sample in samples:
        require(
            sample["monotonic_ns"] > previous and sample["connections"] <= 92,
            "pilot-sample-time-budget",
        )
        previous = sample["monotonic_ns"]
        pods = sample["pods"]
        require(
            len(pods) == 2
            and {p["metadata"]["uid"] for p in pods} == pod_ids
            and sample["worker_deployment"]["spec"]["replicas"] == 2
            and sample["scaledobject"]["metadata"]["annotations"].get(
                "autoscaling.keda.sh/paused-replicas"
            )
            == "2",
            "pilot-sample-topology",
        )
        for pod in pods:
            containers = pod["spec"]["containers"]
            require(
                len(containers) == 1
                and containers[0]["resources"].get("limits")
                and containers[0]["resources"].get("requests")
                and next(e["value"] for e in containers[0]["env"] if e["name"] == "WORKER_SLOTS")
                == "2"
                and pod["status"]["containerStatuses"]
                and all(
                    c["ready"] is True and c["restartCount"] == 0
                    for c in pod["status"]["containerStatuses"]
                ),
                "pilot-sample-worker",
            )
        for workflow in sample["terminal_ids"]:
            observed.setdefault(workflow, sample["observed_at"])
    require(observed == record["terminal_observed_at"], "pilot-terminal-observation-source")
    latencies = []
    clock = json.loads((directory / "clock.json").read_bytes())
    require(set(clock) == {"setup", "measured"}, "pilot-clock-probes-required")
    widths = []
    for phase in ("setup", "measured"):
        before, after, database = (
            datetime.fromisoformat(clock[phase][key])
            for key in ("observer_before", "observer_after", "database")
        )
        require(after >= before, "pilot-observer-clock-order")
        require(before <= database <= after, "pilot-db-clock-bracket")
        widths.append((after - before).total_seconds())
    uncertainty = max(widths)
    history = trace["Snapshot"]["engine.transition_history"]["rows"]
    differences = []
    for row in measured["rows"]:
        workflow = row["workflow_id"]
        terminal_rows = [
            h
            for h in history
            if h["workflow_id"] == workflow
            and h["new_state"] == "SUCCEEDED"
            and h["revision"] == terminal[workflow]["revision"]
        ]
        require(len(terminal_rows) == 1, "pilot-terminal-history-unique")
        scheduled = datetime.fromisoformat(row["scheduled_at"].replace("Z", "+00:00"))
        durable = datetime.fromisoformat(terminal_rows[0]["created_at"])
        observed = datetime.fromisoformat(record["terminal_observed_at"][workflow])
        require(
            (scheduled - durable).total_seconds() <= uncertainty
            and (durable - observed).total_seconds() <= uncertainty,
            "pilot-terminal-clock-cross-check",
        )
        require(
            datetime.fromisoformat(clock["setup"]["observer_after"]) <= scheduled
            and durable <= datetime.fromisoformat(clock["measured"]["observer_before"]),
            "pilot-clock-probe-window",
        )
        latencies.append(
            finite(
                (durable - scheduled).total_seconds() + uncertainty,
                "pilot-terminal-latency",
                zero=True,
            )
        )
        differences.append((observed - durable).total_seconds())
    return {
        **reference(busy, count),
        "latencies": latencies,
        "count": count,
        "p99": nearest_rank(latencies, 0.99),
        "arm": record["arm"],
        "index": record["index"],
        "clock_uncertainty_seconds": uncertainty,
        "latency_source": "transition_history.created_at at final SUCCEEDED revision",
        "polling_minus_durable_seconds": {
            "max": max(differences),
            "p99": sorted(differences)[math.ceil(0.99 * len(differences)) - 1],
        },
        "maximum_sample_gap_seconds": max(
            (b["monotonic_ns"] - a["monotonic_ns"]) / 1e9
            for a, b in zip(samples, samples[1:], strict=False)
        )
        if len(samples) > 1
        else None,
    }


def initial_dependency(directory: Path, arm: str, repository: Path | None = None) -> dict[str, Any]:
    manifest = read_artifact(directory, repository)
    require(
        manifest["status"] in {"PASS", "CARRY_OVER"} and manifest["cleanup"]["status"] == "PASS",
        "pilot-dependency-accepted-job",
    )
    relative = f"{arm}-0"
    require(relative in manifest["completed"], "pilot-dependency-initial-missing")
    result = summarize(directory / relative)
    require(result["arm"] == arm and result["index"] == 0, "pilot-dependency-cell")
    record = json.loads((directory / relative / "record.json").read_bytes())
    require(record["b"] == result["b"] and record["r"] == result["r"], "pilot-dependency-br")
    return {
        "artifact": directory.as_posix(),
        "manifest_sha256": hashlib.sha256((directory / "manifest.json").read_bytes()).hexdigest(),
        **result,
    }
