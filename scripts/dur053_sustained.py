"""Amendment B.6: committed rate registration and independent stationarity gate."""

from __future__ import annotations

import hashlib
import json
import math
import statistics
from datetime import datetime
from pathlib import Path
from typing import Any

from dur053_k3_policy import require
from dur053_payload import materialize
from dur053_pilot import (
    CONCURRENCY,
    EVIDENCE,
    busy_times,
    committed_bytes,
    finite,
    nearest_rank,
    reference,
    window_ok,
)
from dur053_saturation import engine_path_metrics, timestamp

REGISTERED = {
    "short": ("14.272201", "7.136101"),
    "cpu": ("1.738647", "0.869323"),
}
ROOTS = {v: EVIDENCE.with_name(f"k3-pilot-v{v}") for v in (2, 3)}
JOBS = {
    "k3-pilot-4a": ("short", 2),
    "k3-pilot-4b": ("cpu", 2),
    "k3-pilot-5a": ("short", 3),
    "k3-pilot-5b": ("cpu", 3),
    **{f"k3-pilot-{n}c{i}": ("long", v) for n, v in ((4, 2), (5, 3)) for i in (1, 2, 3)},
}
PARTITION_LIMITATION = (
    "Each slot is a separate Kafka consumer and pauses all its assigned partitions while busy. "
    "This causes head-of-line queueing even when other slots are idle. In medium-1, the "
    "reviewed decomposition reports about 20.9 s waiting before 5.0 s execution, p99 26.0 s "
    "at implied utilisation 0.50, versus p99 5.6 s at 0.20. Medium L and K5 comparisons "
    "inherit this queueing. No engine change during DUR-053; "
    "claim fencing and safety are unchanged."
)


def version_rate(registration: dict[str, Any], version: int) -> float:
    require(type(version) is int and version in (2, 3), "b6-version-limit")
    return registration["q"] / (2 if version == 3 else 1)


def operating_record(registration: dict[str, Any], version: int) -> dict[str, Any]:
    q = version_rate(registration, version)
    r = 2 * q
    values = {"X_min": registration["X_min"], "q": q, "r": r}
    return {
        **values,
        "repr": {k: repr(v) for k, v in values.items()},
        "six_decimal_rounded": {k: format(v, ".6f") for k, v in values.items()},
        "source": registration["source"],
        "registered_check_values": registration["six_decimal_rounded"],
    }


def precision(x: float, source: dict[str, Any]) -> dict[str, Any]:
    q = x / 2
    r = 2 * q
    return {
        "X_min": x,
        "q": q,
        "r": r,
        "repr": {"X_min": repr(x), "q": repr(q), "r": repr(r)},
        "six_decimal_rounded": {
            "X_min": format(x, ".6f"),
            "q": format(q, ".6f"),
            "r": format(r, ".6f"),
        },
        "source": source,
    }


def registration(repository: Path, arm: str) -> dict[str, Any]:
    """No metrics cache: materialize committed v1 bytes before deriving the minimum."""
    require(arm in {"short", "cpu", "long"}, "b6-registration-arm")
    values = {}
    for manifest_path in sorted((repository / EVIDENCE).glob("*/*/manifest.json")):
        header = json.loads(committed_bytes(repository, manifest_path))
        if not any(f"{arm}-{i}" in header["completed"] for i in (1, 2, 3)):
            continue
        with materialize(manifest_path.parent, repository) as root:
            manifest = json.loads((root / "manifest.json").read_bytes())
            if not any(f"{arm}-{i}" in manifest["completed"] for i in (1, 2, 3)):
                continue
            require(
                manifest["status"] in {"PASS", "CARRY_OVER"}
                and manifest["cleanup"]["status"] == "PASS",
                "b6-v1-accepted",
            )
            for i in (1, 2, 3):
                cell = f"{arm}-{i}"
                if cell not in manifest["completed"]:
                    continue
                require(i not in values, "b6-v1-duplicate")
                x = finite(
                    engine_path_metrics(root / cell)["completion_throughput_first_to_last"],
                    "b6-v1-throughput",
                )
                values[i] = (
                    x,
                    {
                        "run_id": manifest["run_id"],
                        "cell": cell,
                        "artifact": manifest_path.relative_to(repository).as_posix(),
                    },
                )
    require(set(values) == {1, 2, 3}, "b6-v1-repeats")
    x, source = min(values.values(), key=lambda pair: pair[0])
    result = precision(x, source)
    if arm in REGISTERED:
        require((format(x, ".6f"), format(x / 2, ".6f")) == REGISTERED[arm], "b6-rate-registration")
    return result


def duration(q: float) -> float:
    return max(600.0, 200 / finite(q, "b6-rate"))


def reservation(q: float, *, cpu: bool = False, repeats: int = 3) -> int:
    # Setup/cleanup, each fresh episode's undrained window, full reconciliation,
    # reset/export, and the fixed-unit calibration's 22 bounded invocations.
    return math.ceil(
        300 + 600 + (22 * 120 if cpu else 0) + repeats * (120 + duration(q) + 600 + 60)
    )


def terminal_series(directory: Path) -> tuple[list[dict], list[float]]:
    rows = json.loads((directory / "measured-arrivals.json").read_bytes())["rows"]
    rows = sorted(rows, key=lambda row: (timestamp(row["scheduled_at"]), row["workflow_id"]))
    snapshot = json.loads((directory / "snapshot.json").read_bytes())["Snapshot"]
    workflows = {r["workflow_id"]: r for r in snapshot["engine.workflow_executions"]["rows"]}
    history = snapshot["engine.transition_history"]["rows"]
    terminals = []
    for row in rows:
        key = row["workflow_id"]
        matches = [
            h
            for h in history
            if h["workflow_id"] == key
            and h["new_state"] == "SUCCEEDED"
            and h["revision"] == workflows[key]["revision"]
        ]
        require(len(matches) == 1, "b6-terminal-history")
        terminals.append(timestamp(matches[0]["created_at"]))
    require(len(rows) >= 6, "b6-gate-population")
    return rows, terminals


def stationarity(directory: Path, *, offline: bool = False) -> dict[str, Any]:
    """Pure verdict from persisted evidence; never trusts a persisted gate result."""
    record = json.loads((directory / "record.json").read_bytes())
    rows, terminals = terminal_series(directory)
    if offline:
        start = timestamp(rows[0]["scheduled_at"])
        T = len(rows) / record["measured_rate"]
    else:
        start = timestamp(record["measurement_start_at"])
        T = finite(record["measurement_seconds"], "b6-gate-window")
    scheduled = [timestamp(row["scheduled_at"]) for row in rows]
    require(all(start <= t < start + T for t in scheduled), "b6-gate-schedule")
    bins = []
    for i in range(6):
        lo, hi = start + T * i / 6, start + T * (i + 1) / 6
        arrivals = sum(lo <= t < hi for t in scheduled)
        completions = sum(lo <= t < hi for t in terminals)
        bins.append({"arrivals": arrivals, "completions": completions})
    pooled = {k: sum(b[k] for b in bins[1:]) for k in ("arrivals", "completions")}
    a = pooled["completions"] >= 0.95 * pooled["arrivals"]
    latencies = [end - begin for begin, end in zip(scheduled, terminals, strict=True)]
    thirds = [latencies[i * len(rows) // 3 : (i + 1) * len(rows) // 3] for i in range(3)]
    first = [statistics.median(thirds[0]), nearest_rank(thirds[0], 0.9)]
    last = [statistics.median(thirds[-1]), nearest_rank(thirds[-1], 0.9)]
    b = all(y <= max(1.25 * x, x + 0.5) for x, y in zip(first, last, strict=True))
    evidence: dict[str, Any] = {
        "bins": bins,
        "pooled_bins_2_to_6": pooled,
        "first_third_median_p90": first,
        "last_third_median_p90": last,
    }
    if offline:
        metrics = engine_path_metrics(directory)
        throughput = metrics["completion_throughput_first_to_last"]
        measured_rate = metrics["offered_rate"]  # measured-arrivals.json rate, not warmup
        backlog = metrics["warmup_outbox_unpublished_at_start"]
        c = backlog == 0
        evidence.update(
            completion_throughput_first_to_last=throughput,
            measured_rate=measured_rate,
            warmup_outbox_unpublished_at_start=backlog,
            disclosure_checks={"b6-throughput": a, "b6-latency-drift": b},
        )
        checks = {
            "b6-offline-throughput": throughput >= 0.95 * measured_rate,
            "b6-engine-path": c,
        }
        return {"pass": all(checks.values()), "checks": checks, "offline": True, **evidence}
    else:
        samples = [
            json.loads(line)
            for line in (directory / "engine-samples.jsonl").read_bytes().splitlines()
        ]
        ends = [
            [
                s
                for s in samples
                if start + T * i / 6 <= timestamp(s["observed_at"]) < start + T * (i + 1) / 6
            ]
            for i in (0, 5)
        ]
        require(all(ends), "b6-gate-samples")
        means = [
            {
                k: statistics.mean(finite(s[k], "b6-sample-count", zero=True) for s in group)
                for k in ("outstanding", "unpublished")
            }
            for group in ends
        ]
        c = (
            means[1]["outstanding"] <= means[0]["outstanding"] + 4
            and means[1]["unpublished"] <= means[0]["unpublished"] + 20
        )
        evidence["sample_means"] = means
    checks = {"b6-throughput": a, "b6-latency-drift": b, "b6-engine-path": c}
    return {"pass": all(checks.values()), "checks": checks, "offline": offline, **evidence}


def require_stationary(gate: dict[str, Any]) -> None:
    for name, passed in gate["checks"].items():
        require(passed, name)


def choose(versions: dict[int, list[dict[str, Any]]]) -> int | None:
    require(set(versions) <= {2, 3}, "b6-version-limit")
    require(2 in versions, "b6-v1-L-excluded")
    require(all(len(repeats) == 3 for repeats in versions.values()), "b6-three-repeats")
    passing = all(g["pass"] for g in versions[2])
    require(not (passing and 3 in versions), "b6-first-passing-version")
    if passing:
        return 2
    if 3 in versions and all(g["pass"] for g in versions[3]):
        return 3
    return None


def job_spec(name: str) -> tuple[str, int, list[int]]:
    base = name.split("-c")[0]
    require(base in JOBS, "b6-job-name")
    arm, version = JOBS[base]
    return arm, version, [int(base[-1])] if arm == "long" else [1, 2, 3]


def evidence_root(name: str) -> Path:
    return ROOTS[job_spec(name)[1]]


def harness_capacity_failure(root: Path, manifest: dict[str, Any]) -> bool:
    """B.6.2 content rule on materialized, hash-verified evidence, never metrics.

    Callers must enter materialize(directory, repository) first. In particular,
    a restart alone selects the live guard but does not authorize a replay.
    """
    name = manifest.get("job", "")
    if (
        name.split("-c")[0] not in JOBS
        or manifest.get("version") not in (2, 3)
        or manifest.get("status") != "FAIL"
        or manifest.get("cleanup", {}).get("status") != "PASS"
    ):
        return False
    error = manifest.get("error")
    legacy = name == "k3-pilot-4a" and manifest.get("run_id") == "43159046254"
    if error != "K3 guard: pilot-checker-capacity" and not (
        legacy and error == "K3 guard: pilot-live-checker"
    ):
        return False
    failing = [r for r in manifest.get("unperformed", []) if r.get("started") is True]
    if len(failing) != 1:
        return False
    cell = root / f"{failing[0]['arm']}-{failing[0]['index']}"
    verdict = cell / "checker.json"
    if not verdict.is_file() or verdict.read_bytes() != b"":
        return False
    # The old run has the all-namespace diagnostic. New guards also retain the
    # exact observer pod JSON at failure, before cleanup can erase lastState.
    for path in (cell / "observer-after.json", root / "diagnostics/pods-json.txt"):
        if not path.is_file():
            continue
        try:
            pods = json.loads(path.read_bytes())["items"]
        except (ValueError, KeyError, TypeError):
            continue
        if any(
            p.get("metadata", {}).get("labels", {}).get("app") == "observer"
            and p.get("metadata", {}).get("namespace") == "dur053-kind"
            and any(
                c.get("name") == "observer"
                and c.get("lastState", {}).get("terminated", {}).get("reason") == "OOMKilled"
                for c in p.get("status", {}).get("containerStatuses", [])
            )
            for p in pods
        ):
            return True
    return False


def retained(
    repository: Path, arm: str, version: int, checker: Path | None = None
) -> dict[int, dict]:
    """Committed, retained, independently rederived versions; used before v3 and freeze."""
    from dur053_pilot_evidence import pre_start_refusal, recheck

    result = {}
    capacity_counts: dict[str, int] = {}
    for path in sorted((repository / ROOTS[version]).glob("*/*/manifest.json")):
        with materialize(path.parent, repository) as root:
            manifest = json.loads((root / "manifest.json").read_bytes())
            if job_spec(manifest["job"])[:2] != (arm, version):
                continue
            if harness_capacity_failure(root, manifest):
                name = manifest["job"]
                capacity_counts[name] = capacity_counts.get(name, 0) + 1
                require(capacity_counts[name] <= 1, "b62-capacity-replay-limit")
                continue
            if pre_start_refusal(root, manifest):
                continue
            require(
                manifest["status"] in {"PASS", "CARRY_OVER"}
                and manifest["cleanup"]["status"] == "PASS"
                and manifest["budget_scope"] == "registered-campaign"
                and manifest["measurement_claim"] is True,
                "b6-retained-job",
            )
            require(
                manifest["ci_proof"]["run_id"].isdigit()
                and manifest["artifact_sha256"].get("images.json")
                and manifest["artifact_sha256"].get("versions.json"),
                "b6-retained-provenance",
            )
            if checker is not None:
                recheck(root, checker)
            expected_registration = registration(repository, arm)
            if arm == "cpu":
                verify_drift(root)
            for cell in manifest["completed"]:
                directory = root / cell
                record = json.loads((directory / "record.json").read_bytes())
                index = record["index"]
                require(
                    record["arm"] == arm
                    and record["version"] == version
                    and index in (1, 2, 3)
                    and cell == f"{arm}-{index}"
                    and record["registration"] == expected_registration
                    and record["measured_rate"] == version_rate(expected_registration, version),
                    "b6-retained-registration",
                )
                require(
                    record["rate_precision"] == operating_record(expected_registration, version),
                    "b6-rate-record",
                )
                if arm == "cpu":
                    drift = root / "cpu-drift.json"
                    require(
                        record["cpu_units"] == 1000000
                        and record["cpu_drift_sha256"]
                        == hashlib.sha256(drift.read_bytes()).hexdigest()
                        and record["cpu_drift_persisted_ns"]
                        == json.loads(drift.read_bytes())["persisted_ns"]
                        <= record["warmup_start_ns"],
                        "b6-drift-before-repeat",
                    )
                summary = summarize(directory)
                gate = stationarity(directory)
                require(record["stationarity_gate"] == gate, "b6-gate-record")
                require(index not in result, "b6-duplicate-repeat")
                result[index] = {
                    **summary,
                    "gate": gate,
                    "artifact": path.relative_to(repository).as_posix(),
                    "registration": expected_registration,
                }
    return result


def fallback_required(repository: Path, arm: str) -> dict[int, dict]:
    repeats = retained(repository, arm, 2)
    require(set(repeats) == {1, 2, 3}, "b6-v3-complete-v2")
    require(
        any(not row["gate"]["pass"] for row in repeats.values()),
        "b6-v3-retained-v2-failure",
    )
    return repeats


def verify_drift(root: Path) -> dict[str, Any]:
    from dur053_pilot import busy_times

    record = json.loads((root / "cpu-drift.json").read_bytes())
    require(
        record["status"] == "PASS"
        and record["units"] == 1000000
        and record["reference_median_seconds"] == 0.514
        and record["discarded"] == 1
        and record["concurrency"] == 1
        and record["timeout_seconds"] == 120
        and len(record["invocation_elapsed_seconds"]) == 22
        and all(0 < t <= 120 for t in record["invocation_elapsed_seconds"]),
        "b6-drift-procedure",
    )
    values = []
    for i in range(22):
        directory = root / "cpu-drift"
        arrivals = json.loads((directory / f"invocation-{i}-arrivals.json").read_bytes())
        require(
            arrivals["offered"] == arrivals["concurrency"] == 1
            and arrivals["cpu_units"] == 1000000
            and arrivals["status"] == "PASS",
            "b6-drift-units",
        )
        report = json.loads((directory / f"invocation-{i}-checker.json").read_bytes())
        require(report["valid"] is True, "b6-drift-checker")
        witness = json.loads((directory / f"invocation-{i}-witness.json").read_bytes())
        busy = busy_times(witness, {arrivals["rows"][0]["workflow_id"]})
        require(len(busy) == 1 and busy[0] < 120, "b6-drift-single")
        values.extend(busy)
    require(
        record["discarded_busy_seconds"] == values[0]
        and record["measured_busy_seconds"] == values[1:]
        and record["median_busy_seconds"] == nearest_rank(values[1:], 0.5)
        and record["image"] == json.loads((root / "images.json").read_bytes())["worker"],
        "b6-drift-evidence",
    )
    return record


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
    count = record["measured_count"]
    warmup = json.loads((directory / "warmup-arrivals.json").read_bytes())
    measured = json.loads((directory / "measured-arrivals.json").read_bytes())
    warm_ids = window_ok(warmup, record["warmup_count"])
    ids = window_ok(measured, count)
    continuous = json.loads((directory / "continuous-arrivals.json").read_bytes())
    require(continuous["rows"] == warmup["rows"] + measured["rows"], "b6-continuous-population")
    window_ok(continuous, count + record["warmup_count"])
    q = record["measured_rate"]
    warm_seconds = 6 if engineering else 120
    T = 60 if engineering else duration(q)
    require(
        record["version"] in (2, 3)
        and record["measurement_seconds"] == T
        and record["warmup_rate"] == q
        and count + record["warmup_count"] == math.ceil((warm_seconds + T) * q),
        "b6-window-procedure",
    )
    origin = timestamp(continuous["rows"][0]["scheduled_at"])
    require(
        timestamp(record["measurement_start_at"]) == origin + warm_seconds
        and all(timestamp(r["scheduled_at"]) < origin + warm_seconds for r in warmup["rows"])
        and all(timestamp(r["scheduled_at"]) >= origin + warm_seconds for r in measured["rows"])
        and not warm_ids.intersection(ids),
        "b6-undrained-window",
    )
    require(
        measured["rate"] == record["measured_rate"]
        and warmup["rate"] == record["warmup_rate"]
        and measured["concurrency"] == warmup["concurrency"] == CONCURRENCY,
        "pilot-window-rates",
    )
    for name in ("checker.json", "offline-checker.json"):
        report = json.loads((directory / name).read_bytes())
        require(
            report["valid"] is True
            and report["campaign"]["valid"] is True
            and not report["non_terminal"]
            and not report["campaign"]["non_terminal"],
            "pilot-checker-safety",
        )
    require(
        record["measured_quiet_seconds"] >= 30 and record["reconciliation_timeout_seconds"] == 600,
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
        "slot_reference_r": reference(busy, count)["r"],
        "r": record["rate_precision"]["r"],
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
