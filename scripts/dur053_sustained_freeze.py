"""Apply B.6 only after the complete original pilot inventory passes its checks."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from dur053_k3_policy import committed_ledger, require
from dur053_payload import materialize
from dur053_pilot import EVIDENCE
from dur053_sustained import (
    PARTITION_LIMITATION,
    ROOTS,
    choose,
    harness_capacity_failure,
    harness_deadline_failure,
    operating_record,
    registration,
    retained,
    stationarity,
)


def finish(repository: Path, checker: Path, result: dict[str, Any]) -> dict[str, Any]:
    from dur053_schedule import plan_b6

    offline: dict[str, dict[int, dict]] = {arm: {} for arm in ("medium", "long")}
    for path in (repository / EVIDENCE).glob("*/*/manifest.json"):
        with materialize(path.parent, repository) as root:
            manifest = json.loads((root / "manifest.json").read_bytes())
            for arm in offline:
                for i in (1, 2, 3):
                    if f"{arm}-{i}" in manifest["completed"]:
                        offline[arm][i] = stationarity(root / f"{arm}-{i}", offline=True)
    for arm, gates in offline.items():
        require(set(gates) == {1, 2, 3}, "b6-offline-all-repeats")
        result["arms"][arm]["offline_stationarity_disclosure"] = gates
    ledger = committed_ledger(
        repository / "experiments/portfolio/k8s-recovery/live-time-ledger.json", repository
    )
    version_manifests = {}
    capacity_failures = []
    capacity_counts: dict[str, int] = {}
    deadline_failures = []
    deadline_counts: dict[str, int] = {}
    for _version, evidence in ROOTS.items():
        for path in (repository / evidence).glob("*/*/manifest.json"):
            with materialize(path.parent, repository) as root:
                manifest = json.loads((root / "manifest.json").read_bytes())
                if harness_capacity_failure(root, manifest):
                    capacity_failures.append(path.relative_to(repository).as_posix())
                    name = manifest["job"]
                    capacity_counts[name] = capacity_counts.get(name, 0) + 1
                    require(capacity_counts[name] <= 1, "b62-capacity-replay-limit")
                elif harness_deadline_failure(root, manifest):
                    deadline_failures.append(path.relative_to(repository).as_posix())
                    name = manifest["job"]
                    deadline_counts[name] = deadline_counts.get(name, 0) + 1
                name = manifest["job"]
                require(
                    capacity_counts.get(name, 0) + deadline_counts.get(name, 0) <= 1,
                    "b63-harness-replay-limit",
                )
            relative = path.relative_to(repository).as_posix()
            require(
                any(
                    row["artifact"] == relative
                    and row["job_id"] == manifest["job_id"]
                    and row["category"] == manifest["job"]
                    for row in ledger["entries"]
                ),
                "b6-freeze-ledger-entry",
            )
            version_manifests[relative] = manifest
            result["source_artifacts"][relative] = hashlib.sha256(path.read_bytes()).hexdigest()
    for row in ledger["entries"]:
        if any(row["artifact"].startswith(root.as_posix() + "/") for root in ROOTS.values()):
            require(row["artifact"] in version_manifests, "b6-freeze-ledger-inventory")
    result["unperformed_stationary_arms"] = []
    result["excluded_harness_capacity_failures"] = capacity_failures
    result["excluded_harness_deadline_failures"] = deadline_failures
    replaced_arms: list[str] = []
    # B.6.1: long replacement depends only on the offline engine-path criterion.
    for arm in ("short", "cpu", "long"):
        if arm == "long" and all(g["pass"] for g in offline["long"].values()):
            continue
        replaced_arms.append(arm)
        reg = registration(repository, arm)
        versions = {v: retained(repository, arm, v, checker) for v in (2, 3)}
        require(bool(versions[2]), "b6-v1-L-excluded")
        gates = {
            v: [row["gate"] for _, row in sorted(cells.items())]
            for v, cells in versions.items()
            if cells
        }
        budget_refusals = [
            m
            for m in version_manifests.values()
            if m.get("version") == 3
            and m.get("planned", [[None]])[0][0] == arm
            and m.get("error") in {"K3 guard: ledger-budget", "K3 guard: pilot-job-fit"}
            and m.get("cleanup", {}).get("no_resources_started") is True
        ]
        # A retained budget refusal can terminate an incomplete carry-over v3.
        # It never supplies a passing version or an L population.
        budget_stopped = bool(budget_refusals) and any(
            not row["gate"]["pass"] for row in versions[2].values()
        )
        chosen = None if budget_stopped else choose(gates)
        previous = result["arms"][arm]
        replacement: dict[str, Any] = {
            "b0": previous["b"],
            "r0": previous["r"],
            "registration": reg,
            "overload_calibration_v1_repeats_excluded_from_L": previous["repeats"],
            "versions": versions,
            "passing_version": chosen,
        }
        if chosen is None:
            if not versions[3]:
                require(budget_stopped, "b6-v3-required-or-budget-refusal")
            replacement.update(
                stationary_comparisons="UNPERFORMED",
                L=None,
                r=None,
                reason="v3 stationarity failed or budget refused",
            )
            result["unperformed_stationary_arms"].append(arm)
        else:
            import math

            from dur053_pilot import nearest_rank

            repeats = versions[chosen]
            rate = operating_record(reg, chosen)
            replacement.update(
                **rate,
                b=previous["b"],
                L=math.ceil(
                    1000 * 2 * max(nearest_rank(row["latencies"], 0.99) for row in repeats.values())
                )
                / 1000,
                repeats=list(repeats.values()),
                max_repeat_b=max(row["b"] for row in repeats.values()),
                stationary_comparisons="PERFORMED",
                arrival_concurrency_cap=16,
            )
        result["arms"][arm] = replacement
    removed_disclosures = {arm: set() for arm in replaced_arms}
    retained_limitations = []
    for limitation in result["limitations"]:
        removed = False
        for arm in replaced_arms:
            if limitation.startswith(f"{arm}: L for this arm is derived under engine-path overload. "):
                removed_disclosures[arm].add("engine_path")
                removed = True
            elif limitation.startswith(
                f"{arm}: L was measured under overload; the scheduling reference does not "
            ):
                removed_disclosures[arm].add("busy_time")
                removed = True
        if not removed:
            retained_limitations.append(limitation)
    result["limitations"] = retained_limitations
    for arm in replaced_arms:
        passing_version = result["arms"][arm]["passing_version"]
        disclosure = f"{arm}: v1 repeats are excluded from L under Amendment B.6"
        if "engine_path" in removed_disclosures[arm]:
            disclosure = (
                f"{arm}: v1 repeats showed engine-path saturation and are excluded from L "
                "under Amendment B.6"
            )
        if "busy_time" in removed_disclosures[arm]:
            disclosure += (
                "; the R251 busy-time sensitivity remains disclosed in the v1 busy times in "
                "overload_calibration_v1_repeats_excluded_from_L"
            )
        if passing_version is None:
            disclosure += "; no stationary L was frozen."
        else:
            disclosure += (
                f"; frozen r and L come from the v{passing_version} repeats at q, which passed "
                "the B.6 stationarity gate."
            )
        result["limitations"].append(disclosure)
    result["schema"] = "dur053.k3-freeze.b6"
    result["versioned_environment_artifacts"] = {
        path: {
            name: digest
            for name, digest in manifest["artifact_sha256"].items()
            if name in {"images.json", "versions.json"} or name.endswith("samples.jsonl")
        }
        for path, manifest in version_manifests.items()
    }
    result["limitations"].append(PARTITION_LIMITATION)
    result["observed_schedule"] = plan_b6(repository, result["arms"])
    result["unperformed_cells"] = [
        unit
        for unit in result["observed_schedule"]["units"]
        if unit["arm"] in result["unperformed_stationary_arms"]
    ]
    return result
