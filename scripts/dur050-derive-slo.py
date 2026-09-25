"""Derive the DUR-050 pilot SLO from immutable unloaded calibration rows."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
from pathlib import Path
from statistics import median
from typing import Any

MINIMUM_VALID_SAMPLES_PER_FAMILY = 300
FORMULA_VERSION = "dur050-pilot-slo.v1"
SOURCE_BLOCK_FILES = (
    "unloaded-latency.csv",
    "unloaded-observer-polls.csv",
    "submission-rows.csv",
    "loadgen-summaries.jsonl",
)
MERGED_VIEW_FILES = SOURCE_BLOCK_FILES[:3]


def nearest_rank(values: list[float], percentile: float) -> float:
    if not values:
        raise ValueError("nearest-rank percentile requires at least one value")
    if not 0 < percentile <= 1:
        raise ValueError("percentile must be in (0, 1]")
    ordered = sorted(values)
    rank = math.ceil(percentile * len(ordered))
    return ordered[rank - 1]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_csv_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames is None:
            raise ValueError(f"CSV has no header: {path}")
        rows: list[dict[str, str]] = []
        for row in reader:
            if None in row:
                raise ValueError(f"{path}:{reader.line_num}: CSV row width mismatch: extra fields")
            missing = [name for name, value in row.items() if value is None]
            if missing:
                raise ValueError(
                    f"{path}:{reader.line_num}: CSV row width mismatch: missing fields {missing}"
                )
            rows.append(row)
        return list(reader.fieldnames), rows


def verify_source_blocks(calibration_dir: Path) -> dict[str, Any]:
    assembly_path = calibration_dir / "assembly.json"
    if not assembly_path.is_file():
        raise FileNotFoundError("required pilot input is missing: assembly.json")
    assembly = json.loads(assembly_path.read_text(encoding="utf-8"))
    if assembly.get("schema") != "dur050-pilot-calibration-assembly.v1":
        raise ValueError("unsupported or missing pilot calibration assembly schema")
    blocks = assembly.get("blocks")
    if not isinstance(blocks, list) or len(blocks) != 6 or assembly.get("block_count") != 6:
        raise ValueError("assembly must describe exactly six source blocks")

    family_counts: dict[str, int] = {}
    seen_directories: set[str] = set()
    merged_rows: dict[str, tuple[list[str], list[dict[str, str]]]] = {}
    verified_source_hashes: dict[str, str] = {}
    total_latency_rows = 0
    total_submission_rows = 0
    for expected_index, block in enumerate(blocks, start=1):
        if not isinstance(block, dict) or block.get("index") != expected_index:
            raise ValueError("assembly source block indexes must be ordered from 1 through 6")
        source_name = block.get("source_block_name", "")
        if (
            not source_name
            or Path(source_name).name != source_name
            or source_name in seen_directories
        ):
            raise ValueError("assembly contains an unsafe or repeated source block name")
        seen_directories.add(source_name)
        family = block.get("family", "")
        if family not in {"seq-8", "fanout-8"}:
            raise ValueError(f"assembly contains an unsupported family: {family!r}")
        family_counts[family] = family_counts.get(family, 0) + 1
        source_dir = calibration_dir / "source-blocks" / f"{expected_index:02d}-{source_name}"
        recorded_hashes = block.get("source_sha256")
        if not isinstance(recorded_hashes, dict) or set(recorded_hashes) != set(SOURCE_BLOCK_FILES):
            raise ValueError(f"assembly has an incomplete source hash map for {source_name}")

        block_rows: dict[str, tuple[list[str], list[dict[str, str]]]] = {}
        for filename in SOURCE_BLOCK_FILES:
            path = source_dir / filename
            actual_hash = sha256_file(path)
            if actual_hash != recorded_hashes[filename]:
                raise ValueError(f"source block hash mismatch for {source_dir.name}/{filename}")
            verified_source_hashes[f"source-blocks/{source_dir.name}/{filename}"] = actual_hash
            if filename in MERGED_VIEW_FILES:
                fields, rows = read_csv_rows(path)
                block_rows[filename] = (fields, rows)

        latency_rows = block_rows["unloaded-latency.csv"][1]
        submission_rows = block_rows["submission-rows.csv"][1]
        if len(latency_rows) != block.get("latency_rows") or len(submission_rows) != block.get(
            "submission_rows"
        ):
            raise ValueError(f"assembly row counts do not match source block {source_name}")
        total_latency_rows += len(latency_rows)
        total_submission_rows += len(submission_rows)
        if {row.get("family", "") for row in latency_rows} != {family}:
            raise ValueError(f"source block family does not match assembly for {source_name}")
        latency_ids = {row.get("workflow_id", "") for row in latency_rows}
        submission_ids = {row.get("workflow_id", "") for row in submission_rows}
        if not latency_ids or latency_ids != submission_ids:
            raise ValueError(f"source block submission/latency IDs differ for {source_name}")

        for filename, (fields, rows) in block_rows.items():
            if filename not in merged_rows:
                merged_rows[filename] = (fields, [])
            current_fields, all_rows = merged_rows[filename]
            if current_fields != fields:
                raise ValueError(f"source block CSV headers differ for {filename}")
            all_rows.extend(rows)

    if family_counts != {"seq-8": 3, "fanout-8": 3}:
        raise ValueError(f"assembly needs three source blocks per family, found {family_counts}")
    if (
        assembly.get("latency_rows") != total_latency_rows
        or assembly.get("submission_rows") != total_submission_rows
    ):
        raise ValueError("assembly totals do not match the verified source block rows")

    merged_hashes: dict[str, str] = {}
    for filename in MERGED_VIEW_FILES:
        fields, rows = merged_rows[filename]
        rebuilt = io.StringIO(newline="")
        writer = csv.DictWriter(rebuilt, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
        rebuilt_bytes = rebuilt.getvalue().encode("utf-8")
        merged_path = calibration_dir / filename
        if merged_path.read_bytes() != rebuilt_bytes:
            raise ValueError(
                f"merged calibration view {filename} differs from its source-block reconstruction"
            )
        merged_hashes[filename] = sha256_file(merged_path)

    return {
        "assembly_sha256": sha256_file(assembly_path),
        "verified_source_block_count": len(blocks),
        "verified_source_files_sha256": verified_source_hashes,
        "verified_merged_views_sha256": merged_hashes,
    }


def observer_sampling_metrics(
    path: Path,
) -> tuple[dict[str, Any], dict[str, list[tuple[int, int]]], dict[str, dict[str, Any]]]:
    intervals_ms: list[float] = []
    qps_values: list[float] = []
    reported_gaps_ms: list[float] = []
    observations: dict[str, list[tuple[int, int]]] = {}
    terminal_observations: dict[str, dict[str, Any]] = {}
    poll_error_rows = 0
    reported_failed_queries = 0
    summary_failure_counts: list[int] = []
    query_rows = 0
    _, rows = read_csv_rows(path)
    for row in rows:
        if row.get("record_type") == "poll_error":
            poll_error_rows += 1
        elif row.get("record_type") == "snapshot":
            workflow_id = row.get("workflow_id", "")
            if not workflow_id:
                raise ValueError("observer snapshot is missing its workflow ID")
            try:
                sequence = int(row["sequence"])
                observed_ns = int(row["observed_at_monotonic_ns"])
            except (KeyError, TypeError, ValueError) as error:
                raise ValueError(
                    "observer snapshot lacks monotonic sequence/timestamp"
                ) from error
            observations.setdefault(workflow_id, []).append((sequence, observed_ns))
            query_rows += 1
        elif row.get("record_type") == "first_terminal_observation":
            workflow_id = row.get("workflow_id", "")
            if not workflow_id or workflow_id in terminal_observations:
                raise ValueError("observer terminal row has a missing or duplicate workflow ID")
            try:
                terminal_observations[workflow_id] = {
                    "sequence": int(row["sequence"]),
                    "observed_at_monotonic_ns": int(row["observed_at_monotonic_ns"]),
                    "state": row["state"],
                    "valid": row["valid"].lower() == "true",
                }
            except (KeyError, TypeError, ValueError) as error:
                raise ValueError(
                    "observer terminal row lacks state/timestamp/validity"
                ) from error
        elif row.get("record_type") == "summary":
            try:
                qps_values.append(float(row["observer_qps"]))
                reported_gaps_ms.append(float(row["max_poll_gap_ms"]))
                summary_failure_counts.append(
                    int(row["reason"].split("failed_queries=", 1)[1].split()[0])
                )
            except (KeyError, IndexError, ValueError) as error:
                raise ValueError("observer summary lacks QPS, gap, or failure count") from error

    for workflow_id, samples in observations.items():
        ordered = sorted(samples)
        if len({sequence for sequence, _ in ordered}) != len(ordered):
            raise ValueError(f"observer sequence is duplicated for {workflow_id}")
        for (_, previous_ns), (_, current_ns) in zip(ordered, ordered[1:], strict=False):
            delta_ns = current_ns - previous_ns
            if delta_ns <= 0:
                raise ValueError(
                    f"observer monotonic timestamps did not increase for {workflow_id}"
                )
            intervals_ms.append(delta_ns / 1_000_000)

    if query_rows == 0 or not observations or not qps_values:
        raise ValueError("observer calibration has no snapshot rows or run summaries")
    reported_failed_queries = sum(summary_failure_counts)
    if poll_error_rows != reported_failed_queries:
        raise ValueError("observer poll-error rows disagree with run-summary failed-query counts")
    if reported_failed_queries:
        raise ValueError(f"observer calibration contains {reported_failed_queries} failed queries")
    if not intervals_ms:
        raise ValueError("observer calibration has no repeated polls to measure resolution")
    if len(qps_values) != len(observations):
        raise ValueError("observer run summaries do not match the observed workflow count")
    for workflow_id, samples in observations.items():
        sequences = [sequence for sequence, _ in sorted(samples)]
        if len(sequences) < 2 or sequences != list(range(1, len(sequences) + 1)):
            raise ValueError(f"observer sequence is incomplete for {workflow_id}")
        terminal = terminal_observations.get(workflow_id)
        if terminal is None or terminal["sequence"] != sequences[-1]:
            raise ValueError(
                f"observer terminal record is missing or inconsistent for {workflow_id}"
            )
        if terminal["observed_at_monotonic_ns"] != sorted(samples)[-1][1]:
            raise ValueError(
                f"observer terminal timestamp differs from its final snapshot for {workflow_id}"
            )

    return (
        {
            "target_resolution_ms": 50,
            "actual_observer_query_rows": query_rows,
            "observer_runs": len(qps_values),
            "query_interval_p50_ms_nearest_rank": nearest_rank(intervals_ms, 0.50),
            "query_interval_p95_ms_nearest_rank": nearest_rank(intervals_ms, 0.95),
            "query_interval_max_ms": max(intervals_ms),
            "observer_qps_median": median(qps_values),
            "observer_qps_max": max(qps_values),
            "reported_max_poll_gap_ms": max(reported_gaps_ms, default=0.0),
            "failed_queries": reported_failed_queries,
            "snapshot_workflow_count": len(observations),
            "terminal_workflow_count": len(terminal_observations),
        },
        observations,
        terminal_observations,
    )


def derive(calibration_dir: Path) -> dict[str, Any]:
    latency_path = calibration_dir / "unloaded-latency.csv"
    polls_path = calibration_dir / "unloaded-observer-polls.csv"
    submissions_path = calibration_dir / "submission-rows.csv"
    input_paths = [latency_path, polls_path, submissions_path]
    missing = [str(path.name) for path in input_paths if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"required pilot inputs are missing: {', '.join(missing)}")

    source_integrity = verify_source_blocks(calibration_dir)

    _, rows = read_csv_rows(latency_path)
    observer_metrics, observer_workflows, terminal_observations = observer_sampling_metrics(
        polls_path
    )
    families = sorted({row.get("family", "") for row in rows if row.get("family")})
    if families != ["fanout-8", "seq-8"]:
        raise ValueError(f"expected exactly seq-8 and fanout-8 rows, found {families}")

    family_stats: dict[str, Any] = {}
    p95_inputs: list[float] = []
    all_ready = True
    latency_workflows: dict[str, dict[str, str]] = {}
    for family in families:
        family_rows = [row for row in rows if row["family"] == family]
        if len(family_rows) > 300:
            raise ValueError(f"{family} has more than the planned 300 unloaded calibration rows")
        block_counts: dict[str, int] = {}
        for row in family_rows:
            block_id = row.get("block_id", "")
            if not block_id:
                raise ValueError(f"{family} has a row without a block ID")
            block_counts[block_id] = block_counts.get(block_id, 0) + 1
            workflow_id = row.get("workflow_id", "")
            if not workflow_id or workflow_id in latency_workflows:
                raise ValueError(f"latency workflow ID is missing or duplicated: {workflow_id!r}")
            try:
                scheduled_ns = int(row["scheduled_at_monotonic_ns"])
                observed_ns = int(row["observed_at_monotonic_ns"])
                observed_latency_ms = float(row["observed_latency_ms"])
                preterminal_gap_ms = float(row["max_preterminal_gap_ms"])
            except (KeyError, TypeError, ValueError) as error:
                raise ValueError(
                    f"latency row is missing valid clock measurements for {workflow_id}"
                ) from error
            measured_latency_ms = (observed_ns - scheduled_ns) / 1_000_000
            if (
                not math.isfinite(observed_latency_ms)
                or not math.isfinite(preterminal_gap_ms)
                or observed_ns <= scheduled_ns
                or observed_latency_ms < 0
                or not math.isclose(observed_latency_ms, measured_latency_ms, abs_tol=0.000001)
            ):
                raise ValueError(
                    f"latency row has inconsistent or invalid monotonic times for {workflow_id}"
                )
            row_is_valid = row.get("valid", "").lower() == "true"
            if row_is_valid and (row.get("invalid_reason") or preterminal_gap_ms > 100):
                raise ValueError(
                    f"latency row is marked valid despite its invalid reason/gap for {workflow_id}"
                )
            if not row_is_valid and not row.get("invalid_reason"):
                raise ValueError(f"invalid latency row lacks a rejection reason for {workflow_id}")
            latency_workflows[workflow_id] = row
        if len(family_rows) == 300 and (
            len(block_counts) != 3 or set(block_counts.values()) != {100}
        ):
            raise ValueError(f"{family} must contain three fresh blocks of 100 rows")
        valid_rows: list[dict[str, str]] = []
        rejected: dict[str, int] = {}
        for row in family_rows:
            reason = row.get("invalid_reason", "")
            eligible = (
                row.get("valid", "").lower() == "true"
                and row.get("outcome") == "accepted"
                and row.get("terminal_state") == "SUCCEEDED"
            )
            if eligible:
                valid_rows.append(row)
            else:
                category = reason or (
                    f"outcome={row.get('outcome')};state={row.get('terminal_state')}"
                )
                rejected[category] = rejected.get(category, 0) + 1

        latencies = [float(row["observed_latency_ms"]) for row in valid_rows]
        qps = [float(row["observer_qps"]) for row in family_rows if row.get("observer_qps")]
        gaps = [
            float(row["max_preterminal_gap_ms"])
            for row in family_rows
            if row.get("max_preterminal_gap_ms")
        ]
        valid_count = len(latencies)
        enough = valid_count >= MINIMUM_VALID_SAMPLES_PER_FAMILY
        all_ready = all_ready and enough
        p50 = nearest_rank(latencies, 0.50) if latencies else None
        p95 = nearest_rank(latencies, 0.95) if latencies else None
        if p95 is not None:
            p95_inputs.append(p95)
        family_stats[family] = {
            "total_rows": len(family_rows),
            "valid_successful_samples": valid_count,
            "minimum_required": MINIMUM_VALID_SAMPLES_PER_FAMILY,
            "sufficient": enough,
            "invalid_or_non_successful_rows": len(family_rows) - valid_count,
            "invalid_reasons": rejected,
            "p50_ms_nearest_rank": p50,
            "p50_rank": math.ceil(0.50 * valid_count) if valid_count else None,
            "p95_ms_nearest_rank": p95,
            "p95_rank": math.ceil(0.95 * valid_count) if valid_count else None,
            "min_ms": min(latencies) if latencies else None,
            "max_ms": max(latencies) if latencies else None,
            "observer_qps_median": median(qps) if qps else None,
            "observer_qps_max": max(qps) if qps else None,
            "maximum_preterminal_gap_ms": max(gaps) if gaps else None,
        }

    _, submission_rows = read_csv_rows(submissions_path)
    submission_by_id: dict[str, dict[str, str]] = {}
    for row in submission_rows:
        workflow_id = row.get("workflow_id", "")
        if (
            row.get("record_type") != "submission"
            or not workflow_id
            or workflow_id in submission_by_id
            or row.get("outcome") != "accepted"
        ):
            raise ValueError("pilot submission rows must be unique, accepted submissions")
        submission_by_id[workflow_id] = row
    if set(submission_by_id) != set(latency_workflows):
        raise ValueError("submission and latency workflow ID sets differ")
    if set(observer_workflows) != set(latency_workflows) or set(terminal_observations) != set(
        latency_workflows
    ):
        raise ValueError("observer snapshot/terminal IDs do not match the latency cohort")
    for workflow_id, latency_row in latency_workflows.items():
        submitted = submission_by_id[workflow_id]
        terminal = terminal_observations[workflow_id]
        if submitted.get("family") != latency_row.get("family"):
            raise ValueError(f"submission and latency family differ for {workflow_id}")
        try:
            observed_ns = int(latency_row["observed_at_monotonic_ns"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(
                f"latency row lacks the observed monotonic time for {workflow_id}"
            ) from error
        if (
            terminal["observed_at_monotonic_ns"] != observed_ns
            or terminal["state"] != latency_row.get("terminal_state")
            or terminal["state"] != "SUCCEEDED"
            or terminal["valid"] != (latency_row.get("valid", "").lower() == "true")
        ):
            raise ValueError(
                f"latency row disagrees with observer terminal evidence for {workflow_id}"
            )

    slo_seconds: float | None = None
    formula_inputs: dict[str, float] | None = None
    if all_ready and len(p95_inputs) == 2:
        max_p95_seconds = max(p95_inputs) / 1000.0
        unrounded = max(2.5, 2 * max_p95_seconds + 1.0)
        slo_seconds = math.ceil(unrounded * 2) / 2
        formula_inputs = {
            "maximum_family_p95_seconds": max_p95_seconds,
            "headroom_multiplier": 2.0,
            "observer_allowance_seconds": 1.0,
            "minimum_slo_seconds": 2.5,
            "unrounded_seconds": unrounded,
        }

    return {
        "schema": "dur050-slo-derivation.v1",
        "label": "CALIBRATION — NOT RESULTS",
        "status": "PASS" if slo_seconds is not None else "INCOMPLETE",
        "formula_version": FORMULA_VERSION,
        "unloaded_observer_method": "single_workflow_read_only_poll",
        "unloaded_observer_sampling": observer_metrics,
        "validity_max_preterminal_gap_ms": 100,
        "percentile_method": "one-based nearest rank ceil(p*N)",
        "input_sha256": {
            **{path.name: sha256_file(path) for path in input_paths},
            "assembly.json": source_integrity["assembly_sha256"],
            **source_integrity["verified_source_files_sha256"],
        },
        "source_integrity": source_integrity,
        "families": family_stats,
        "slo_formula": "ceil_to_0.5s(max(2.5s, 2 * max(family_p95_seconds) + 1.0s))",
        "formula_inputs": formula_inputs,
        "numeric_p99_slo_seconds": slo_seconds,
        "reason": (
            None
            if slo_seconds is not None
            else "each family requires at least 300 valid successful observations"
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("calibration_dir", type=Path)
    args = parser.parse_args()
    output = args.calibration_dir / "slo-derivation.json"
    if output.exists():
        parser.error(f"refusing to overwrite existing derivation: {output}")
    result = derive(args.calibration_dir)
    with output.open("x", encoding="utf-8", newline="\n") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")
    print(
        json.dumps(
            {
                "status": result["status"],
                "numeric_p99_slo_seconds": result["numeric_p99_slo_seconds"],
            },
            sort_keys=True,
        )
    )
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
