"""Validate and summarize the two DUR-050 admission-gate pilot comparisons."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any

EXPECTED_MODES = ("OFF", "ON", "ON", "OFF", "OFF", "ON")
EXPECTED_FAMILIES = {"seq-8", "fanout-8"}
OPERATIONS = {"submission", "terminal_transition"}


def read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames is None:
            raise ValueError(f"CSV has no header: {path}")
        return list(reader.fieldnames), list(reader)


def nearest_rank(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("cannot summarize an empty timing group")
    import math

    return ordered[math.ceil(percentile * len(ordered)) - 1]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def summarize(manifest_path: Path, timings_path: Path, output_dir: Path) -> dict[str, Any]:
    if output_dir.exists():
        raise FileExistsError(f"refusing to overwrite gate-overhead output: {output_dir}")
    manifest_fields, manifest = read_csv(manifest_path)
    required_manifest = {
        "rate",
        "block_id",
        "pair_id",
        "mode",
        "run_id",
        "submissions_csv",
        "block_started_at_utc",
    }
    if not required_manifest.issubset(manifest_fields) or len(manifest) != 12:
        raise ValueError(
            "manifest must have 12 blocks and the required rate/pair/mode/run/files columns"
        )

    expected: dict[str, dict[str, str]] = {}
    by_rate: dict[str, list[dict[str, str]]] = defaultdict(list)
    seen_blocks: set[str] = set()
    seen_runs: set[str] = set()
    previous_block_start: datetime | None = None
    for row in manifest:
        if row["mode"] not in {"ON", "OFF"} or not row["run_id"] or not row["block_id"]:
            raise ValueError("manifest contains an invalid mode or empty identity")
        if row["block_id"] in seen_blocks or row["run_id"] in seen_runs:
            raise ValueError("manifest block IDs and run IDs must be unique")
        seen_blocks.add(row["block_id"])
        seen_runs.add(row["run_id"])
        try:
            block_start = datetime.fromisoformat(row["block_started_at_utc"].replace("Z", "+00:00"))
        except ValueError as error:
            raise ValueError(f"invalid block start time for {row['block_id']}") from error
        if block_start.tzinfo is None or block_start.utcoffset() != UTC.utcoffset(block_start):
            raise ValueError(f"block start time must be UTC for {row['block_id']}")
        if previous_block_start is not None and block_start <= previous_block_start:
            raise ValueError(
                "manifest rows are not in strictly increasing execution start-time order"
            )
        previous_block_start = block_start
        row["_parsed_block_start"] = block_start
        try:
            if float(row["rate"]) <= 0:
                raise ValueError("manifest rates must be positive")
        except ValueError as error:
            raise ValueError(f"invalid offered rate in block {row['block_id']}") from error
        by_rate[row["rate"]].append(row)
        submissions_path = Path(row["submissions_csv"])
        if not submissions_path.is_absolute():
            submissions_path = manifest_path.parent / submissions_path
        _, submissions = read_csv(submissions_path.resolve(strict=True))
        if len(submissions) != 100 or any(
            item.get("record_type") != "submission" for item in submissions
        ):
            raise ValueError(f"block {row['block_id']} must contain 100 scheduled submission rows")
        if any(item.get("outcome") != "accepted" for item in submissions):
            raise ValueError(f"block {row['block_id']} has a rejected or uncertain submission")
        families = [item.get("family", "") for item in submissions]
        if (
            set(families) != EXPECTED_FAMILIES
            or families.count("seq-8") != 50
            or families.count("fanout-8") != 50
        ):
            raise ValueError(f"block {row['block_id']} is not a 50/50 two-family cohort")
        for item in submissions:
            workflow_id = item.get("workflow_id", "")
            if not workflow_id or workflow_id in expected:
                raise ValueError(f"workflow ID is empty or duplicated: {workflow_id!r}")
            expected[workflow_id] = {**row, "family": item["family"]}

    if len(by_rate) != 2:
        raise ValueError("the comparison requires the low and pilot-informed loaded rates")
    for rate, rows in by_rate.items():
        ordered = rows
        if len(rows) != 6 or tuple(row["mode"] for row in ordered) != EXPECTED_MODES:
            raise ValueError(f"rate {rate} must contain six blocks in OFF/ON/ON/OFF/OFF/ON order")
        pairs: dict[str, list[str]] = defaultdict(list)
        for row in ordered:
            pairs[row["pair_id"]].append(row["mode"])
        if len(pairs) != 3 or any(sorted(modes) != ["OFF", "ON"] for modes in pairs.values()):
            raise ValueError(f"rate {rate} must contain three ON/OFF matched pairs")
    numeric_rates = {float(rate) for rate in by_rate}
    if 0.25 not in numeric_rates or any(rate <= 0.25 for rate in numeric_rates - {0.25}):
        raise ValueError("one comparison rate must be the frozen low rate of 0.25 workflows/s")

    source_timings = timings_path.resolve(strict=True)
    transaction_rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    requested_run_ids = {row["run_id"] for row in manifest}
    with source_timings.open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, start=1):
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(f"invalid transaction JSON on line {line_number}") from error
            if item.get("run_id") not in requested_run_ids:
                continue
            workflow_id = item.get("workflow_id", "")
            identity = expected.get(workflow_id)
            if identity is None or identity["run_id"] != item.get("run_id"):
                raise ValueError(f"timing row references an unknown workflow/run: {workflow_id}")
            operation = item.get("operation", "")
            if operation not in OPERATIONS:
                raise ValueError(f"unexpected timing operation for {workflow_id}: {operation}")
            if (
                item.get("measurement_clock") != "runtime_process_monotonic"
                or item.get("measurement_scope")
                != "pool_begin_to_commit_return_including_pool_wait_network"
            ):
                raise ValueError(f"unexpected duration clock/scope for {workflow_id}")
            key = (workflow_id, operation)
            if key in seen:
                raise ValueError(f"duplicate {operation} timing row for {workflow_id}")
            seen.add(key)
            try:
                duration_us = int(item["transaction_duration_us"])
            except (KeyError, TypeError, ValueError) as error:
                raise ValueError(f"invalid duration for {workflow_id}/{operation}") from error
            if duration_us < 0 or not item.get("log_source_host"):
                raise ValueError(f"invalid duration/source host for {workflow_id}/{operation}")
            if operation == "submission" and item.get("outcome") != "created":
                raise ValueError(f"submission was not a first creation for {workflow_id}")
            if operation == "terminal_transition" and item.get("state") != "SUCCEEDED":
                raise ValueError(f"workflow did not succeed for {workflow_id}")
            transaction_rows.append(
                {
                    **identity,
                    "workflow_id": workflow_id,
                    "operation": operation,
                    "outcome": item.get("outcome", ""),
                    "state": item.get("state", ""),
                    "measurement_clock": item["measurement_clock"],
                    "measurement_scope": item["measurement_scope"],
                    "transaction_duration_us": duration_us,
                    "log_source_host": item["log_source_host"],
                    "recorded_at_utc": item.get("recorded_at_utc", ""),
                }
            )

    required_rows = len(expected) * len(OPERATIONS)
    if len(transaction_rows) != required_rows:
        raise ValueError(f"found {len(transaction_rows)} transaction rows; require {required_rows}")
    if len(seen) != required_rows:
        raise ValueError(
            f"found {len(seen)} unique workflow-operation rows; require {required_rows}"
        )

    block_groups: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in transaction_rows:
        block_groups[(row["rate"], row["block_id"], row["family"], row["operation"])].append(row)
    block_summary = []
    for (rate, block_id, family, operation), rows in sorted(block_groups.items()):
        values = [float(row["transaction_duration_us"]) for row in rows]
        block_summary.append(
            {
                "rate": float(rate),
                "block_id": block_id,
                "family": family,
                "mode": rows[0]["mode"],
                "pair_id": rows[0]["pair_id"],
                "block_started_at_utc": rows[0]["block_started_at_utc"],
                "operation": operation,
                "count": len(values),
                "p50_us_nearest_rank": nearest_rank(values, 0.50),
                "p95_us_nearest_rank": nearest_rank(values, 0.95),
                "source_hosts": sorted({row["log_source_host"] for row in rows}),
            }
        )

    pairs = []
    summary_index = {
        (str(row["rate"]), row["pair_id"], row["family"], row["operation"], row["mode"]): row
        for row in block_summary
    }
    for rate, rows in sorted(by_rate.items(), key=lambda item: float(item[0])):
        for pair_id in sorted({row["pair_id"] for row in rows}):
            for family in sorted(EXPECTED_FAMILIES):
                for operation in sorted(OPERATIONS):
                    off = summary_index.get((rate, pair_id, family, operation, "OFF"))
                    on = summary_index.get((rate, pair_id, family, operation, "ON"))
                    if off is None or on is None:
                        raise ValueError(
                            f"missing paired summary for {rate}/{pair_id}/{family}/{operation}"
                        )
                    pairs.append(
                        {
                            "rate": float(rate),
                            "pair_id": pair_id,
                            "execution_order": [
                                {
                                    "mode": row["mode"],
                                    "block_id": row["block_id"],
                                    "block_started_at_utc": row["block_started_at_utc"],
                                }
                                for row in sorted(
                                    (
                                        candidate
                                        for candidate in rows
                                        if candidate["pair_id"] == pair_id
                                    ),
                                    key=lambda candidate: candidate["_parsed_block_start"],
                                )
                            ],
                            "family": family,
                            "operation": operation,
                            "off_p50_us": off["p50_us_nearest_rank"],
                            "on_p50_us": on["p50_us_nearest_rank"],
                            "on_minus_off_p50_us": on["p50_us_nearest_rank"]
                            - off["p50_us_nearest_rank"],
                            "off_p95_us": off["p95_us_nearest_rank"],
                            "on_p95_us": on["p95_us_nearest_rank"],
                            "on_minus_off_p95_us": on["p95_us_nearest_rank"]
                            - off["p95_us_nearest_rank"],
                        }
                    )

    paired_groups: dict[tuple[float, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in pairs:
        paired_groups[(row["rate"], row["family"], row["operation"])].append(row)
    paired_effect_summary = []
    for (rate, family, operation), rows in sorted(paired_groups.items()):
        paired_effect_summary.append(
            {
                "rate": rate,
                "family": family,
                "operation": operation,
                "matched_pairs": len(rows),
                "median_on_minus_off_p50_us": median([row["on_minus_off_p50_us"] for row in rows]),
                "median_on_minus_off_p95_us": median([row["on_minus_off_p95_us"] for row in rows]),
                "pair_deltas": rows,
            }
        )

    output_dir.mkdir(parents=True, exist_ok=False)
    output_fields = [
        "rate",
        "block_id",
        "pair_id",
        "mode",
        "family",
        "run_id",
        "block_started_at_utc",
        "workflow_id",
        "operation",
        "outcome",
        "state",
        "measurement_clock",
        "measurement_scope",
        "transaction_duration_us",
        "log_source_host",
        "recorded_at_utc",
    ]
    with (output_dir / "gate-overhead.csv").open("x", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=output_fields)
        writer.writeheader()
        writer.writerows({key: row[key] for key in output_fields} for row in transaction_rows)
    shutil.copy2(manifest_path, output_dir / "block-manifest.csv")
    shutil.copy2(source_timings, output_dir / "transaction-timings.jsonl")
    result = {
        "schema": "dur050-gate-overhead-summary.v1",
        "label": "CALIBRATION — NOT RESULTS",
        "capacity_claim_use": "prohibited",
        "block_count": len(manifest),
        "transaction_rows": len(transaction_rows),
        "manifest_sha256": sha256_file(manifest_path),
        "transaction_timings_sha256": sha256_file(source_timings),
        "blocks": block_summary,
        "paired_effect_summary": paired_effect_summary,
    }
    (output_dir / "gate-overhead-summary.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest", type=Path)
    parser.add_argument("transaction_timings", type=Path)
    parser.add_argument("output_dir", type=Path)
    args = parser.parse_args()
    try:
        result = summarize(args.manifest, args.transaction_timings, args.output_dir)
    except (OSError, ValueError) as error:
        parser.error(str(error))
    print(
        json.dumps(
            {
                "status": "SUMMARIZED",
                "blocks": result["block_count"],
                "rows": result["transaction_rows"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
