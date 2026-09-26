"""Assemble six immutable unloaded pilot blocks into one calibration directory."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import shutil
from collections import Counter
from pathlib import Path
from typing import Any

EXPECTED_FAMILIES = ("seq-8", "fanout-8")
BLOCK_FILES = (
    "unloaded-latency.csv",
    "unloaded-observer-polls.csv",
    "submission-rows.csv",
    "loadgen-summaries.jsonl",
)


def read_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
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


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def valid_generator_cpu_summary(cpu: dict[str, Any]) -> bool:
    if cpu.get("status") != "PASS":
        return False
    try:
        window = float(cpu["window_seconds"])
        samples = cpu["samples"]
        intervals = [
            (float(sample["start_elapsed_seconds"]), float(sample["end_elapsed_seconds"]))
            for sample in samples
        ]
    except (KeyError, TypeError, ValueError):
        return False
    if not math.isfinite(window) or window <= 0 or not intervals:
        return False
    if any(
        not math.isfinite(start) or not math.isfinite(end) or end <= start
        for start, end in intervals
    ):
        return False
    covered = sum(end - start for start, end in intervals)
    final_capture = intervals[-1][1]
    return (
        math.isclose(window, final_capture, rel_tol=0.0, abs_tol=5e-9) and covered >= window * 0.99
    )


def assemble(output: Path, block_paths: list[Path]) -> dict[str, Any]:
    if len(block_paths) != 6:
        raise ValueError("exactly six source blocks are required (three per family)")
    if output.exists():
        raise FileExistsError(f"refusing to overwrite calibration directory: {output}")
    resolved_output = output.resolve()
    resolved_blocks = [path.resolve(strict=True) for path in block_paths]
    if len(set(resolved_blocks)) != len(resolved_blocks):
        raise ValueError("a source block was supplied more than once")
    if any(path == resolved_output or resolved_output in path.parents for path in resolved_blocks):
        raise ValueError("output directory cannot contain a source block")

    blocks: list[dict[str, Any]] = []
    merged: dict[str, list[tuple[list[str], dict[str, str]]]] = {
        name: [] for name in BLOCK_FILES if name.endswith(".csv")
    }
    family_counts: Counter[str] = Counter()
    seen_workflow_ids: set[str] = set()
    for index, block_path in enumerate(resolved_blocks, start=1):
        if not block_path.is_dir():
            raise ValueError(f"source block is not a directory: {block_path}")
        missing = [name for name in BLOCK_FILES if not (block_path / name).is_file()]
        if missing:
            raise ValueError(f"block {block_path.name} lacks {', '.join(missing)}")
        latency_fields, latency_rows = read_rows(block_path / "unloaded-latency.csv")
        if len(latency_rows) != 100:
            raise ValueError(
                f"block {block_path.name} has {len(latency_rows)} latency rows; want 100"
            )
        families = {row.get("family", "") for row in latency_rows}
        if len(families) != 1 or next(iter(families)) not in EXPECTED_FAMILIES:
            raise ValueError(f"block {block_path.name} must contain exactly one declared family")
        family = next(iter(families))
        family_counts[family] += 1
        workflow_ids = [row.get("workflow_id", "") for row in latency_rows]
        if any(not workflow_id for workflow_id in workflow_ids):
            raise ValueError(f"block {block_path.name} has a latency row without workflow_id")
        duplicate_ids = seen_workflow_ids.intersection(workflow_ids)
        if duplicate_ids or len(set(workflow_ids)) != len(workflow_ids):
            raise ValueError(
                f"workflow IDs repeat across pilot blocks: {sorted(duplicate_ids)[:3]}"
            )
        seen_workflow_ids.update(workflow_ids)

        submission_fields, submission_rows = read_rows(block_path / "submission-rows.csv")
        if len(submission_rows) != 100 or any(
            row.get("record_type") != "submission" for row in submission_rows
        ):
            raise ValueError(
                f"block {block_path.name} must retain all 100 scheduled submission rows"
            )
        if {row.get("workflow_id", "") for row in submission_rows} != set(workflow_ids):
            raise ValueError(f"submission and latency workflow IDs differ in {block_path.name}")
        if any(row.get("outcome") != "accepted" for row in submission_rows):
            raise ValueError(f"block {block_path.name} contains a non-accepted submission")
        with (block_path / "loadgen-summaries.jsonl").open(encoding="utf-8") as stream:
            summaries = [json.loads(line) for line in stream if line.strip()]
        if len(summaries) != 100:
            raise ValueError(f"block {block_path.name} needs 100 per-workflow loadgen summaries")
        if any(
            item.get("summary", {}).get("status") != "PASS"
            or item.get("summary", {}).get("accepted") != 1
            or not valid_generator_cpu_summary(item.get("summary", {}).get("generator_cpu", {}))
            for item in summaries
        ):
            raise ValueError(
                f"block {block_path.name} contains an invalid loadgen summary "
                "or CPU window/coverage"
            )

        for name, fields in (
            ("unloaded-latency.csv", latency_fields),
            ("submission-rows.csv", submission_fields),
        ):
            current_fields, rows = read_rows(block_path / name)
            if current_fields != fields:
                raise ValueError(f"inconsistent CSV header in {block_path.name}/{name}")
            merged[name].extend((fields, row) for row in rows)
        poll_fields, poll_rows = read_rows(block_path / "unloaded-observer-polls.csv")
        merged["unloaded-observer-polls.csv"].extend((poll_fields, row) for row in poll_rows)

        blocks.append(
            {
                "index": index,
                "source_block_name": block_path.name,
                "family": family,
                "latency_rows": len(latency_rows),
                "submission_rows": len(submission_rows),
                "source_sha256": {name: sha256_file(block_path / name) for name in BLOCK_FILES},
            }
        )
    if family_counts != Counter({"seq-8": 3, "fanout-8": 3}):
        raise ValueError(f"need three blocks per family, found {dict(family_counts)}")

    output.mkdir(parents=True, exist_ok=False)
    try:
        raw_root = output / "source-blocks"
        raw_root.mkdir()
        for item, source in zip(blocks, resolved_blocks, strict=True):
            target = raw_root / f"{item['index']:02d}-{source.name}"
            shutil.copytree(source, target, copy_function=shutil.copy2)

        for name, rows in merged.items():
            if not rows:
                raise ValueError(f"no rows found for {name}")
            fields = rows[0][0]
            if any(row_fields != fields for row_fields, _ in rows):
                raise ValueError(f"CSV headers differ across source blocks for {name}")
            with (output / name).open("x", newline="", encoding="utf-8") as stream:
                writer = csv.DictWriter(stream, fieldnames=fields)
                writer.writeheader()
                writer.writerows(row for _, row in rows)

        summary = {
            "schema": "dur050-pilot-calibration-assembly.v1",
            "label": "CALIBRATION — NOT RESULTS",
            "capacity_claim_use": "prohibited",
            "block_count": len(blocks),
            "latency_rows": sum(item["latency_rows"] for item in blocks),
            "submission_rows": sum(item["submission_rows"] for item in blocks),
            "blocks": blocks,
        }
        (output / "assembly.json").write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        lines = [
            "# DUR-050 pilot calibration",
            "",
            "**CALIBRATION — NOT RESULTS.** These observations derive the SLO only.",
            "Do not use them as capacity, optimization, overload, or soak results.",
            "",
            "Six fresh 100-workflow blocks are preserved under `source-blocks/`.",
            "The merged CSV files are convenience views; the source block files",
            "and their SHA-256 values in `assembly.json` remain authoritative.",
            "",
            "Run `python scripts/dur050-derive-slo.py <this-directory>` after",
            "reviewing the assembled rows. The derivation refuses to overwrite an",
            "existing result and withholds a numeric SLO unless each family has",
            "at least 300 valid successful samples.",
            "",
            "## Source blocks",
            "",
        ]
        lines.extend(
            f"- `{item['source_block_name']}` — {item['family']}: "
            f"{item['latency_rows']} observations; immutable file hashes in `assembly.json`."
            for item in blocks
        )
        (output / "README.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    except Exception:
        shutil.rmtree(output)
        raise
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("output_dir", type=Path)
    parser.add_argument("blocks", type=Path, nargs=6)
    args = parser.parse_args()
    try:
        summary = assemble(args.output_dir, args.blocks)
    except (OSError, ValueError) as error:
        parser.error(str(error))
    print(
        json.dumps(
            {
                "status": "ASSEMBLED",
                "block_count": summary["block_count"],
                "latency_rows": summary["latency_rows"],
                "submission_rows": summary["submission_rows"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
