#!/usr/bin/env python3
"""Prove the assembler accepts a CPU window ending at its final sample."""

from __future__ import annotations

import csv
import importlib.util
import json
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "dur050_assembler", ROOT / "scripts/dur050-assemble-pilot-calibration.py"
)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("cannot load pilot calibration assembler")
ASSEMBLER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ASSEMBLER)


def write_csv(path: Path, fields: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="dur050-assembly-cpu-") as temp:
        root = Path(temp)
        blocks: list[Path] = []
        for block_index in range(6):
            family = "seq-8" if block_index < 3 else "fanout-8"
            block = root / f"block-{block_index:02d}"
            block.mkdir()
            latency_rows = []
            submission_rows = []
            summaries = []
            for sample in range(100):
                workflow_id = f"dur050-ci-b{block_index}-w{sample:03d}"
                latency_rows.append({"family": family, "workflow_id": workflow_id, "valid": "true"})
                submission_rows.append(
                    {"record_type": "submission", "workflow_id": workflow_id, "outcome": "accepted"}
                )
                summaries.append(
                    {
                        "summary": {
                            "status": "PASS",
                            "accepted": 1,
                            "generator_cpu": {
                                "status": "PASS",
                                "window_seconds": 0.0004,
                                "samples": [
                                    {
                                        "start_elapsed_seconds": 0.0,
                                        "end_elapsed_seconds": 0.0004,
                                        "cpu_percent_normalized_per_core": 25.0,
                                    }
                                ],
                            },
                        }
                    }
                )
            write_csv(
                block / "unloaded-latency.csv", ["family", "workflow_id", "valid"], latency_rows
            )
            write_csv(
                block / "submission-rows.csv",
                ["record_type", "workflow_id", "outcome"],
                submission_rows,
            )
            write_csv(
                block / "unloaded-observer-polls.csv",
                ["record_type", "workflow_id"],
                [
                    {"record_type": "summary", "workflow_id": workflow_id}
                    for workflow_id in (row["workflow_id"] for row in latency_rows)
                ],
            )
            (block / "loadgen-summaries.jsonl").write_text(
                "".join(json.dumps(item) + "\n" for item in summaries), encoding="utf-8"
            )
            blocks.append(block)
        result = ASSEMBLER.assemble(root / "assembled", blocks)
        if result["block_count"] != 6 or result["latency_rows"] != 600:
            raise SystemExit(f"corrected final-sample CPU windows did not assemble: {result}")
        summary_path = blocks[0] / "loadgen-summaries.jsonl"
        original = summary_path.read_text(encoding="utf-8")
        malformed = json.loads(original.splitlines()[0])
        malformed["summary"]["generator_cpu"]["samples"][-1]["end_elapsed_seconds"] = 0.0005
        summary_path.write_text(
            json.dumps(malformed) + "\n" + "\n".join(original.splitlines()[1:]) + "\n",
            encoding="utf-8",
        )
        try:
            ASSEMBLER.assemble(root / "rejected", blocks)
        except ValueError as error:
            if "CPU window/coverage" not in str(error):
                raise
        else:
            raise SystemExit("assembler accepted a CPU window that did not end at its last sample")
    print("PASS: assembler accepts 600 samples whose CPU window ends at the final capture.")
    print("PASS: assembler rejects a summary whose CPU window extends past its final capture.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
