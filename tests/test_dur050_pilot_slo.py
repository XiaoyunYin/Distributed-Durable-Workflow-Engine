from __future__ import annotations

import csv
import importlib.util
import io
import json
import sys
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "dur050-derive-slo.py"
SPEC = importlib.util.spec_from_file_location("dur050_derive_slo", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


LATENCY_FIELDS = [
    "family",
    "block_id",
    "sample",
    "run_id",
    "workflow_id",
    "outcome",
    "terminal_state",
    "scheduled_at_utc",
    "scheduled_at_monotonic_ns",
    "first_terminal_observed_at_utc",
    "observed_at_monotonic_ns",
    "observed_latency_ms",
    "max_preterminal_gap_ms",
    "query_count",
    "observer_qps",
    "valid",
    "invalid_reason",
]


def write_calibration(root: Path, *, valid_count: int = 300, failed_observer: bool = False) -> None:
    root.mkdir()
    poll_rows: list[dict[str, object]] = []
    submission_rows: list[dict[str, object]] = []
    latency_rows: list[dict[str, object]] = []
    with (root / "unloaded-latency.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=LATENCY_FIELDS)
        writer.writeheader()
        for family, latency in (("seq-8", 1000.0), ("fanout-8", 2000.0)):
            for sample in range(1, valid_count + 1):
                workflow_id = f"wf-{family}-{sample}"
                latency_row = {
                    "family": family,
                    "block_id": f"block-{(sample - 1) // 100}",
                    "sample": sample,
                    "run_id": f"run-{family}-{sample}",
                    "workflow_id": workflow_id,
                    "outcome": "accepted",
                    "terminal_state": "SUCCEEDED",
                    "scheduled_at_utc": "2026-09-25T00:00:00Z",
                    "scheduled_at_monotonic_ns": sample * 3_000_000_000
                    + 50_000_000
                    - int(latency * 1_000_000),
                    "first_terminal_observed_at_utc": "2026-09-25T00:00:01Z",
                    "observed_at_monotonic_ns": sample * 3_000_000_000 + 50_000_000,
                    "observed_latency_ms": latency,
                    "max_preterminal_gap_ms": 50,
                    "query_count": 20,
                    "observer_qps": 19.5,
                    "valid": "true",
                    "invalid_reason": "",
                }
                writer.writerow(latency_row)
                latency_rows.append(latency_row)
                poll_rows.extend(
                    [
                        {
                            "record_type": "snapshot",
                            "sequence": 1,
                            "workflow_id": f"wf-{family}-{sample}",
                            "observed_at_monotonic_ns": sample * 3_000_000_000,
                            "observer_qps": "",
                            "max_poll_gap_ms": "",
                            "reason": "",
                        },
                        {
                            "record_type": "first_terminal_observation",
                            "sequence": 2,
                            "workflow_id": f"wf-{family}-{sample}",
                            "observed_at_monotonic_ns": sample * 3_000_000_000 + 50_000_000,
                            "observer_qps": "",
                            "max_poll_gap_ms": "",
                            "reason": "",
                            "state": "SUCCEEDED",
                            "valid": "true",
                        },
                        {
                            "record_type": "snapshot",
                            "sequence": 2,
                            "workflow_id": f"wf-{family}-{sample}",
                            "observed_at_monotonic_ns": sample * 3_000_000_000 + 50_000_000,
                            "observer_qps": "",
                            "max_poll_gap_ms": "",
                            "reason": "",
                        },
                        {
                            "record_type": "summary",
                            "sequence": 2,
                            "workflow_id": workflow_id,
                            "observed_at_monotonic_ns": sample * 3_000_000_000 + 50_000_000,
                            "observer_qps": "19.5",
                            "max_poll_gap_ms": "50",
                            "reason": "queries=2 terminal_workflows_seen=1 failed_queries=0",
                        },
                    ]
                )
                if failed_observer and family == "seq-8" and sample == 1:
                    poll_rows.insert(
                        len(poll_rows) - 1,
                        {
                            "record_type": "poll_error",
                            "sequence": 3,
                            "workflow_id": workflow_id,
                            "observed_at_monotonic_ns": sample * 3_000_000_000 + 75_000_000,
                            "observer_qps": "",
                            "max_poll_gap_ms": "",
                            "reason": "synthetic query failure",
                        },
                    )
                    poll_rows[-1]["reason"] = (
                        "queries=3 terminal_workflows_seen=1 failed_queries=1 "
                        "observer_query_failures"
                    )
                submission_rows.append(
                    {
                        "record_type": "submission",
                        "workflow_id": f"wf-{family}-{sample}",
                        "family": family,
                        "outcome": "accepted",
                    }
                )
    with (root / "unloaded-observer-polls.csv").open("w", newline="", encoding="utf-8") as stream:
        fields = [
            "record_type",
            "sequence",
            "workflow_id",
            "observed_at_monotonic_ns",
            "observer_qps",
            "max_poll_gap_ms",
            "reason",
            "state",
            "valid",
        ]
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(poll_rows)
    with (root / "submission-rows.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(
            stream, fieldnames=["record_type", "workflow_id", "family", "outcome"]
        )
        writer.writeheader()
        writer.writerows(submission_rows)

    source_root = root / "source-blocks"
    source_root.mkdir()
    poll_fields = [
        "record_type",
        "sequence",
        "workflow_id",
        "observed_at_monotonic_ns",
        "observer_qps",
        "max_poll_gap_ms",
        "reason",
        "state",
        "valid",
    ]
    submission_fields = ["record_type", "workflow_id", "family", "outcome"]
    blocks = []
    for family in ("seq-8", "fanout-8"):
        family_rows = [row for row in latency_rows if row["family"] == family]
        for block_index in range(3):
            block_rows = family_rows[block_index * 100 : (block_index + 1) * 100]
            ids = {str(row["workflow_id"]) for row in block_rows}
            block_name = f"{family}-{block_index + 1}"
            block_dir = source_root / f"{len(blocks) + 1:02d}-{block_name}"
            block_dir.mkdir()
            datasets = {
                "unloaded-latency.csv": (LATENCY_FIELDS, block_rows),
                "unloaded-observer-polls.csv": (
                    poll_fields,
                    [row for row in poll_rows if row["workflow_id"] in ids],
                ),
                "submission-rows.csv": (
                    submission_fields,
                    [row for row in submission_rows if row["workflow_id"] in ids],
                ),
            }
            for filename, (fields, rows) in datasets.items():
                with (block_dir / filename).open("w", newline="", encoding="utf-8") as stream:
                    writer = csv.DictWriter(stream, fieldnames=fields)
                    writer.writeheader()
                    writer.writerows(rows)
            with (block_dir / "loadgen-summaries.jsonl").open("w", encoding="utf-8") as stream:
                for _ in block_rows:
                    stream.write(
                        json.dumps(
                            {
                                "summary": {
                                    "status": "PASS",
                                    "accepted": 1,
                                    "generator_cpu": {"status": "PASS"},
                                }
                            }
                        )
                        + "\n"
                    )
            blocks.append(
                {
                    "index": len(blocks) + 1,
                    "source_block_name": block_name,
                    "family": family,
                    "latency_rows": len(block_rows),
                    "submission_rows": len(block_rows),
                    "source_sha256": {
                        filename: ASSEMBLER.sha256_file(block_dir / filename)
                        for filename in ASSEMBLER.BLOCK_FILES
                    },
                }
            )
    (root / "assembly.json").write_text(
        json.dumps(
            {
                "schema": "dur050-pilot-calibration-assembly.v1",
                "label": "CALIBRATION — NOT RESULTS",
                "capacity_claim_use": "prohibited",
                "block_count": 6,
                "latency_rows": len(latency_rows),
                "submission_rows": len(submission_rows),
                "blocks": blocks,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def test_slo_derivation_uses_valid_family_p95_and_frozen_formula(tmp_path: Path) -> None:
    calibration = tmp_path / "calibration"
    write_calibration(calibration)

    result = MODULE.derive(calibration)

    assert result["status"] == "PASS"
    assert result["families"]["seq-8"]["valid_successful_samples"] == 300
    assert result["families"]["fanout-8"]["p95_ms_nearest_rank"] == 2000.0
    assert result["families"]["fanout-8"]["p95_rank"] == 285
    assert result["numeric_p99_slo_seconds"] == 5.0
    assert result["unloaded_observer_sampling"]["query_interval_p50_ms_nearest_rank"] == 50.0
    assert result["unloaded_observer_sampling"]["observer_qps_median"] == 19.5
    assert {"unloaded-latency.csv", "unloaded-observer-polls.csv", "submission-rows.csv"} <= set(
        result["input_sha256"]
    )
    assert result["source_integrity"]["verified_source_block_count"] == 6
    assert result["source_integrity"]["assembly_sha256"] == result["input_sha256"]["assembly.json"]
    assert len(result["source_integrity"]["verified_source_files_sha256"]) == 24
    assert len(result["source_integrity"]["verified_merged_views_sha256"]) == 3


def test_slo_derivation_rejects_edited_merged_view_after_assembly(tmp_path: Path) -> None:
    calibration = tmp_path / "calibration"
    write_calibration(calibration)
    path = calibration / "unloaded-latency.csv"
    path.write_text(
        path.read_text(encoding="utf-8").replace(",1000.0,", ",9000.0,", 1), encoding="utf-8"
    )

    try:
        MODULE.derive(calibration)
    except ValueError as error:
        assert "differs from its source-block reconstruction" in str(error)
    else:
        raise AssertionError("SLO derivation accepted an edited merged view")


def test_slo_derivation_rejects_edited_source_block_after_assembly(tmp_path: Path) -> None:
    calibration = tmp_path / "calibration"
    write_calibration(calibration)
    path = calibration / "source-blocks" / "01-seq-8-1" / "unloaded-latency.csv"
    path.write_text(
        path.read_text(encoding="utf-8").replace(",1000.0,", ",9000.0,", 1), encoding="utf-8"
    )

    try:
        MODULE.derive(calibration)
    except ValueError as error:
        assert "source block hash mismatch" in str(error)
    else:
        raise AssertionError("SLO derivation accepted an edited source block")


def test_slo_derivation_withholds_number_below_minimum_sample_count(
    tmp_path: Path,
) -> None:
    calibration = tmp_path / "calibration"
    write_calibration(calibration, valid_count=299)

    result = MODULE.derive(calibration)

    assert result["status"] == "INCOMPLETE"
    assert result["numeric_p99_slo_seconds"] is None
    assert result["formula_inputs"] is None


def test_nearest_rank_uses_one_based_ceiling_rank() -> None:
    assert MODULE.nearest_rank([5.0, 1.0, 4.0, 2.0, 3.0], 0.95) == 5.0


def test_slo_derivation_rejects_failed_observer_queries(tmp_path: Path) -> None:
    calibration = tmp_path / "calibration"
    write_calibration(calibration, failed_observer=True)

    try:
        MODULE.derive(calibration)
    except ValueError as error:
        assert "failed queries" in str(error)
    else:
        raise AssertionError("failed observer query was accepted as calibration")


def test_slo_derivation_rejects_source_block_submission_cohort_mismatch(tmp_path: Path) -> None:
    calibration = tmp_path / "calibration"
    write_calibration(calibration)
    source_dir = calibration / "source-blocks" / "01-seq-8-1"
    submission_path = source_dir / "submission-rows.csv"
    with submission_path.open("a", encoding="utf-8") as stream:
        stream.write("submission,unexpected-id,seq-8,accepted\r\n")
    assembly_path = calibration / "assembly.json"
    assembly = json.loads(assembly_path.read_text(encoding="utf-8"))
    assembly["blocks"][0]["submission_rows"] += 1
    assembly["submission_rows"] += 1
    assembly["blocks"][0]["source_sha256"]["submission-rows.csv"] = ASSEMBLER.sha256_file(
        submission_path
    )
    assembly_path.write_text(
        json.dumps(assembly, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    try:
        MODULE.derive(calibration)
    except ValueError as error:
        assert "source block submission/latency IDs differ" in str(error)
    else:
        raise AssertionError("SLO derivation accepted a source-block cohort mismatch")


ASSEMBLER_PATH = (
    Path(__file__).resolve().parents[1] / "scripts" / "dur050-assemble-pilot-calibration.py"
)
ASSEMBLER_SPEC = importlib.util.spec_from_file_location("dur050_assemble_pilot", ASSEMBLER_PATH)
assert ASSEMBLER_SPEC is not None and ASSEMBLER_SPEC.loader is not None
ASSEMBLER = importlib.util.module_from_spec(ASSEMBLER_SPEC)
ASSEMBLER_SPEC.loader.exec_module(ASSEMBLER)


def write_source_block(root: Path, family: str, block_number: int) -> None:
    root.mkdir()
    latency_fields = ["family", "workflow_id"]
    with (root / "unloaded-latency.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=latency_fields)
        writer.writeheader()
        for sample in range(100):
            writer.writerow(
                {"family": family, "workflow_id": f"wf-{family}-{block_number}-{sample}"}
            )
    with (root / "unloaded-observer-polls.csv").open("w", encoding="utf-8") as stream:
        stream.write("record_type,workflow_id\nsnapshot,wf\n")
    with (root / "submission-rows.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=["record_type", "workflow_id", "outcome"])
        writer.writeheader()
        for sample in range(100):
            writer.writerow(
                {
                    "record_type": "submission",
                    "workflow_id": f"wf-{family}-{block_number}-{sample}",
                    "outcome": "accepted",
                }
            )
    summaries = [
        {
            "summary": {
                "status": "PASS",
                "accepted": 1,
                "generator_cpu": {"status": "PASS"},
            }
        }
        for _ in range(100)
    ]
    (root / "loadgen-summaries.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in summaries), encoding="utf-8"
    )


def make_six_blocks(tmp_path: Path) -> list[Path]:
    blocks = []
    for family in ("seq-8", "fanout-8"):
        for index in range(3):
            block = tmp_path / f"{family}-{index}"
            write_source_block(block, family, index)
            blocks.append(block)
    return blocks


def test_calibration_assembler_preserves_six_blocks_and_merges_raw_rows(tmp_path: Path) -> None:
    blocks = make_six_blocks(tmp_path)
    output = tmp_path / "assembled"

    summary = ASSEMBLER.assemble(output, blocks)

    assert summary["label"] == "CALIBRATION — NOT RESULTS"
    assert summary["capacity_claim_use"] == "prohibited"
    assert summary["block_count"] == 6
    assert summary["latency_rows"] == 600
    assert summary["submission_rows"] == 600
    assert len(list((output / "source-blocks").iterdir())) == 6
    with (output / "unloaded-latency.csv").open(newline="", encoding="utf-8") as stream:
        assert len(list(csv.DictReader(stream))) == 600


def test_calibration_assembler_rejects_missing_family_block_before_creating_output(
    tmp_path: Path,
) -> None:
    blocks = make_six_blocks(tmp_path)
    for filename in ("unloaded-latency.csv", "submission-rows.csv"):
        path = blocks[-1] / filename
        fields = (
            ["family", "workflow_id"]
            if filename == "unloaded-latency.csv"
            else ["record_type", "workflow_id", "outcome"]
        )
        with path.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            for sample in range(100):
                row = {
                    fields[0]: ("seq-8" if filename == "unloaded-latency.csv" else "submission"),
                    "workflow_id": f"wf-seq-8-rewritten-{sample}",
                }
                if "outcome" in fields:
                    row["outcome"] = "accepted"
                writer.writerow(row)
    output = tmp_path / "assembled"

    try:
        ASSEMBLER.assemble(output, blocks)
    except ValueError as error:
        assert "three blocks per family" in str(error)
    else:
        raise AssertionError("assembler accepted an unbalanced family/block design")
    assert not output.exists()


GATE_SUMMARY_PATH = (
    Path(__file__).resolve().parents[1] / "scripts" / "dur050-summarize-gate-overhead.py"
)
GATE_SUMMARY_SPEC = importlib.util.spec_from_file_location(
    "dur050_summarize_gate_overhead", GATE_SUMMARY_PATH
)
assert GATE_SUMMARY_SPEC is not None and GATE_SUMMARY_SPEC.loader is not None
GATE_SUMMARY = importlib.util.module_from_spec(GATE_SUMMARY_SPEC)
GATE_SUMMARY_SPEC.loader.exec_module(GATE_SUMMARY)


def write_gate_inputs(root: Path) -> tuple[Path, Path]:
    root.mkdir()
    measurement_scope = "pool_begin_to_commit_return_including_pool_wait_network"
    manifest_path = root / "block-manifest.csv"
    timings_path = root / "transaction-timings.jsonl"
    modes = ("OFF", "ON", "ON", "OFF", "OFF", "ON")
    pairs = ("pair-1", "pair-1", "pair-2", "pair-2", "pair-3", "pair-3")
    manifest_rows = []
    timing_rows = []
    for rate in ("0.25", "0.5"):
        for index, (mode, pair_id) in enumerate(zip(modes, pairs, strict=True)):
            block_id = f"rate-{rate.replace('.', '')}-{index + 1:02d}"
            run_id = f"run-{block_id}"
            submissions_path = root / f"{block_id}-submissions.csv"
            with submissions_path.open("w", newline="", encoding="utf-8") as stream:
                writer = csv.DictWriter(
                    stream, fieldnames=["record_type", "workflow_id", "family", "outcome"]
                )
                writer.writeheader()
                for sample in range(100):
                    family = "seq-8" if sample % 2 == 0 else "fanout-8"
                    workflow_id = f"wf-{block_id}-{sample:03d}"
                    writer.writerow(
                        {
                            "record_type": "submission",
                            "workflow_id": workflow_id,
                            "family": family,
                            "outcome": "accepted",
                        }
                    )
                    for operation, outcome, state in (
                        ("submission", "created", ""),
                        ("terminal_transition", "committed", "SUCCEEDED"),
                    ):
                        timing_rows.append(
                            {
                                "run_id": run_id,
                                "workflow_id": workflow_id,
                                "operation": operation,
                                "outcome": outcome,
                                "state": state,
                                "measurement_clock": "runtime_process_monotonic",
                                "measurement_scope": measurement_scope,
                                "transaction_duration_us": 100 if mode == "OFF" else 120,
                                "log_source_host": f"app-{sample % 2 + 1}",
                            }
                        )
            manifest_rows.append(
                {
                    "rate": rate,
                    "block_id": block_id,
                    "pair_id": pair_id,
                    "mode": mode,
                    "run_id": run_id,
                    "block_started_at_utc": f"2026-09-25T00:{len(manifest_rows):02d}:00Z",
                    "submissions_csv": submissions_path.name,
                }
            )
    with manifest_path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(manifest_rows[0]))
        writer.writeheader()
        writer.writerows(manifest_rows)
    timings_path.write_text(
        "".join(json.dumps(row) + "\n" for row in timing_rows), encoding="utf-8"
    )
    return manifest_path, timings_path


def test_gate_overhead_summary_requires_complete_pairs_and_computes_observed_delta(
    tmp_path: Path,
) -> None:
    inputs = tmp_path / "inputs"
    manifest, timings = write_gate_inputs(inputs)
    output = tmp_path / "gate-summary"

    result = GATE_SUMMARY.summarize(manifest, timings, output)

    assert result["block_count"] == 12
    assert result["transaction_rows"] == 2_400
    assert len(result["paired_effect_summary"]) == 8
    assert all(
        row["median_on_minus_off_p50_us"] == 20
        and row["median_on_minus_off_p95_us"] == 20
        and row["matched_pairs"] == 3
        for row in result["paired_effect_summary"]
    )
    with (output / "gate-overhead.csv").open(newline="", encoding="utf-8") as stream:
        assert len(list(csv.DictReader(stream))) == 2_400


def test_gate_overhead_summary_rejects_missing_transaction_evidence(tmp_path: Path) -> None:
    manifest, timings = write_gate_inputs(tmp_path / "inputs")
    incomplete = tmp_path / "incomplete.jsonl"
    lines = timings.read_text(encoding="utf-8").splitlines()
    incomplete.write_text("\n".join(lines[:-1]) + "\n", encoding="utf-8")
    output = tmp_path / "gate-summary"

    try:
        GATE_SUMMARY.summarize(manifest, incomplete, output)
    except ValueError as error:
        assert "transaction rows" in str(error)
    else:
        raise AssertionError("incomplete transaction timing evidence was accepted")
    assert not output.exists()


def test_gate_overhead_summary_rejects_manifest_out_of_execution_order(tmp_path: Path) -> None:
    manifest, timings = write_gate_inputs(tmp_path / "inputs")
    rows = list(csv.DictReader(manifest.open(newline="", encoding="utf-8")))
    rows[1]["block_started_at_utc"] = rows[0]["block_started_at_utc"]
    with manifest.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    output = tmp_path / "out-of-order-summary"

    try:
        GATE_SUMMARY.summarize(manifest, timings, output)
    except ValueError as error:
        assert "strictly increasing execution start-time order" in str(error)
    else:
        raise AssertionError("gate summary accepted a manifest inconsistent with execution order")
    assert not output.exists()


FILTER_PATH = Path(__file__).resolve().parents[1] / "scripts" / "dur050-filter-txn-logs.py"
FILTER_SPEC = importlib.util.spec_from_file_location("dur050_filter_txn_logs", FILTER_PATH)
assert FILTER_SPEC is not None and FILTER_SPEC.loader is not None
FILTER = importlib.util.module_from_spec(FILTER_SPEC)
FILTER_SPEC.loader.exec_module(FILTER)


def test_transaction_log_filter_selects_only_requested_run_and_is_immutable(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    output = tmp_path / "timings.jsonl"
    row = {
        "schema": "dur050-transaction-timing.v1",
        "operation": "submission",
        "namespace": "dur050-calibration",
        "workflow_id": "dur050-calibration-run-a-000001",
        "outcome": "created",
        "measurement_clock": "runtime_process_monotonic",
        "measurement_scope": "pool_begin_to_commit_return_including_pool_wait_network",
        "transaction_duration_us": 42,
    }
    unrelated = {**row, "workflow_id": "dur050-calibration-run-a-extra-000001"}
    monkeypatch.setattr(
        sys,
        "stdin",
        io.StringIO(
            "runtime log prefix DUR050_TXN "
            + json.dumps(row)
            + "\n"
            + "runtime log prefix DUR050_TXN "
            + json.dumps(unrelated)
            + "\n"
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["dur050-filter-txn-logs.py", str(output), "dur050-calibration", "run-a"],
    )

    assert FILTER.main() == 0
    assert "DUR050_TRANSACTION_ROWS=1" in capsys.readouterr().out
    saved = json.loads(output.read_text(encoding="utf-8"))
    assert saved["workflow_id"] == row["workflow_id"]
    assert saved["run_id"] == "run-a"
    assert saved["log_source_host"]

    monkeypatch.setattr(sys, "stdin", io.StringIO(""))
    assert FILTER.main() == 1
