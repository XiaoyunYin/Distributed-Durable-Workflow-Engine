from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "dur050-cost-ledger.py"
SPEC = importlib.util.spec_from_file_location("dur050_cost_ledger", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
LEDGER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(LEDGER)


def manifest(*, cap: float = 75.0, destroyed: bool = False) -> dict[str, Any]:
    roles = [
        ("app-1", "c7i.large"),
        ("app-2", "c7i.large"),
        ("dependency", "m7i.large"),
        ("load-generator", "c7i.large"),
    ]
    return {
        "schema": "dur050-cost-manifest.v1",
        "budget_cap_usd": cap,
        "prices_usd_per_hour": {"c7i.large": 0.1113, "m7i.large": 0.1176},
        "budget_notification": {
            "budget_name": "durable-engine-D022-aggregate-20260923",
            "type": "ACTUAL",
            "threshold_type": "ABSOLUTE_VALUE",
            "aggregate_actual_at_setup_usd": 1.476,
            "threshold_usd": 1.476 + cap - 0.01,
            "state": "OK",
            "recipient_verified": True,
        },
        "cost_allocation_tags": {
            "Task": {"status": "Active", "activated_at_utc": "2026-09-24T23:00:00Z"},
            "Environment": {"status": "Active", "activated_at_utc": "2026-09-24T23:00:00Z"},
        },
        "hosts": [
            {
                "role": role,
                "instance_type": instance_type,
                "instance_id": f"i-{index:017d}",
                "apply_started_at_utc": "2026-09-25T00:00:00Z",
                "destroy_completed_at_utc": "2026-09-25T00:30:00Z" if destroyed else None,
            }
            for index, (role, instance_type) in enumerate(roles, start=1)
        ],
    }


def test_ledger_counts_each_host_and_reserves_the_next_block() -> None:
    result = LEDGER.evaluate(
        manifest(),
        datetime(2026, 9, 25, 1, 0, tzinfo=UTC),
        reserve_minutes=60,
    )

    assert result["status"] == "PASS"
    assert Decimal(result["accrued_instance_cost_usd"]) == Decimal("0.451500")
    assert Decimal(result["projected_instance_cost_usd"]) == Decimal("0.903000")
    assert len(result["hosts"]) == 4


def test_ledger_blocks_when_next_block_reserve_reaches_cap() -> None:
    result = LEDGER.evaluate(
        manifest(cap=0.90),
        datetime(2026, 9, 25, 1, 0, tzinfo=UTC),
        reserve_minutes=60,
    )

    assert result["status"] == "FAIL"
    assert Decimal(result["projected_instance_cost_usd"]) >= Decimal("0.90")


def test_destroyed_hosts_do_not_accrue_next_block_reserve() -> None:
    result = LEDGER.evaluate(
        manifest(destroyed=True),
        datetime(2026, 9, 25, 1, 0, tzinfo=UTC),
        reserve_minutes=60,
    )

    assert result["status"] == "PASS"
    assert Decimal(result["accrued_instance_cost_usd"]) == Decimal("0.225750")
    assert result["next_block_reserve_usd"] == "0.000000"


def test_incomplete_host_ledger_fails_closed() -> None:
    incomplete = manifest()
    incomplete["hosts"][0]["instance_id"] = None

    try:
        LEDGER.evaluate(incomplete, datetime(2026, 9, 25, 1, 0, tzinfo=UTC), 30)
    except LEDGER.LedgerError as exc:
        assert "app-1 is not provisioned" in str(exc)
    else:
        raise AssertionError("incomplete ledger must fail closed")


def test_duplicate_host_ids_are_rejected() -> None:
    duplicate = manifest()
    duplicate["hosts"][1]["instance_id"] = duplicate["hosts"][0]["instance_id"]

    try:
        LEDGER.evaluate(duplicate, datetime(2026, 9, 25, 1, 0, tzinfo=UTC), 30)
    except LEDGER.LedgerError as exc:
        assert "duplicate instance_id" in str(exc)
    else:
        raise AssertionError("duplicate host IDs must be rejected")


def test_unverified_budget_or_tag_controls_fail_closed() -> None:
    pending = manifest()
    pending["budget_notification"]["state"] = "PENDING_USER_APPROVAL"

    try:
        LEDGER.evaluate(pending, datetime(2026, 9, 25, 1, 0, tzinfo=UTC), 30)
    except LEDGER.LedgerError as exc:
        assert "notification on the D022 budget is not verified OK" in str(exc)
    else:
        raise AssertionError("an unverified spend alert must block a paid block")

    inactive_tag = manifest()
    inactive_tag["cost_allocation_tags"]["Task"]["status"] = "Inactive"
    try:
        LEDGER.evaluate(inactive_tag, datetime(2026, 9, 25, 1, 0, tzinfo=UTC), 30)
    except LEDGER.LedgerError as exc:
        assert "Task is not Active" in str(exc)
    else:
        raise AssertionError("inactive cost-allocation tags must block a paid block")


def test_reports_required_cost_tag_backfill() -> None:
    late_activation = manifest()
    late_activation["cost_allocation_tags"]["Environment"]["activated_at_utc"] = (
        "2026-09-25T00:30:00Z"
    )

    result = LEDGER.evaluate(
        late_activation,
        datetime(2026, 9, 25, 1, 0, tzinfo=UTC),
        reserve_minutes=10,
    )

    assert result["cost_allocation_backfill_needed"] is True


def test_cli_writes_unique_per_block_check_record(tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest()), encoding="utf-8")
    output_path = tmp_path / "block-check.json"

    completed = subprocess.run(
        [
            sys.executable,
            str(MODULE_PATH),
            str(manifest_path),
            "--reserve-minutes",
            "30",
            "--now-utc",
            "2026-09-25T01:00:00Z",
            "--output",
            str(output_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert json.loads(output_path.read_text(encoding="utf-8"))["status"] == "PASS"

    repeated = subprocess.run(
        [
            sys.executable,
            str(MODULE_PATH),
            str(manifest_path),
            "--reserve-minutes",
            "30",
            "--now-utc",
            "2026-09-25T01:00:00Z",
            "--output",
            str(output_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert repeated.returncode == 2
    assert "refusing to overwrite" in repeated.stderr
