from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "dur050-cost-ledger.py"
SPEC = importlib.util.spec_from_file_location("dur050_cost_ledger", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
LEDGER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(LEDGER)

ROLES = {
    "app-1": "c7i.large",
    "app-2": "c7i.large",
    "dependency": "m7i.large",
    "load-generator": "c7i.large",
}


def campaign_manifest(*, campaign_id: str = "first", cap: float = 75.0) -> dict:
    return {
        "schema": "dur050-cost-manifest.v2",
        "campaign_id": campaign_id,
        "task_ledger_path": LEDGER.TASK_LEDGER_RELATIVE_PATH,
        "budget_cap_usd": cap,
        "budget_notification": {
            "budget_name": "durable-engine-D022-aggregate-20260923",
            "type": "ACTUAL",
            "threshold_type": "ABSOLUTE_VALUE",
            "aggregate_actual_at_setup_usd": 1.476,
            "threshold_usd": 76.47,
            "state": "OK",
            "recipient_verified": True,
        },
        "cost_allocation_tags": {
            "Task": {"status": "Active", "activated_at_utc": "2026-09-24T23:00:00Z"},
            "Environment": {
                "status": "Active",
                "activated_at_utc": "2026-09-24T23:00:00Z",
            },
        },
    }


def interval(
    role: str,
    instance_id: str,
    start: str,
    end: str | None,
    *,
    cycle_id: str,
) -> dict:
    return {
        "cycle_id": cycle_id,
        "instance_id": instance_id,
        "instance_type": ROLES[role],
        "apply_started_at_utc": start,
        "destroy_completed_at_utc": end,
    }


def task_ledger(*, cycles: int = 1) -> dict:
    first_start = "2026-09-25T10:34:38.8773119Z"
    first_end = "2026-09-25T10:45:53Z"
    ids = {
        "app-1": "i-03a77d4d089b70cb8",
        "app-2": "i-03fc3b13714dc4134",
        "dependency": "i-05027ac0bdf9f3816",
        "load-generator": "i-09813309975a6d24b",
    }
    roles: dict[str, list[dict]] = {}
    for role in ROLES:
        role_intervals = [
            interval(role, ids[role], first_start, first_end, cycle_id="cycle-1-aa7de7e")
        ]
        if cycles >= 2:
            role_intervals.append(
                interval(
                    role,
                    f"i-2-{role}",
                    "2026-09-25T12:00:00Z",
                    None,
                    cycle_id="cycle-2-test",
                )
            )
        roles[role] = role_intervals
    return {
        "schema": "dur050-task-cost-ledger.v1",
        "task_id": "DUR-050",
        "scope": "task-wide",
        "budget_cap_usd": 75.0,
        "prices_usd_per_hour": {"c7i.large": 0.1113, "m7i.large": 0.1176},
        "roles": roles,
    }


def evaluate(data: dict, *, now: str = "2026-09-25T13:00:00Z", reserve: int = 60) -> dict:
    return LEDGER.evaluate(
        campaign_manifest(),
        data,
        datetime.fromisoformat(now.replace("Z", "+00:00")),
        reserve,
    )


def test_task_ledger_counts_historical_cycle_and_reserves_only_open_intervals() -> None:
    result = evaluate(task_ledger(cycles=2))

    assert result["status"] == "PASS"
    assert result["ledger_scope"] == "DUR-050 task-wide shared ledger"
    assert result["open_interval_count"] == 4
    assert Decimal(result["accrued_instance_cost_usd"]) > Decimal("0.084")
    assert Decimal(result["next_block_reserve_usd"]) > 0
    assert len(result["intervals"]) == 8


def test_two_cycles_per_role_and_app1_replacement_keep_every_interval() -> None:
    data = task_ledger(cycles=2)
    data["roles"]["app-1"].append(
        interval(
            "app-1",
            "i-3-app-1-replacement",
            "2026-09-25T12:30:00Z",
            None,
            cycle_id="cycle-2-test",
        )
    )
    # Close the prior app-1 interval at the replacement boundary.
    data["roles"]["app-1"][1]["destroy_completed_at_utc"] = "2026-09-25T12:30:00Z"
    result = evaluate(data, now="2026-09-25T13:00:00Z")

    app1_rows = [row for row in result["intervals"] if row["role"] == "app-1"]
    assert len(app1_rows) == 3
    assert [row["instance_id"] for row in app1_rows] == [
        "i-03a77d4d089b70cb8",
        "i-2-app-1",
        "i-3-app-1-replacement",
    ]
    assert app1_rows[-1]["destroyed"] is False


def test_overlapping_intervals_for_a_role_are_rejected() -> None:
    data = task_ledger(cycles=2)
    data["roles"]["app-1"][1]["apply_started_at_utc"] = "2026-09-25T10:40:00Z"

    with pytest.raises(LEDGER.LedgerError, match="overlapping intervals for role app-1"):
        evaluate(data)


def test_reused_instance_id_across_intervals_is_rejected() -> None:
    data = task_ledger(cycles=2)
    data["roles"]["app-1"][1]["instance_id"] = data["roles"]["app-2"][0]["instance_id"]

    with pytest.raises(LEDGER.LedgerError, match="duplicate instance_id"):
        evaluate(data)


def test_open_interval_must_be_last_for_its_role() -> None:
    data = task_ledger(cycles=2)
    data["roles"]["app-1"][0]["destroy_completed_at_utc"] = None

    with pytest.raises(
        LEDGER.LedgerError, match="only the last interval for role app-1 may be open"
    ):
        evaluate(data)


def test_cap_rejects_a_task_wide_total_at_or_over_the_cap() -> None:
    data = task_ledger()
    data["budget_cap_usd"] = 0.08
    manifest = campaign_manifest(cap=0.08)
    manifest["budget_notification"]["threshold_usd"] = 1.55
    result = LEDGER.evaluate(
        manifest, data, datetime(2026, 9, 25, 13, 0, tzinfo=UTC), reserve_minutes=1
    )
    assert result["status"] == "FAIL"
    assert Decimal(result["projected_instance_cost_usd"]) >= Decimal("0.08")


def test_missing_role_or_interval_fails_closed() -> None:
    data = task_ledger()
    del data["roles"]["load-generator"]
    with pytest.raises(LEDGER.LedgerError, match="exactly the four DUR-050 host roles"):
        evaluate(data)

    data = task_ledger()
    data["roles"]["load-generator"] = []
    with pytest.raises(
        LEDGER.LedgerError, match="load-generator must have a non-empty interval list"
    ):
        evaluate(data)


def test_100_hours_destroyed_then_24_hours_open_costs_about_55_99() -> None:
    start = "2026-01-01T00:00:00Z"
    first_end = "2026-01-05T04:00:00Z"  # 100 hours
    second_start = "2026-01-09T08:00:00Z"
    now = "2026-01-10T08:00:00Z"  # 24 hours open
    data = {
        "schema": "dur050-task-cost-ledger.v1",
        "task_id": "DUR-050",
        "scope": "task-wide",
        "budget_cap_usd": 75.0,
        "prices_usd_per_hour": {"c7i.large": 0.1113, "m7i.large": 0.1176},
        "roles": {},
    }
    for role in ROLES:
        rows = [interval(role, f"i-100h-{role}", start, first_end, cycle_id="cycle-a")]
        if role == "app-1":
            replacement_at = "2026-01-09T18:00:00Z"
            rows.extend(
                [
                    interval(
                        role,
                        "i-app-1-before-replacement",
                        second_start,
                        replacement_at,
                        cycle_id="cycle-b",
                    ),
                    interval(
                        role, "i-app-1-after-replacement", replacement_at, None, cycle_id="cycle-b"
                    ),
                ]
            )
        else:
            rows.append(interval(role, f"i-24h-{role}", second_start, None, cycle_id="cycle-b"))
        data["roles"][role] = rows

    manifest = campaign_manifest()
    for key in ("Task", "Environment"):
        manifest["cost_allocation_tags"][key]["activated_at_utc"] = "2025-12-31T00:00:00Z"
    result = LEDGER.evaluate(
        manifest,
        data,
        datetime.fromisoformat(now.replace("Z", "+00:00")),
        reserve_minutes=1,
    )

    assert Decimal(result["accrued_instance_cost_usd"]).quantize(Decimal("0.01")) == Decimal(
        "55.99"
    )
    assert result["open_interval_count"] == 4
    assert len([row for row in result["intervals"] if row["role"] == "app-1"]) == 3


def test_new_campaign_manifest_cannot_reset_or_select_another_task_ledger() -> None:
    data = task_ledger()
    first = campaign_manifest(campaign_id="pilot-one")
    second = campaign_manifest(campaign_id="a-new-manifest")
    # Every campaign manifest is evaluated against the same task ledger loaded
    # from the single repository path, so a new directory cannot zero history.
    first_result = LEDGER.evaluate(
        first, data, datetime(2026, 9, 25, 13, 0, tzinfo=UTC), reserve_minutes=10
    )
    second_result = LEDGER.evaluate(
        second, data, datetime(2026, 9, 25, 13, 0, tzinfo=UTC), reserve_minutes=10
    )
    assert second_result["accrued_instance_cost_usd"] == first_result["accrued_instance_cost_usd"]
    assert Decimal(second_result["accrued_instance_cost_usd"]) > 0

    second["task_ledger_path"] = (
        "experiments/m8/dur050-capacity-overload/new/dur050-cost-ledger.json"
    )
    with pytest.raises(LEDGER.LedgerError, match="single task-wide DUR-050 ledger"):
        LEDGER.evaluate(second, data, datetime(2026, 9, 25, 13, 0, tzinfo=UTC), 10)


def test_cap_alert_and_tag_controls_fail_closed() -> None:
    manifest = campaign_manifest()
    manifest["budget_notification"]["state"] = "PENDING_USER_APPROVAL"
    with pytest.raises(
        LEDGER.LedgerError, match="notification on the D022 budget is not verified OK"
    ):
        LEDGER.evaluate(manifest, task_ledger(), datetime(2026, 9, 25, 13, 0, tzinfo=UTC), 10)

    manifest = campaign_manifest()
    manifest["cost_allocation_tags"]["Task"]["status"] = "Inactive"
    with pytest.raises(LEDGER.LedgerError, match="cost-allocation tag Task is not Active"):
        LEDGER.evaluate(manifest, task_ledger(), datetime(2026, 9, 25, 13, 0, tzinfo=UTC), 10)


def test_cli_reads_one_shared_ledger_and_writes_unique_check_record(tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(campaign_manifest()), encoding="utf-8")
    ledger_path = tmp_path / "task-ledger.json"
    ledger = task_ledger()
    ledger_path.write_text(json.dumps(ledger), encoding="utf-8")
    output_path = tmp_path / "block-check.json"
    now_arg = "2026-09-25T13:00:00Z"
    expected = LEDGER.evaluate(
        campaign_manifest(),
        ledger,
        datetime.fromisoformat("2026-09-25T13:00:00+00:00"),
        30,
        ledger_path=ledger_path,
        ledger_path_override_used=True,
    )
    env = os.environ.copy()
    env["DUR050_ENABLE_TEST_LEDGER_OVERRIDE"] = "1"
    completed = subprocess.run(
        [
            sys.executable,
            str(MODULE_PATH),
            str(manifest_path),
            "--reserve-minutes",
            "30",
            "--now-utc",
            now_arg,
            "--ledger-path",
            str(ledger_path),
            "--output",
            str(output_path),
        ],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    assert completed.returncode == 0, completed.stderr
    result = json.loads(output_path.read_text(encoding="utf-8"))
    assert result["task_ledger_path"] == LEDGER.TASK_LEDGER_RELATIVE_PATH
    assert result["ledger_path_used"] == str(ledger_path.resolve())
    assert result["ledger_path_override_used"] is True
    assert result["accrued_instance_cost_usd"] == expected["accrued_instance_cost_usd"]
    assert result["projected_instance_cost_usd"] == expected["projected_instance_cost_usd"]
    assert result["open_interval_count"] == expected["open_interval_count"]

    repeated = subprocess.run(
        [
            sys.executable,
            str(MODULE_PATH),
            str(manifest_path),
            "--reserve-minutes",
            "30",
            "--now-utc",
            now_arg,
            "--ledger-path",
            str(ledger_path),
            "--output",
            str(output_path),
        ],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    assert repeated.returncode == 2
    assert "refusing to overwrite" in repeated.stderr


def test_cli_rejects_test_ledger_override_without_explicit_test_environment(tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(campaign_manifest()), encoding="utf-8")
    ledger_path = tmp_path / "ledger.json"
    ledger_path.write_text(json.dumps(task_ledger()), encoding="utf-8")
    env = os.environ.copy()
    env.pop("DUR050_ENABLE_TEST_LEDGER_OVERRIDE", None)
    completed = subprocess.run(
        [
            sys.executable,
            str(MODULE_PATH),
            str(manifest_path),
            "--reserve-minutes",
            "30",
            "--ledger-path",
            str(ledger_path),
        ],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    assert completed.returncode == 2
    assert "test-only" in completed.stderr


def test_cli_returns_fail_when_fixture_projection_reaches_cap(tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(campaign_manifest()), encoding="utf-8")
    ledger = task_ledger()
    for _role, role_intervals in ledger["roles"].items():
        role_intervals[0]["apply_started_at_utc"] = "2026-09-10T00:00:00Z"
        role_intervals[0]["destroy_completed_at_utc"] = None
    ledger_path = tmp_path / "over-cap-ledger.json"
    ledger_path.write_text(json.dumps(ledger), encoding="utf-8")
    output_path = tmp_path / "over-cap-check.json"
    env = os.environ.copy()
    env["DUR050_ENABLE_TEST_LEDGER_OVERRIDE"] = "1"
    completed = subprocess.run(
        [
            sys.executable,
            str(MODULE_PATH),
            str(manifest_path),
            "--reserve-minutes",
            "30",
            "--now-utc",
            "2026-09-25T13:00:00Z",
            "--ledger-path",
            str(ledger_path),
            "--output",
            str(output_path),
        ],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    assert completed.returncode == 1
    result = json.loads(output_path.read_text(encoding="utf-8"))
    assert result["status"] == "FAIL"
    assert Decimal(result["projected_instance_cost_usd"]) >= Decimal("75")
    assert result["ledger_path_override_used"] is True


def test_committed_ledger_evidence_parses_and_matches_active_cycles() -> None:
    ledger = json.loads(LEDGER.TASK_LEDGER_PATH.read_text(encoding="utf-8"))
    assert ledger["schema"] == "dur050-task-cost-ledger.v1"
    open_cycles = {
        interval["cycle_id"]
        for intervals in ledger["roles"].values()
        for interval in intervals
        if interval["destroy_completed_at_utc"] is None
    }

    campaign_root = LEDGER.TASK_LEDGER_PATH.parent
    active_cycles: set[str] = set()
    for manifest_path in campaign_root.rglob("cost-manifest.json"):
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        for key, cycle in manifest.items():
            if not key.startswith("provisioning_cycle_") or not isinstance(cycle, dict):
                continue
            if cycle.get("apply_started_at_utc") and not cycle.get(
                "destroy_completed_observed_at_utc"
            ):
                active_cycles.add(cycle.get("cycle_id") or key)

    normalized_open = {value for value in open_cycles if value}
    assert normalized_open == active_cycles
