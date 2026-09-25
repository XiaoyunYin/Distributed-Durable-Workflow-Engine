#!/usr/bin/env python3
"""Fail closed on DUR-050 task-wide instance-hours plus the next block reserve."""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

EXPECTED_HOSTS = {
    "app-1": "c7i.large",
    "app-2": "c7i.large",
    "dependency": "m7i.large",
    "load-generator": "c7i.large",
}
TASK_LEDGER_RELATIVE_PATH = "experiments/m8/dur050-capacity-overload/dur050-cost-ledger.json"
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
TASK_LEDGER_PATH = REPOSITORY_ROOT / TASK_LEDGER_RELATIVE_PATH


class LedgerError(ValueError):
    """Raised when ledger input is incomplete or cannot support a safe check."""


def parse_utc(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise LedgerError(f"{field} must be an RFC3339 UTC timestamp ending in Z")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise LedgerError(f"{field} is not a valid RFC3339 UTC timestamp") from exc
    offset = parsed.utcoffset()
    if offset is None or offset.total_seconds() != 0:
        raise LedgerError(f"{field} must be UTC")
    return parsed


def decimal_value(value: Any, field: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise LedgerError(f"{field} must be a finite decimal") from exc
    if not result.is_finite():
        raise LedgerError(f"{field} must be a finite decimal")
    return result


def billable_seconds(start: datetime, end: datetime) -> int:
    duration = (end - start).total_seconds()
    if duration < 0:
        raise LedgerError("ledger interval ends before it starts")
    return max(60, math.ceil(duration))


def _validate_controls(manifest: dict[str, Any], now: datetime) -> Decimal:
    if manifest.get("schema") != "dur050-cost-manifest.v2":
        raise LedgerError("unsupported cost manifest schema")
    if manifest.get("task_ledger_path") != TASK_LEDGER_RELATIVE_PATH:
        raise LedgerError("manifest must use the single task-wide DUR-050 ledger")

    cap = decimal_value(manifest.get("budget_cap_usd"), "budget_cap_usd")
    if cap <= 0:
        raise LedgerError("budget_cap_usd must be positive")

    notification = manifest.get("budget_notification")
    if not isinstance(notification, dict):
        raise LedgerError("budget_notification record is missing")
    if (
        notification.get("budget_name") != "durable-engine-D022-aggregate-20260923"
        or notification.get("type") != "ACTUAL"
        or notification.get("threshold_type") != "ABSOLUTE_VALUE"
        or notification.get("state") != "OK"
        or notification.get("recipient_verified") is not True
    ):
        raise LedgerError(
            "the approved ACTUAL absolute notification on the D022 budget is not verified OK"
        )
    threshold = decimal_value(notification.get("threshold_usd"), "budget notification threshold")
    baseline = decimal_value(
        notification.get("aggregate_actual_at_setup_usd"), "aggregate actual at notification setup"
    )
    if threshold <= baseline or threshold - baseline > cap:
        raise LedgerError(
            "budget alert must fire no later than aggregate baseline plus the task cap"
        )

    allocation_tags = manifest.get("cost_allocation_tags")
    if not isinstance(allocation_tags, dict):
        raise LedgerError("cost_allocation_tags record is missing")
    activation_times: dict[str, datetime] = {}
    for tag_key in ("Task", "Environment"):
        tag = allocation_tags.get(tag_key)
        if not isinstance(tag, dict) or tag.get("status") != "Active":
            raise LedgerError(f"cost-allocation tag {tag_key} is not Active")
        activation_times[tag_key] = parse_utc(
            tag.get("activated_at_utc"), f"{tag_key}.activated_at_utc"
        )
        if activation_times[tag_key] > now:
            raise LedgerError(f"cost-allocation tag {tag_key} activation time is in the future")
    return cap


def evaluate(
    manifest: dict[str, Any],
    task_ledger: dict[str, Any],
    now: datetime,
    reserve_minutes: int,
    *,
    ledger_path: Path = TASK_LEDGER_PATH,
    ledger_path_override_used: bool = False,
) -> dict[str, Any]:
    if reserve_minutes < 1:
        raise LedgerError("reserve_minutes must be at least one for a paid block check")
    cap = _validate_controls(manifest, now)
    if task_ledger.get("schema") != "dur050-task-cost-ledger.v1":
        raise LedgerError("unsupported task-wide cost ledger schema")
    if task_ledger.get("task_id") != "DUR-050" or task_ledger.get("scope") != "task-wide":
        raise LedgerError("ledger must cover the complete DUR-050 task")
    ledger_cap = decimal_value(task_ledger.get("budget_cap_usd"), "task ledger budget_cap_usd")
    if ledger_cap != cap:
        raise LedgerError("campaign manifest and task-wide ledger budget caps differ")

    notification = manifest["budget_notification"]
    activation_times = {
        key: parse_utc(
            manifest["cost_allocation_tags"][key]["activated_at_utc"], f"{key}.activated_at_utc"
        )
        for key in ("Task", "Environment")
    }

    prices = task_ledger.get("prices_usd_per_hour")
    if not isinstance(prices, dict):
        raise LedgerError("task ledger prices_usd_per_hour must be an object")
    normalized_prices = {
        instance_type: decimal_value(prices.get(instance_type), f"price for {instance_type}")
        for instance_type in set(EXPECTED_HOSTS.values())
    }
    if any(price <= 0 for price in normalized_prices.values()):
        raise LedgerError("instance prices must be positive")

    roles = task_ledger.get("roles")
    if not isinstance(roles, dict) or set(roles) != set(EXPECTED_HOSTS):
        raise LedgerError("task ledger must contain exactly the four DUR-050 host roles")

    seen_ids: set[str] = set()
    projected_rows: list[dict[str, Any]] = []
    accrued_total = Decimal("0")
    projected_total = Decimal("0")
    reserve_delta = Decimal("0")
    backfill_needed = False
    open_intervals = 0

    for role, expected_type in EXPECTED_HOSTS.items():
        intervals = roles[role]
        if not isinstance(intervals, list) or not intervals:
            raise LedgerError(f"{role} must have a non-empty interval list")
        prior_start: datetime | None = None
        prior_end: datetime | None = None
        for index, interval in enumerate(intervals):
            if not isinstance(interval, dict):
                raise LedgerError(f"{role} interval {index} must be an object")
            if interval.get("instance_type") != expected_type:
                raise LedgerError(f"{role} interval {index} must use {expected_type}")
            instance_id = interval.get("instance_id")
            if not isinstance(instance_id, str) or not instance_id.strip():
                raise LedgerError(f"{role} interval {index} is missing instance_id")
            if instance_id in seen_ids:
                raise LedgerError(f"duplicate instance_id: {instance_id}")
            seen_ids.add(instance_id)

            started = parse_utc(
                interval.get("apply_started_at_utc"), f"{role}[{index}].apply_started_at_utc"
            )
            destroyed_value = interval.get("destroy_completed_at_utc")
            ended = (
                now
                if destroyed_value is None
                else parse_utc(destroyed_value, f"{role}[{index}].destroy_completed_at_utc")
            )
            if started > now or ended > now:
                raise LedgerError(f"{role} interval {index} has a timestamp in the future")
            if ended < started:
                raise LedgerError(f"{role} interval {index} ends before it starts")
            if prior_start is not None and started < prior_start:
                raise LedgerError(f"{role} intervals are not ordered by apply start")
            if prior_end is None and prior_start is not None:
                raise LedgerError(f"{role} has an open interval before a later interval")
            if prior_end is not None and started < prior_end:
                raise LedgerError(f"overlapping intervals for role {role}")
            if destroyed_value is None:
                if index != len(intervals) - 1:
                    raise LedgerError(f"only the last interval for role {role} may be open")
                open_intervals += 1

            if any(started < activation for activation in activation_times.values()):
                backfill_needed = True
            accrued = billable_seconds(started, ended)
            projected = accrued
            if destroyed_value is None:
                projected = billable_seconds(started, now + timedelta(minutes=reserve_minutes))
            price = normalized_prices[expected_type]
            accrued_cost = price * Decimal(accrued) / Decimal(3600)
            projected_cost = price * Decimal(projected) / Decimal(3600)
            accrued_total += accrued_cost
            projected_total += projected_cost
            reserve_delta += projected_cost - accrued_cost
            row = {
                "role": role,
                "interval_index": index,
                "cycle_id": interval.get("cycle_id"),
                "instance_id": instance_id,
                "instance_type": expected_type,
                "billable_seconds_to_check": accrued,
                "projected_billable_seconds_after_reserve": projected,
                "hourly_price_usd": str(price),
                "accrued_instance_cost_usd": str(accrued_cost.quantize(Decimal("0.000001"))),
                "projected_instance_cost_usd": str(projected_cost.quantize(Decimal("0.000001"))),
                "destroyed": destroyed_value is not None,
            }
            projected_rows.append(row)
            prior_start, prior_end = started, None if destroyed_value is None else ended

    return {
        "schema": "dur050-cost-ledger-check.v2",
        "ledger_scope": "DUR-050 task-wide shared ledger",
        "task_ledger_path": TASK_LEDGER_RELATIVE_PATH,
        "ledger_path_used": (
            TASK_LEDGER_RELATIVE_PATH
            if ledger_path.resolve() == TASK_LEDGER_PATH.resolve()
            else str(ledger_path.resolve())
        ),
        "ledger_path_override_used": ledger_path_override_used,
        "checked_at_utc": now.isoformat().replace("+00:00", "Z"),
        "reserve_minutes": reserve_minutes,
        "open_interval_count": open_intervals,
        "budget_cap_usd": str(cap),
        "accrued_instance_cost_usd": str(accrued_total.quantize(Decimal("0.000001"))),
        "next_block_reserve_usd": str(reserve_delta.quantize(Decimal("0.000001"))),
        "projected_instance_cost_usd": str(projected_total.quantize(Decimal("0.000001"))),
        "cost_allocation_backfill_needed": backfill_needed,
        "budget_notification_threshold_usd": str(notification["threshold_usd"]),
        "status": "PASS" if projected_total < cap else "FAIL",
        "intervals": projected_rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "manifest", type=Path, help="campaign manifest; accounting always uses the task-wide ledger"
    )
    parser.add_argument("--reserve-minutes", type=int, required=True)
    parser.add_argument("--now-utc", help="Deterministic test hook; RFC3339 UTC ending in Z")
    parser.add_argument(
        "--output", type=Path, help="Write a unique JSON check record; refuses overwrite"
    )
    parser.add_argument(
        "--ledger-path",
        type=Path,
        help="test-only ledger input; requires DUR050_ENABLE_TEST_LEDGER_OVERRIDE=1",
    )
    args = parser.parse_args()
    try:
        manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
        if args.ledger_path and os.environ.get("DUR050_ENABLE_TEST_LEDGER_OVERRIDE") != "1":
            raise LedgerError(
                "--ledger-path is test-only; set DUR050_ENABLE_TEST_LEDGER_OVERRIDE=1"
            )
        ledger_path = args.ledger_path or TASK_LEDGER_PATH
        ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
        now = parse_utc(args.now_utc, "--now-utc") if args.now_utc else datetime.now(UTC)
        result = evaluate(
            manifest,
            ledger,
            now,
            args.reserve_minutes,
            ledger_path=ledger_path,
            ledger_path_override_used=args.ledger_path is not None,
        )
        output = json.dumps(result, indent=2, sort_keys=True) + "\n"
        if args.output:
            try:
                with args.output.open("x", encoding="utf-8", newline="\n") as stream:
                    stream.write(output)
            except FileExistsError as exc:
                raise LedgerError(f"refusing to overwrite ledger check: {args.output}") from exc
    except (OSError, json.JSONDecodeError, LedgerError) as exc:
        print(json.dumps({"status": "BLOCKED", "error": str(exc)}, indent=2), file=sys.stderr)
        return 2

    print(output, end="")
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
