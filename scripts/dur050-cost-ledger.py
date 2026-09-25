#!/usr/bin/env python3
"""Fail closed when the DUR-050 host-hour ledger plus the next block reserve reaches its cap."""

from __future__ import annotations

import argparse
import json
import math
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


class LedgerError(ValueError):
    """Raised when ledger input is incomplete or cannot support a safe check."""


def parse_utc(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise LedgerError(f"{field} must be an RFC3339 UTC timestamp ending in Z")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise LedgerError(f"{field} is not a valid RFC3339 timestamp") from exc
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


def evaluate(manifest: dict[str, Any], now: datetime, reserve_minutes: int) -> dict[str, Any]:
    if manifest.get("schema") != "dur050-cost-manifest.v1":
        raise LedgerError("unsupported cost manifest schema")
    if reserve_minutes < 1:
        raise LedgerError("reserve_minutes must be at least one for a paid block check")

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

    prices = manifest.get("prices_usd_per_hour")
    if not isinstance(prices, dict):
        raise LedgerError("prices_usd_per_hour must be an object")
    normalized_prices = {
        instance_type: decimal_value(prices.get(instance_type), f"price for {instance_type}")
        for instance_type in set(EXPECTED_HOSTS.values())
    }
    if any(price <= 0 for price in normalized_prices.values()):
        raise LedgerError("instance prices must be positive")

    hosts = manifest.get("hosts")
    if not isinstance(hosts, list):
        raise LedgerError("hosts must be an array")
    by_role: dict[str, dict[str, Any]] = {}
    for host in hosts:
        if not isinstance(host, dict):
            raise LedgerError("each host entry must be an object")
        role = host.get("role")
        if role not in EXPECTED_HOSTS or role in by_role:
            raise LedgerError(f"unexpected or duplicate host role: {role!r}")
        by_role[role] = host
    if set(by_role) != set(EXPECTED_HOSTS):
        missing = sorted(set(EXPECTED_HOSTS) - set(by_role))
        raise LedgerError(f"host ledger is incomplete; missing roles: {', '.join(missing)}")

    seen_ids: set[str] = set()
    projected_rows: list[dict[str, Any]] = []
    accrued_total = Decimal("0")
    projected_total = Decimal("0")
    reserve_delta = Decimal("0")
    backfill_needed = False

    for role, expected_type in EXPECTED_HOSTS.items():
        host = by_role[role]
        if host.get("instance_type") != expected_type:
            raise LedgerError(f"{role} must use the recorded profile type {expected_type}")
        instance_id = host.get("instance_id")
        if not isinstance(instance_id, str) or not instance_id.strip():
            raise LedgerError(f"{role} is not provisioned: instance_id is missing")
        if instance_id in seen_ids:
            raise LedgerError(f"duplicate instance_id: {instance_id}")
        seen_ids.add(instance_id)

        started = parse_utc(host.get("apply_started_at_utc"), f"{role}.apply_started_at_utc")
        if any(started < activation for activation in activation_times.values()):
            backfill_needed = True
        destroyed_value = host.get("destroy_completed_at_utc")
        ended = (
            now
            if destroyed_value is None
            else parse_utc(destroyed_value, f"{role}.destroy_completed_at_utc")
        )
        if started > now or ended > now:
            raise LedgerError(f"{role} has a timestamp in the future")
        if ended < started:
            raise LedgerError(f"{role} destroy timestamp precedes apply timestamp")

        accrued = billable_seconds(started, ended)
        projected = accrued
        if destroyed_value is None:
            projected = billable_seconds(
                started,
                now + timedelta(minutes=reserve_minutes),
            )
        price = normalized_prices[expected_type]
        accrued_cost = price * Decimal(accrued) / Decimal(3600)
        projected_cost = price * Decimal(projected) / Decimal(3600)
        accrued_total += accrued_cost
        projected_total += projected_cost
        reserve_delta += projected_cost - accrued_cost
        projected_rows.append(
            {
                "role": role,
                "instance_id": instance_id,
                "instance_type": expected_type,
                "billable_seconds_to_check": accrued,
                "projected_billable_seconds_after_reserve": projected,
                "hourly_price_usd": str(price),
                "accrued_instance_cost_usd": str(accrued_cost.quantize(Decimal("0.000001"))),
                "projected_instance_cost_usd": str(projected_cost.quantize(Decimal("0.000001"))),
                "destroyed": destroyed_value is not None,
            }
        )

    return {
        "schema": "dur050-cost-ledger-check.v1",
        "checked_at_utc": now.isoformat().replace("+00:00", "Z"),
        "reserve_minutes": reserve_minutes,
        "budget_cap_usd": str(cap),
        "accrued_instance_cost_usd": str(accrued_total.quantize(Decimal("0.000001"))),
        "next_block_reserve_usd": str(reserve_delta.quantize(Decimal("0.000001"))),
        "projected_instance_cost_usd": str(projected_total.quantize(Decimal("0.000001"))),
        "cost_allocation_backfill_needed": backfill_needed,
        "status": "PASS" if projected_total < cap else "FAIL",
        "hosts": projected_rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--reserve-minutes", type=int, required=True)
    parser.add_argument("--now-utc", help="Deterministic test hook; RFC3339 UTC ending in Z")
    parser.add_argument(
        "--output", type=Path, help="Write a unique JSON check record; refuses overwrite"
    )
    args = parser.parse_args()
    try:
        manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
        now = parse_utc(args.now_utc, "--now-utc") if args.now_utc else datetime.now(UTC)
        result = evaluate(manifest, now, args.reserve_minutes)
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
