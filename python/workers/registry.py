"""Versioned activity registry used by the bounded M2 runner."""

from __future__ import annotations

import hashlib
import os
import time
from collections.abc import Callable
from typing import Any

Activity = Callable[[Any], Any]


class ActivityRegistry:
    def __init__(self) -> None:
        self._activities: dict[str, Activity] = {}

    def register(self, name: str, version: str, activity: Activity) -> None:
        if not name or not version:
            raise ValueError("activity name and version are required")
        key = self.key(name, version)
        if key in self._activities:
            raise ValueError(f"activity already registered: {key}")
        self._activities[key] = activity

    def resolve(self, name: str, version: str) -> Activity:
        try:
            return self._activities[self.key(name, version)]
        except KeyError as error:
            raise LookupError(f"activity is not registered: {name}@{version}") from error

    @staticmethod
    def key(name: str, version: str) -> str:
        return f"{name}@{version}"


def default_registry() -> ActivityRegistry:
    registry = ActivityRegistry()
    registry.register("pure.echo", "v1", lambda value: value)
    registry.register(
        "pure.add", "v1", lambda value: sum(value) if isinstance(value, list) else value
    )
    # Opt-in fixture activity used only by the deployed DUR-048 recovery
    # episode. Normal deployments leave the delay at zero.
    delay_ms = int(os.getenv("DUR048_ACTIVITY_DELAY_MS", "0"))
    if delay_ms > 0:

        def delayed(value: Any) -> Any:
            time.sleep(delay_ms / 1000)
            return value

        registry.register("dur048.sleep", "v1", delayed)

    dur050_enabled = os.getenv("DUR050_ENABLE_SHA256_ACTIVITY", "0")
    if dur050_enabled not in {"0", "1"}:
        raise ValueError("DUR050_ENABLE_SHA256_ACTIVITY must be 0 or 1")
    if dur050_enabled == "1":
        raw_work_units = os.getenv("DUR050_SHA256_WORK_UNITS", "5000")
        try:
            work_units = int(raw_work_units)
        except ValueError as error:
            raise ValueError("DUR050_SHA256_WORK_UNITS must be a positive integer") from error
        if work_units <= 0 or work_units > 1_000_000:
            raise ValueError("DUR050_SHA256_WORK_UNITS must be between 1 and 1000000")

        def sha256_workload(_value: Any) -> dict[str, Any]:
            block = bytearray(range(256))
            digest = bytes(32)
            for round_index in range(work_units):
                offset = round_index % len(block)
                block[offset] = (block[offset] + digest[round_index % len(digest)]) & 0xFF
                digest = hashlib.sha256(block).digest()
            return {"digest": digest.hex(), "work_units": work_units}

        # Workflow definitions use node IDs as activity names. These explicit
        # per-node aliases all resolve to the same versioned campaign activity
        # implementation while keeping the graph's node IDs unique.
        activity_names = [
            *(f"dur050.sha256.seq8.{index}" for index in range(8)),
            *(f"dur050.sha256.fanout8.{index}" for index in range(8)),
        ]
        for name in activity_names:
            registry.register(name, "v1", sha256_workload)
    return registry
