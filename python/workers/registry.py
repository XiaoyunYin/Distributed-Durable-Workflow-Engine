"""Versioned activity registry used by the bounded M2 runner."""

from __future__ import annotations

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
    return registry
