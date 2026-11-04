"""Process-local admission and shutdown; durable claims remain PostgreSQL authority."""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Callable
from typing import Any


class WorkerLifecycle:
    def __init__(self, drain_seconds: float = 30.0, legacy: bool = False) -> None:
        if not 0 < drain_seconds <= 30:
            raise ValueError("worker drain deadline must be in (0, 30] seconds")
        self.drain = threading.Event()
        self.abort = threading.Event()
        self.deadline: float | None = None
        self.drain_seconds = drain_seconds
        self.legacy = legacy
        self.lock = threading.RLock()
        self.in_flight = 0
        self.rebalances = 0

    def begin_drain(self) -> None:
        with self.lock:
            if not self.drain.is_set():
                self.deadline = time.monotonic() + self.drain_seconds
                self.drain.set()
            if self.legacy:
                self.abort.set()

    def remaining(self) -> float:
        with self.lock:
            return max(0.0, self.deadline - time.monotonic()) if self.deadline is not None else 30.0

    def aborted(self) -> bool:
        if self.drain.is_set() and self.remaining() <= 0:
            self.abort.set()
        return self.abort.is_set()

    def can_start(self) -> bool:
        return not self.drain.is_set() and not self.aborted()

    def render(self) -> str:
        with self.lock:
            return (
                f"durable_worker_in_flight {self.in_flight}\n"
                f"durable_worker_draining {int(self.drain.is_set())}\n"
                f"durable_worker_aborted {int(self.aborted())}\n"
                f"durable_worker_rebalances_total {self.rebalances}\n"
            )


class BoundedExecutor:
    """No queue, no interpreter-exit join: at most one daemon per admitted slot."""

    def __init__(self, lifecycle: WorkerLifecycle, capacity: int) -> None:
        self.lifecycle = lifecycle
        self.capacity = threading.BoundedSemaphore(capacity)

    def submit(self, run: Callable[[], Any]) -> threading.Event:
        if not self.capacity.acquire(blocking=False):
            raise RuntimeError("activity executor is full")
        done = threading.Event()
        with self.lifecycle.lock:
            self.lifecycle.in_flight += 1

        def execute() -> None:
            try:
                run()
            finally:
                with self.lifecycle.lock:
                    self.lifecycle.in_flight -= 1
                self.capacity.release()
                done.set()

        threading.Thread(target=execute, name="activity-executor", daemon=True).start()
        return done


def worker_identity() -> str:
    return os.getenv("POD_NAME") or os.getenv("WORKER_ID") or "worker"
