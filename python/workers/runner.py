"""Bounded, heartbeat-aware activity runner for direct M2 dispatch tests."""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial
from typing import Any, Protocol

from workers import k6_evidence
from workers.control import Claim, ControlClient, ControlError
from workers.k6_effect import EffectAdapter
from workers.recorded_activity import RecordedActivity, recorded_provider_modes
from workers.recorded_retry import complete_resilient
from workers.registry import K6_ACTIVITY, K6_EFFECT_CLASS, K6_VERSION, ActivityRegistry
from workers.witness import configured_witness

LOG = logging.getLogger(__name__)
DeploymentHook = Callable[[str, dict[str, str]], None]


class WorkerControl(Protocol):
    def claim(
        self,
        workflow_id: str,
        node_id: str,
        iteration: int,
        worker_id: str,
        request_id: str,
        attempt_lease_ms: int = 60_000,
    ) -> Claim: ...

    def heartbeat(
        self,
        workflow_id: str,
        node_id: str,
        iteration: int,
        attempt_number: int,
        claim_token: str,
        extension_ms: int = 60_000,
    ) -> str: ...

    def result(
        self,
        workflow_id: str,
        node_id: str,
        iteration: int,
        attempt_number: int,
        claim_token: str,
        attempt_state: str,
        payload: Any,
        event_type: str = "",
        reconciliation_ref: str = "",
    ) -> dict[str, Any]: ...

    def provider_exchange(
        self, workflow_id: str, node_id: str, iteration: int, action: str, body: dict[str, Any]
    ) -> dict[str, Any]: ...


@dataclass(frozen=True)
class ActivityTask:
    workflow_id: str
    node_id: str
    iteration: int
    worker_id: str
    request_id: str
    activity_name: str
    activity_version: str
    input: Any
    attempt_lease_ms: int = 60_000


class ActivityRunner:
    def __init__(
        self,
        control: WorkerControl,
        registry: ActivityRegistry,
        max_workers: int = 4,
        heartbeat_interval: float = 5.0,
        k6: EffectAdapter | None = None,
        deployment_hook: DeploymentHook | None = None,
    ) -> None:
        if max_workers <= 0 or heartbeat_interval <= 0:
            raise ValueError("max_workers and heartbeat_interval must be positive")
        self.control = control
        self.registry = registry
        self.max_workers = max_workers
        self.heartbeat_interval = heartbeat_interval
        self.k6 = k6
        self.deployment_hook = deployment_hook

    def aborted(self) -> bool:
        check = getattr(self.control, "aborted", None)
        return bool(check is not None and check())

    def run_task(self, task: ActivityTask) -> dict[str, Any]:
        # Resolve before claiming. A configuration error must not create a
        # claimed attempt that can only ever be retried.
        cooperating = (task.activity_name, task.activity_version) == (K6_ACTIVITY, K6_VERSION)
        activity = (
            None
            if cooperating
            else self.registry.resolve(task.activity_name, task.activity_version)
        )
        if cooperating and (self.k6 is None or task.node_id != K6_ACTIVITY):
            raise ControlError(403, "K6_ACTIVITY_DISABLED", "K6 activity unavailable")
        recorded = isinstance(activity, RecordedActivity)
        j4, resilience = recorded_provider_modes()
        lease_ms = 12_000 if j4 else task.attempt_lease_ms
        heartbeat_interval = 2.0 if j4 else self.heartbeat_interval
        claim_started = time.monotonic_ns() if cooperating else 0
        try:
            claim = self.control.claim(
                task.workflow_id,
                task.node_id,
                task.iteration,
                task.worker_id,
                task.request_id,
                lease_ms,
            )
        except ControlError as error:
            error.operation = "claim"
            raise
        if not cooperating and (
            claim.effect_class == K6_EFFECT_CLASS or claim.k6_dispatch is not None
        ):
            raise ControlError(403, "K6_ENVELOPE_INVALID", "unexpected K6 claim envelope")
        hook_fields: dict[str, str] = {}
        if self.deployment_hook is not None and not cooperating:
            hook_fields = {
                "workflow": task.workflow_id,
                "node": task.node_id,
                "iteration": str(task.iteration),
                "attempt": str(claim.attempt_number),
                "worker": task.worker_id,
                "request": task.request_id,
                "token_hash": hashlib.sha256(claim.claim_token.encode()).hexdigest(),
            }
            self.deployment_hook("attempt_claimed", hook_fields)
        stop = threading.Event()
        definitive_heartbeat_errors: list[ControlError] = []

        def heartbeat_loop() -> None:
            delay = heartbeat_interval
            while not stop.wait(delay):
                if self.aborted():
                    return
                try:
                    self.control.heartbeat(
                        task.workflow_id,
                        task.node_id,
                        task.iteration,
                        claim.attempt_number,
                        claim.claim_token,
                        lease_ms,
                    )
                    delay = heartbeat_interval
                except Exception as error:
                    if isinstance(error, ControlError):
                        error.operation = "heartbeat"
                    if isinstance(error, ControlError) and error.code in {
                        "STALE_CLAIM",
                        "STALE_ATTEMPT",
                    }:
                        definitive_heartbeat_errors.append(error)
                        return
                    # A transient control outage must not discard work that
                    # has already run. Retry with bounded backoff and still
                    # submit the terminal result after the activity returns.
                    delay = min(max(heartbeat_interval, delay * 2), 60.0)

        heartbeat_thread = threading.Thread(
            target=heartbeat_loop, name="activity-heartbeat", daemon=True
        )
        heartbeat_thread.start()
        evidence = k6_evidence.ledger()
        if cooperating:
            assert self.k6 is not None

            def ensure_current() -> None:
                if self.aborted():
                    raise ControlError(503, "SHUTTING_DOWN", "worker is stopping")
                if definitive_heartbeat_errors:
                    raise definitive_heartbeat_errors[0]

            g_invocation = (
                evidence.begin(
                    task.workflow_id,
                    task.node_id,
                    task.iteration,
                    claim.attempt_number,
                    claim.claim_token,
                    task.request_id,
                )
                if evidence is not None
                else None
            )
            context = (
                k6_evidence.active.set((evidence, g_invocation[0]))
                if evidence is not None and g_invocation is not None
                else None
            )
            completed = False
            effect_started = time.monotonic_ns() if g_invocation is not None else 0
            try:
                result = self.k6.run(task, claim, self.control, ensure_current, claim_started)
                completed = True
                return result
            finally:
                if context is not None:
                    k6_evidence.active.reset(context)
                if evidence is not None and g_invocation is not None:
                    invocation_id, attempt = g_invocation
                    evidence.record_timing("activity", effect_started, invocation=invocation_id)
                    evidence.append(
                        "finish" if completed else "abort", invocation_id, attempt=attempt
                    )
                stop.set()
                heartbeat_thread.join(timeout=max(1.0, self.heartbeat_interval))
        assert activity is not None
        witness = configured_witness()
        invocation = None
        if witness is not None:
            invocation = witness.begin(
                {
                    "workflow_id": task.workflow_id,
                    "node_id": task.node_id,
                    "iteration": task.iteration,
                    "attempt_number": claim.attempt_number,
                    "claim_token": claim.claim_token,
                    "worker_id": task.worker_id,
                    "activity_name": task.activity_name,
                    "activity_version": task.activity_version,
                }
            )
        attempt_state = "ABORTED"
        pure_entry = (
            evidence.begin(
                task.workflow_id,
                task.node_id,
                task.iteration,
                claim.attempt_number,
                claim.claim_token,
                task.request_id,
            )
            if evidence is not None
            else None
        )
        try:
            if self.deployment_hook is not None:
                self.deployment_hook(
                    "activity_start",
                    {
                        **hook_fields,
                        "invocation": str(
                            (invocation or {}).get("invocation_id")
                            or (invocation or {}).get("id", "")
                        ),
                    },
                )
            if isinstance(activity, RecordedActivity):

                def record_provider(event: dict[str, Any]) -> None:
                    if self.deployment_hook is not None:
                        fields = {key: str(value) for key, value in event.items()}
                        self.deployment_hook("j4_provider", {**hook_fields, **fields})

                if resilience:
                    attempt_state, payload = complete_resilient(
                        task.input,
                        workflow_id=task.workflow_id,
                        node_id=task.node_id,
                        iteration=task.iteration,
                        attempt=claim.attempt_number,
                        claim_token=claim.claim_token,
                        exchange=partial(
                            self.control.provider_exchange,
                            task.workflow_id,
                            task.node_id,
                            task.iteration,
                        ),
                        trace=record_provider,
                    )
                else:
                    attempt_state, payload = activity.complete(
                        task.input,
                        workflow_id=task.workflow_id,
                        node_id=task.node_id,
                        iteration=task.iteration,
                        attempt=claim.attempt_number,
                        trace=record_provider,
                    )
            elif evidence is None or pure_entry is None:
                payload = activity(task.input)
                attempt_state = "SUCCEEDED"
            else:
                payload_started = time.monotonic_ns()
                try:
                    payload = activity(task.input)
                    attempt_state = "SUCCEEDED"
                finally:
                    evidence.record_timing("activity", payload_started, invocation=pure_entry[0])
        except Exception as error:
            if recorded and resilience:
                raise
            payload = {"error": str(error), "type": type(error).__name__}
            attempt_state = "FAILED_FINAL" if recorded else "FAILED_RETRYABLE"
        finally:
            if evidence is not None and pure_entry is not None:
                invocation_id, attempt = pure_entry
                evidence.append(
                    "finish" if attempt_state != "ABORTED" else "abort",
                    invocation_id,
                    attempt=attempt,
                )
            if witness is not None and invocation is not None:
                witness.end(invocation, attempt_state)
            stop.set()
            heartbeat_thread.join(timeout=max(1.0, heartbeat_interval))
        if self.aborted():
            raise ControlError(503, "SHUTTING_DOWN", "aborted activity reports no result")
        payload_fingerprint = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
                "utf-8"
            )
        ).hexdigest()
        try:
            receipt = self.control.result(
                task.workflow_id,
                task.node_id,
                task.iteration,
                claim.attempt_number,
                claim.claim_token,
                attempt_state,
                payload,
            )
        except ControlError as error:
            error.operation = "result"
            LOG.info(
                "activity result submission "
                "workflow_id=%s node_id=%s iteration=%s attempt_number=%s worker_id=%s "
                "attempt_state=%s payload_sha256=%s outcome=rejected status=%s code=%s retries=%s",
                task.workflow_id,
                task.node_id,
                task.iteration,
                claim.attempt_number,
                task.worker_id,
                attempt_state,
                payload_fingerprint,
                error.status,
                error.code,
                error.retry_count,
            )
            raise
        if recorded and self.deployment_hook is not None:
            self.deployment_hook("j4_result_receipt", hook_fields)
        retry_count = int(getattr(self.control, "last_result_retry_count", 0))
        LOG.info(
            "activity result submission "
            "workflow_id=%s node_id=%s iteration=%s attempt_number=%s worker_id=%s "
            "attempt_state=%s payload_sha256=%s outcome=accepted retries=%s",
            task.workflow_id,
            task.node_id,
            task.iteration,
            claim.attempt_number,
            task.worker_id,
            attempt_state,
            payload_fingerprint,
            retry_count,
        )
        if definitive_heartbeat_errors:
            # The result/evidence call above is always attempted first. The
            # stale claim remains visible to the caller after that durable
            # handoff, while successful evidence is never hidden.
            raise definitive_heartbeat_errors[0]
        return receipt

    def run_many(self, tasks: list[ActivityTask]) -> list[dict[str, Any]]:
        with ThreadPoolExecutor(
            max_workers=self.max_workers, thread_name_prefix="activity"
        ) as pool:
            futures: list[Future[dict[str, Any]]] = [
                pool.submit(self.run_task, task) for task in tasks
            ]
            return [future.result() for future in futures]


def runner_for_url(
    control_url: str, registry: ActivityRegistry, max_workers: int = 4
) -> ActivityRunner:
    return ActivityRunner(ControlClient(control_url), registry, max_workers=max_workers)
