"""HTTP control client for the M2 worker claim/heartbeat/result protocol."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from workers.k4_control import CURRENT, new_request
from workers.witness import campaign_witness


class ControlError(RuntimeError):
    """A control request was rejected or could not be completed."""

    def __init__(self, status: int, code: str, message: str, operation: str = "") -> None:
        super().__init__(f"{code}: {message}")
        self.status = status
        self.code = code
        self.message = message
        self.retry_count = 0
        # Filled by ActivityRunner at the control-call boundary. The same
        # STALE_ATTEMPT code can arise from claim, heartbeat, or result.
        self.operation = operation


@dataclass(frozen=True)
class Claim:
    attempt_number: int
    claim_token: str
    effect_class: str
    heartbeat_deadline: str


class ControlClient:
    """HTTP control client; broker payloads never grant permission to execute."""

    def __init__(self, base_url: str, timeout: float = 10.0, traceparent: str = "") -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.traceparent = traceparent

    def request_timeout(self) -> float:
        return self.timeout

    def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        witness = campaign_witness()
        if witness is None:
            return self._post_once(path, body)
        trace = CURRENT.get() or new_request(body)
        trace.attempt += 1
        trace.status = 0
        token = CURRENT.set(trace)
        error_code = ""
        try:
            witness.trace(
                "http_start",
                {
                    "role": "worker",
                    "request": trace.request,
                    "body_hash": trace.body_hash,
                    "attempt": trace.attempt,
                    "operation": path.rsplit("/", 1)[-1],
                    "status": 0,
                    "error": "pending",
                },
            )
            return self._post_once(path, body)
        except ControlError as error:
            trace.status, error_code = error.status, error.code
            raise
        except Exception as error:
            error_code = type(error).__name__
            raise
        finally:
            CURRENT.reset(token)
            witness.trace(
                "http",
                {
                    "role": "worker",
                    "request": trace.request,
                    "body_hash": trace.body_hash,
                    "attempt": trace.attempt,
                    "operation": path.rsplit("/", 1)[-1],
                    "status": trace.status,
                    "error": error_code,
                },
            )

    def _post_once(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        headers = {"Content-Type": "application/json"}
        if trace := CURRENT.get():
            headers["X-Dur053-Request-ID"] = trace.request
        if self.traceparent:
            headers["traceparent"] = self.traceparent
        token_file = os.environ.get("WORKER_TOKEN_FILE", "")
        if token_file:
            try:
                token = Path(token_file).read_text(encoding="utf-8").strip()
                if not token:
                    raise ValueError("empty projected token")
                headers["Authorization"] = f"Bearer {token}"
            except (OSError, ValueError) as error:
                raise ControlError(
                    401, "TOKEN_UNAVAILABLE", "projected token unavailable"
                ) from error
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=json.dumps(body, separators=(",", ":")).encode(),
            headers=headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.request_timeout()) as response:  # noqa: S310
                if trace := CURRENT.get():
                    trace.status = response.status
                decoded = json.loads(response.read())
        except urllib.error.HTTPError as error:
            try:
                decoded = json.loads(error.read())
            except (json.JSONDecodeError, UnicodeDecodeError):
                decoded = {}
            detail = decoded.get("error", {})
            raise ControlError(
                error.code,
                str(detail.get("code", "HTTP_ERROR")),
                str(detail.get("message", error.reason)),
            ) from error
        except (urllib.error.URLError, TimeoutError) as error:
            raise ControlError(503, "CONTROL_UNAVAILABLE", str(error)) from error
        if not isinstance(decoded, dict):
            raise ControlError(
                502, "INVALID_CONTROL_RESPONSE", "control response was not an object"
            )
        return decoded

    @staticmethod
    def _path(workflow_id: str, node_id: str, iteration: int, action: str) -> str:
        # IDs are contract-level opaque tokens and may not contain '/'.
        if not workflow_id or not node_id or "/" in workflow_id or "/" in node_id:
            raise ValueError("workflow_id and node_id must be non-empty path-safe IDs")
        if iteration < 0:
            raise ValueError("iteration must be non-negative")
        return f"/v1/workflows/{workflow_id}/nodes/{node_id}/iterations/{iteration}/{action}"

    def claim(
        self,
        workflow_id: str,
        node_id: str,
        iteration: int,
        worker_id: str,
        request_id: str,
        attempt_lease_ms: int = 60_000,
        expected_attempt: int = 0,
    ) -> Claim:
        response = self._post(
            self._path(workflow_id, node_id, iteration, "claim"),
            {
                "worker_id": worker_id,
                "request_id": request_id,
                "attempt_lease_ms": attempt_lease_ms,
                "expected_attempt": expected_attempt,
            },
        )
        return Claim(
            attempt_number=int(response["attempt_number"]),
            claim_token=str(response["claim_token"]),
            effect_class=str(response["effect_class"]),
            heartbeat_deadline=str(response["heartbeat_deadline"]),
        )

    def heartbeat(
        self,
        workflow_id: str,
        node_id: str,
        iteration: int,
        attempt_number: int,
        claim_token: str,
        extension_ms: int = 60_000,
    ) -> str:
        response = self._post(
            self._path(workflow_id, node_id, iteration, "heartbeat"),
            {
                "attempt_number": attempt_number,
                "claim_token": claim_token,
                "extension_ms": extension_ms,
            },
        )
        return str(response["heartbeat_deadline"])

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
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "attempt_number": attempt_number,
            "claim_token": claim_token,
            "attempt_state": attempt_state,
            "payload": payload,
        }
        if event_type:
            body["event_type"] = event_type
        if reconciliation_ref:
            body["reconciliation_ref"] = reconciliation_ref
        return self._post(self._path(workflow_id, node_id, iteration, "result"), body)
