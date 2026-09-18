"""Schema-constrained local MCP-style evidence and action surface.

This is deliberately transport-neutral.  The durable workflow calls one
allowlisted method at a time; a future wire-level MCP adapter can wrap this
dispatcher without changing the authorization or provenance rules.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

from incident_agent.fixtures import build_incident_cases
from incident_agent.models import RetrievalArm, RetrievalResponse, ToolResult
from incident_agent.redaction import redact
from incident_agent.retrieval import RetrievalIndex

SCHEMA_VERSION = "m6-mcp-v1"
METHOD_ARGUMENTS = {
    "query_logs": ("case_id", "service", "level"),
    "query_metrics": ("case_id", "service", "metric"),
    "search_runbooks": ("query", "arm"),
    "remediation_status": ("resource_id",),
}
METHODS = tuple(METHOD_ARGUMENTS)


@dataclass(frozen=True)
class MCPCall:
    sequence: int
    method: str
    latency_ms: float
    evidence_ids: tuple[str, ...]
    redactions: int


class MCPValidationError(ValueError):
    pass


class BoundedMCPServer:
    def __init__(self, index: RetrievalIndex, max_calls: int = 8, max_rows: int = 20) -> None:
        self.index = index
        self.max_calls = max_calls
        self.max_rows = max_rows
        self._calls: dict[str, list[MCPCall]] = {}
        self._status: dict[str, dict[str, Any]] = {}
        self._cases = {case.case_id: case for case in build_incident_cases()}

    def call(self, run_id: str, method: str, arguments: dict[str, Any]) -> ToolResult:
        """Dispatch one schema-constrained, allowlisted MCP-style call."""
        try:
            if method == "query_logs":
                return self.query_logs(run_id, **arguments)
            if method == "query_metrics":
                return self.query_metrics(run_id, **arguments)
            if method == "search_runbooks":
                return self.search_runbooks(run_id, **arguments)
            if method == "remediation_status":
                return self.remediation_status(run_id, **arguments)
        except TypeError as error:
            raise MCPValidationError(f"invalid arguments for {method}: {error}") from error
        raise MCPValidationError(f"method is not allowlisted: {method}")

    def _validate(
        self, run_id: str, method: str, arguments: dict[str, Any], allowed: set[str]
    ) -> None:
        if method not in METHODS:
            raise MCPValidationError(f"method is not allowlisted: {method}")
        if not isinstance(arguments, dict) or set(arguments) - allowed:
            unknown = sorted(set(arguments) - allowed)
            raise MCPValidationError(f"unknown arguments for {method}: {unknown}")
        calls = self._calls.setdefault(run_id, [])
        if len(calls) >= self.max_calls:
            raise MCPValidationError("MCP call budget exceeded")

    def _record(self, run_id: str, method: str, started: float, result: ToolResult) -> ToolResult:
        self._calls.setdefault(run_id, []).append(
            MCPCall(
                sequence=len(self._calls.get(run_id, [])) + 1,
                method=method,
                latency_ms=round((time.perf_counter() - started) * 1000, 3),
                evidence_ids=result.evidence_ids,
                redactions=result.redactions,
            )
        )
        return result

    def query_logs(self, run_id: str, case_id: str, service: str, level: str = "") -> ToolResult:
        started = time.perf_counter()
        self._validate(
            run_id,
            "query_logs",
            {"case_id": case_id, "service": service, "level": level},
            set(METHOD_ARGUMENTS["query_logs"]),
        )
        case = self._cases.get(case_id)
        if case is None or case.service != service:
            rows: tuple[dict[str, Any], ...] = ()
        else:
            rows = tuple(row for row in case.logs if not level or row["level"] == level)[
                : self.max_rows
            ]
        safe_rows: list[dict[str, Any]] = []
        redactions = 0
        for row in rows:
            safe = dict(row)
            result = redact(str(safe.get("message", "")))
            safe["message"] = result.text
            redactions += result.count
            safe_rows.append(safe)
        return self._record(
            run_id,
            "query_logs",
            started,
            ToolResult(
                "query_logs",
                SCHEMA_VERSION,
                {"rows": safe_rows},
                tuple(f"log:{case_id}:{row['timestamp']}" for row in safe_rows),
                redactions,
            ),
        )

    def query_metrics(
        self, run_id: str, case_id: str, service: str, metric: str = ""
    ) -> ToolResult:
        started = time.perf_counter()
        self._validate(
            run_id,
            "query_metrics",
            {"case_id": case_id, "service": service, "metric": metric},
            set(METHOD_ARGUMENTS["query_metrics"]),
        )
        case = self._cases.get(case_id)
        rows = (
            ()
            if case is None or case.service != service
            else tuple(row for row in case.metrics if not metric or row["name"] == metric)[
                : self.max_rows
            ]
        )
        return self._record(
            run_id,
            "query_metrics",
            started,
            ToolResult(
                "query_metrics",
                SCHEMA_VERSION,
                {"rows": list(rows)},
                tuple(f"metric:{case_id}:{row['name']}:{row['timestamp']}" for row in rows),
            ),
        )

    def search_runbooks(self, run_id: str, query: str, arm: RetrievalArm = "hybrid") -> ToolResult:
        started = time.perf_counter()
        self._validate(
            run_id,
            "search_runbooks",
            {"query": query, "arm": arm},
            set(METHOD_ARGUMENTS["search_runbooks"]),
        )
        if arm not in {"keyword", "dense", "hybrid"}:
            raise MCPValidationError("arm must be keyword, dense, or hybrid")
        response: RetrievalResponse = self.index.search(query, arm)
        evidence = [
            {
                "chunk_id": hit.chunk_id,
                "document_id": hit.document_id,
                "version": hit.version,
                "service": hit.service,
                "score": hit.score,
                "snippet": redact(hit.snippet).text,
            }
            for hit in response.delivered
        ]
        return self._record(
            run_id,
            "search_runbooks",
            started,
            ToolResult(
                "search_runbooks",
                SCHEMA_VERSION,
                {
                    "arm": arm,
                    "sufficient": response.sufficient,
                    "reason": response.reason,
                    "evidence": evidence,
                },
                tuple(hit.chunk_id for hit in response.delivered),
            ),
        )

    def remediation_status(self, run_id: str, resource_id: str) -> ToolResult:
        started = time.perf_counter()
        self._validate(
            run_id,
            "remediation_status",
            {"resource_id": resource_id},
            set(METHOD_ARGUMENTS["remediation_status"]),
        )
        status = self._status.get(resource_id, {"resource_id": resource_id, "state": "UNCHANGED"})
        return self._record(
            run_id,
            "remediation_status",
            started,
            ToolResult("remediation_status", SCHEMA_VERSION, status),
        )

    def set_remediation_status(self, resource_id: str, state: str) -> None:
        if state not in {"UNCHANGED", "APPLIED", "VERIFIED", "FAILED"}:
            raise ValueError("invalid remediation status")
        self._status[resource_id] = {"resource_id": resource_id, "state": state}

    def calls(self, run_id: str) -> tuple[MCPCall, ...]:
        return tuple(self._calls.get(run_id, ()))


def serialize_tool_result(result: ToolResult) -> str:
    return json.dumps(
        {
            "method": result.method,
            "schema_version": result.schema_version,
            "data": result.data,
            "evidence_ids": list(result.evidence_ids),
            "redactions": result.redactions,
        },
        sort_keys=True,
    )
