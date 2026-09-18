"""Bounded incident timeline and Prometheus metrics."""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass

from incident_agent.models import TimelineEvent

_ALLOWED_ARMS = {"keyword", "dense", "hybrid", "unknown"}
_ALLOWED_METHODS = {"query_logs", "query_metrics", "search_runbooks", "remediation_status"}
_ALLOWED_EVENT_TYPES = {
    "incident_accepted",
    "investigation_started",
    "investigation_round_opened",
    "diagnosis_committed",
    "approval_requested",
    "approval_granted",
    "approval_rejected",
    "effect_dispatched",
    "effect_verified",
    "incident_completed",
    "abstained",
    "interrupted",
    "mcp_tool_call",
    "model_usage",
    "citation_violation",
}


@dataclass
class IncidentMetrics:
    role: str = "incident-agent"

    def __post_init__(self) -> None:
        self.counters: Counter[tuple[str, str]] = Counter()
        self.outcomes: Counter[tuple[str, str]] = Counter()
        self.tool_calls: Counter[tuple[str, str]] = Counter()
        self.citation_violations: Counter[str] = Counter()
        self.approvals: Counter[str] = Counter()
        self.latency_seconds: dict[str, float] = {}
        self.tokens: Counter[str] = Counter()
        self.cost_cents: Counter[str] = Counter()

    def observe(self, event: TimelineEvent) -> None:
        arm = str(event.data.get("arm", "unknown"))
        bounded_arm = arm if arm in _ALLOWED_ARMS else "unknown"
        event_type = event.event_type if event.event_type in _ALLOWED_EVENT_TYPES else "other"
        self.counters[(event_type, bounded_arm)] += 1
        if event_type == "mcp_tool_call":
            method = str(event.data.get("method", "unknown"))
            self.tool_calls[(method if method in _ALLOWED_METHODS else "unknown", "ok")] += 1
            self.latency_seconds["tool"] = (
                self.latency_seconds.get("tool", 0.0)
                + float(event.data.get("latency_ms", 0)) / 1000
            )
        if event_type == "model_usage":
            self.tokens["model"] += int(event.data.get("input_tokens", 0))
            self.tokens["model"] += int(event.data.get("output_tokens", 0))
            self.cost_cents["model"] += int(event.data.get("cost_cents", 0))
            self.latency_seconds["model"] = (
                self.latency_seconds.get("model", 0.0)
                + float(event.data.get("latency_ms", 0)) / 1000
            )
        if event_type in {"incident_completed", "abstained", "approval_rejected"}:
            outcome = {
                "incident_completed": "completed",
                "abstained": "abstained",
                "approval_rejected": "rejected",
            }[event_type]
            self.outcomes[(bounded_arm, outcome)] += 1
        if event_type in {"approval_granted", "approval_rejected"}:
            self.approvals["approved" if event_type == "approval_granted" else "rejected"] += 1
        if event_type == "citation_violation":
            kind = str(event.data.get("kind", "unknown"))
            self.citation_violations[kind if kind == "unknown_evidence" else "other"] += 1

    def render(self) -> str:
        bounded_role = self.role if self.role == "incident-agent" else "incident-agent"
        lines = [
            "# HELP durable_incident_events_total Incident workflow events by "
            "bounded type and arm.",
            "# TYPE durable_incident_events_total counter",
        ]
        for (event_type, arm), value in sorted(self.counters.items()):
            lines.append(
                f'durable_incident_events_total{{role="{bounded_role}",'
                f'event_type="{event_type}",arm="{arm}"}} {value}'
            )
        lines.extend(
            [
                "# TYPE durable_incident_outcomes_total counter",
                *[
                    self._line(
                        "durable_incident_outcomes_total",
                        {"role": bounded_role, "arm": arm, "outcome": outcome},
                        value,
                    )
                    for (arm, outcome), value in sorted(self.outcomes.items())
                ],
                "# TYPE durable_incident_tool_calls_total counter",
                *[
                    self._line(
                        "durable_incident_tool_calls_total",
                        {"role": bounded_role, "method": method, "outcome": outcome},
                        value,
                    )
                    for (method, outcome), value in sorted(self.tool_calls.items())
                ],
                "# TYPE durable_incident_citation_violations_total counter",
                *[
                    self._line(
                        "durable_incident_citation_violations_total",
                        {"role": bounded_role, "kind": kind},
                        value,
                    )
                    for kind, value in sorted(self.citation_violations.items())
                ],
                "# TYPE durable_incident_approvals_total counter",
                *[
                    self._line(
                        "durable_incident_approvals_total",
                        {"role": bounded_role, "decision": decision},
                        value,
                    )
                    for decision, value in sorted(self.approvals.items())
                ],
                "# TYPE durable_incident_latency_seconds counter",
                *[
                    self._line(
                        "durable_incident_latency_seconds",
                        {"role": bounded_role, "component": component},
                        value,
                    )
                    for component, value in sorted(self.latency_seconds.items())
                ],
                "# TYPE durable_incident_tokens_total counter",
                *[
                    self._line(
                        "durable_incident_tokens_total",
                        {"role": bounded_role, "component": component},
                        value,
                    )
                    for component, value in sorted(self.tokens.items())
                ],
                "# TYPE durable_incident_cost_cents_total counter",
                *[
                    self._line(
                        "durable_incident_cost_cents_total",
                        {"role": bounded_role, "component": component},
                        value,
                    )
                    for component, value in sorted(self.cost_cents.items())
                ],
            ]
        )
        return "\n".join(lines) + "\n"

    @staticmethod
    def _line(name: str, labels: dict[str, str], value: float | int) -> str:
        label_text = ",".join(f'{key}="{item}"' for key, item in labels.items())
        return f"{name}{{{label_text}}} {value}"


def timeline_json(events: Iterable[TimelineEvent]) -> str:
    return json.dumps(
        [
            {
                "sequence": event.sequence,
                "event_type": event.event_type,
                "actor": event.actor,
                "data": event.data,
            }
            for event in events
        ],
        indent=2,
        sort_keys=True,
    )


def dashboard_manifest() -> dict[str, object]:
    return {
        "schema": "incident-dashboard.v1",
        "panels": [
            {
                "title": "Abstention rate",
                "metric": "durable_incident_outcomes_total",
                "group_by": ["role", "arm", "outcome"],
            },
            {
                "title": "Invalid or denied tool calls",
                "metric": "durable_incident_tool_calls_total",
                "group_by": ["role", "method", "outcome"],
            },
            {
                "title": "Citation provenance violations",
                "metric": "durable_incident_citation_violations_total",
                "group_by": ["role", "kind"],
            },
            {
                "title": "Approver decisions",
                "metric": "durable_incident_approvals_total",
                "group_by": ["role", "decision"],
            },
            {
                "title": "Model and tool latency",
                "metric": "durable_incident_latency_seconds",
                "group_by": ["role", "component"],
            },
            {
                "title": "Model tokens",
                "metric": "durable_incident_tokens_total",
                "group_by": ["role", "component"],
            },
            {
                "title": "Model cost",
                "metric": "durable_incident_cost_cents_total",
                "group_by": ["role", "component"],
            },
        ],
        "label_policy": (
            "Only bounded role, arm, method, outcome, kind, decision, and "
            "component labels are permitted."
        ),
    }
