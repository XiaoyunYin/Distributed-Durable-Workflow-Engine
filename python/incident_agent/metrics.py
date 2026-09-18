"""Bounded incident timeline and Prometheus metrics."""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass

from incident_agent.models import TimelineEvent

_ALLOWED_ARMS = {"keyword", "dense", "hybrid", "unknown"}
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
}


@dataclass
class IncidentMetrics:
    role: str = "incident-agent"

    def __post_init__(self) -> None:
        self.counters: Counter[tuple[str, str]] = Counter()
        self.timings: Counter[tuple[str, str]] = Counter()

    def observe(self, event: TimelineEvent) -> None:
        arm = str(event.data.get("arm", "unknown"))
        bounded_arm = arm if arm in _ALLOWED_ARMS else "unknown"
        event_type = event.event_type if event.event_type in _ALLOWED_EVENT_TYPES else "other"
        self.counters[(event_type, bounded_arm)] += 1

    def render(self) -> str:
        lines = [
            "# HELP durable_incident_events_total Incident workflow events by "
            "bounded type and arm.",
            "# TYPE durable_incident_events_total counter",
        ]
        for (event_type, arm), value in sorted(self.counters.items()):
            lines.append(
                f'durable_incident_events_total{{role="{self.role}",'
                f'event_type="{event_type}",arm="{arm}"}} {value}'
            )
        return "\n".join(lines) + "\n"


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
        ],
        "label_policy": (
            "Only bounded role, arm, method, outcome, kind, decision, and "
            "component labels are permitted."
        ),
    }
