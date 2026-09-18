"""Stable data contracts for the deterministic incident agent.

The contracts deliberately keep ground-truth labels out of tool responses.  A
fixture can carry labels for an evaluator, while the MCP-facing evidence type
contains only provenance and the text a real investigator is allowed to see.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

RetrievalArm = Literal["keyword", "dense", "hybrid"]
DefenseProfile = Literal["defended", "plain"]
WorkflowState = Literal[
    "ACCEPTED",
    "INVESTIGATING",
    "WAITING_APPROVAL",
    "DISPATCHED",
    "VERIFYING",
    "COMPLETED",
    "ABSTAINED",
    "REJECTED",
    "INTERRUPTED",
    "FAILED",
]


@dataclass(frozen=True)
class EvidenceChunk:
    chunk_id: str
    document_id: str
    version: str
    service: str
    text: str
    source_type: Literal["runbook", "postmortem"]
    relevant_families: tuple[str, ...] = ()
    canary: bool = False
    near_duplicate_group: str | None = None
    decisive_detail: str = ""


@dataclass(frozen=True)
class IncidentCase:
    case_id: str
    family: str
    split: Literal["development", "heldout"]
    service: str
    query: str
    answerable: bool
    document_dependent: bool
    relevant_chunk_ids: tuple[str, ...]
    logs: tuple[dict[str, Any], ...]
    metrics: tuple[dict[str, Any], ...]
    expected_diagnosis: str
    expected_action: dict[str, Any] | None
    canary_fixture: bool = False


@dataclass(frozen=True)
class RetrievalHit:
    chunk_id: str
    document_id: str
    version: str
    service: str
    score: float
    snippet: str


@dataclass(frozen=True)
class RetrievalResponse:
    arm: RetrievalArm
    query: str
    pre_gate_keyword: tuple[RetrievalHit, ...]
    pre_gate_dense: tuple[RetrievalHit, ...]
    pre_gate_hybrid: tuple[RetrievalHit, ...]
    delivered: tuple[RetrievalHit, ...]
    sufficient: bool
    reason: str


@dataclass(frozen=True)
class ToolResult:
    method: str
    schema_version: str
    data: dict[str, Any]
    evidence_ids: tuple[str, ...] = ()
    redactions: int = 0


@dataclass(frozen=True)
class Proposal:
    action_type: str
    target_resource_id: str
    arguments: dict[str, Any]
    canonical_argument_hash: str


@dataclass(frozen=True)
class TimelineEvent:
    sequence: int
    event_type: str
    actor: str
    data: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class WorkflowSnapshot:
    run_id: str
    case_id: str
    state: WorkflowState
    revision: int
    diagnosis: str | None
    citations: tuple[str, ...]
    proposal: Proposal | None
    action_receipt: dict[str, Any] | None
    timeline: tuple[TimelineEvent, ...]
