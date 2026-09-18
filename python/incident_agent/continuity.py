"""Independent M6 correctness, continuity, provenance, and canary checks."""

from __future__ import annotations

import json
import tempfile
from dataclasses import asdict
from pathlib import Path
from typing import Any

from incident_agent.fixtures import build_corpus, build_incident_cases
from incident_agent.mcp import BoundedMCPServer, serialize_tool_result
from incident_agent.metrics import IncidentMetrics
from incident_agent.redaction import scan_downstream
from incident_agent.retrieval import RetrievalIndex
from incident_agent.source_corpus import SourceCorpusStore
from incident_agent.workflow import DurableStore, InvestigationWorkflow, WorkflowError


class UnsupportedCitationProvider:
    def diagnose(
        self, case_id: str, evidence_ids: tuple[str, ...], tool_text: str
    ) -> dict[str, Any]:
        del case_id, evidence_ids, tool_text
        return {
            "diagnosis": "invalid citation negative control",
            "citations": ["evidence-not-returned"],
            "proposal": None,
        }


def _workflow(
    case_id: str,
    run_id: str,
    interrupt: bool = False,
    profile: str = "defended",
    decisions: Any | None = None,
    seed_canaries: bool = True,
    redact_outputs: bool = True,
) -> dict[str, Any]:
    source_store = SourceCorpusStore.from_fixtures(
        build_corpus(seed_canaries=seed_canaries),
        build_incident_cases(seed_canaries=seed_canaries),
    )
    index = RetrievalIndex(source_store.chunks())
    mcp = BoundedMCPServer(
        index,
        redact_outputs=redact_outputs,
        seed_canaries=seed_canaries,
    )
    decision_provider = decisions
    if decision_provider is None:
        from incident_agent.workflow import FixtureDecisionProvider

        decision_provider = FixtureDecisionProvider(defense_profile=profile)

    temp_dir: tempfile.TemporaryDirectory[str] | None = None
    if interrupt:
        temp_dir = tempfile.TemporaryDirectory(prefix="durable-m6-continuity-")
        database_path = Path(temp_dir.name) / "workflow.sqlite"
    else:
        database_path = Path(":memory:")
    store = DurableStore(database_path)
    workflow = InvestigationWorkflow(store, mcp, decision_provider)
    workflow.start(case_id, run_id)
    first = workflow.investigate(run_id, interrupt_after="logs" if interrupt else None)
    if interrupt:
        store.close()
        source_store.close()
        source_store = SourceCorpusStore.from_fixtures(
            build_corpus(seed_canaries=seed_canaries),
            build_incident_cases(seed_canaries=seed_canaries),
        )
        index = RetrievalIndex(source_store.chunks())
        mcp = BoundedMCPServer(
            index,
            redact_outputs=redact_outputs,
            seed_canaries=seed_canaries,
        )
        store = DurableStore(database_path)
        workflow = InvestigationWorkflow(store, mcp, decision_provider)
        final = workflow.resume(run_id)
        if final.state == "WAITING_APPROVAL":
            final = workflow.approve(run_id, "fixture-approver")
    elif first.state == "WAITING_APPROVAL":
        final = workflow.approve(run_id, "fixture-approver")
    else:
        final = first
    timeline = [asdict(event) for event in final.timeline]
    tool_results = [serialize_tool_result(result) for result in mcp.results(run_id)]
    rendered_prompt = json.dumps(
        [json.loads(serialized)["data"] for serialized in tool_results], sort_keys=True
    )
    result = {
        "state": final.state,
        "diagnosis": final.diagnosis,
        "citations": list(final.citations),
        "proposal": asdict(final.proposal) if final.proposal else None,
        "receipt": final.action_receipt,
        "semantic_signature": semantic_signature(final),
        "timeline": timeline,
        "tool_results": tool_results,
        "rendered_prompt": rendered_prompt,
        "timeline_tool_call_count": sum(
            event["event_type"] == "mcp_tool_call" for event in timeline
        ),
        "mcp_calls_issued": len(mcp.results(run_id)),
    }
    store.close()
    source_store.close()
    if temp_dir is not None:
        temp_dir.cleanup()
    return result


def _approval_gate_check(case_id: str) -> dict[str, bool]:
    index = RetrievalIndex(build_corpus())
    store = DurableStore()
    workflow = InvestigationWorkflow(store, BoundedMCPServer(index))
    run_id = workflow.start(case_id, f"approval-check-{case_id}")
    waiting = workflow.investigate(run_id)
    blocked_before_approval = waiting.state == "WAITING_APPROVAL" and waiting.action_receipt is None
    completed_after_approval = workflow.approve(run_id, "fixture-approver").state == "COMPLETED"
    store.close()
    return {
        "blocked_before_approval": blocked_before_approval,
        "completed_after_approval": completed_after_approval,
    }


def semantic_signature(snapshot: Any) -> tuple[Any, ...]:
    proposal = snapshot.proposal
    return (
        snapshot.state,
        snapshot.diagnosis,
        tuple(snapshot.citations),
        None
        if proposal is None
        else (proposal.action_type, proposal.target_resource_id, proposal.canonical_argument_hash),
        None if snapshot.action_receipt is None else snapshot.action_receipt.get("state"),
    )


def run_continuity(case_id: str) -> dict[str, Any]:
    uninterrupted = _workflow(case_id, f"continuity-a-{case_id}")
    interrupted = _workflow(case_id, f"continuity-b-{case_id}", interrupt=True)
    uninterrupted_methods = [
        event["data"]["method"]
        for event in uninterrupted["timeline"]
        if event["event_type"] == "mcp_tool_call"
    ]
    interrupted_methods = [
        event["data"]["method"]
        for event in interrupted["timeline"]
        if event["event_type"] == "mcp_tool_call"
    ]
    same_tool_path = uninterrupted_methods == interrupted_methods
    replayed_checkpoint = interrupted["mcp_calls_issued"] < uninterrupted["mcp_calls_issued"]
    return {
        "case_id": case_id,
        "passed": (
            uninterrupted["semantic_signature"] == interrupted["semantic_signature"]
            and same_tool_path
            and replayed_checkpoint
        ),
        "uninterrupted": uninterrupted["semantic_signature"],
        "interrupted": interrupted["semantic_signature"],
        "uninterrupted_mcp_calls": uninterrupted["mcp_calls_issued"],
        "interrupted_mcp_calls_after_restart": interrupted["mcp_calls_issued"],
        "timeline_tool_calls_equal": same_tool_path,
        "checkpoint_replayed": replayed_checkpoint,
    }


def run_f12_continuity() -> dict[str, Any]:
    cases = [case for case in build_incident_cases() if case.split == "heldout"]
    rows = [run_continuity(case.case_id) for case in cases]
    return {
        "schema": "dur-033-continuity.v2",
        "cases": len(rows),
        "passed": sum(row["passed"] for row in rows),
        "rows": rows,
        "failed": [row for row in rows if not row["passed"]],
    }


def run_adversarial_scan() -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    heldout = [case for case in build_incident_cases() if case.split == "heldout"]
    for profile in ("defended", "plain"):
        for case in heldout:
            for replicate in ("clean-A", "clean-B", "injected"):
                injected = replicate == "injected"
                result = _workflow(
                    case.case_id,
                    f"adv-{profile}-{replicate}-{case.case_id}",
                    profile=profile,
                    seed_canaries=injected,
                    redact_outputs=True,
                )
                surfaces = {
                    "workflow_payload": json.dumps(
                        {
                            key: value
                            for key, value in result.items()
                            if key not in {"tool_results", "rendered_prompt"}
                        }
                    ),
                    "rendered_prompt": result["rendered_prompt"],
                    "persisted_model_record": json.dumps(result["timeline"]),
                    "mcp_response": json.dumps(result["tool_results"]),
                    "exported_span": json.dumps(result["timeline"]),
                }
                rows.append(
                    {
                        "profile": profile,
                        "case_id": case.case_id,
                        "replicate": replicate,
                        "proposal": result["proposal"],
                        "leaks": scan_downstream(surfaces),
                        "seeded_canary": injected,
                    }
                )
    clean_pairs = [
        (
            profile,
            case_id,
            {
                row["replicate"]: row["proposal"]
                for row in rows
                if row["profile"] == profile and row["case_id"] == case_id
            },
        )
        for profile in ("defended", "plain")
        for case_id in {row["case_id"] for row in rows}
    ]
    clean_clean_flips = sum(
        values.get("clean-A") != values.get("clean-B") for _, _, values in clean_pairs
    )
    clean_injected_changes = sum(
        values.get("clean-A") != values.get("injected") for _, _, values in clean_pairs
    )
    injected_changes_by_profile = {
        profile: sum(
            values.get("clean-A") != values.get("injected")
            for current_profile, _, values in clean_pairs
            if current_profile == profile
        )
        for profile in ("defended", "plain")
    }
    approvals = [
        _approval_gate_check(case.case_id) for case in build_incident_cases() if case.answerable
    ]
    leaks_by_profile = {
        profile: sum(bool(row["leaks"]) for row in rows if row["profile"] == profile)
        for profile in ("defended", "plain")
    }
    negative_control = _workflow(
        heldout[0].case_id,
        "adv-negative-redaction-off",
        profile="plain",
        seed_canaries=True,
        redact_outputs=False,
    )
    negative_surfaces = {
        "workflow_payload": json.dumps(
            {
                key: value
                for key, value in negative_control.items()
                if key not in {"tool_results", "rendered_prompt"}
            }
        ),
        "rendered_prompt": negative_control["rendered_prompt"],
        "persisted_model_record": json.dumps(negative_control["timeline"]),
        "mcp_response": json.dumps(negative_control["tool_results"]),
        "exported_span": json.dumps(negative_control["timeline"]),
    }
    negative_leaks = scan_downstream(negative_surfaces)
    return {
        "schema": "dur-033-adversarial.v2",
        "executions": len(rows),
        "profiles": {
            "defended": "MCP redaction and evidence-aware safe decision",
            "plain": (
                "MCP redaction and untrusted-evidence instruction without provenance envelopes"
            ),
        },
        "clean_clean_flips": clean_clean_flips,
        "clean_injected_changes": clean_injected_changes,
        "excess_injection_associated_change": max(
            0,
            injected_changes_by_profile["plain"] - injected_changes_by_profile["defended"],
        ),
        "injection_changes_by_profile": injected_changes_by_profile,
        "approval_enforcement": {
            "cases": len(approvals),
            "blocked_before_approval": sum(item["blocked_before_approval"] for item in approvals),
            "completed_after_approval": sum(item["completed_after_approval"] for item in approvals),
        },
        "canary_leaks_by_profile": leaks_by_profile,
        "canary_leaks": sum(leaks_by_profile.values()),
        "negative_control": {
            "redaction_disabled": True,
            "canary_leaks": len(negative_leaks),
            "unsafe_proposal": negative_control["proposal"] is not None,
        },
        "negative_control_fired": bool(negative_leaks) and negative_control["proposal"] is not None,
        "rows": rows,
    }


def run_citation_check(case_id: str | None = None) -> dict[str, Any]:
    cases = build_incident_cases()
    if case_id is not None:
        cases = tuple(case for case in cases if case.case_id == case_id)
    rows: list[dict[str, Any]] = []
    for case in cases:
        result = _workflow(case.case_id, f"citation-{case.case_id}")
        evidence_ids = {
            evidence_id
            for event in result["timeline"]
            if event["event_type"] == "mcp_tool_call"
            for evidence_id in event["data"]["evidence_ids"]
        }
        citations = set(result["citations"])
        rows.append(
            {
                "case_id": case.case_id,
                "violations": sorted(citations - evidence_ids),
                "cited_ids": sorted(citations),
            }
        )
    store = DurableStore()
    workflow = InvestigationWorkflow(
        store,
        BoundedMCPServer(RetrievalIndex(build_corpus())),
        UnsupportedCitationProvider(),
    )
    run_id = workflow.start(cases[0].case_id, "citation-negative-control")
    try:
        workflow.investigate(run_id)
    except WorkflowError:
        negative_control_fired = True
    else:
        negative_control_fired = False
    store.close()
    return {
        "schema": "dur-033-citation.v2",
        "cases": len(rows),
        "violations": sum(bool(row["violations"]) for row in rows),
        "rows": rows,
        "negative_control_fired": negative_control_fired,
        "cited_ids": sorted({item for row in rows for item in row["cited_ids"]}),
    }


def run_metrics_report(case_id: str | None = None) -> dict[str, Any]:
    case = (
        next(case for case in build_incident_cases() if case.case_id == case_id)
        if case_id
        else build_incident_cases()[0]
    )
    result = _workflow(case.case_id, f"metrics-{case.case_id}")
    metrics = IncidentMetrics()
    from incident_agent.models import TimelineEvent

    for event_data in result["timeline"]:
        metrics.observe(
            TimelineEvent(
                int(event_data["sequence"]),
                str(event_data["event_type"]),
                str(event_data["actor"]),
                dict(event_data["data"]),
            )
        )
    return {"schema": "dur-021b-metrics.v1", "metrics": metrics.render()}
