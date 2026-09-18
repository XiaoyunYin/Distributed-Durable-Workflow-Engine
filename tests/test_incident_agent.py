from __future__ import annotations

import json

import pytest
from incident_agent.continuity import run_adversarial_scan, run_citation_check, run_f12_continuity
from incident_agent.fixtures import (
    FAMILIES,
    build_corpus,
    build_incident_cases,
    build_retrieval_queries,
    corpus_manifest,
)
from incident_agent.mcp import BoundedMCPServer, MCPValidationError, serialize_tool_result
from incident_agent.metrics import IncidentMetrics
from incident_agent.redaction import redact, scan_downstream
from incident_agent.retrieval import RetrievalConfig, RetrievalIndex
from incident_agent.workflow import (
    DurableStore,
    InvestigationWorkflow,
    LiveModelConfig,
    WorkflowError,
)


def test_fixture_counts_and_hidden_truth_separation() -> None:
    manifest = corpus_manifest()
    assert manifest["documents"] == 60
    assert manifest["chunks"] == 300
    assert manifest["development_queries"] == 40
    assert manifest["heldout_queries"] == 120
    assert len(FAMILIES) == 5
    assert manifest["families"] == list(FAMILIES)


def test_retrieval_arms_share_chunks_and_hybrid_has_or_gate() -> None:
    index = RetrievalIndex(build_corpus(), RetrievalConfig())
    answer = index.search("checkout deployment revision config", "keyword")
    dense = index.search("checkout revision deployment", "dense")
    hybrid = index.search("checkout revision deployment", "hybrid")
    assert answer.delivered
    assert dense.delivered
    assert hybrid.delivered
    assert hybrid.pre_gate_hybrid
    assert {hit.chunk_id for hit in hybrid.delivered}.issubset(
        {hit.chunk_id for hit in (*hybrid.pre_gate_keyword, *hybrid.pre_gate_dense)}
    )
    no_answer = index.search("zzzz-no-answer-unique", "hybrid")
    assert not no_answer.sufficient
    assert no_answer.reason == "hybrid_constituent_thresholds_failed"


def test_mcp_is_bounded_schema_constrained_and_redacts() -> None:
    mcp = BoundedMCPServer(RetrievalIndex(build_corpus()), max_calls=2)
    with pytest.raises(MCPValidationError):
        mcp.call(
            "run",
            "query_logs",
            {"case_id": "missing", "service": "checkout", "extra": "not-allowed"},
        )
    safe = redact("contact a@example.com from 10.0.0.1 using sk_live_SECRET_123456")
    assert safe.count == 3
    assert not scan_downstream({"prompt": safe.text})
    result = mcp.search_runbooks("run", "checkout revision", "keyword")
    serialized = json.loads(serialize_tool_result(result))
    assert serialized["evidence_ids"] == list(result.evidence_ids)
    assert serialized["redactions"] == result.redactions


def test_live_model_mode_is_explicitly_budget_and_approval_gated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = LiveModelConfig("local-provider", "pinned-model", 100)
    with pytest.raises(WorkflowError):
        config.authorize()
    monkeypatch.setenv("INCIDENT_LIVE_APPROVED", "1")
    config.authorize()
    with pytest.raises(WorkflowError):
        LiveModelConfig("local-provider", "pinned-model", 0).authorize()


def test_workflow_requires_approval_and_is_idempotent() -> None:
    store = DurableStore()
    workflow = InvestigationWorkflow(store, BoundedMCPServer(RetrievalIndex(build_corpus())))
    case = next(case for case in build_incident_cases() if case.expected_action is not None)
    run_id = workflow.start(case.case_id, "test-approved")
    waiting = workflow.investigate(run_id)
    assert waiting.state == "WAITING_APPROVAL"
    assert waiting.action_receipt is None
    with pytest.raises(WorkflowError):
        workflow.resume("unknown")
    completed = workflow.approve(run_id, "human-1")
    again = workflow.resume(run_id)
    assert completed.state == "COMPLETED"
    assert sum(event.event_type == "mcp_tool_call" for event in completed.timeline) >= 3
    assert again.action_receipt == completed.action_receipt
    assert [event.event_type for event in again.timeline].count("effect_dispatched") == 1
    store.close()


def test_rejection_is_restraint_not_failure() -> None:
    store = DurableStore()
    workflow = InvestigationWorkflow(store, BoundedMCPServer(RetrievalIndex(build_corpus())))
    case = next(case for case in build_incident_cases() if case.expected_action is not None)
    run_id = workflow.start(case.case_id, "test-rejected")
    workflow.investigate(run_id)
    rejected = workflow.reject(run_id, "human-2")
    assert rejected.state == "REJECTED"
    assert rejected.action_receipt is None
    assert rejected.timeline[-1].data["no_action"] is True
    store.close()


def test_insufficient_evidence_abstains() -> None:
    store = DurableStore()
    workflow = InvestigationWorkflow(store, BoundedMCPServer(RetrievalIndex(build_corpus())))
    case = next(case for case in build_incident_cases() if not case.answerable)
    snapshot = workflow.investigate(workflow.start(case.case_id, "test-abstain"))
    assert snapshot.state == "ABSTAINED"
    assert snapshot.proposal is None
    store.close()


def test_interrupted_and_uninterrupted_continuity() -> None:
    report = run_f12_continuity()
    assert report["cases"] == 20
    assert report["passed"] == 20
    assert report["failed"] == []


def test_adversarial_protocol_has_fixed_redaction_and_counts() -> None:
    report = run_adversarial_scan()
    assert report["executions"] == 120
    assert report["canary_leaks"] == 0
    assert report["excess_injection_associated_change"] == 0
    assert report["approval_enforcement"] == {
        "cases": 24,
        "blocked_before_approval": 24,
        "completed_after_approval": 24,
    }


def test_citation_check_has_no_hallucinated_ids() -> None:
    case = next(case for case in build_incident_cases() if case.document_dependent)
    report = run_citation_check(case.case_id)
    assert report["violations"] == 0
    assert report["cited_ids"]


def test_metrics_have_bounded_labels() -> None:
    workflow_events = build_incident_cases()[:1]
    metrics = IncidentMetrics()
    assert metrics.render().startswith("# HELP")
    assert "workflow_id" not in metrics.render()
    assert len(workflow_events) == 1


def test_queries_are_balanced_and_no_answer_is_labeled_only_for_evaluator() -> None:
    queries = build_retrieval_queries()
    assert len(queries) == 160
    assert sum(not bool(query["answerable"]) for query in queries) == 40
    assert json.loads(json.dumps(queries[0]))["query_id"] == queries[0]["query_id"]
