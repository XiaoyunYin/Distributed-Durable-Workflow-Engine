from __future__ import annotations

import json

import pytest
from incident_agent.continuity import (
    run_adversarial_scan,
    run_citation_check,
    run_f12_continuity,
    run_metrics_report,
)
from incident_agent.evaluation import live_phase_status, run_retrieval_final
from incident_agent.fixtures import (
    FAMILIES,
    build_corpus,
    build_incident_cases,
    build_retrieval_queries,
    corpus_manifest,
)
from incident_agent.mcp import BoundedMCPServer, MCPValidationError, serialize_tool_result
from incident_agent.metrics import IncidentMetrics, dashboard_manifest
from incident_agent.models import Proposal
from incident_agent.openai_provider import (
    CostLedger,
    OpenAIDecisionProvider,
    OpenAIProviderSettings,
)
from incident_agent.redaction import redact, scan_downstream
from incident_agent.retrieval import RetrievalConfig, RetrievalIndex
from incident_agent.source_corpus import SourceCorpusStore, source_corpus_contract
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
    assert manifest["distinct_development_query_strings"] == 40
    assert manifest["distinct_heldout_query_strings"] == 120
    assert manifest["near_duplicate_document_pairs"] == 30
    assert manifest["canary_chunks"] == 60
    assert len(FAMILIES) == 5
    assert manifest["families"] == list(FAMILIES)


def test_source_corpus_persists_raw_chunks_behind_declared_boundary() -> None:
    chunks = build_corpus(seed_canaries=True)
    cases = build_incident_cases(seed_canaries=True)
    store = SourceCorpusStore.from_fixtures(chunks, cases)
    assert source_corpus_contract()["schema"] == "source_corpus"
    assert store.counts() == {"documents": 60, "chunks": 300, "fixture_cases": 30}
    assert store.chunks() == chunks
    store.close()


def test_retrieval_arms_share_chunks_and_hybrid_has_or_gate() -> None:
    index = RetrievalIndex(build_corpus(), RetrievalConfig())
    query = next(query for query in build_retrieval_queries() if query["answerable"])
    answer = index.search(str(query["query"]), "keyword")
    dense = index.search(str(query["query"]), "dense")
    hybrid = index.search(str(query["query"]), "hybrid")
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
    assert completed.citations
    assert sum(event.event_type == "mcp_tool_call" for event in completed.timeline) >= 3
    effect_key = f"{run_id}:{completed.proposal.canonical_argument_hash}"
    assert store.grant(effect_key)["state"] == "DISPATCHED"
    assert store.effect(effect_key) == completed.action_receipt
    assert again.action_receipt == completed.action_receipt
    assert [event.event_type for event in again.timeline].count("effect_dispatched") == 1
    forged = Proposal(
        completed.proposal.action_type,
        completed.proposal.target_resource_id,
        {"revision": "forged"},
        completed.proposal.canonical_argument_hash,
    )
    with pytest.raises(WorkflowError, match="arguments do not match"):
        workflow.effect.apply(run_id, forged)
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
    assert all(row["checkpoint_replayed"] for row in report["rows"])


def test_adversarial_protocol_has_fixed_redaction_and_counts() -> None:
    report = run_adversarial_scan()
    assert report["executions"] == 120
    assert report["canary_leaks"] == 0
    assert report["canary_leaks_by_profile"] == {"defended": 0, "plain": 0}
    assert report["clean_clean_flip_rate"] == 0
    assert report["clean_injected_change_rate_by_profile"] == {"defended": 0, "plain": 1}
    assert report["excess_injection_associated_change_rate_difference"] == 1
    assert report["injection_changes_by_profile"] == {"defended": 0, "plain": 20}
    assert report["negative_control_fired"] is True
    assert report["negative_control"] == {
        "redaction_disabled": True,
        "canary_leaks": 5,
        "unsafe_proposal": True,
    }
    assert report["approval_enforcement"] == {
        "cases": 24,
        "blocked_before_approval": 24,
        "completed_after_approval": 24,
    }


def test_citation_check_has_no_hallucinated_ids() -> None:
    report = run_citation_check()
    assert report["cases"] == 30
    assert report["violations"] == 0
    assert report["uncited_proposals"] == 0
    assert report["cited_ids"]
    assert report["negative_control_fired"] is True
    assert report["uncited_proposal_negative_control_fired"] is True


def test_metrics_have_bounded_labels() -> None:
    workflow_events = build_incident_cases()[:1]
    metrics = IncidentMetrics()
    assert metrics.render().startswith("# HELP")
    assert "workflow_id" not in metrics.render()
    assert len(workflow_events) == 1
    rendered = run_metrics_report()["metrics"]
    for name in (
        "durable_incident_outcomes_total",
        "durable_incident_tool_calls_total",
        "durable_incident_citation_violations_total",
        "durable_incident_approvals_total",
        "durable_incident_latency_seconds",
        "durable_incident_tokens_total",
        "durable_incident_cost_cents_total",
    ):
        assert name in rendered
    assert all(panel["metric"] in rendered for panel in dashboard_manifest()["panels"])


def test_queries_are_balanced_and_no_answer_is_labeled_only_for_evaluator() -> None:
    queries = build_retrieval_queries()
    assert len(queries) == 160
    assert sum(not bool(query["answerable"]) for query in queries) == 40
    assert json.loads(json.dumps(queries[0]))["query_id"] == queries[0]["query_id"]


def test_dur029_retrieval_final_reports_frozen_split_and_family_metrics() -> None:
    config = RetrievalConfig()
    report = run_retrieval_final(
        RetrievalIndex(build_corpus(), config), build_retrieval_queries(), config
    )
    assert report["status"] == "PASS"
    assert report["schema"] == "dur-029-retrieval-final.v1"
    assert report["development_queries"] == 40
    assert report["heldout_queries"] == 120
    assert report["config_fingerprint"] == config.fingerprint()
    for arm in ("keyword", "dense", "hybrid"):
        assert set(report["arms"][arm]) == {"development", "heldout"}
        assert set(report["arms"][arm]["heldout"]["by_family"]) == set(FAMILIES)
        assert report["arms"][arm]["heldout"]["summary"]["latency_ms"]["count"] == 120
    assert report["conclusions"]["heldout_summary_by_arm"]["keyword"]["ranking_mrr"] >= 0
    assert report["limitations"]


def test_dur029_live_phase_fails_closed_without_authorization() -> None:
    report = live_phase_status(LiveModelConfig("unconfigured", "unconfigured", 0))
    assert report["status"] == "BLOCKED"
    assert report["provider_calls"] == 0


def test_openai_provider_uses_structured_decision_and_budget_ledger(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("INCIDENT_LIVE_APPROVED", "1")
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    provider = OpenAIDecisionProvider(OpenAIProviderSettings(budget_cents=1))
    provider._request = lambda instructions, prompt: {
        "id": "resp-test",
        "output_text": json.dumps(
            {
                "diagnosis": "The checkout deployment is using a bad configuration revision.",
                "citations": ["evidence-1"],
                "proposal": {"action": "rollback", "revision": "v1"},
                "uncertainty": False,
            }
        ),
        "usage": {"input_tokens": 10, "output_tokens": 20},
    }
    decision = provider.diagnose("case-1", ("evidence-1",), '{"evidence": "text"}')
    assert decision["proposal"]["action"] == "rollback"
    assert decision["_response_id"] == "resp-test"
    assert provider.ledger.snapshot()["spent_cents"] > 0
    with pytest.raises(Exception, match="aggregate cap"):
        CostLedger(1).reserve(10_000_000, 0)
