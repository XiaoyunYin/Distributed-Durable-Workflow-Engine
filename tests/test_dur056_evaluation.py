from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from incident_agent.dur056_agent import (
    Dur056ProviderError,
    OpenAIResponsesDecisionProvider,
)
from incident_agent.dur056_budget import Dur056SpendLedger
from incident_agent.dur056_evaluation import (
    _dev_summary,
    analyze_development_report,
    run_workflow_case,
)
from incident_agent.dur056_fixtures import Dur056Case, as_incident_case, build_cases
from incident_agent.dur056_retrieval import NoRetrievalAdapter
from incident_agent.mcp import BoundedMCPServer

LEDGER_SOURCE = (
    Path(__file__).parents[1]
    / "experiments"
    / "m8"
    / "dur056-solvable-agent-study"
    / "spend-ledger.json"
)


class StubDecisionProvider:
    def __init__(
        self, case: Dur056Case, *, invalid_citation: bool = False, fail: bool = False
    ) -> None:
        self.case = case
        self.invalid_citation = invalid_citation
        self.fail = fail
        self.config = {"prompt_profile": "evidence-contract-v2"}
        self.records: list[dict[str, Any]] = []
        self.current_arm = "unknown"
        self.last_rendered_prompt = "stub"
        self.last_model_record: dict[str, Any] = {}

    def diagnose(
        self, case_id: str, evidence_ids: tuple[str, ...], tool_text: str
    ) -> dict[str, Any]:
        if self.fail:
            raise Dur056ProviderError("stub transport exhausted", attempts=3, uncertain=True)
        citations = ["unknown-evidence"] if self.invalid_citation else list(evidence_ids)
        return {
            "diagnosis": self.case.expected_diagnosis,
            "citations": citations,
            "proposal": self.case.expected_action,
            "uncertainty": self.case.expected_action is None,
        }


class RecordingNoRetrievalAdapter(NoRetrievalAdapter):
    def __init__(self) -> None:
        self.queries: list[str] = []

    def search(self, query: str, arm: Any = "hybrid") -> Any:
        self.queries.append(query)
        return super().search(query, arm)


def _run(
    case: Dur056Case,
    provider: StubDecisionProvider,
    index: NoRetrievalAdapter | None = None,
) -> dict[str, Any]:
    return run_workflow_case(
        case,
        all_cases=(case,),
        index=index or NoRetrievalAdapter(),
        decision_provider=provider,  # type: ignore[arg-type]
        arm="no_retrieval",
        run_id=f"stub-{case.case_id}-{len(provider.records)}-{provider.fail}-{provider.invalid_citation}",
    )


def test_stub_model_runs_through_official_mcp_client_and_approval_receipt() -> None:
    case = next(case for case in build_cases("development") if case.category == "answerable")
    index = RecordingNoRetrievalAdapter()
    row = _run(case, StubDecisionProvider(case), index)
    assert row["state"] == "COMPLETED"
    assert row["safe_end_to_end"] is True
    assert row["approval_actor"] == "fixture-approver"
    assert row["receipt"]["state"] == "APPLIED"
    assert row["mcp_call_methods"][:2] == ["query_logs", "query_metrics"]
    assert "search_runbooks" in row["mcp_call_methods"]
    assert row["retrieval_sufficiency_predictions"] == [False]
    search_event = next(
        event
        for event in row["timeline"]
        if event["event_type"] == "mcp_tool_call"
        and event["data"].get("method") == "search_runbooks"
    )
    search_result = search_event["data"]["result_data"]
    assert "sufficient" not in search_result
    assert "reason" not in search_result
    assert row["citation_provenance_violations"] == 0
    assert index.queries
    assert set(index.queries) == {case.query_clean_a}


def test_stub_model_correctly_abstains_on_insufficient_evidence() -> None:
    case = next(
        case for case in build_cases("development") if case.category == "insufficient_no_guidance"
    )
    row = _run(case, StubDecisionProvider(case))
    assert row["state"] == "ABSTAINED"
    assert row["safe_end_to_end"] is True
    assert row["correct_abstention"] is True
    assert row["action_and_parameters_correct"] is False
    assert row["receipt"] is None


def test_development_summary_separates_correct_actions_from_abstentions() -> None:
    common = {
        "safe_end_to_end": True,
        "unsafe_negative_proposal": False,
        "wrong_parameter_proposal": False,
        "citation_provenance_violations": 0,
        "diagnosis_correct": True,
        "input_tokens": 10,
        "output_tokens": 5,
        "reasoning_tokens": 0,
        "latency_ms": 100.0,
        "wall_latency_ms": 110.0,
        "cost_usd": "0.0001",
    }
    rows = [
        {
            **common,
            "expected_action": {"action": "scale_pool"},
            "action_and_parameters_correct": True,
            "correct_abstention": False,
            "false_abstention": False,
            "category": "answerable",
        },
        {
            **common,
            "expected_action": None,
            "action_and_parameters_correct": True,
            "correct_abstention": True,
            "false_abstention": False,
            "category": "insufficient_no_guidance",
        },
        {
            **common,
            "expected_action": {"action": "set_timeout"},
            "action_and_parameters_correct": False,
            "correct_abstention": False,
            "false_abstention": True,
            "category": "answerable",
        },
    ]
    summary = _dev_summary(rows)
    assert summary["answerable_case_count"] == 2
    assert summary["correct_action_and_parameters"] == 1
    assert summary["correct_action_and_parameters_rate"] == 0.5
    assert summary["correct_abstentions"] == 1
    assert summary["false_abstentions"] == 1


def test_analysis_recomputes_answerable_metrics_from_immutable_run_rows() -> None:
    rows: list[dict[str, Any]] = []
    arms = ("no_retrieval", "dense", "keyword", "hybrid")
    for candidate in ("candidate-v1", "candidate-v2"):
        for arm in arms:
            for index in range(30):
                expected_action = {"action": "scale_pool"} if index < 21 else None
                rows.append(
                    {
                        "run_id": f"dur056-dev-{candidate}-{arm}-case-{index:02d}",
                        "arm": arm,
                        "category": "answerable" if expected_action else "insufficient_no_guidance",
                        "expected_action": expected_action,
                        # Reproduce the old aggregate defect: no-action signatures match.
                        "action_and_parameters_correct": True,
                        "safe_end_to_end": candidate == "candidate-v2" or index < 29,
                        "correct_abstention": expected_action is None,
                        "false_abstention": False,
                        "unsafe_negative_proposal": False,
                        "wrong_parameter_proposal": False,
                        "citation_provenance_violations": 0,
                        "diagnosis_correct": True,
                        "input_tokens": 10,
                        "output_tokens": 5,
                        "reasoning_tokens": 1,
                        "latency_ms": 100.0,
                        "wall_latency_ms": 110.0,
                        "cost_usd": "0.0001",
                    }
                )
    report = analyze_development_report(
        {
            "schema": "dur056-agent-development.v1",
            "split": "development",
            "heldout_model_calls": 0,
            "heldout_runs": 0,
            "candidate_definitions": {"candidate-v1": {}, "candidate-v2": {}},
            "rows": rows,
        }
    )
    assert report["source_run_count"] == 240
    assert report["selected_candidate"] == "candidate-v2"
    v2_keyword = report["candidate_summaries"]["candidate-v2"]["arms"]["keyword"]
    assert v2_keyword["answerable_case_count"] == 21
    assert v2_keyword["correct_action_and_parameters"] == 21
    assert v2_keyword["correct_abstentions"] == 9


def test_invalid_citation_is_recorded_as_unsafe_and_provenance_violation() -> None:
    case = next(case for case in build_cases("development") if case.category == "answerable")
    row = _run(case, StubDecisionProvider(case, invalid_citation=True))
    assert row["state"] == "FAILED"
    assert row["safe_end_to_end"] is False
    assert row["citation_provenance_violations"] == 1


def test_exhausted_provider_error_counts_as_not_safe() -> None:
    case = next(case for case in build_cases("development") if case.category == "answerable")
    row = _run(case, StubDecisionProvider(case, fail=True))
    assert row["state"] == "FAILED"
    assert row["safe_end_to_end"] is False
    assert row["provider_error"]["kind"] == "PROVIDER_ERROR"
    assert row["provider_error"]["attempts"] == 3


def test_agent_provider_refuses_heldout_case_without_gate_a_authorization(
    tmp_path: Path, monkeypatch: Any
) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "stub-no-network")
    ledger_path = tmp_path / "spend-ledger.json"
    source_ledger = json.loads(LEDGER_SOURCE.read_text(encoding="utf-8"))
    expected_entries = source_ledger["entries"]
    ledger_path.write_text(json.dumps(source_ledger), encoding="utf-8")
    provider = OpenAIResponsesDecisionProvider(
        "candidate-v2",
        [],
        Dur056SpendLedger(ledger_path),
        heldout_authorized=False,
    )
    with pytest.raises(Dur056ProviderError, match="held-out model call requires"):
        provider.diagnose("hel-fake-case", (), "{}")
    data = json.loads(ledger_path.read_text(encoding="utf-8"))
    assert data["entries"] == expected_entries


def test_all_development_canaries_are_redacted_before_mcp_output() -> None:
    for case in build_cases("development"):
        server = BoundedMCPServer(
            NoRetrievalAdapter(),
            cases=(as_incident_case(case),),
        )
        result = server.query_logs("redaction-audit", case.case_id, case.service)
        serialized = json.dumps(result.data, sort_keys=True)
        assert result.redactions == 1
        assert case.canary not in serialized
        assert "[REDACTED:CANARY]" in serialized
