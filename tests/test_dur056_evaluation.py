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
from incident_agent.dur056_evaluation import run_workflow_case
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
    assert row["receipt"] is None


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
