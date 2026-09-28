from __future__ import annotations

import json
import shutil
import urllib.error
from pathlib import Path
from typing import Any

import pytest
from incident_agent.dur055_agent import (
    DUR055ProviderError,
    OpenAIResponsesDecisionProvider,
)
from incident_agent.dur055_heldout import (
    GATE_A_COMMIT,
    MAX_TRANSPORT_RETRIES,
    REVIEWED_AGENT_FINGERPRINT,
    REVIEWED_RETRIEVAL_FINGERPRINT,
    RetrievalStudyError,
    _aggregate_agent_rows,
    _create_one_shot_marker,
    _load_frozen_setup,
    _study_directory,
    baseline_equivalence_counts,
    heldout_scoring_guard,
    summarize_injection_matrix,
)

ROOT = Path(__file__).resolve().parents[1]
STUDY_SOURCE = ROOT / "experiments" / "m8" / "dur055-applied-ai-upgrade"
TARGET_COMMIT = "a" * 40


def _copy_frozen_configs(root: Path) -> Path:
    directory = _study_directory(root)
    directory.mkdir(parents=True)
    shutil.copyfile(
        STUDY_SOURCE / "retrieval-frozen-config-20260928T050903Z.json",
        directory / "retrieval-frozen-config-test.json",
    )
    shutil.copyfile(
        STUDY_SOURCE / "agent-frozen-config-20260928T050959Z.json",
        directory / "agent-frozen-config-test.json",
    )
    return directory


def _write_receipt(directory: Path, target_commit: str = TARGET_COMMIT) -> None:
    receipt = {
        "schema": "dur055-scorer-review.v1",
        "reviewer": "Claude",
        "verdict": "NO_BLOCKING_FINDINGS",
        "target_commit": target_commit,
        "gate_a_commit": GATE_A_COMMIT,
        "retrieval_config_fingerprint": REVIEWED_RETRIEVAL_FINGERPRINT,
        "agent_config_fingerprint": REVIEWED_AGENT_FINGERPRINT,
    }
    (directory / "gate-a-scorer-accepted.json").write_text(json.dumps(receipt), encoding="utf-8")


def test_baseline_equivalence_reproduces_four_of_twenty_per_arm() -> None:
    counts = baseline_equivalence_counts(ROOT / "experiments/m7/dur029/live-evaluation.json")

    assert counts == {
        arm: {
            "safe": 4,
            "cases": 20,
            "document_dependent_safe": 0,
            "document_dependent_cases": 16,
        }
        for arm in ("keyword", "dense", "hybrid")
    }


def test_guard_refuses_without_scorer_review_receipt(tmp_path: Path) -> None:
    _copy_frozen_configs(tmp_path)

    with pytest.raises(RetrievalStudyError, match="scorer-only receipt is missing"):
        heldout_scoring_guard(tmp_path, head_commit=TARGET_COMMIT)


def test_guard_refuses_fingerprint_mismatch_even_with_receipt(tmp_path: Path) -> None:
    directory = _copy_frozen_configs(tmp_path)
    config_path = directory / "retrieval-frozen-config-test.json"
    retrieval = json.loads(config_path.read_text(encoding="utf-8"))
    retrieval["keyword"]["threshold"] = -1
    config_path.write_text(json.dumps(retrieval), encoding="utf-8")
    _write_receipt(directory)

    with pytest.raises(RetrievalStudyError, match="no retrieval-frozen-config"):
        heldout_scoring_guard(tmp_path, head_commit=TARGET_COMMIT)


def test_one_shot_marker_refuses_a_second_run(tmp_path: Path) -> None:
    setup = _load_frozen_setup(ROOT)
    _create_one_shot_marker(tmp_path, setup, TARGET_COMMIT)

    with pytest.raises(RetrievalStudyError, match="one-shot"):
        _create_one_shot_marker(tmp_path, setup, TARGET_COMMIT)


def test_exhausted_provider_error_is_counted_not_safe() -> None:
    rows = [
        {
            "document_dependent": index < 16,
            "primary_safe_end_to_end": False,
            "diagnosis_accuracy": False,
            "correct_abstention": index >= 16,
            "false_abstention": False,
            "citation_provenance_violation_count": 0,
            "status": "PROVIDER_ERROR" if index == 0 else "COMPLETED",
            "case_id": f"heldout-{index:02d}",
            "input_tokens": 0,
            "output_tokens": 0,
            "reasoning_tokens": 0,
            "api_latency_ms": 0.0,
            "wall_latency_ms": 1.0,
            "known_cost_usd": "0.00000000",
            "uncertain_cost_usd": "0.00000000",
            "provider_attempts": 3 if index == 0 else 1,
            "provider_failures": [{"kind": "transport"}] if index == 0 else [],
        }
        for index in range(20)
    ]

    summary = _aggregate_agent_rows(rows)

    assert summary["cases"] == 20
    assert summary["primary_safe_end_to_end"] == 0
    assert summary["provider_error_cases_counted_not_safe"] == 1
    assert summary["runs"][0]["provider_attempts"] == 3


def test_provider_transport_retries_are_bounded_and_stubbed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StubLedger:
        def __init__(self) -> None:
            self.reservations = 0
            self.failures: list[tuple[str, bool]] = []

        def reserve(self, **_values: Any) -> str:
            self.reservations += 1
            return f"call-{self.reservations}"

        def fail(self, call_id: str, *, outcome_uncertain: bool, detail: str) -> None:
            del detail
            self.failures.append((call_id, outcome_uncertain))

    def fail_transport(*_args: Any, **_kwargs: Any) -> Any:
        raise urllib.error.URLError("stubbed transport failure")

    monkeypatch.setenv("OPENAI_API_KEY", "stub-key")
    monkeypatch.setattr("urllib.request.urlopen", fail_transport)
    ledger = StubLedger()
    provider = OpenAIResponsesDecisionProvider(
        "evidence-contract-v1", [], ledger, transport_retries=MAX_TRANSPORT_RETRIES
    )

    with pytest.raises(DUR055ProviderError) as raised:
        provider.diagnose("case-1", (), "{}")

    assert raised.value.attempts == 3
    assert ledger.reservations == 3
    assert ledger.failures == [("call-1", True), ("call-2", True), ("call-3", True)]
    assert len(provider.failures) == 3


def test_injection_excess_is_raw_injected_changes_minus_clean_flips() -> None:
    rows = []
    for profile in ("defended", "plain"):
        for index in range(20):
            signatures = {
                "clean-A": "A",
                "clean-B": "B" if index == 0 else "A",
                "injected": "B" if index < 3 else "A",
            }
            for replicate, signature in signatures.items():
                rows.append(
                    {
                        "profile": profile,
                        "case_id": f"case-{index:02d}",
                        "replicate": replicate,
                        "actual_proposal_signature": signature,
                        "diagnosis": signature,
                        "canary_leaks": [],
                    }
                )

    summary = summarize_injection_matrix(rows)

    assert summary["raw_clean_clean_flip_count_by_profile"] == {"defended": 1, "plain": 1}
    assert summary["raw_clean_injected_change_count_by_profile"] == {"defended": 3, "plain": 3}
    assert summary["raw_excess_injection_associated_change_count_by_profile"] == {
        "defended": 2,
        "plain": 2,
    }
