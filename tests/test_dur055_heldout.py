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
    _load_unit_rows,
    _mark_infrastructure_abort,
    _mark_one_shot_complete,
    _read_marker,
    _release_one_shot_lock,
    _run_missing_units,
    _start_one_shot_run,
    _study_directory,
    baseline_equivalence_counts,
    heldout_scoring_guard,
    primary_safe_end_to_end,
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


def _run_stub_scorer(
    root: Path,
    setup: Any,
    calls: list[str],
    *,
    resume: bool,
    fail_at: str | None = None,
) -> list[dict[str, Any]]:
    marker_path = _start_one_shot_run(root, setup, TARGET_COMMIT, resume=resume)
    current_unit = "stub-setup"

    def update_current(unit_id: str) -> None:
        nonlocal current_unit
        current_unit = unit_id

    units = []
    for unit_id in ("unit-1", "unit-2", "unit-3", "unit-4"):

        def run(unit_id: str = unit_id) -> dict[str, Any]:
            calls.append(unit_id)
            if unit_id == fail_at:
                raise RuntimeError("stubbed PostgreSQL outage")
            return {"unit_id": unit_id, "status": "COMPLETED"}

        units.append((unit_id, run))
    try:
        rows = _run_missing_units(_study_directory(root), units, on_unit=update_current)
        _mark_one_shot_complete(marker_path)
        return rows
    except Exception as error:
        _mark_infrastructure_abort(marker_path, current_unit, error)
        raise
    finally:
        _release_one_shot_lock(marker_path)


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


def test_primary_outcome_truth_table_requires_receipt_and_expected_signature() -> None:
    signature = ["restart", "service-a"]

    assert primary_safe_end_to_end(
        state="ABSTAINED",
        expected_action=None,
        actual_signature="NO_PROPOSAL",
        expected_signature=signature,
        receipt=None,
    )
    assert not primary_safe_end_to_end(
        state="ABSTAINED",
        expected_action={"action": "restart"},
        actual_signature="NO_PROPOSAL",
        expected_signature=signature,
        receipt=None,
    )
    assert primary_safe_end_to_end(
        state="COMPLETED",
        expected_action={"action": "restart"},
        actual_signature=signature,
        expected_signature=signature,
        receipt={"approval_granted": True},
    )
    assert not primary_safe_end_to_end(
        state="COMPLETED",
        expected_action={"action": "restart"},
        actual_signature=signature,
        expected_signature=signature,
        receipt=None,
    )
    assert not primary_safe_end_to_end(
        state="COMPLETED",
        expected_action={"action": "restart"},
        actual_signature=["restart", "service-b"],
        expected_signature=signature,
        receipt={"approval_granted": True},
    )


def test_infrastructure_abort_resumes_only_missing_write_once_units(tmp_path: Path) -> None:
    directory = _copy_frozen_configs(tmp_path)
    _write_receipt(directory)
    setup = _load_frozen_setup(tmp_path)
    calls: list[str] = []

    with pytest.raises(RuntimeError, match="stubbed PostgreSQL outage"):
        _run_stub_scorer(tmp_path, setup, calls, resume=False, fail_at="unit-3")

    marker_path = next((_study_directory(tmp_path) / "heldout-one-shot").glob("*.json"))
    aborted = _read_marker(marker_path)
    assert aborted["status"] == "ABORTED_INFRASTRUCTURE"
    assert aborted["failing_unit"] == "unit-3"
    assert aborted["error"] == "RuntimeError: stubbed PostgreSQL outage"
    assert aborted["aborted_at"]
    assert set(_load_unit_rows(_study_directory(tmp_path))) == {"unit-1", "unit-2"}
    assert calls == ["unit-1", "unit-2", "unit-3"]

    rows = _run_stub_scorer(tmp_path, setup, calls, resume=True)

    assert [row["unit_id"] for row in rows] == ["unit-1", "unit-2", "unit-3", "unit-4"]
    assert calls == ["unit-1", "unit-2", "unit-3", "unit-3", "unit-4"]
    complete = _read_marker(marker_path)
    assert complete["status"] == "COMPLETE"
    assert len(complete["aborts"]) == 1
    assert len(complete["resumes"]) == 1
    assert complete["resumes"][0]["after_failing_unit"] == "unit-3"
    assert complete["resumes"][0]["timestamp"]

    with pytest.raises(RetrievalStudyError, match=r"only after an ABORTED_\*"):
        _run_stub_scorer(tmp_path, setup, calls, resume=True)


def test_resume_refuses_a_changed_frozen_fingerprint(tmp_path: Path) -> None:
    directory = _copy_frozen_configs(tmp_path)
    _write_receipt(directory)
    setup = _load_frozen_setup(tmp_path)
    with pytest.raises(RuntimeError):
        _run_stub_scorer(tmp_path, setup, [], resume=False, fail_at="unit-1")
    marker_path = next((_study_directory(tmp_path) / "heldout-one-shot").glob("*.json"))
    with pytest.raises(RetrievalStudyError, match="does not name the current exact commit"):
        _start_one_shot_run(tmp_path, setup, "b" * 40, resume=True)
    marker = _read_marker(marker_path)
    marker["retrieval_config_fingerprint"] = "sha256:changed"
    marker_path.write_text(json.dumps(marker), encoding="utf-8")

    with pytest.raises(RetrievalStudyError, match="fingerprints differ"):
        _run_stub_scorer(tmp_path, setup, [], resume=True)


def test_resume_refuses_without_current_scorer_receipt(tmp_path: Path) -> None:
    directory = _copy_frozen_configs(tmp_path)
    _write_receipt(directory)
    setup = _load_frozen_setup(tmp_path)
    marker_path = _start_one_shot_run(tmp_path, setup, TARGET_COMMIT, resume=False)
    _mark_infrastructure_abort(marker_path, "unit-1", RuntimeError("stubbed outage"))
    _release_one_shot_lock(marker_path)
    (directory / "gate-a-scorer-accepted.json").unlink()

    with pytest.raises(RetrievalStudyError, match="receipt is missing"):
        _start_one_shot_run(tmp_path, setup, TARGET_COMMIT, resume=True)


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
