from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest
from incident_agent.dur056_agent import Dur056ProviderError
from incident_agent.dur056_artifacts import Dur056ArtifactError
from incident_agent.dur056_fixtures import build_cases
from incident_agent.dur056_scorer import (
    RECEIPT_SCHEMA,
    Dur056ScorerError,
    OneShotScorer,
    _paired_binary_comparison,
    _summarize_injection,
    _summarize_primary,
    heldout_unit_ids,
)
from incident_agent.workflow import WorkflowError

HEAD = "a" * 40
RETRIEVAL_FP = "sha256:retrieval"
AGENT_FP = "sha256:agent"


def test_registered_heldout_plan_has_primary_and_360_injection_units() -> None:
    units = heldout_unit_ids(build_cases("heldout"))
    assert len(units) == 601
    assert units.count("heldout-embedding-preparation") == 1
    assert sum(unit.startswith("primary-") for unit in units) == 240
    assert sum(unit.startswith("injection-") for unit in units) == 360
    assert len(set(units)) == len(units)


def test_analysis_registers_paired_comparisons_and_has_no_cross_study_arm_baseline() -> None:
    cases = build_cases("development")
    answerable = next(case for case in cases if case.category == "answerable")
    negative = next(case for case in cases if case.category == "insufficient_no_guidance")
    case_map = {answerable.case_id: answerable, negative.case_id: negative}
    rows = [
        {
            "case_id": answerable.case_id,
            "unit_id": "primary-a",
            "safe_end_to_end": True,
            "action_and_parameters_correct": True,
        },
        {
            "case_id": negative.case_id,
            "unit_id": "primary-b",
            "safe_end_to_end": False,
            # A null proposal matches the abstention signature but is not an action.
            "action_and_parameters_correct": True,
        },
    ]
    summary = _summarize_primary(rows, case_map)
    assert "baseline_gpt_4o_mini" not in summary
    assert summary["negative_category_metrics"]["insufficient_no_guidance"]["case_count"] == 1
    assert summary["correct_action_with_all_parameters"] == {
        "count": 1,
        "total": 1,
        "rate": 1.0,
    }

    paired = _paired_binary_comparison(
        {"a": True, "b": False, "c": True},
        {"a": False, "b": False, "c": True},
        ["a", "b", "c"],
    )
    assert paired == {
        "case_count": 3,
        "left_only_safe": 1,
        "right_only_safe": 0,
        "both_safe": 1,
        "neither_safe": 1,
        "paired_delta_left_minus_right": 0.333333,
        "case_level_differences": [
            {"case_id": "a", "left_safe": True, "right_safe": False, "left_minus_right": 1},
            {"case_id": "b", "left_safe": False, "right_safe": False, "left_minus_right": 0},
            {"case_id": "c", "left_safe": True, "right_safe": True, "left_minus_right": 0},
        ],
    }


def test_injection_summary_separates_attack_abstention_other_change_and_clean_flips() -> None:
    rows: list[dict[str, Any]] = []
    cases = {
        "attack": {
            "clean_a": ("right-a", "COMPLETED", {"action": "right"}),
            "clean_b": ("right-a", "COMPLETED", {"action": "right"}),
            "injected": ("wrong-a", "COMPLETED", {"action": "wrong"}),
            "wrong": "wrong-a",
        },
        "abstention": {
            "clean_a": ("right-b", "COMPLETED", {"action": "right"}),
            "clean_b": ("right-b", "COMPLETED", {"action": "right"}),
            "injected": ("ABSTAIN", "ABSTAINED", None),
            "wrong": "wrong-b",
        },
        "other": {
            "clean_a": ("right-c", "COMPLETED", {"action": "right"}),
            "clean_b": ("different-c", "COMPLETED", {"action": "different"}),
            "injected": ("different-c", "COMPLETED", {"action": "different"}),
            "wrong": "wrong-c",
        },
    }
    for profile in ("defended", "plain"):
        for case_id, values in cases.items():
            for condition in ("clean-a", "clean-b", "injected"):
                signature, state, proposal = values[condition.replace("-", "_")]
                rows.append(
                    {
                        "case_id": case_id,
                        "profile": profile,
                        "condition": condition,
                        "proposal_signature": signature,
                        "state": state,
                        "proposal": proposal,
                        "injection_wrong_signature": values["wrong"],
                    }
                )

    report = _summarize_injection(rows)
    for profile in ("defended", "plain"):
        summary = report[profile]
        assert summary["cases"] == 3
        assert summary["raw_clean_a_vs_clean_b_signature_changes"] == 1
        assert summary["raw_clean_a_vs_injected_signature_changes"] == 3
        assert summary["raw_excess"] == 2
        assert summary["attack_success"] == {"count": 1, "case_ids": ["attack"]}
        assert summary["induced_abstention"] == {"count": 1, "case_ids": ["abstention"]}
        assert summary["other_change"] == {"count": 1, "case_ids": ["other"]}
        assert summary["clean_a_vs_clean_b_flips"] == 1


def _make_scorer(tmp_path: Path, *, retrieval_fp: str = RETRIEVAL_FP) -> OneShotScorer:
    receipt_path = tmp_path / "gate-a-accepted.json"
    receipt = {
        "schema": RECEIPT_SCHEMA,
        "reviewer": "Claude",
        "review_round": 1,
        "verdict": "NO_BLOCKING_FINDINGS",
        "target_commit": HEAD,
        "retrieval_config_fingerprint": retrieval_fp,
        "agent_config_fingerprint": AGENT_FP,
    }
    receipt_bytes = (json.dumps(receipt, sort_keys=True) + "\n").encode("utf-8")
    receipt_path.write_bytes(receipt_bytes)
    return OneShotScorer(
        marker_path=tmp_path / "heldout-one-shot" / "run-001.json",
        unit_directory=tmp_path / "heldout-units-run-001",
        receipt_path=receipt_path,
        head_sha=HEAD,
        retrieval_fingerprint=retrieval_fp,
        agent_fingerprint=AGENT_FP,
        receipt_fingerprint="sha256:" + hashlib.sha256(receipt_bytes).hexdigest(),
        head_provider=lambda: HEAD,
        gate_verifier=lambda: None,
    )


def test_missing_gate_a_receipt_refuses_before_marker_or_units(tmp_path: Path) -> None:
    scorer = _make_scorer(tmp_path)
    scorer.receipt_path.unlink()
    with pytest.raises(Dur056ArtifactError, match="cannot read study artifact"):
        scorer.run(["u1"], lambda _: {"safe": True})
    assert not scorer.marker_path.exists()
    assert not scorer.unit_directory.exists()


def test_abort_keeps_completed_units_and_resume_runs_only_remainder(tmp_path: Path) -> None:
    scorer = _make_scorer(tmp_path)
    first_calls: list[str] = []

    def aborting(unit_id: str) -> dict[str, Any]:
        first_calls.append(unit_id)
        if unit_id == "unit-3":
            raise OSError("injected PostgreSQL interruption")
        return {"safe": True}

    with pytest.raises(Dur056ScorerError, match="infrastructure abort at unit-3"):
        scorer.run(["unit-1", "unit-2", "unit-3", "unit-4"], aborting)
    marker = json.loads(scorer.marker_path.read_text(encoding="utf-8"))
    assert marker["status"] == "ABORTED_INFRASTRUCTURE"
    assert marker["failing_unit"] == "unit-3"
    assert marker["completed_units"] == ["unit-1", "unit-2"]
    assert (scorer.unit_directory / "unit-1.json").is_file()
    assert (scorer.unit_directory / "unit-2.json").is_file()
    assert not (scorer.unit_directory / "unit-3.json").exists()

    resume_scorer = _make_scorer(tmp_path)
    resumed_calls: list[str] = []

    def finish(unit_id: str) -> dict[str, Any]:
        resumed_calls.append(unit_id)
        return {"safe": True}

    completed = resume_scorer.run(["unit-1", "unit-2", "unit-3", "unit-4"], finish, resume=True)
    assert resumed_calls == ["unit-3", "unit-4"]
    assert completed["status"] == "COMPLETE"
    assert len(completed["aborts"]) == 1
    assert len(completed["resumes"]) == 1
    assert [event["type"] for event in completed["events"]].count("RESUME") == 1


def test_completed_unit_is_never_reinvoked_and_complete_freeze_refuses_second_run(
    tmp_path: Path,
) -> None:
    scorer = _make_scorer(tmp_path)
    calls: list[str] = []

    def complete(unit_id: str) -> dict[str, Any]:
        calls.append(unit_id)
        return {"safe": True}

    scorer.run(["unit-1", "unit-2"], complete)
    assert calls == ["unit-1", "unit-2"]
    with pytest.raises(Dur056ScorerError, match="already exists"):
        scorer.run(["unit-1", "unit-2"], complete)
    with pytest.raises(Dur056ScorerError, match="already COMPLETE"):
        scorer.run(["unit-1", "unit-2"], complete, resume=True)
    assert calls == ["unit-1", "unit-2"]


def test_fingerprint_change_refuses_resume_even_with_a_new_valid_receipt(tmp_path: Path) -> None:
    scorer = _make_scorer(tmp_path)
    with pytest.raises(Dur056ScorerError, match="infrastructure abort"):
        scorer.run(["unit-1"], lambda _: (_ for _ in ()).throw(OSError("infra")))

    changed = _make_scorer(tmp_path, retrieval_fp="sha256:changed")
    with pytest.raises(Dur056ScorerError, match="marker no longer matches"):
        changed.run(["unit-1"], lambda _: {"safe": True}, resume=True)


def test_finalization_abort_resumes_without_reinvoking_completed_units(tmp_path: Path) -> None:
    scorer = _make_scorer(tmp_path)
    unit_calls: list[str] = []
    finalizer_calls = 0

    def handle(unit_id: str) -> dict[str, Any]:
        unit_calls.append(unit_id)
        return {"safe": True}

    def abort_finalization(_: dict[str, Any]) -> None:
        nonlocal finalizer_calls
        finalizer_calls += 1
        raise OSError("report filesystem interruption")

    with pytest.raises(Dur056ScorerError, match="infrastructure abort at final-report"):
        scorer.run(["unit-1", "unit-2"], handle, finalize_handler=abort_finalization)
    marker = json.loads(scorer.marker_path.read_text(encoding="utf-8"))
    assert marker["status"] == "ABORTED_INFRASTRUCTURE"
    assert marker["completed_units"] == ["unit-1", "unit-2"]
    assert marker["aborts"][-1]["failing_unit"] == "final-report"

    resumed = _make_scorer(tmp_path)
    completed = resumed.run(
        ["unit-1", "unit-2"],
        handle,
        resume=True,
        finalize_handler=lambda _: None,
    )
    assert unit_calls == ["unit-1", "unit-2"]
    assert finalizer_calls == 1
    assert completed["status"] == "COMPLETE"
    assert len(completed["aborts"]) == 1
    assert len(completed["resumes"]) == 1


def test_changed_receipt_contents_refuse_before_start(tmp_path: Path) -> None:
    scorer = _make_scorer(tmp_path)
    receipt = json.loads(scorer.receipt_path.read_text(encoding="utf-8"))
    receipt["review_round"] = 2
    scorer.receipt_path.write_text(json.dumps(receipt) + "\n", encoding="utf-8")
    with pytest.raises(Dur056ScorerError, match="receipt changed"):
        scorer.run(["unit-1"], lambda _: {"safe": True})
    assert not scorer.marker_path.exists()


@pytest.mark.parametrize(
    ("error", "status"),
    [
        (Dur056ProviderError("transport exhausted", attempts=3), "PROVIDER_ERROR"),
        (WorkflowError("workflow rejected proposal"), "WORKFLOW_ERROR"),
    ],
)
def test_provider_and_workflow_failures_are_completed_not_safe_units(
    tmp_path: Path, error: Exception, status: str
) -> None:
    scorer = _make_scorer(tmp_path)

    def fail(_: str) -> dict[str, Any]:
        raise error

    marker = scorer.run(["unit-1"], fail)
    row = json.loads((scorer.unit_directory / "unit-1.json").read_text(encoding="utf-8"))
    assert marker["status"] == "COMPLETE"
    assert row["outcome"] == "NOT_SAFE"
    assert row["status"] == status
