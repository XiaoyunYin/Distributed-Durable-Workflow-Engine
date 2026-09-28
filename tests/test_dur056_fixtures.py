from __future__ import annotations

import json
import re

from incident_agent.dur056_fixtures import (
    ACTION_PARAMETERS,
    DEV_QUERY_TEMPLATES,
    FAMILIES,
    HELDOUT_QUERY_TEMPLATES,
    all_fixture_text,
    build_cases,
    build_corpus,
    oracle_decision,
    oracle_report,
)


def test_dur056_split_counts_and_family_balance_are_registered() -> None:
    development = build_cases("development")
    heldout = build_cases("heldout")
    assert len(development) == 30
    assert len(heldout) == 60
    assert all(case.split == "development" for case in development)
    assert all(case.split == "heldout" for case in heldout)
    assert len(build_corpus("development")) > 0
    assert len(build_corpus("heldout")) > 0
    for family in FAMILIES:
        assert (
            sum(case.family == family and case.category == "answerable" for case in development)
            == 3
        )
        assert sum(case.family == family and case.category == "answerable" for case in heldout) == 6
    assert sum(case.category.startswith("insufficient_") for case in development) == 6
    assert sum(case.category.startswith("insufficient_") for case in heldout) == 12
    assert sum(case.category.startswith("stale_") for case in development) == 6
    assert sum(case.category.startswith("stale_") for case in heldout) == 12
    assert sum(case.category == "near_duplicate_decoy" for case in development) == 6
    assert sum(case.category == "near_duplicate_decoy" for case in heldout) == 12


def test_answerable_evidence_states_action_and_every_required_parameter() -> None:
    for split in ("development", "heldout"):
        for case in build_cases(split):
            if case.category not in {"answerable", "stale_current_action", "near_duplicate_decoy"}:
                continue
            assert case.expected_action is not None
            current_evidence = "\n".join(
                chunk.text
                for chunk in case.evidence
                if chunk.service == case.service and chunk.version == case.active_version
            )
            assert f"action={case.expected_action['action']}" in current_evidence
            for parameter in ACTION_PARAMETERS[case.expected_action["action"]]:
                assert parameter in current_evidence
                assert str(case.expected_action["parameters"][parameter]) in current_evidence


def test_insufficient_cases_lack_guidance_or_a_required_parameter() -> None:
    for split in ("development", "heldout"):
        for case in build_cases(split):
            if case.category == "insufficient_no_guidance":
                assert all("DIRECTIVE:" not in chunk.text for chunk in case.evidence)
            elif case.category == "insufficient_missing_parameter":
                match = re.search(
                    r"DIRECTIVE: action=([a-z_]+); service=[a-z-]+; version=v\d+; "
                    r"parameters=(\{[^}]*\})\.",
                    all_fixture_text(case),
                )
                assert match is not None
                action, raw_parameters = match.groups()
                assert action in ACTION_PARAMETERS
                required = ACTION_PARAMETERS[action]
                parsed_parameters = json.loads(raw_parameters)
                assert any(parameter not in parsed_parameters for parameter in required)


def test_stale_and_decoy_cases_have_preregistered_conflict_shapes() -> None:
    for split, count in (("development", 3), ("heldout", 6)):
        cases = build_cases(split)
        assert sum(case.category == "stale_current_action" for case in cases) == count
        assert sum(case.category == "stale_unresolved_conflict" for case in cases) == count
        for case in cases:
            if case.category == "stale_current_action":
                assert any("STATUS=SUPERSEDED" in chunk.text for chunk in case.evidence)
                assert oracle_decision(case) == case.expected_action
            elif case.category == "stale_unresolved_conflict":
                assert case.expected_action is None
                assert oracle_decision(case) is None
            elif case.category == "near_duplicate_decoy":
                assert any(chunk.service != case.service for chunk in case.evidence)
                assert oracle_decision(case) == case.expected_action


def test_dev_and_heldout_query_template_sets_are_disjoint() -> None:
    assert set(DEV_QUERY_TEMPLATES).isdisjoint(HELDOUT_QUERY_TEMPLATES)
    for split in ("development", "heldout"):
        queries = [case.query_clean_a for case in build_cases(split)]
        assert len(queries) == len(set(queries))


def test_deterministic_local_oracle_scores_both_splits_at_100_percent() -> None:
    for split, expected_count in (("development", 30), ("heldout", 60)):
        report = oracle_report(split)
        assert report["cases"] == expected_count
        assert report["correct"] == expected_count
        assert report["accuracy"] == 1.0
        assert report["provider_calls"] == 0
        for case in build_cases(split):
            assert oracle_decision(case) == case.expected_action
