from __future__ import annotations

import json
from dataclasses import replace

import pytest
from incident_agent.dur056_agent import PROMPT_CANDIDATES, RESPONSE_SCHEMAS
from incident_agent.dur056_fixtures import (
    ACTION_PARAMETERS,
    CATEGORY_ANNOUNCEMENTS,
    DEV_QUERY_TEMPLATES,
    DEV_SEED,
    FAMILIES,
    HELDOUT_QUERY_TEMPLATES,
    HELDOUT_SEED,
    LEGACY_QUERY_PADDING,
    STUDY_VERSION,
    Dur056Case,
    _complete_action,
    _fact_text,
    _parse_facts,
    all_fixture_text,
    build_cases,
    build_corpus,
    corpus_oracle_decision,
    derive_incident_key,
    oracle_decision,
    oracle_report,
    validate_corpus_labels,
    validate_key_label_consistency,
)
from incident_agent.models import EvidenceChunk


def _answer_values(cases: tuple[Dur056Case, ...]) -> set[tuple[str, str, str]]:
    return {
        (str(case.expected_action["action"]), name, str(value))
        for case in cases
        if case.expected_action is not None
        for name, value in case.expected_action["parameters"].items()
    }


def test_dur056_v3_splits_and_family_balance_are_registered() -> None:
    development = build_cases("development")
    heldout = build_cases("heldout")
    assert STUDY_VERSION == "dur056-solvable-evidence-v3"
    assert DEV_SEED == 5703
    assert HELDOUT_SEED == 5704
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


def test_answerable_facts_contain_seeded_actions_and_all_parameters() -> None:
    for split in ("development", "heldout"):
        cases = build_cases(split)
        corpus = build_corpus(split)
        for case in cases:
            if case.expected_action is None:
                continue
            assert corpus_oracle_decision(case, corpus) == case.expected_action
            key = derive_incident_key(case.logs, case.metrics)
            current = [
                chunk.text
                for chunk in corpus
                if chunk.text.startswith("STATUS=CURRENT")
                and f"SERVICE={key[0]}" in chunk.text
                and f"VERSION={key[1]}" in chunk.text
                and f"SIGNAL={key[2]}" in chunk.text
            ]
            assert current
            assert f"ACTION={case.expected_action['action']}" in "\n".join(current)
            for parameter in ACTION_PARAMETERS[str(case.expected_action["action"])]:
                assert (
                    f"PARAMETER={parameter}:{case.expected_action['parameters'][parameter]}"
                    in "\n".join(current)
                )


def test_insufficient_truth_is_corpus_wide_for_both_negative_shapes() -> None:
    for split in ("development", "heldout"):
        cases = build_cases(split)
        corpus = build_corpus(split)
        for case in cases:
            if case.category in {
                "insufficient_no_guidance",
                "insufficient_missing_parameter",
            }:
                assert case.expected_action is None
                assert corpus_oracle_decision(case, corpus) is None


def test_stale_current_winner_and_unresolved_conflict_are_structural() -> None:
    for split in ("development", "heldout"):
        cases = build_cases(split)
        corpus = build_corpus(split)
        for case in cases:
            if case.category == "stale_current_action":
                assert any("STATUS=SUPERSEDED" in chunk.text for chunk in case.evidence)
                superseded = next(
                    chunk for chunk in case.evidence if "STATUS=SUPERSEDED" in chunk.text
                )
                assert "SUPERSEDED_BY=policy-" in superseded.text
                assert "SUPERSEDES=policy-" not in superseded.text
                assert corpus_oracle_decision(case, corpus) == case.expected_action
            elif case.category == "stale_unresolved_conflict":
                assert case.expected_action is None
                assert corpus_oracle_decision(case, corpus) is None
            elif case.category == "near_duplicate_decoy":
                target_key = derive_incident_key(case.logs, case.metrics)
                target_action = case.expected_action
                assert target_action is not None
                decoy_chunks = []
                for chunk in case.evidence:
                    facts = _parse_facts(chunk.text)
                    if (
                        facts.get("STATUS") == "CURRENT"
                        and facts.get("SERVICE") == target_key[0]
                        and facts.get("SIGNAL") == target_key[2]
                        and facts.get("VERSION") != target_key[1]
                    ):
                        decoy_chunks.append((chunk, facts))
                assert len(decoy_chunks) == 1
                _chunk, decoy_facts = decoy_chunks[0]
                decoy_key = (
                    decoy_facts["SERVICE"],
                    decoy_facts["VERSION"],
                    decoy_facts["SIGNAL"],
                )
                shared_fields = sum(
                    left == right for left, right in zip(target_key, decoy_key, strict=True)
                )
                assert shared_fields >= 2
                assert 0 < abs(int(decoy_key[1][1:]) - int(target_key[1][1:])) <= 3
                decoy_action = _complete_action(decoy_facts, decoy_key)
                assert decoy_action is not None
                assert decoy_action["action"] == target_action["action"]
                assert decoy_action["parameters"] != target_action["parameters"]
                assert corpus_oracle_decision(case, corpus) == case.expected_action


def test_each_split_has_one_unique_incident_key_per_case() -> None:
    for split in ("development", "heldout"):
        cases = build_cases(split)
        keys = {derive_incident_key(case.logs, case.metrics) for case in cases}
        assert len(keys) == len(cases)
        assert validate_key_label_consistency(cases) is None


def test_decoy_keys_do_not_collide_with_targets_or_other_decoys() -> None:
    for split in ("development", "heldout"):
        cases = build_cases(split)
        target_keys = {derive_incident_key(case.logs, case.metrics) for case in cases}
        decoy_keys = set()
        for case in cases:
            if case.category != "near_duplicate_decoy":
                continue
            target_key = derive_incident_key(case.logs, case.metrics)
            for chunk in case.evidence:
                facts = _parse_facts(chunk.text)
                if (
                    facts.get("STATUS") == "CURRENT"
                    and facts.get("SERVICE") == target_key[0]
                    and facts.get("SIGNAL") == target_key[2]
                    and facts.get("VERSION") != target_key[1]
                ):
                    decoy_key = (
                        facts["SERVICE"],
                        facts["VERSION"],
                        facts["SIGNAL"],
                    )
                    assert decoy_key not in target_keys
                    assert decoy_key not in decoy_keys
                    decoy_keys.add(decoy_key)
        assert len(decoy_keys) == sum(
            case.category == "near_duplicate_decoy" for case in cases
        )


def test_fixture_fails_if_repeated_incident_key_has_different_expected_outcomes() -> None:
    cases = build_cases("development")
    answerable = next(case for case in cases if case.category == "answerable")
    insufficient = next(case for case in cases if case.category == "insufficient_no_guidance")
    duplicate_key = replace(
        insufficient,
        service=answerable.service,
        active_version=answerable.active_version,
        signal=answerable.signal,
        logs=answerable.logs,
        metrics=answerable.metrics,
    )
    with pytest.raises(ValueError, match="disagree on corpus-key truth"):
        validate_key_label_consistency((answerable, duplicate_key))


def test_corpus_oracle_catches_complete_current_directive_mutant_elsewhere() -> None:
    cases = build_cases("development")
    corpus = build_corpus("development")
    insufficient = next(case for case in cases if case.category == "insufficient_no_guidance")
    donor = next(
        case
        for case in cases
        if case.category == "answerable" and case.family == insufficient.family
    )
    action = donor.expected_action
    assert action is not None
    key = derive_incident_key(insufficient.logs, insufficient.metrics)
    same_key_cases = [case for case in cases if derive_incident_key(case.logs, case.metrics) == key]
    mutant_action = {
        "action": action["action"],
        "service": key[0],
        "parameters": dict(action["parameters"]),
    }
    if "revision" in mutant_action["parameters"]:
        mutant_action["parameters"]["revision"] = f"v{int(key[1][1:]) - 1}"
    injected = EvidenceChunk(
        chunk_id="chunk-mutant-complete-current",
        document_id="document-mutant-complete-current",
        version=key[1],
        service=key[0],
        text=_fact_text(
            status="CURRENT",
            service=key[0],
            version=key[1],
            signal=key[2],
            effective="2026-09-28",
            symptom="synthetic load increase",
            action=mutant_action,
        ),
        source_type="runbook",
    )
    mutated_corpus = (*corpus, injected)
    assert corpus_oracle_decision(insufficient, mutated_corpus) == mutant_action
    validation = validate_corpus_labels(cases, mutated_corpus)
    assert {case.case_id for case in same_key_cases}.issubset(validation["mismatches"])
    assert validation["correct"] == len(cases) - len(same_key_cases)


def test_case_ids_split_prefixes_and_legacy_padding_never_reach_queries_or_chunks() -> None:
    for split in ("development", "heldout"):
        prefix = "dev-" if split == "development" else "hel-"
        for case in build_cases(split):
            assert case.canary in case.logs[0]["message"]
            assert case.logs[0]["case_id"] == case.case_id
            assert case.metrics[0]["case_id"] == case.case_id
            for text in (
                case.query_clean_a,
                case.query_clean_b,
                *(chunk.text for chunk in case.evidence),
            ):
                assert case.case_id not in text
                assert "case-id" not in text.lower()
                assert prefix not in text
                assert LEGACY_QUERY_PADDING not in text
                assert "reference=" not in text.lower()


def test_negative_evidence_does_not_announce_its_category() -> None:
    for split in ("development", "heldout"):
        for case in build_cases(split):
            if case.category == "answerable":
                continue
            evidence = all_fixture_text(case).lower()
            assert all(phrase not in evidence for phrase in CATEGORY_ANNOUNCEMENTS)


def test_seeded_answer_values_differ_between_development_and_heldout() -> None:
    development = build_cases("development")
    heldout = build_cases("heldout")
    assert _answer_values(development) != _answer_values(heldout)
    for cases in (development, heldout):
        for family in FAMILIES:
            assert len({case.signal for case in cases if case.family == family}) == 2
        for case in cases:
            if case.expected_action is None:
                continue
            parameters = case.expected_action["parameters"]
            if case.family == "bad_configuration":
                assert int(parameters["revision"][1:]) < int(case.active_version[1:])
            elif case.family == "connection_pool":
                assert 3 <= parameters["replicas"] <= 12
            elif case.family == "downstream_latency":
                assert 800 <= parameters["timeout_ms"] <= 3000
                assert parameters["timeout_ms"] % 100 == 0
            elif case.family == "disk_pressure":
                assert 7 <= parameters["retention_days"] <= 60


def test_agent_prompts_and_structured_schemas_contain_no_answer_values() -> None:
    cases = (*build_cases("development"), *build_cases("heldout"))
    for candidate in PROMPT_CANDIDATES.values():
        prompt = str(candidate["instructions"])
        schema = json.dumps(RESPONSE_SCHEMAS[str(candidate["schema_id"])], sort_keys=True)
        for case in cases:
            if case.expected_action is None:
                continue
            for value in case.expected_action["parameters"].values():
                assert str(value) not in prompt
                assert str(value) not in schema


def test_dev_and_heldout_query_template_sets_are_disjoint_and_fact_based() -> None:
    assert set(DEV_QUERY_TEMPLATES).isdisjoint(HELDOUT_QUERY_TEMPLATES)
    for split in ("development", "heldout"):
        queries = [
            q for case in build_cases(split) for q in (case.query_clean_a, case.query_clean_b)
        ]
        assert len(queries) == len(set(queries))
        assert all(
            not any(phrase in q.lower() for phrase in CATEGORY_ANNOUNCEMENTS) for q in queries
        )


def test_deterministic_full_corpus_oracle_scores_both_splits_at_100_percent() -> None:
    for split, expected_count in (("development", 30), ("heldout", 60)):
        report = oracle_report(split)
        assert report["cases"] == expected_count
        assert report["correct"] == expected_count
        assert report["accuracy"] == 1.0
        assert report["provider_calls"] == 0
        assert report["unique_incident_keys"] == expected_count
        for case in build_cases(split):
            assert oracle_decision(case) == case.expected_action
