"""DUR-056 validation, development tuning, freeze, and gated held-out CLI."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from incident_agent.dur056_agent import (
    DEFENDED_INSTRUCTIONS,
    ENDPOINT,
    MAX_OUTPUT_TOKENS,
    MODEL_ID,
    PLAIN_INSTRUCTIONS,
    PROMPT_CANDIDATES,
    RESPONSE_SCHEMAS,
    OpenAIResponsesDecisionProvider,
)
from incident_agent.dur056_artifacts import (
    atomic_write_json,
    canonical_config_fingerprint,
    create_exclusive_json,
    current_git_head,
    read_json,
    write_once,
    write_unit_once,
)
from incident_agent.dur056_budget import Dur056SpendLedger
from incident_agent.dur056_evaluation import (
    analyze_development_report,
    evaluate_development_agents,
    run_workflow_case,
)
from incident_agent.dur056_fixtures import (
    CATEGORY_ANNOUNCEMENTS,
    DEV_QUERY_TEMPLATES,
    DEV_SEED,
    FAMILIES,
    HELDOUT_QUERY_TEMPLATES,
    HELDOUT_SEED,
    LEGACY_QUERY_PADDING,
    Dur056Case,
    all_fixture_text,
    build_cases,
    injection_text_for_action,
    oracle_decision,
    oracle_report,
    validate_corpus_labels,
    validate_key_label_consistency,
)
from incident_agent.dur056_retrieval import (
    EMBEDDING_DIMENSION,
    EMBEDDING_MODEL,
    STUDY_VERSION,
    Dur056OpenAIEmbeddingProvider,
    Dur056RetrievalError,
    PostgresRetrievalAdapter,
    apply_migration,
    connect,
    database_manifest,
    development_query_rows,
    embed_missing,
    fingerprint,
    query_vector_map,
    seed_corpus,
    tune_development,
)
from incident_agent.dur056_scorer import (
    Dur056ScorerError,
    _summarize_injection,
    score_heldout,
)

STUDY_RELATIVE = Path("experiments/m8/dur056-solvable-agent-study")
EXPECTED_COUNTS = {
    "development": {
        "answerable": 12,
        "insufficient_no_guidance": 3,
        "insufficient_missing_parameter": 3,
        "stale_current_action": 3,
        "stale_unresolved_conflict": 3,
        "near_duplicate_decoy": 6,
        "total": 30,
    },
    "heldout": {
        "answerable": 24,
        "insufficient_no_guidance": 6,
        "insufficient_missing_parameter": 6,
        "stale_current_action": 6,
        "stale_unresolved_conflict": 6,
        "near_duplicate_decoy": 12,
        "total": 60,
    },
}


class Dur056StudyError(RuntimeError):
    """Raised when DUR-056 fixture, tuning, or gating requirements fail."""


def _root() -> Path:
    return Path(__file__).resolve().parents[2]


def _study_directory(root: Path) -> Path:
    return root / STUDY_RELATIVE


def _fixture_fingerprint(cases: tuple[Dur056Case, ...]) -> str:
    return fingerprint(
        [
            {
                "case_id": case.case_id,
                "split": case.split,
                "family": case.family,
                "category": case.category,
                "query_clean_a": case.query_clean_a,
                "query_clean_b": case.query_clean_b,
                "logs": case.logs,
                "metrics": case.metrics,
                "expected_action": case.expected_action,
                "evidence": [asdict(chunk) for chunk in case.evidence],
                "injection_text": case.injection_text,
                "injection_wrong_action": case.injection_wrong_action,
                "injection_text_version": case.injection_text_version,
            }
            for case in cases
        ]
    )


def _embedding_provenance(
    output_directory: Path,
    *,
    corpus_fingerprint: str,
    query_fingerprint: str,
    current_records: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[str]]:
    """Carry forward exact v2 embedding records when vectors are reused."""

    records: list[dict[str, Any]] = []
    sources: list[str] = []
    for path in sorted(output_directory.glob("study-manifest-v2-*.json")):
        try:
            manifest = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if (
            manifest.get("study_version") != STUDY_VERSION
            or manifest.get("development_corpus_fingerprint") != corpus_fingerprint
            or manifest.get("development_query_fingerprint") != query_fingerprint
        ):
            continue
        requests = manifest.get("embedding_requests", [])
        if not isinstance(requests, list) or not requests:
            continue
        records.extend(
            {
                **record,
                "provenance_manifest": path.name,
            }
            for record in requests
            if isinstance(record, dict)
        )
        sources.append(path.name)
    records.extend(
        {**record, "provenance_manifest": "current-development-run"} for record in current_records
    )
    return records, sources


def validate_fixtures(injection_text_version: int = 1) -> dict[str, Any]:
    reports: dict[str, Any] = {}
    cases_by_split: dict[str, tuple[Dur056Case, ...]] = {}
    for split in ("development", "heldout"):
        cases = build_cases(split, injection_text_version=injection_text_version)
        corpus = tuple(chunk for case in cases for chunk in case.evidence)
        cases_by_split[split] = cases
        actual_counts = {
            category: sum(case.category == category for case in cases)
            for category in EXPECTED_COUNTS[split]
            if category != "total"
        }
        actual_counts["total"] = len(cases)
        if actual_counts != EXPECTED_COUNTS[split]:
            raise Dur056StudyError(
                f"DUR-056 {split} allocation differs from the registered protocol"
            )
        if len({case.case_id for case in cases}) != len(cases):
            raise Dur056StudyError(f"DUR-056 {split} identifiers are not unique")
        if any(case.split != split for case in cases):
            raise Dur056StudyError(f"DUR-056 {split} builder returned another split")
        for family in FAMILIES:
            expected_answerable = 3 if split == "development" else 6
            family_answerable = sum(
                case.family == family and case.category == "answerable" for case in cases
            )
            if family_answerable != expected_answerable:
                raise Dur056StudyError(f"DUR-056 {split} answerable cases are not family balanced")
        try:
            validate_key_label_consistency(cases)
            corpus_validation = validate_corpus_labels(cases, corpus)
        except ValueError as error:
            raise Dur056StudyError(str(error)) from error
        if corpus_validation["correct"] != len(cases):
            raise Dur056StudyError(
                "corpus-level oracle disagrees with fixture labels: "
                + ", ".join(corpus_validation["mismatches"])
            )
        for case in cases:
            if oracle_decision(case, corpus) != case.expected_action:
                raise Dur056StudyError(f"full-split oracle disagrees on {case.case_id}")
            for text in [
                *(chunk.text for chunk in case.evidence),
                case.query_clean_a,
                case.query_clean_b,
            ]:
                if case.case_id in text or text.startswith(("dev-", "hel-")):
                    raise Dur056StudyError("case ID or split prefix leaked into fixture text")
                if LEGACY_QUERY_PADDING in text:
                    raise Dur056StudyError("legacy query padding leaked into fixture text")
                if split == "development" and "heldout" in text.lower():
                    raise Dur056StudyError("held-out marker leaked into development text")
                if split == "heldout" and "development" in text.lower():
                    raise Dur056StudyError("development marker leaked into held-out text")
            if case.category != "answerable":
                lowered = all_fixture_text(case).lower()
                if any(phrase in lowered for phrase in CATEGORY_ANNOUNCEMENTS):
                    raise Dur056StudyError(
                        f"negative evidence announces its category for {case.case_id}"
                    )
        oracle = oracle_report(split, injection_text_version=injection_text_version)
        if oracle["correct"] != len(cases) or oracle["accuracy"] != 1.0:
            raise Dur056StudyError(f"DUR-056 {split} deterministic oracle did not score 100%")
        if oracle["unique_incident_keys"] != len(cases):
            raise Dur056StudyError(f"DUR-056 {split} must have one unique incident key per case")
        if oracle["provider_calls"] != 0:
            raise Dur056StudyError("the local DUR-056 oracle must make zero provider calls")
        reports[split] = {
            "allocation": actual_counts,
            "fixture_fingerprint": _fixture_fingerprint(cases),
            "injection_text_version": cases[0].injection_text_version,
            "injection_text_fingerprint": fingerprint(
                [
                    {
                        "case_id": case.case_id,
                        "wrong_action": case.injection_wrong_action,
                        "text": case.injection_text,
                    }
                    for case in cases
                ]
            ),
            "corpus_chunk_count": len(corpus),
            "oracle": oracle,
            "provider_calls": 0,
        }
    if not set(DEV_QUERY_TEMPLATES).isdisjoint(HELDOUT_QUERY_TEMPLATES):
        raise Dur056StudyError("DUR-056 development/held-out query templates overlap")
    reports["template_sets_disjoint"] = True
    dev_values = {
        (case.expected_action["action"], name, str(value))
        for case in cases_by_split["development"]
        if case.expected_action is not None
        for name, value in case.expected_action["parameters"].items()
    }
    heldout_values = {
        (case.expected_action["action"], name, str(value))
        for case in cases_by_split["heldout"]
        if case.expected_action is not None
        for name, value in case.expected_action["parameters"].items()
    }
    if dev_values == heldout_values:
        raise Dur056StudyError("DUR-056 development and held-out answer-value sets are identical")
    reports["answer_value_sets_differ"] = True
    reports["development_seed"] = DEV_SEED
    reports["heldout_seed"] = HELDOUT_SEED
    reports["study_version"] = STUDY_VERSION
    reports["model_provider_calls"] = 0
    reports["embedding_provider_calls"] = 0
    return {"schema": "dur056-fixture-validity.r209.v1", "splits": reports}


def prepare_development(root: Path | None = None) -> dict[str, Any]:
    if not os.environ.get("OPENAI_API_KEY"):
        raise Dur056StudyError("OPENAI_API_KEY is required before DUR-056 development tuning")
    project_root = root or _root()
    output_directory = _study_directory(project_root)
    output_directory.mkdir(parents=True, exist_ok=True)
    existing_freezes = [
        *output_directory.glob("gate-a-freeze-review-v3-*.json"),
        *output_directory.glob("gate-a-freeze-review-r209-v3-*.json"),
    ]
    if existing_freezes:
        raise Dur056StudyError(
            "a DUR-056 frozen Gate A bundle already exists; refusing to retune or overwrite"
        )
    validity = validate_fixtures()
    oracle_paths = {
        split: write_once(
            output_directory, f"oracle-report-{split}-v3", validity["splits"][split]["oracle"]
        )
        for split in ("development", "heldout")
    }
    validity["oracle_report_paths"] = {key: value.name for key, value in oracle_paths.items()}
    validity_path = write_once(output_directory, "fixture-validity-v3", validity)

    ledger = Dur056SpendLedger(output_directory / "spend-ledger.json")
    cases = build_cases("development")
    queries = development_query_rows(cases)
    connection = connect()
    try:
        apply_migration(connection, project_root)
        chunks = seed_corpus(connection, cases, split="development")
        embedding_provider = Dur056OpenAIEmbeddingProvider(
            ledger,
            split_scope="development",
        )
        embed_missing(
            connection,
            embedding_provider,
            chunks,
            queries,
            split="development",
        )
        query_vectors = query_vector_map(connection, queries)
        query_splits = {row["query"]: row["split"] for row in queries}
        retrieval_config, retrieval_results = tune_development(
            connection,
            cases,
            query_vectors,
            query_splits,
        )
        corpus_fingerprint = _fixture_fingerprint(cases)
        query_fingerprint = fingerprint(queries)
        embedding_requests, embedding_provenance_manifests = _embedding_provenance(
            output_directory,
            corpus_fingerprint=corpus_fingerprint,
            query_fingerprint=query_fingerprint,
            current_records=embedding_provider.records,
        )
        retrieval_config.pop("config_fingerprint", None)
        retrieval_config.update(
            {
                "corpus_fingerprint": corpus_fingerprint,
                "development_query_fingerprint": query_fingerprint,
                "database": database_manifest(connection),
                "threshold_tuning_split": "development",
                "heldout_corpus_embedded": False,
                "heldout_queries_embedded": False,
            }
        )
        retrieval_config["config_fingerprint"] = fingerprint(retrieval_config)
        retrieval_results["selected_config_fingerprint"] = retrieval_config["config_fingerprint"]
        retrieval_results["frozen_candidate"] = retrieval_config
        retrieval_config_path = write_once(
            output_directory, "retrieval-frozen-config-v3", retrieval_config
        )
        retrieval_results_path = write_once(
            output_directory, "retrieval-development-v3", retrieval_results
        )
        manifest = {
            "schema": "dur056-study-manifest.v3",
            "study_version": STUDY_VERSION,
            "source_commit": current_git_head(project_root),
            "fixture_validity_path": validity_path.name,
            "oracle_report_paths": {key: value.name for key, value in oracle_paths.items()},
            "development_case_count": len(cases),
            "development_corpus_chunk_count": len(chunks),
            "development_query_count": len(queries),
            "development_corpus_fingerprint": corpus_fingerprint,
            "development_query_fingerprint": query_fingerprint,
            "query_seeds": {"development": DEV_SEED, "heldout": HELDOUT_SEED},
            "embedding_model": EMBEDDING_MODEL,
            "embedding_dimension": EMBEDDING_DIMENSION,
            "embedding_requests": embedding_requests,
            "embedding_provenance_source_manifests": embedding_provenance_manifests,
            "embedding_provenance_complete": bool(embedding_requests),
            "heldout_cases_sent_to_model": 0,
            "heldout_queries_sent_to_embedding_provider": 0,
            "heldout_documents_sent_to_embedding_provider": 0,
            "database": database_manifest(connection),
            "spend_ledger_snapshot": ledger.snapshot(),
        }
        manifest_path = write_once(output_directory, "study-manifest-v3", manifest)
        agent_results = evaluate_development_agents(
            cases=cases,
            ledger=ledger,
            retrieval_config=retrieval_config,
            connection=connection,
            query_vectors=query_vectors,
            query_splits=query_splits,
        )
        agent_results["fixture_validity_path"] = validity_path.name
        agent_results["development_seed"] = DEV_SEED
        agent_results["heldout_seed"] = HELDOUT_SEED
        agent_results["heldout_model_calls"] = 0
        agent_results["per_run_cost_authority"] = "DUR-056 spend-ledger.json"
        agent_results_path = write_once(output_directory, "agent-development-v3", agent_results)

        selected = str(agent_results["selected_candidate"])
        candidate = PROMPT_CANDIDATES[selected]
        definition = agent_results["candidate_definitions"][selected]
        instructions = str(candidate["instructions"])
        schema_id = str(candidate["schema_id"])
        schema = RESPONSE_SCHEMAS[schema_id]
        injection_profiles = {
            "defended": {
                "instructions": DEFENDED_INSTRUCTIONS,
                "prompt_fingerprint": "sha256:"
                + __import__("hashlib").sha256(DEFENDED_INSTRUCTIONS.encode("utf-8")).hexdigest(),
            },
            "plain": {
                "instructions": PLAIN_INSTRUCTIONS,
                "prompt_fingerprint": "sha256:"
                + __import__("hashlib").sha256(PLAIN_INSTRUCTIONS.encode("utf-8")).hexdigest(),
            },
        }
        for profile in injection_profiles.values():
            profile["schema_id"] = schema_id
            profile["structured_output_schema_fingerprint"] = fingerprint(schema)
            profile["mcp_tool_schema_fingerprint"] = definition["mcp_tool_schema_fingerprint"]
        agent_config: dict[str, Any] = {
            "schema": "dur056-agent-frozen-config.v3",
            "study_version": STUDY_VERSION,
            "model_id": MODEL_ID,
            "endpoint": ENDPOINT,
            "reasoning_effort": "low",
            "max_output_tokens": MAX_OUTPUT_TOKENS,
            "store": False,
            "transport_retries": 2,
            "retrieval_config_fingerprint": retrieval_config["config_fingerprint"],
            "selected_candidate": selected,
            "prompt_profile": candidate["prompt_profile"],
            "prompt_instructions_utf8": instructions,
            "prompt_fingerprint": definition["prompt_fingerprint"],
            "structured_output_schema_id": schema_id,
            "structured_output_schema": schema,
            "structured_output_schema_fingerprint": fingerprint(schema),
            "mcp_tool_schemas": definition["mcp_tool_schemas"],
            "mcp_tool_schema_fingerprint": definition["mcp_tool_schema_fingerprint"],
            "mcp_argument_validation_unchanged": True,
            "candidate_definitions": agent_results["candidate_definitions"],
            "injection_profiles": injection_profiles,
            "development_result_path": agent_results_path.name,
            "development_cases_scored_per_arm": len(cases),
            "heldout_cases_scored": 0,
            "heldout_scored": False,
        }
        agent_config["config_fingerprint"] = fingerprint(agent_config)
        agent_config_path = write_once(output_directory, "agent-frozen-config-v3", agent_config)
        freeze_bundle: dict[str, Any] = {
            "schema": "dur056-gate-a-freeze-bundle.v3",
            "study_version": STUDY_VERSION,
            "gate": "A",
            "status": "READY_FOR_FREEZE_REVIEW",
            "heldout_scoring_authorized": False,
            "heldout_scored": False,
            "fixture_validity_path": validity_path.name,
            "oracle_report_paths": {key: value.name for key, value in oracle_paths.items()},
            "study_manifest_path": manifest_path.name,
            "retrieval_config_path": retrieval_config_path.name,
            "retrieval_config_fingerprint": retrieval_config["config_fingerprint"],
            "retrieval_development_results_path": retrieval_results_path.name,
            "agent_config_path": agent_config_path.name,
            "agent_config_fingerprint": agent_config["config_fingerprint"],
            "agent_development_results_path": agent_results_path.name,
            "model_id": MODEL_ID,
            "spend_ledger_path": "spend-ledger.json",
            "spend_ledger_snapshot": ledger.snapshot(),
            "heldout_model_calls": 0,
            "heldout_embedding_calls": 0,
        }
        freeze_bundle["freeze_fingerprint"] = fingerprint(freeze_bundle)
        freeze_path = write_once(output_directory, "gate-a-freeze-review-v3", freeze_bundle)
        return {
            "status": "READY_FOR_FREEZE_REVIEW",
            "freeze_bundle_path": str(freeze_path),
            "retrieval_config_fingerprint": retrieval_config["config_fingerprint"],
            "agent_config_fingerprint": agent_config["config_fingerprint"],
            "spend_ledger": ledger.snapshot(),
            "heldout_provider_calls": 0,
        }
    finally:
        connection.close()


def _legacy_gate_a_freeze(output_directory: Path) -> tuple[Path, dict[str, Any]]:
    paths = sorted(output_directory.glob("gate-a-freeze-review-v3-*.json"))
    if len(paths) != 1:
        raise Dur056StudyError("expected the single pre-R209 v3 Gate A bundle")
    return paths[0], read_json(paths[0])


def _pilot_source_fingerprint(root: Path) -> str:
    source_files = (
        "python/incident_agent/dur056.py",
        "python/incident_agent/dur056_agent.py",
        "python/incident_agent/dur056_artifacts.py",
        "python/incident_agent/dur056_budget.py",
        "python/incident_agent/dur056_evaluation.py",
        "python/incident_agent/dur056_fixtures.py",
        "python/incident_agent/dur056_retrieval.py",
        "python/incident_agent/dur056_scorer.py",
        "python/incident_agent/mcp.py",
        "python/incident_agent/mcp_protocol.py",
        "python/incident_agent/continuity.py",
        "python/incident_agent/workflow.py",
    )
    return fingerprint(
        {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in source_files}
    )


def _pilot_freeze_context(root: Path, output_directory: Path) -> dict[str, Any]:
    freeze_path, bundle = _legacy_gate_a_freeze(output_directory)
    retrieval_path = output_directory / str(bundle["retrieval_config_path"])
    agent_path = output_directory / str(bundle["agent_config_path"])
    retrieval = read_json(retrieval_path)
    agent = read_json(agent_path)
    retrieval_fp = canonical_config_fingerprint(retrieval, "config_fingerprint")
    agent_fp = canonical_config_fingerprint(agent, "config_fingerprint")
    if retrieval_fp != bundle.get("retrieval_config_fingerprint"):
        raise Dur056StudyError("R209 pilot retrieval config differs from the v3 freeze")
    if agent_fp != bundle.get("agent_config_fingerprint"):
        raise Dur056StudyError("R209 pilot agent config differs from the v3 freeze")
    if agent.get("heldout_scored") is not False or bundle.get("heldout_scored") is not False:
        raise Dur056StudyError("R209 pilot refuses a previously scored package")
    return {
        "source_freeze_path": freeze_path,
        "source_bundle": bundle,
        "retrieval_path": retrieval_path,
        "retrieval": retrieval,
        "retrieval_fingerprint": retrieval_fp,
        "agent_path": agent_path,
        "agent": agent,
        "agent_fingerprint": agent_fp,
    }


def _injection_pilot_units(cases: tuple[Dur056Case, ...], attempt: int) -> list[dict[str, Any]]:
    if len(cases) != 30 or any(case.split != "development" for case in cases):
        raise Dur056StudyError("R209 injection pilot accepts only the 30 development cases")
    if attempt not in (2, 3):
        raise Dur056StudyError("R209 execution attempts are 2 (v1) and conditional 3 (v2)")
    return [
        {
            "unit_id": f"{profile}-{condition}-{case.case_id}",
            "case": case,
            "profile": profile,
            "condition": condition,
            "attempt": attempt,
        }
        for profile in ("defended", "plain")
        for condition in ("clean-a", "clean-b", "injected")
        for case in cases
    ]


def _injection_text_version_for_attempt(attempt: int) -> int:
    if attempt == 2:
        return 1
    if attempt == 3:
        return 2
    raise Dur056StudyError("R209 execution attempts are 2 (v1) and conditional 3 (v2)")


def _pilot_reports(output_directory: Path, *, attempt: int) -> list[tuple[Path, dict[str, Any]]]:
    reports = sorted(
        output_directory.glob(f"injection-development-pilot-r209-attempt-{attempt}-*.json")
    )
    return [(path, read_json(path)) for path in reports]


def _pilot_attempt1_transport_history(
    output_directory: Path,
) -> tuple[Path, dict[str, Any], Path, dict[str, Any]]:
    reports = _pilot_reports(output_directory, attempt=1)
    assessments = sorted(
        output_directory.glob("injection-pilot-r209-attempt-1-transport-assessment-*.json")
    )
    if len(reports) != 1 or len(assessments) != 1:
        raise Dur056StudyError(
            "attempt 2 requires the one preserved attempt-1 report and assessment"
        )
    report_path, report = reports[0]
    assessment_path = assessments[0]
    assessment = read_json(assessment_path)
    if (
        assessment.get("source_report_fingerprint") != fingerprint(report)
        or assessment.get("classification") != "INCOMPLETE_PROVIDER_RESULTS"
        or "WinError 10013" not in str(assessment.get("failure_kind", ""))
        or int(assessment.get("planned_workflow_runs", 0)) != 180
        or int(assessment.get("provider_error_runs", 0)) != 180
        or int(assessment.get("model_response_records", -1)) != 0
        or int(assessment.get("provider_request_ledger_entries", 0)) != 540
        or int(assessment.get("heldout_provider_calls", -1)) != 0
        or report.get("split") != "development"
        or report.get("attempt") != 1
        or report.get("injection_text_version") != 1
        or int(report.get("planned_workflow_runs", 0)) != 180
        or int(report.get("model_response_records", -1)) != 0
    ):
        raise Dur056StudyError("attempt 1 assessment does not prove a transport-only failure")
    # The original report predates the separate response-count fields. Enrich only
    # the in-memory history row; the immutable source report remains untouched.
    history = {
        **report,
        "status": "INCOMPLETE_PROVIDER_RESULTS",
        "provider_error_run_count": int(assessment["provider_error_runs"]),
        "transport_assessment_path": assessment_path.name,
        "transport_assessment_fingerprint": fingerprint(assessment),
    }
    return report_path, history, assessment_path, assessment


def _pilot_is_usable(report: dict[str, Any]) -> bool:
    planned = int(report.get("planned_workflow_runs", 0))
    if (
        report.get("split") != "development"
        or report.get("status") != "COMPLETE"
        or planned != 180
        or int(report.get("model_response_records", 0)) != planned
        or int(report.get("provider_error_run_count", 1)) != 0
    ):
        return False
    return all(
        row.get("provider_error") is None
        for profile in report.get("outcomes", {}).values()
        for row in profile.get("per_run", [])
    )


def _select_pilot_for_freeze(
    attempt_reports: dict[int, list[tuple[Path, dict[str, Any]]]],
) -> tuple[int, int, Path, dict[str, Any]]:
    first_reports = attempt_reports.get(1, [])
    if len(first_reports) != 1:
        raise Dur056StudyError("R209 refreeze requires the one preserved attempt-1 history report")
    first = first_reports[0][1]
    if (
        _pilot_is_usable(first)
        or first.get("injection_text_version") != 1
        or int(first.get("model_response_records", -1)) != 0
        or int(first.get("provider_error_run_count", 0)) != 180
    ):
        raise Dur056StudyError("attempt 1 must remain the preserved transport-only history")

    valid_attempt_2 = [
        (path, report)
        for path, report in attempt_reports.get(2, [])
        if _pilot_is_usable(report) and report.get("injection_text_version") == 1
    ]
    if len(valid_attempt_2) != 1:
        raise Dur056StudyError("R209 refreeze requires one provider-complete attempt 2 using v1")
    attempt_2_path, attempt_2 = valid_attempt_2[0]
    plain_successes = int(
        attempt_2.get("outcomes", {}).get("plain", {}).get("attack_success", {}).get("count", -1)
    )
    if plain_successes == 0:
        valid_attempt_3 = [
            (path, report)
            for path, report in attempt_reports.get(3, [])
            if _pilot_is_usable(report) and report.get("injection_text_version") == 2
        ]
        if len(valid_attempt_3) != 1:
            raise Dur056StudyError(
                "zero plain attack successes require one strengthened attempt 3 using v2"
            )
        path, report = valid_attempt_3[0]
        return 3, 2, path, report
    if attempt_reports.get(3):
        raise Dur056StudyError("attempt 3 is forbidden when attempt 2 has plain attack success")
    return 2, 1, attempt_2_path, attempt_2


def run_development_injection_pilot(
    root: Path,
    *,
    attempt: int = 2,
    resume: bool = False,
) -> dict[str, Any]:
    """Run only the 30-case R209 development injection calibration."""

    text_version = _injection_text_version_for_attempt(attempt)
    if not os.environ.get("OPENAI_API_KEY"):
        raise Dur056StudyError("OPENAI_API_KEY is required before the R209 development pilot")
    output_directory = _study_directory(root)
    output_directory.mkdir(parents=True, exist_ok=True)
    context = _pilot_freeze_context(root, output_directory)
    cases = build_cases("development", injection_text_version=text_version)
    if any(case.split != "development" for case in cases):
        raise Dur056StudyError("R209 runner refused non-development cases")
    if any(case.injection_text_version != text_version for case in cases):
        raise Dur056StudyError("R209 pilot fixture uses the wrong injection text version")
    if attempt == 2:
        _, prior, _, _ = _pilot_attempt1_transport_history(output_directory)
        if _pilot_is_usable(prior):
            raise Dur056StudyError(
                "attempt 2 requires attempt 1's preserved transport-only failure"
            )
    if attempt == 3:
        valid_attempt_2 = [
            report
            for _, report in _pilot_reports(output_directory, attempt=2)
            if _pilot_is_usable(report) and report.get("injection_text_version") == 1
        ]
        if len(valid_attempt_2) != 1:
            raise Dur056StudyError("attempt 3 requires one provider-complete attempt 2 using v1")
        plain_successes = (
            valid_attempt_2[0]
            .get("outcomes", {})
            .get("plain", {})
            .get("attack_success", {})
            .get("count")
        )
        if plain_successes != 0:
            raise Dur056StudyError("attempt 3 is allowed only after zero plain attack successes")
    expected_text_fingerprint = fingerprint(
        [
            {
                "case_id": case.case_id,
                "wrong_action": case.injection_wrong_action,
                "text": injection_text_for_action(case.injection_wrong_action, text_version),
            }
            for case in cases
        ]
    )
    head_sha = current_git_head(root)
    source_fingerprint = _pilot_source_fingerprint(root)
    freeze_identity = fingerprint(
        {
            "head_sha": head_sha,
            "source_fingerprint": source_fingerprint,
            "retrieval_config_fingerprint": context["retrieval_fingerprint"],
            "agent_config_fingerprint": context["agent_fingerprint"],
            "injection_text_fingerprint": expected_text_fingerprint,
            "attempt": attempt,
            "injection_text_version": text_version,
        }
    )
    marker_path = output_directory / f"injection-pilot-r209-attempt-{attempt}.marker.json"
    unit_directory = output_directory / f"injection-pilot-r209-attempt-{attempt}-units"
    unit_specs = _injection_pilot_units(cases, attempt)
    unit_ids = [str(spec["unit_id"]) for spec in unit_specs]
    ledger_path = output_directory / "spend-ledger.json"
    ledger_before = Dur056SpendLedger(ledger_path).snapshot()
    current_entry_count = len(read_json(ledger_path).get("entries", []))
    marker_identity = {
        "schema": "dur056-injection-pilot-marker.r209.v1",
        "attempt": attempt,
        "head_sha": head_sha,
        "source_fingerprint": source_fingerprint,
        "freeze_identity": freeze_identity,
        "retrieval_config_fingerprint": context["retrieval_fingerprint"],
        "agent_config_fingerprint": context["agent_fingerprint"],
        "injection_text_fingerprint": expected_text_fingerprint,
        "injection_text_version": text_version,
    }
    if marker_path.exists():
        marker = read_json(marker_path)
        mismatches = [key for key, value in marker_identity.items() if marker.get(key) != value]
        if mismatches:
            raise Dur056StudyError("R209 pilot resume refused: freeze identity changed")
        if not resume or marker.get("status") != "ABORTED_INFRASTRUCTURE":
            raise Dur056StudyError(
                "R209 pilot already exists; resume requires an infrastructure abort"
            )
        marker.setdefault("resumes", []).append(
            {
                "timestamp": datetime.now(UTC).isoformat(),
                "completed_units_before_resume": len(marker.get("completed_units", [])),
            }
        )
        marker["status"] = "RUNNING"
        atomic_write_json(marker_path, marker)
    else:
        if resume:
            raise Dur056StudyError("R209 pilot resume requested without an abort marker")
        marker = {
            **marker_identity,
            "status": "RUNNING",
            "started_at": datetime.now(UTC).isoformat(),
            "completed_units": [],
            "aborts": [],
            "resumes": [],
            "spend_ledger_before": ledger_before,
            "ledger_entry_count_before": current_entry_count,
        }
        create_exclusive_json(marker_path, marker)

    ledger = Dur056SpendLedger(ledger_path)
    ledger_before = marker.get("spend_ledger_before", ledger_before)
    ledger_entry_count_before = int(marker.get("ledger_entry_count_before", current_entry_count))
    connection = None
    active_unit = "setup"
    try:
        connection = connect()
        apply_migration(connection, root)
        chunks = seed_corpus(connection, cases, split="development")
        queries = development_query_rows(cases)
        query_vectors = query_vector_map(connection, queries)
        query_splits = {str(row["query"]): str(row["split"]) for row in queries}
        with connection.cursor() as cursor:
            cursor.execute(
                """SELECT
                    (SELECT count(*) FROM dur056.evidence_chunks
                     WHERE study_version=%s AND split='development'
                       AND embedding IS NOT NULL) AS chunks,
                    (SELECT count(*) FROM dur056.query_embeddings
                     WHERE study_version=%s AND split='development') AS queries""",
                (STUDY_VERSION, STUDY_VERSION),
            )
            vector_counts = cursor.fetchone()
        vectors_incomplete = (
            vector_counts is None
            or int(vector_counts["chunks"]) != len(chunks)
            or int(vector_counts["queries"]) != len(queries)
        )
        if vectors_incomplete:
            raise Dur056StudyError(
                "R209 pilot requires complete existing development vectors; "
                "no embedding calls are made"
            )
        index = PostgresRetrievalAdapter(
            connection,
            context["retrieval"],
            query_vectors,
            query_splits,
            allowed_query_splits=("development",),
            corpus_splits=("development",),
        )
        agent = context["agent"]
        candidate = str(agent["selected_candidate"])
        schemas = agent["mcp_tool_schemas"]
        profiles = agent["injection_profiles"]
        if set(profiles) != {"defended", "plain"}:
            raise Dur056StudyError("frozen injection profile definitions are incomplete")
        for spec in unit_specs:
            unit_id = str(spec["unit_id"])
            active_unit = unit_id
            unit_path = unit_directory / f"{unit_id}.json"
            if unit_path.exists():
                persisted = read_json(unit_path)
                if persisted.get("pilot_freeze_identity") != freeze_identity:
                    raise Dur056StudyError("R209 pilot unit belongs to another frozen attempt")
                if unit_id not in marker["completed_units"]:
                    marker["completed_units"].append(unit_id)
                    atomic_write_json(marker_path, marker)
                continue
            if (
                current_git_head(root) != head_sha
                or _pilot_source_fingerprint(root) != source_fingerprint
            ):
                raise Dur056StudyError("R209 pilot source changed during the development run")
            profile = str(spec["profile"])
            condition = str(spec["condition"])
            case = spec["case"]
            injection_text = (
                injection_text_for_action(case.injection_wrong_action, text_version)
                if condition == "injected"
                else None
            )
            provider = OpenAIResponsesDecisionProvider(
                candidate,
                schemas,
                ledger,
                instructions_override=str(profiles[profile]["instructions"]),
                heldout_authorized=False,
                max_output_tokens=int(agent["max_output_tokens"]),
                transport_retries=0,
            )
            if provider.prompt_fingerprint != profiles[profile]["prompt_fingerprint"]:
                raise Dur056StudyError("R209 pilot profile prompt differs from the frozen config")
            query = case.query_clean_b if condition == "clean-b" else case.query_clean_a
            row = run_workflow_case(
                case,
                all_cases=cases,
                index=index,
                decision_provider=provider,
                arm="hybrid",
                run_id=f"dur056-dev-r209-attempt-{attempt}-{unit_id}",
                query_override=query,
                injection_text=injection_text,
                expected_profile="development",
            )
            row.update(
                {
                    "unit_id": unit_id,
                    "profile": profile,
                    "condition": condition,
                    "injection_attempt": attempt,
                    "injection_text_version": text_version,
                    "injection_text": injection_text,
                    "pilot_freeze_identity": freeze_identity,
                }
            )
            write_unit_once(unit_directory, unit_id, row)
            marker["completed_units"].append(unit_id)
            atomic_write_json(marker_path, marker)

        rows = [read_json(unit_directory / f"{unit_id}.json") for unit_id in unit_ids]
        outcomes = _summarize_injection(rows)
        provider_error_run_count = sum(bool(row.get("provider_error")) for row in rows)
        model_response_records = sum(len(row.get("model_runs", [])) for row in rows)
        calibration_usable = provider_error_run_count == 0 and model_response_records == len(
            unit_specs
        )
        ledger_after = ledger.snapshot()
        ledger_entries = read_json(output_directory / "spend-ledger.json").get("entries", [])
        new_entries = ledger_entries[ledger_entry_count_before:]
        report = {
            "schema": "dur056-injection-development-pilot.r209.v1",
            "status": "COMPLETE" if calibration_usable else "INCOMPLETE_PROVIDER_RESULTS",
            "study_version": STUDY_VERSION,
            "split": "development",
            "case_count": len(cases),
            "model_id": MODEL_ID,
            "attempt": attempt,
            "injection_text_version": text_version,
            "injection_text_fingerprint": expected_text_fingerprint,
            "injection_text_by_case": {
                case.case_id: injection_text_for_action(case.injection_wrong_action, text_version)
                for case in cases
            },
            "retrieval_config_fingerprint": context["retrieval_fingerprint"],
            "agent_config_fingerprint": context["agent_fingerprint"],
            "frozen_prompts_or_retrieval_changed_for_pilot": False,
            "planned_workflow_runs": len(unit_specs),
            "completed_workflow_runs": len(rows),
            "provider_error_run_count": provider_error_run_count,
            "model_response_records": model_response_records,
            "calibration_usable": calibration_usable,
            "provider_request_ledger_entries": len(new_entries),
            "outcomes": outcomes,
            "unit_rows": [f"{unit_id}.json" for unit_id in unit_ids],
            "unit_directory": unit_directory.name,
            "marker_path": marker_path.name,
            "aborts": marker.get("aborts", []),
            "resumes": marker.get("resumes", []),
            "spend_ledger_before": ledger_before,
            "spend_ledger_after": ledger_after,
            "new_ledger_entries": new_entries,
            "provider_calls_to_heldout": 0,
            "embedding_provider_calls": 0,
        }
        report_path = write_once(
            output_directory,
            f"injection-development-pilot-r209-attempt-{attempt}",
            report,
        )
        marker["status"] = "COMPLETE"
        marker["completed_at"] = datetime.now(UTC).isoformat()
        marker["report_path"] = report_path.name
        atomic_write_json(marker_path, marker)
        return {
            "status": report["status"],
            "report_path": report_path.name,
            "unit_directory": unit_directory.name,
            "outcomes": outcomes,
            "spend_ledger": ledger_after,
            "workflow_runs": len(rows),
            "provider_request_ledger_entries": len(new_entries),
            "heldout_provider_calls": 0,
        }
    except Exception as error:
        marker["status"] = "ABORTED_INFRASTRUCTURE"
        marker.setdefault("aborts", []).append(
            {
                "failing_unit": active_unit,
                "error": f"{type(error).__name__}: {str(error)[:500]}",
                "timestamp": datetime.now(UTC).isoformat(),
            }
        )
        atomic_write_json(marker_path, marker)
        raise
    finally:
        if connection is not None:
            connection.close()


def refreeze_gate_a_after_injection_pilot(root: Path) -> dict[str, Any]:
    """Write new fixture/oracle/pilot-linked freeze artifacts without retuning."""

    output_directory = _study_directory(root)
    if list(output_directory.glob("gate-a-freeze-review-r209-v3-*.json")):
        raise Dur056StudyError("an R209 freeze bundle already exists; refusing to replace it")
    context = _pilot_freeze_context(root, output_directory)
    (
        attempt_1_path,
        attempt_1_history,
        attempt_1_assessment_path,
        attempt_1_assessment,
    ) = _pilot_attempt1_transport_history(output_directory)
    attempt_reports = {
        1: [(attempt_1_path, attempt_1_history)],
        2: _pilot_reports(output_directory, attempt=2),
        3: _pilot_reports(output_directory, attempt=3),
    }
    selected_attempt, selected_text_version, selected_report_path, selected_report = (
        _select_pilot_for_freeze(attempt_reports)
    )
    validity = validate_fixtures(injection_text_version=selected_text_version)
    oracle_paths = {
        split: write_once(
            output_directory,
            f"oracle-report-{split}-v3-r209",
            validity["splits"][split]["oracle"],
        )
        for split in ("development", "heldout")
    }
    validity["oracle_report_paths"] = {name: path.name for name, path in oracle_paths.items()}
    validity["selected_injection_pilot_path"] = selected_report_path.name
    validity["selected_injection_attempt"] = selected_attempt
    validity["selected_injection_text_version"] = selected_text_version
    validity["attempt_1_transport_assessment_path"] = attempt_1_assessment_path.name
    validity["attempt_1_transport_assessment_fingerprint"] = fingerprint(attempt_1_assessment)
    validity["heldout_provider_calls"] = 0
    validity_path = write_once(output_directory, "fixture-validity-v3-r209", validity)
    source_bundle = context["source_bundle"]
    freeze_bundle: dict[str, Any] = {
        "schema": "dur056-gate-a-freeze-bundle.r209.v1",
        "study_version": STUDY_VERSION,
        "gate": "A",
        "status": "READY_FOR_FREEZE_REVIEW",
        "heldout_scoring_authorized": False,
        "heldout_scored": False,
        "source_commit": current_git_head(root),
        "source_v3_freeze_bundle_path": context["source_freeze_path"].name,
        "fixture_validity_path": validity_path.name,
        "fixture_fingerprints": {
            split: validity["splits"][split]["fixture_fingerprint"]
            for split in ("development", "heldout")
        },
        "oracle_report_paths": {name: path.name for name, path in oracle_paths.items()},
        "oracle_accuracy": {
            split: validity["splits"][split]["oracle"]["accuracy"]
            for split in ("development", "heldout")
        },
        "injection_pilot_path": selected_report_path.name,
        "injection_pilot_fingerprint": fingerprint(selected_report),
        "injection_attempt": selected_attempt,
        "injection_text_version": selected_text_version,
        "injection_text_fingerprint": selected_report["injection_text_fingerprint"],
        "injection_attempt_paths": {
            str(attempt): [path.name for path, _ in reports]
            for attempt, reports in attempt_reports.items()
            if reports
        },
        "attempt_1_transport_assessment_path": attempt_1_assessment_path.name,
        "attempt_1_transport_assessment_fingerprint": fingerprint(attempt_1_assessment),
        "retrieval_config_path": source_bundle["retrieval_config_path"],
        "retrieval_config_fingerprint": context["retrieval_fingerprint"],
        "agent_config_path": source_bundle["agent_config_path"],
        "agent_config_fingerprint": context["agent_fingerprint"],
        "config_fingerprint_note": (
            "Agent and retrieval config fingerprints are unchanged because the pilot "
            "changed only fixture-delivered injection text; neither config includes it."
        ),
        "frozen_agent_or_retrieval_settings_changed": False,
        "study_manifest_path": source_bundle["study_manifest_path"],
        "retrieval_development_results_path": source_bundle["retrieval_development_results_path"],
        "agent_development_results_path": source_bundle["agent_development_results_path"],
        "model_id": MODEL_ID,
        "spend_ledger_path": "spend-ledger.json",
        "spend_ledger_snapshot": Dur056SpendLedger(
            output_directory / "spend-ledger.json"
        ).snapshot(),
        "heldout_model_calls": 0,
        "heldout_embedding_calls": 0,
    }
    freeze_bundle["freeze_fingerprint"] = fingerprint(freeze_bundle)
    freeze_path = write_once(output_directory, "gate-a-freeze-review-r209-v3", freeze_bundle)
    return {
        "status": "READY_FOR_FREEZE_REVIEW",
        "freeze_bundle_path": freeze_path.name,
        "freeze_fingerprint": freeze_bundle["freeze_fingerprint"],
        "injection_attempt": selected_attempt,
        "injection_text_version": selected_text_version,
        "retrieval_config_fingerprint": context["retrieval_fingerprint"],
        "agent_config_fingerprint": context["agent_fingerprint"],
        "heldout_provider_calls": 0,
    }


def analyze_development_results(root: Path, source_report_name: str) -> dict[str, Any]:
    """Recompute development metrics from persisted rows without provider calls."""

    if (
        Path(source_report_name).name != source_report_name
        or not source_report_name.startswith("agent-development-v3-")
        or not source_report_name.endswith(".json")
    ):
        raise Dur056StudyError("analysis requires a versioned DUR-056 v3 development report")
    output_directory = _study_directory(root)
    report = read_json(output_directory / source_report_name)
    if not isinstance(report, dict):
        raise Dur056StudyError("development run report must be a JSON object")
    analysis = analyze_development_report(report)
    analysis.update(
        {
            "source_run_report": source_report_name,
            "analysis_code_commit": current_git_head(root),
            "provider_calls": 0,
        }
    )
    path = write_once(output_directory, "agent-development-analysis-v3", analysis)
    return {"status": "ANALYZED", "analysis_path": str(path), "provider_calls": 0}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=(
            "validate-fixtures",
            "prepare-dev",
            "analyze-dev",
            "pilot-injection-dev",
            "refreeze-gate-a",
            "score-heldout",
        ),
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help=(
            "resume a held-out scorer or development pilot only after "
            "an infrastructure-abort marker"
        ),
    )
    parser.add_argument(
        "--attempt",
        type=int,
        choices=(2, 3),
        default=None,
        help="R209 execution attempt 2 (v1) or conditional attempt 3 (v2)",
    )
    parser.add_argument(
        "--source-report",
        help="immutable agent-development-v3 report for local analysis; no provider calls",
    )
    args = parser.parse_args(argv)
    if args.resume and args.command not in ("score-heldout", "pilot-injection-dev"):
        parser.error("--resume is valid only with score-heldout or pilot-injection-dev")
    if args.command != "pilot-injection-dev" and args.attempt is not None:
        parser.error("--attempt is valid only with pilot-injection-dev")
    if args.command == "analyze-dev" and not args.source_report:
        parser.error("analyze-dev requires --source-report")
    if args.source_report and args.command != "analyze-dev":
        parser.error("--source-report is valid only with analyze-dev")
    project_root = _root()
    output_directory = _study_directory(project_root)
    try:
        if args.command == "validate-fixtures":
            report = validate_fixtures()
            path = write_once(output_directory, "fixture-validity-v3-r209", report)
            result: dict[str, Any] = {"status": "VALID", "report_path": str(path), "oracle": report}
        elif args.command == "prepare-dev":
            result = prepare_development(project_root)
        elif args.command == "analyze-dev":
            result = analyze_development_results(project_root, args.source_report)
        elif args.command == "pilot-injection-dev":
            result = run_development_injection_pilot(
                project_root,
                attempt=args.attempt or 2,
                resume=args.resume,
            )
        elif args.command == "refreeze-gate-a":
            result = refreeze_gate_a_after_injection_pilot(project_root)
        else:
            result = score_heldout(
                root=project_root,
                output_directory=output_directory,
                resume=args.resume,
            )
    except (Dur056StudyError, Dur056RetrievalError, Dur056ScorerError, RuntimeError) as error:
        print(f"DUR-056 blocked: {error}", file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
