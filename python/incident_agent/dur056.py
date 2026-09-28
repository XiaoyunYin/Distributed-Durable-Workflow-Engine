"""DUR-056 validation, development tuning, freeze, and gated held-out CLI."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import asdict
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
)
from incident_agent.dur056_artifacts import current_git_head, write_once
from incident_agent.dur056_budget import Dur056SpendLedger
from incident_agent.dur056_evaluation import evaluate_development_agents
from incident_agent.dur056_fixtures import (
    ACTION_PARAMETERS,
    DEV_QUERY_TEMPLATES,
    FAMILIES,
    HELDOUT_QUERY_TEMPLATES,
    Dur056Case,
    all_fixture_text,
    build_cases,
    build_corpus,
    oracle_decision,
    oracle_report,
)
from incident_agent.dur056_retrieval import (
    EMBEDDING_DIMENSION,
    EMBEDDING_MODEL,
    STUDY_VERSION,
    Dur056OpenAIEmbeddingProvider,
    Dur056RetrievalError,
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
from incident_agent.dur056_scorer import Dur056ScorerError, score_heldout

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
                "expected_action": case.expected_action,
                "evidence": [asdict(chunk) for chunk in case.evidence],
            }
            for case in cases
        ]
    )


def validate_fixtures() -> dict[str, Any]:
    reports: dict[str, Any] = {}
    for split in ("development", "heldout"):
        cases = build_cases(split)  # local deterministic oracle only; no provider API is used
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
        for case in cases:
            if case.expected_action is not None:
                current_text = "\n".join(
                    chunk.text
                    for chunk in case.evidence
                    if chunk.service == case.service and chunk.version == case.active_version
                )
                action = str(case.expected_action["action"])
                if f"action={action}" not in current_text:
                    raise Dur056StudyError(
                        f"expected action absent from evidence for {case.case_id}"
                    )
                required = ACTION_PARAMETERS[action]
                for parameter in required:
                    value = case.expected_action["parameters"].get(parameter)
                    if parameter not in current_text or str(value) not in current_text:
                        raise Dur056StudyError(
                            "required parameter absent from evidence for "
                            f"{case.case_id}: {parameter}"
                        )
            if case.category == "insufficient_no_guidance" and any(
                "DIRECTIVE:" in chunk.text for chunk in case.evidence
            ):
                raise Dur056StudyError(
                    f"insufficient no-guidance case has an action: {case.case_id}"
                )
            if case.category == "insufficient_missing_parameter":
                evidence = all_fixture_text(case)
                match = re.search(
                    r"DIRECTIVE: action=([a-z_]+); service=[a-z-]+; version=v\d+; "
                    r"parameters=(\{[^}]*\})\.",
                    evidence,
                )
                if match is None:
                    raise Dur056StudyError(
                        f"missing-parameter case lost its partial guidance: {case.case_id}"
                    )
                action, raw_params = match.groups()
                parsed = json.loads(raw_params)
                if all(key in parsed for key in ACTION_PARAMETERS[action]):
                    raise Dur056StudyError(
                        f"insufficient case includes all parameters: {case.case_id}"
                    )
            if oracle_decision(case) != case.expected_action:
                raise Dur056StudyError(f"deterministic oracle disagrees on {case.case_id}")
        oracle = oracle_report(split)
        if oracle["correct"] != len(cases) or oracle["accuracy"] != 1.0:
            raise Dur056StudyError(f"DUR-056 {split} deterministic oracle did not score 100%")
        if oracle["provider_calls"] != 0:
            raise Dur056StudyError("the local DUR-056 oracle must make zero provider calls")
        reports[split] = {
            "allocation": actual_counts,
            "fixture_fingerprint": _fixture_fingerprint(cases),
            "corpus_chunk_count": len(build_corpus(split)),
            "oracle": oracle,
            "provider_calls": 0,
        }
    if not set(DEV_QUERY_TEMPLATES).isdisjoint(HELDOUT_QUERY_TEMPLATES):
        raise Dur056StudyError("DUR-056 development/held-out query templates overlap")
    reports["template_sets_disjoint"] = True
    reports["development_seed"] = 5601
    reports["heldout_seed"] = 5602
    reports["model_provider_calls"] = 0
    reports["embedding_provider_calls"] = 0
    return {"schema": "dur056-fixture-validity.v1", "splits": reports}


def prepare_development(root: Path | None = None) -> dict[str, Any]:
    if not os.environ.get("OPENAI_API_KEY"):
        raise Dur056StudyError("OPENAI_API_KEY is required before DUR-056 development tuning")
    project_root = root or _root()
    output_directory = _study_directory(project_root)
    output_directory.mkdir(parents=True, exist_ok=True)
    existing_freezes = list(output_directory.glob("gate-a-freeze-review-*.json"))
    if existing_freezes:
        raise Dur056StudyError(
            "a DUR-056 frozen Gate A bundle already exists; refusing to retune or overwrite"
        )
    validity = validate_fixtures()
    oracle_paths = {
        split: write_once(
            output_directory, f"oracle-report-{split}", validity["splits"][split]["oracle"]
        )
        for split in ("development", "heldout")
    }
    validity["oracle_report_paths"] = {key: value.name for key, value in oracle_paths.items()}
    validity_path = write_once(output_directory, "fixture-validity", validity)

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
            output_directory, "retrieval-frozen-config", retrieval_config
        )
        retrieval_results_path = write_once(
            output_directory, "retrieval-development", retrieval_results
        )
        manifest = {
            "schema": "dur056-study-manifest.v1",
            "study_version": STUDY_VERSION,
            "source_commit": current_git_head(project_root),
            "fixture_validity_path": validity_path.name,
            "oracle_report_paths": {key: value.name for key, value in oracle_paths.items()},
            "development_case_count": len(cases),
            "development_corpus_chunk_count": len(chunks),
            "development_query_count": len(queries),
            "development_corpus_fingerprint": corpus_fingerprint,
            "development_query_fingerprint": query_fingerprint,
            "query_seeds": {"development": 5601, "heldout": 5602},
            "embedding_model": EMBEDDING_MODEL,
            "embedding_dimension": EMBEDDING_DIMENSION,
            "embedding_requests": embedding_provider.records,
            "heldout_cases_sent_to_model": 0,
            "heldout_queries_sent_to_embedding_provider": 0,
            "heldout_documents_sent_to_embedding_provider": 0,
            "database": database_manifest(connection),
            "spend_ledger_snapshot": ledger.snapshot(),
        }
        manifest_path = write_once(output_directory, "study-manifest", manifest)
        agent_results = evaluate_development_agents(
            cases=cases,
            ledger=ledger,
            retrieval_config=retrieval_config,
            connection=connection,
            query_vectors=query_vectors,
            query_splits=query_splits,
        )
        agent_results["fixture_validity_path"] = validity_path.name
        agent_results["development_seed"] = 5601
        agent_results["heldout_model_calls"] = 0
        agent_results["per_run_cost_authority"] = "DUR-056 spend-ledger.json"
        agent_results_path = write_once(output_directory, "agent-development", agent_results)

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
            "schema": "dur056-agent-frozen-config.v1",
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
        agent_config_path = write_once(output_directory, "agent-frozen-config", agent_config)
        freeze_bundle: dict[str, Any] = {
            "schema": "dur056-gate-a-freeze-bundle.v1",
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
            "baseline_reference": {
                "study": "DUR-029",
                "model_id": "gpt-4o-mini",
                "primary_safe_end_to_end": "4/20",
                "preserved": True,
            },
        }
        freeze_bundle["freeze_fingerprint"] = fingerprint(freeze_bundle)
        freeze_path = write_once(output_directory, "gate-a-freeze-review", freeze_bundle)
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=("validate-fixtures", "prepare-dev", "score-heldout"),
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="resume score-heldout only after an ABORTED_INFRASTRUCTURE marker",
    )
    args = parser.parse_args(argv)
    if args.resume and args.command != "score-heldout":
        parser.error("--resume is valid only with score-heldout")
    project_root = _root()
    output_directory = _study_directory(project_root)
    try:
        if args.command == "validate-fixtures":
            report = validate_fixtures()
            path = write_once(output_directory, "fixture-validity", report)
            result: dict[str, Any] = {"status": "VALID", "report_path": str(path), "oracle": report}
        elif args.command == "prepare-dev":
            result = prepare_development(project_root)
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
