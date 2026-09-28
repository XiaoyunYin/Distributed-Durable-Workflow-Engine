"""CLI for the gated DUR-055 applied-AI development phase."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

from incident_agent.dur055_agent import (
    MAX_OUTPUT_TOKENS,
    MODEL_ID,
    PROMPT_VARIANTS,
    evaluate_development_agent,
)
from incident_agent.dur055_budget import SpendLedger
from incident_agent.dur055_heldout import score_heldout
from incident_agent.dur055_retrieval import (
    OpenAIEmbeddingProvider,
    RetrievalStudyError,
    apply_migrations,
    connect,
    database_manifest,
    development_incident_search_queries,
    development_retrieval_queries,
    ensure_embeddings,
    fingerprint,
    retrieval_artifact_bundle,
    seed_corpus,
    tune_development,
    write_once,
)


def _root() -> Path:
    return Path(__file__).resolve().parents[2]


def _study_directory(root: Path) -> Path:
    return root / "experiments" / "m8" / "dur055-applied-ai-upgrade"


def prepare_development(root: Path | None = None) -> dict[str, Any]:
    project_root = root or _root()
    output_directory = _study_directory(project_root)
    ledger = SpendLedger(output_directory / "spend-ledger.json")
    connection = connect()
    try:
        apply_migrations(connection, project_root)
        chunks = seed_corpus(connection)
        queries = development_retrieval_queries()
        agent_queries = development_incident_search_queries()
        if len(chunks) != 300 or len(queries) != 40 or len(agent_queries) != 10:
            raise RetrievalStudyError("DUR-055 source fixture counts do not match the decision")
        embedding_provider = OpenAIEmbeddingProvider(ledger)
        ensure_embeddings(connection, embedding_provider, chunks, queries, agent_queries)
        retrieval_config, retrieval_results = tune_development(connection, queries)
        retrieval_config["database"] = database_manifest(connection)
        core = {
            key: value for key, value in retrieval_config.items() if key != "config_fingerprint"
        }
        retrieval_config["config_fingerprint"] = fingerprint(core)
        retrieval_results["config_fingerprint"] = retrieval_config["config_fingerprint"]
        retrieval_config_path, retrieval_results_path, manifest_path = retrieval_artifact_bundle(
            connection,
            embedding_provider,
            retrieval_config,
            retrieval_results,
            output_directory,
        )
        agent_results = evaluate_development_agent(
            connection,
            retrieval_config,
            embedding_provider,
            ledger,
            output_directory,
        )
        selected_variant = str(agent_results["selected_prompt_schema"])
        variant = PROMPT_VARIANTS[selected_variant]
        instructions = str(variant["instructions"]).encode("utf-8")
        agent_config = {
            "schema": "dur055-agent-frozen-config.v1",
            "model_id": MODEL_ID,
            "endpoint": "https://api.openai.com/v1/responses",
            "reasoning_effort": "low",
            "max_output_tokens": MAX_OUTPUT_TOKENS,
            "store": False,
            "retrieval_config_fingerprint": retrieval_config["config_fingerprint"],
            "retrieval_arm": retrieval_config["agent_retrieval_arm"],
            "selected_prompt_schema": selected_variant,
            "prompt_instructions_utf8": instructions.decode("utf-8"),
            "prompt_instructions_utf8_base64": base64.b64encode(instructions).decode("ascii"),
            "prompt_fingerprint": "sha256:" + hashlib.sha256(instructions).hexdigest(),
            "structured_output_schema": variant["schema"],
            "structured_output_schema_fingerprint": fingerprint(variant["schema"]),
            "mcp_tool_schemas": agent_results["tool_schemas"],
            "mcp_tool_schema_fingerprint": agent_results["tool_schema_fingerprint"],
            "mcp_tool_schema_variant": selected_variant,
            "mcp_argument_validation_unchanged": True,
            "development_result_path": Path(str(agent_results["path"])).name,
            "development_cases_scored": 10,
            "heldout_cases_scored": 0,
            "heldout_scored": False,
        }
        agent_config["config_fingerprint"] = fingerprint(agent_config)
        agent_config_path = write_once(output_directory, "agent-frozen-config", agent_config)
        freeze_bundle = {
            "schema": "dur055-gate-a-freeze-bundle.v1",
            "gate": "A",
            "status": "READY_FOR_FREEZE_REVIEW",
            "heldout_scoring_authorized": False,
            "retrieval_config_path": retrieval_config_path.name,
            "retrieval_config_fingerprint": retrieval_config["config_fingerprint"],
            "retrieval_development_results_path": retrieval_results_path.name,
            "study_manifest_path": manifest_path.name,
            "agent_config_path": agent_config_path.name,
            "agent_config_fingerprint": agent_config["config_fingerprint"],
            "agent_development_results_path": Path(str(agent_results["path"])).name,
            "agent_development_result_fingerprint": fingerprint(agent_results),
            "model_id": MODEL_ID,
            "spend_ledger_path": "spend-ledger.json",
            "spend_ledger_snapshot": ledger.snapshot(),
            "baseline_reference": {
                "study": "DUR-029",
                "model_id": "gpt-4o-mini",
                "heldout_safe_end_to_end": "4/20",
                "preserved": True,
            },
            "heldout_scored": False,
        }
        freeze_bundle["freeze_fingerprint"] = fingerprint(freeze_bundle)
        freeze_path = write_once(output_directory, "gate-a-freeze-review", freeze_bundle)
        return {"status": "READY_FOR_FREEZE_REVIEW", "freeze_bundle": str(freeze_path)}
    finally:
        connection.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare-dev", "score-heldout"))
    args = parser.parse_args(argv)
    try:
        result = prepare_development() if args.command == "prepare-dev" else score_heldout()
    except (RetrievalStudyError, RuntimeError) as error:
        print(f"DUR-055 blocked: {error}", file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
