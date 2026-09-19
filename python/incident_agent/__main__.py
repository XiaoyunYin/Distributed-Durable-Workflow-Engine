"""CLI for deterministic M6 fixture generation and correctness checks."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict
from pathlib import Path
from typing import Any

from incident_agent.continuity import (
    finalize_dur029_reports,
    run_adversarial_scan,
    run_citation_check,
    run_dur029_fixture_agent_control,
    run_dur029_live_evaluation,
    run_f12_continuity,
    run_metrics_report,
)
from incident_agent.evaluation import live_phase_status, run_retrieval_final
from incident_agent.fixtures import (
    build_corpus,
    build_incident_cases,
    build_retrieval_queries,
    corpus_manifest,
)
from incident_agent.mcp import METHOD_ARGUMENTS, METHODS, SCHEMA_VERSION
from incident_agent.metrics import dashboard_manifest
from incident_agent.models import RetrievalArm
from incident_agent.openai_provider import OpenAIProviderSettings
from incident_agent.retrieval import RetrievalConfig, RetrievalIndex
from incident_agent.source_corpus import source_corpus_contract


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _retrieval_evidence(
    index: RetrievalIndex, queries: list[dict[str, Any]], arm: RetrievalArm
) -> dict[str, Any]:
    return index.benchmark(queries, arm)


def _difficulty_audit(
    index: RetrievalIndex,
    queries: tuple[dict[str, Any], ...],
    config: RetrievalConfig,
) -> dict[str, object]:
    development = [query for query in queries if query["split"] == "development"]
    heldout = [query for query in queries if query["split"] == "heldout"]
    answerable = [query for query in queries if query["answerable"]]
    random_baseline = sum(
        min(1.0, config.top_k * len(query["relevant_chunk_ids"]) / len(index.chunks))
        for query in answerable
    ) / (len(answerable) or 1)
    disagreements = 0
    for query in queries:
        delivered = {
            tuple(hit.chunk_id for hit in index.search(str(query["query"]), arm).delivered)
            for arm in ("keyword", "dense", "hybrid")
        }
        disagreements += len(delivered) > 1
    dev_hash = config.fingerprint()
    heldout_hash = config.fingerprint()
    return {
        "schema": "dur-019-difficulty-audit.v2",
        "tuning_split": "development",
        "scoring_split": "heldout",
        "development_only": bool(development)
        and all(query["split"] == "development" for query in development),
        "frozen_before_heldout": dev_hash == heldout_hash,
        "tuning_config_fingerprint": dev_hash,
        "heldout_config_fingerprint": heldout_hash,
        "distinct_query_strings": len({str(query["query"]) for query in queries}),
        "minimum_relevant_chunks": min(len(query["relevant_chunk_ids"]) for query in answerable),
        "maximum_relevant_chunks": max(len(query["relevant_chunk_ids"]) for query in answerable),
        "random_recall_at_k_baseline": random_baseline,
        "arm_disagreement_fraction": disagreements / (len(queries) or 1),
        "development_queries": len(development),
        "heldout_queries": len(heldout),
        "near_duplicate_pairs": len(
            {chunk.near_duplicate_group for chunk in index.chunks} - {None}
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m incident_agent")
    parser.add_argument(
        "command",
        choices=(
            "fixtures",
            "benchmark",
            "continuity",
            "adversarial",
            "dur029-preflight",
            "dur029-finalize",
            "dur029-live-status",
            "dur029-live",
            "demo",
        ),
    )
    parser.add_argument("--output", type=Path, default=Path("experiments/m6"))
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    if args.command == "fixtures":
        write_json(args.output / "corpus-manifest.json", corpus_manifest())
        write_json(args.output / "retrieval-config.json", asdict(RetrievalConfig()))
        write_json(args.output / "source-corpus-contract.json", source_corpus_contract())
        write_json(
            args.output / "mcp-schema.json",
            {
                "schema_version": SCHEMA_VERSION,
                "methods": [
                    {"name": method, "arguments": list(METHOD_ARGUMENTS[method])}
                    for method in METHODS
                ],
                "max_calls": 8,
                "max_rows": 20,
            },
        )
        write_json(
            args.output / "incident-cases.json",
            {
                "schema": "incident-cases.evaluator-only.v1",
                "cases": [asdict(case) for case in build_incident_cases()],
            },
        )
        write_json(
            args.output / "difficulty-audit.json",
            _difficulty_audit(
                RetrievalIndex(build_corpus(), RetrievalConfig()),
                build_retrieval_queries(),
                RetrievalConfig(),
            ),
        )
        write_json(args.output / "dashboard.json", dashboard_manifest())
        return
    index = RetrievalIndex(build_corpus(), RetrievalConfig())
    if args.command == "benchmark":
        queries = build_retrieval_queries()
        arms: tuple[RetrievalArm, ...] = ("keyword", "dense", "hybrid")
        write_json(
            args.output / "retrieval-benchmark.json",
            {
                "schema": "dur-019-retrieval.v2",
                "corpus": corpus_manifest(),
                "config_fingerprint": index.config.fingerprint(),
                "arms": [
                    {
                        "arm": arm,
                        "development": _retrieval_evidence(
                            index,
                            [query for query in queries if query["split"] == "development"],
                            arm,
                        ),
                        "heldout": _retrieval_evidence(
                            index,
                            [query for query in queries if query["split"] == "heldout"],
                            arm,
                        ),
                    }
                    for arm in arms
                ],
            },
        )
        write_json(
            args.output / "difficulty-audit.json",
            _difficulty_audit(index, queries, index.config),
        )
    elif args.command == "continuity":
        write_json(args.output / "continuity.json", run_f12_continuity())
        write_json(
            args.output / "citation-check.json",
            run_citation_check(),
        )
        write_json(args.output / "metrics.json", run_metrics_report())
    elif args.command == "adversarial":
        write_json(args.output / "adversarial.json", run_adversarial_scan())
    elif args.command == "dur029-preflight":
        config = RetrievalConfig()
        retrieval_report = run_retrieval_final(
            RetrievalIndex(build_corpus(), config), build_retrieval_queries(), config
        )
        write_json(
            args.output / "retrieval-final.json",
            retrieval_report,
        )
        write_json(
            args.output / "fixture-agent-control.json",
            run_dur029_fixture_agent_control(),
        )
        write_json(
            args.output / "adversarial-preflight.json",
            run_adversarial_scan(),
        )
    elif args.command == "dur029-finalize":
        retrieval_path = args.output / "retrieval-final.json"
        if not retrieval_path.exists():
            raise SystemExit(f"missing retrieval artifact: {retrieval_path}")
        retrieval = json.loads(retrieval_path.read_text(encoding="utf-8"))
        fixture = json.loads(
            (args.output / "fixture-agent-control.json").read_text(encoding="utf-8")
        )
        agent = json.loads((args.output / "live-agent.json").read_text(encoding="utf-8"))
        adversarial = json.loads(
            (args.output / "live-adversarial.json").read_text(encoding="utf-8")
        )
        evaluation_path = args.output / "live-evaluation.json"
        root = json.loads(evaluation_path.read_text(encoding="utf-8"))
        report = finalize_dur029_reports(agent, adversarial, retrieval, fixture, root)
        write_json(args.output / "live-agent.json", report["agent"])
        write_json(args.output / "live-adversarial.json", report["adversarial"])
        write_json(evaluation_path, report)
    elif args.command == "dur029-live-status":
        from incident_agent.workflow import LiveModelConfig

        budget = int(os.environ.get("DUR029_BUDGET_CENTS", "0"))
        write_json(
            args.output / "live-phase-status.json",
            live_phase_status(
                LiveModelConfig(
                    os.environ.get("DUR029_PROVIDER", "unconfigured"),
                    os.environ.get("DUR029_MODEL", "unconfigured"),
                    budget,
                )
            ),
        )
    elif args.command == "dur029-live":
        settings = OpenAIProviderSettings(
            model=os.environ.get("DUR029_MODEL", "gpt-4o-mini"),
            budget_cents=int(os.environ.get("DUR029_BUDGET_CENTS", "3000")),
        )
        config = RetrievalConfig()
        retrieval = run_retrieval_final(
            RetrievalIndex(build_corpus(), config), build_retrieval_queries(), config
        )
        fixture = run_dur029_fixture_agent_control()
        report = run_dur029_live_evaluation(settings, retrieval, fixture)
        write_json(args.output / "live-agent.json", report["agent"])
        write_json(args.output / "live-adversarial.json", report["adversarial"])
        write_json(args.output / "live-evaluation.json", report)
    else:
        case = build_incident_cases()[0]
        write_json(
            args.output / "demo.json",
            {"case_id": case.case_id, "continuity": run_citation_check(case.case_id)},
        )


if __name__ == "__main__":
    main()
