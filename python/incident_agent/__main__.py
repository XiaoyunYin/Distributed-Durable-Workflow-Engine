"""CLI for deterministic M6 fixture generation and correctness checks."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from incident_agent.continuity import run_adversarial_scan, run_citation_check, run_f12_continuity
from incident_agent.fixtures import (
    build_corpus,
    build_incident_cases,
    build_retrieval_queries,
    corpus_manifest,
)
from incident_agent.mcp import METHOD_ARGUMENTS, METHODS, SCHEMA_VERSION
from incident_agent.metrics import dashboard_manifest
from incident_agent.models import RetrievalArm
from incident_agent.retrieval import RetrievalConfig, RetrievalIndex


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m incident_agent")
    parser.add_argument(
        "command", choices=("fixtures", "benchmark", "continuity", "adversarial", "demo")
    )
    parser.add_argument("--output", type=Path, default=Path("experiments/m6"))
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    if args.command == "fixtures":
        write_json(args.output / "corpus-manifest.json", corpus_manifest())
        write_json(args.output / "retrieval-config.json", asdict(RetrievalConfig()))
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
            {
                "schema": "dur-019-difficulty-audit.v1",
                "development_only": True,
                "criteria": {
                    "minimum_development_queries": 40,
                    "minimum_heldout_queries": 120,
                    "minimum_documents": 60,
                    "minimum_chunks": 300,
                    "no_answer_fraction": 0.25,
                    "near_duplicate_distractors": True,
                },
                "observed": corpus_manifest(),
                "frozen_before_heldout": True,
            },
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
                "schema": "dur-019-retrieval.v1",
                "corpus": corpus_manifest(),
                "arms": [index.benchmark(queries, arm) for arm in arms],
            },
        )
    elif args.command == "continuity":
        write_json(args.output / "continuity.json", run_f12_continuity())
        write_json(
            args.output / "citation-check.json",
            run_citation_check(build_incident_cases()[0].case_id),
        )
    elif args.command == "adversarial":
        write_json(args.output / "adversarial.json", run_adversarial_scan())
    else:
        case = build_incident_cases()[0]
        write_json(
            args.output / "demo.json",
            {"case_id": case.case_id, "continuity": run_citation_check(case.case_id)},
        )


if __name__ == "__main__":
    main()
