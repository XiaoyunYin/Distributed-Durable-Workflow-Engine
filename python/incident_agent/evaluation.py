"""DUR-029 preflight evaluation and fail-closed live-run gating."""

from __future__ import annotations

import statistics
import time
from collections.abc import Iterable
from typing import Any, cast

from incident_agent.fixtures import FAMILIES
from incident_agent.models import RetrievalArm
from incident_agent.retrieval import RetrievalConfig, RetrievalIndex
from incident_agent.workflow import LiveModelConfig, WorkflowError


def _quantiles(values: list[float]) -> dict[str, float | int]:
    if not values:
        return {"count": 0, "min": 0.0, "median": 0.0, "max": 0.0}
    return {
        "count": len(values),
        "min": min(values),
        "median": statistics.median(values),
        "max": max(values),
    }


def _family_for_query(index: int) -> str:
    # The fixture generator freezes held-out queries in family round-robin order.
    return FAMILIES[index % len(FAMILIES)]


def _retrieval_arm_report(
    index: RetrievalIndex, queries: list[dict[str, Any]], arm: RetrievalArm
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for query_index, query in enumerate(queries):
        started = time.perf_counter()
        response = index.search(str(query["query"]), arm)
        latency_ms = (time.perf_counter() - started) * 1000
        relevant = set(cast(tuple[str, ...], query["relevant_chunk_ids"]))
        distractors = set(cast(tuple[str, ...], query["distractor_chunk_ids"]))
        ranking = {
            "keyword": response.pre_gate_keyword,
            "dense": response.pre_gate_dense,
            "hybrid": response.pre_gate_hybrid,
        }[arm]
        ranking_ids = [hit.chunk_id for hit in ranking]
        delivered_ids = [hit.chunk_id for hit in response.delivered]
        ranking_recall = (
            len(relevant.intersection(ranking_ids)) / len(relevant) if relevant else None
        )
        delivered_recall = (
            len(relevant.intersection(delivered_ids)) / len(relevant) if relevant else None
        )
        first_rank = next(
            (position for position, chunk_id in enumerate(ranking_ids, 1) if chunk_id in relevant),
            None,
        )
        rows.append(
            {
                "query_id": str(query["query_id"]),
                "family": _family_for_query(query_index),
                "answerable": bool(query["answerable"]),
                "ranking_recall": ranking_recall,
                "delivered_recall": delivered_recall,
                "reciprocal_rank": 1.0 / first_rank if first_rank else 0.0,
                "no_answer_false_positive": not relevant and response.sufficient,
                "distractor_hits": len(distractors.intersection(ranking_ids)),
                "ranking_count": len(ranking_ids),
                "delivered_count": len(delivered_ids),
                "latency_ms": latency_ms,
                "sufficient": response.sufficient,
                "reason": response.reason,
            }
        )

    def summarize(selected: list[dict[str, Any]]) -> dict[str, Any]:
        answerable = [row for row in selected if row["answerable"]]
        no_answer = [row for row in selected if not row["answerable"]]
        return {
            "queries": len(selected),
            "answerable_queries": len(answerable),
            "no_answer_queries": len(no_answer),
            "ranking_recall_at_k": statistics.mean(
                [float(row["ranking_recall"]) for row in answerable]
            )
            if answerable
            else 0.0,
            "delivered_recall_at_k": statistics.mean(
                [float(row["delivered_recall"]) for row in answerable]
            )
            if answerable
            else 0.0,
            "ranking_mrr": statistics.mean(
                [float(row["reciprocal_rank"]) for row in answerable]
            )
            if answerable
            else 0.0,
            "no_answer_false_positive_rate": statistics.mean(
                [float(row["no_answer_false_positive"]) for row in no_answer]
            )
            if no_answer
            else 0.0,
            "distractor_hit_rate": statistics.mean(
                [
                    float(row["distractor_hits"]) / max(1, int(row["ranking_count"]))
                    for row in answerable
                ]
            )
            if answerable
            else 0.0,
            "latency_ms": _quantiles([float(row["latency_ms"]) for row in selected]),
        }

    by_family = {
        family: summarize([row for row in rows if row["family"] == family])
        for family in FAMILIES
    }
    return {"summary": summarize(rows), "by_family": by_family, "rows": rows}


def run_retrieval_preflight(
    index: RetrievalIndex,
    queries: Iterable[dict[str, Any]],
    config: RetrievalConfig,
) -> dict[str, Any]:
    """Evaluate frozen settings without changing or tuning them."""

    all_queries = [dict(query) for query in queries]
    development = [query for query in all_queries if query["split"] == "development"]
    heldout = [query for query in all_queries if query["split"] == "heldout"]
    arms: dict[str, Any] = {}
    for arm in ("keyword", "dense", "hybrid"):
        arms[arm] = {
            "development": _retrieval_arm_report(index, development, arm),
            "heldout": _retrieval_arm_report(index, heldout, arm),
        }
    return {
        "schema": "dur-029-retrieval-preflight.v1",
        "status": "PREPARED_NOT_FINAL",
        "config_fingerprint": config.fingerprint(),
        "development_queries": len(development),
        "heldout_queries": len(heldout),
        "arms": arms,
        "live_model": {
            "status": "NOT_RUN",
            "reason": (
                "separate provider, cost-cap, and INCIDENT_LIVE_APPROVED=1 "
                "authorization required"
            ),
        },
    }


def live_phase_status(config: LiveModelConfig) -> dict[str, Any]:
    """Return an auditable status without ever making a provider call."""

    try:
        config.authorize()
    except WorkflowError as error:
        return {
            "status": "BLOCKED",
            "provider": config.provider,
            "model": config.model,
            "budget_cents": config.budget_cents,
            "reason": str(error),
            "provider_calls": 0,
        }
    return {
        "status": "AUTHORIZED_BUT_NOT_EXECUTED",
        "provider": config.provider,
        "model": config.model,
        "budget_cents": config.budget_cents,
        "reason": "provider adapter is not invoked by the preflight command",
        "provider_calls": 0,
    }
