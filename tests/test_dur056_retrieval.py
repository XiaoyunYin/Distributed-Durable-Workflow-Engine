from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest
from incident_agent import dur056_retrieval
from incident_agent.dur056_budget import Dur056SpendLedger
from incident_agent.dur056_fixtures import LEGACY_QUERY_PADDING, build_cases
from incident_agent.dur056_retrieval import (
    Dur056OpenAIEmbeddingProvider,
    Dur056RetrievalError,
    NoRetrievalAdapter,
    PostgresRetrievalAdapter,
    _disjunctive_tsquery,
    development_query_rows,
    heldout_query_rows,
    reciprocal_rank_fusion,
)
from incident_agent.models import RetrievalHit


def _hit(chunk_id: str, score: float = 0.5) -> RetrievalHit:
    return RetrievalHit(chunk_id, f"doc-{chunk_id}", "v2", "checkout", score, chunk_id)


def test_rrf_combines_both_arms_and_breaks_ties_by_chunk_id() -> None:
    keyword = (_hit("b"), _hit("a"))
    dense = (_hit("a"), _hit("c"))
    rows = reciprocal_rank_fusion(keyword, dense, rrf_k=60, top_k=3)
    assert [row.chunk_id for row in rows] == ["a", "b", "c"]
    assert rows[0].score > rows[1].score > rows[2].score


def test_dev_queries_are_dev_only_and_holdout_build_has_registered_count() -> None:
    development = development_query_rows()
    assert len(development) == 60
    assert {row["split"] for row in development} == {"development"}
    assert all(row["query_id"].startswith("dev-") for row in development)

    heldout = heldout_query_rows(build_cases("heldout"))
    assert len(heldout) == 120
    assert {row["split"] for row in heldout} == {"heldout"}
    assert all(row["query_id"].startswith("hel-") for row in heldout)
    for row in (*development, *heldout):
        assert row["case_id"] not in row["query"]
        assert LEGACY_QUERY_PADDING not in row["query"]
        assert row["query"].endswith(".")


def test_keyword_query_is_sanitized_disjunctive_terms() -> None:
    assert _disjunctive_tsquery("Checkout v42: CFG_PARSE_REJECTED; slow requests!") == (
        "checkout | v42 | cfg_parse_rejected | slow | requests"
    )


def test_v2_database_migration_preserves_v1_study_version() -> None:
    migration = (
        Path(__file__).resolve().parents[1]
        / "migrations"
        / "dur056"
        / "000057_version_v2_support.up.sql"
    )
    sql = migration.read_text(encoding="utf-8").lower()
    assert "dur056-solvable-evidence-v1" in sql
    assert "dur056-solvable-evidence-v2" in sql
    assert "delete from" not in sql
    assert "truncate" not in sql
    assert "drop table" not in sql


def test_v3_database_migration_preserves_v1_and_v2_study_versions() -> None:
    migration = (
        Path(__file__).resolve().parents[1]
        / "migrations"
        / "dur056"
        / "000058_version_v3_support.up.sql"
    )
    sql = migration.read_text(encoding="utf-8").lower()
    assert "dur056-solvable-evidence-v1" in sql
    assert "dur056-solvable-evidence-v2" in sql
    assert "dur056-solvable-evidence-v3" in sql
    assert "delete from" not in sql
    assert "truncate" not in sql
    assert "drop table" not in sql


def test_embedding_provider_refuses_heldout_scope_before_gate_a() -> None:
    with pytest.raises(Dur056RetrievalError, match="requires accepted Gate A receipt"):
        Dur056OpenAIEmbeddingProvider(
            cast(Dur056SpendLedger, object()),
            split_scope="heldout",
            heldout_authorized=False,
        )


def test_development_retrieval_adapter_refuses_heldout_query_before_sql() -> None:
    adapter = PostgresRetrievalAdapter(
        cast(Any, object()),
        {},
        {"heldout query": ("hel-q-1", "[0.0]")},
        {"heldout query": "heldout"},
        allowed_query_splits=("development",),
        corpus_splits=("development",),
    )
    with pytest.raises(Dur056RetrievalError, match="refused query split"):
        adapter.search("heldout query")


@pytest.mark.parametrize("arm", ["keyword", "dense", "hybrid"])
def test_retrieval_arm_delivers_ranked_results_when_sufficiency_classifier_is_negative(
    monkeypatch: pytest.MonkeyPatch, arm: str
) -> None:
    keyword = [_hit("keyword", 0.2)]
    dense = [_hit("dense", 0.3)]
    monkeypatch.setattr(dur056_retrieval, "_keyword_hits", lambda *_args: keyword)
    monkeypatch.setattr(dur056_retrieval, "_dense_hits", lambda *_args: dense)
    adapter = PostgresRetrievalAdapter(
        cast(Any, object()),
        {
            "ranking": {"top_k": 3},
            "dense": {"hnsw_ef_search": 40, "threshold": 0.99},
            "keyword": {"threshold": 0.99},
            "hybrid": {"rrf_k": 60},
        },
        {"query": ("dev-q", "[0.0]")},
        {"query": "development"},
    )
    response = adapter.search("query", arm)  # type: ignore[arg-type]
    assert response.sufficient is False
    assert response.delivered
    assert response.reason == "sufficiency_classifier_negative"


def test_no_retrieval_arm_returns_no_documents() -> None:
    response = NoRetrievalAdapter().search("diagnostic query")
    assert response.sufficient is False
    assert response.delivered == ()
    assert response.reason == "no_retrieval_arm"
