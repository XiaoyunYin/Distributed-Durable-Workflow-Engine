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
    tune_development,
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


def test_development_retrieval_selection_prioritizes_ranking_over_classifier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cases = build_cases("development")
    query_cases = {
        query: case for case in cases for query in (case.query_clean_a, case.query_clean_b)
    }

    def fake_keyword(_connection: Any, query: str, _splits: Any, top_k: int) -> list[RetrievalHit]:
        case = query_cases[query]
        relevant = case.relevant_chunk_ids[0] if case.relevant_chunk_ids else "zzzz-keyword"
        chunk_id = relevant if top_k == 3 else "zzzz-keyword"
        aligned = top_k != 3
        positive = case.expected_action is not None
        score = 0.9 if positive == aligned else 0.1
        return [_hit(chunk_id, score)]

    def fake_dense(
        _connection: Any,
        _query_id: str,
        _vector: str,
        _split: str,
        top_k: int,
        ef_search: int,
        _corpus_splits: Any,
    ) -> list[RetrievalHit]:
        # This synthetic candidate has worse classifier scores but perfect
        # rankings only at the preferred top_k/ef_search combination.
        query = next(query for query, pair in vectors.items() if pair[0] == _query_id)
        case = query_cases[query]
        relevant = case.relevant_chunk_ids[0] if case.relevant_chunk_ids else "zzzz-dense"
        rankable = top_k == 3 and ef_search == 40
        chunk_id = relevant if rankable else "zzzz-dense"
        aligned = top_k == 5 and ef_search == 80
        positive = case.expected_action is not None
        score = 0.9 if positive == aligned else 0.1
        return [_hit(chunk_id, score)]

    monkeypatch.setattr(dur056_retrieval, "_keyword_hits", fake_keyword)
    monkeypatch.setattr(dur056_retrieval, "_dense_hits", fake_dense)
    query_rows = development_query_rows(cases)
    vectors = {row["query"]: (row["query_id"], "[0.0]") for row in query_rows}
    query_splits = {row["query"]: row["split"] for row in query_rows}
    config, report = tune_development(cast(Any, object()), cases, vectors, query_splits)
    selection = config["selection"]
    assert config["ranking"]["top_k"] == 3
    assert config["dense"]["hnsw_ef_search"] == 40
    assert selection["selection_key_order"] == [
        "mean_ranking_recall_at_k",
        "mean_mrr",
        "mean_delivered_chunk_recall",
        "lower_top_k",
        "lower_hnsw_ef_search",
        "rrf_k_nearest_60",
    ]
    assert selection["mean_ranking_recall_at_k"] > 0
    assert "arm_balanced_accuracy" in selection
    assert "sufficiency_classifier_prediction" in report["rows"][0]


def test_no_retrieval_arm_returns_no_documents() -> None:
    response = NoRetrievalAdapter().search("diagnostic query")
    assert response.sufficient is False
    assert response.delivered == ()
    assert response.reason == "no_retrieval_arm"
