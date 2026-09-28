from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest
from incident_agent.dur055_retrieval import (
    STUDY_VERSION,
    _dense_hits,
    _keyword_hits,
    _vector_text,
    apply_migrations,
    connect,
    seed_corpus,
)


@pytest.fixture
def dur055_database() -> Any:
    dsn = os.environ.get("DUR055_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("DUR055_TEST_DATABASE_URL is not configured")
    connection = connect(dsn)
    apply_migrations(connection, Path(__file__).parents[1])
    chunks = seed_corpus(connection)
    assert len(chunks) == 300
    try:
        yield connection
    finally:
        connection.close()


def test_postgresql_fts_uses_tsvector_and_ts_rank(dur055_database: Any) -> None:
    hits = _keyword_hits(dur055_database, "checkout revision", 5)
    assert hits
    assert hits[0].chunk_id.startswith("doc-01-")
    assert all(hit.score > 0 for hit in hits)


def test_pgvector_hnsw_dense_query_returns_1536_dimension_rows(
    dur055_database: Any,
) -> None:
    with dur055_database.transaction():
        existing = dur055_database.execute(
            """SELECT embedding::text AS embedding FROM source_corpus.dur055_chunk_embeddings
               WHERE study_version=%s AND chunk_id=%s""",
            (STUDY_VERSION, "doc-01-01-chunk-01"),
        ).fetchone()
        vector = (
            str(existing["embedding"])
            if existing is not None
            else _vector_text([1.0, *([0.0] * 1535)])
        )
        if existing is None:
            dur055_database.execute(
                """INSERT INTO source_corpus.dur055_chunk_embeddings
                   (study_version,chunk_id,embedding_model,embedding_dimension,embedding)
                   VALUES (%s,%s,%s,%s,%s::vector)""",
                (
                    STUDY_VERSION,
                    "doc-01-01-chunk-01",
                    "text-embedding-3-small",
                    1536,
                    vector,
                ),
            )
        hits = _dense_hits(dur055_database, "test-development-query", vector, 3, 40)
        assert hits[0].chunk_id == "doc-01-01-chunk-01"
        assert hits[0].score == pytest.approx(1.0)
        index = dur055_database.execute(
            """SELECT indexdef FROM pg_indexes
               WHERE schemaname='source_corpus'
                 AND indexname='source_corpus_dur055_embeddings_hnsw'"""
        ).fetchone()
        assert index is not None
        assert "using hnsw" in index["indexdef"].lower()
