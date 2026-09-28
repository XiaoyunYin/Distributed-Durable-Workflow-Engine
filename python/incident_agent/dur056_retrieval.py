"""Isolated PostgreSQL FTS, pgvector, and RRF retrieval for DUR-056."""

from __future__ import annotations

import hashlib
import json
import os
import re
import statistics
import time
from collections import defaultdict
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Literal, cast
from urllib.parse import quote

import psycopg
from psycopg.rows import dict_row

from incident_agent.dur056_budget import Dur056SpendError, Dur056SpendLedger
from incident_agent.dur056_fixtures import Dur056Case, build_cases
from incident_agent.models import EvidenceChunk, RetrievalArm, RetrievalHit, RetrievalResponse

STUDY_VERSION = "dur056-solvable-evidence-v2"
EMBEDDING_MODEL = "text-embedding-3-small"
EMBEDDING_DIMENSION = 1536
EMBEDDING_BATCH_SIZE = 48
TOP_K_CANDIDATES = (3, 5, 8)
EF_SEARCH_CANDIDATES = (40, 80)
RRF_K_CANDIDATES = (30, 60, 90)
DEV_THRESHOLD_GRID = tuple(i / 1000 for i in range(0, 1001, 25))


class Dur056RetrievalError(RuntimeError):
    """Raised when the isolated DUR-056 retrieval study cannot proceed safely."""


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def fingerprint(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def connect(database_url: str | None = None) -> psycopg.Connection[Any]:
    dsn = database_url or os.environ.get("DUR056_DATABASE_URL")
    if not dsn:
        user = os.environ.get("POSTGRES_USER")
        database = os.environ.get("POSTGRES_DB")
        password = os.environ.get("POSTGRES_PASSWORD")
        if user and database and password:
            port = os.environ.get("DUR056_POSTGRES_PORT", "55433")
            dsn = (
                f"postgresql://{quote(user, safe='')}:{quote(password, safe='')}"
                f"@127.0.0.1:{port}/{quote(database, safe='')}"
            )
    if not dsn:
        raise Dur056RetrievalError("DUR056_DATABASE_URL or PostgreSQL credentials are required")
    try:
        return psycopg.connect(dsn, row_factory=dict_row, autocommit=True)
    except psycopg.Error as error:
        raise Dur056RetrievalError(
            f"cannot connect to isolated DUR-056 PostgreSQL: {error}"
        ) from error


def apply_migration(connection: psycopg.Connection[Any], root: Path) -> None:
    migration_56 = root / "migrations" / "dur056" / "000056_solvable_evidence.up.sql"
    migration_57 = root / "migrations" / "dur056" / "000057_version_v2_support.up.sql"
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT to_regclass('dur056.schema_migrations') AS relation")
            relation_row = cursor.fetchone()
            exists = relation_row is not None and relation_row["relation"] is not None
        if not exists:
            if not migration_56.is_file():
                raise Dur056RetrievalError(f"DUR-056 migration is missing: {migration_56}")
            with connection.cursor() as cursor:
                cursor.execute(migration_56.read_text(encoding="utf-8"))
        for version, migration in ((56, migration_56), (57, migration_57)):
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT EXISTS (SELECT 1 FROM dur056.schema_migrations WHERE version=%s)",
                    (version,),
                )
                applied_row = cursor.fetchone()
            if applied_row is not None and bool(applied_row["exists"]):
                continue
            if not migration.is_file():
                raise Dur056RetrievalError(f"DUR-056 migration is missing: {migration}")
            with connection.cursor() as cursor:
                cursor.execute(migration.read_text(encoding="utf-8"))
        with connection.cursor() as cursor:
            cursor.execute("SELECT extversion FROM pg_extension WHERE extname='vector'")
            if cursor.fetchone() is None:
                raise Dur056RetrievalError("isolated DUR-056 PostgreSQL is missing pgvector")
    except psycopg.Error as error:
        raise Dur056RetrievalError(f"DUR-056 migration failed: {error}") from error


def _rows_for_cases(cases: Sequence[Dur056Case]) -> tuple[EvidenceChunk, ...]:
    return tuple(chunk for case in cases for chunk in case.evidence)


def seed_corpus(
    connection: psycopg.Connection[Any],
    cases: Sequence[Dur056Case],
    *,
    split: Literal["development", "heldout"],
) -> tuple[EvidenceChunk, ...]:
    if not cases or any(case.split != split for case in cases):
        raise Dur056RetrievalError(f"DUR-056 {split} corpus input has mixed or empty splits")
    chunks = _rows_for_cases(cases)
    try:
        with connection.transaction(), connection.cursor() as cursor:
            for chunk in chunks:
                cursor.execute(
                    """INSERT INTO dur056.evidence_chunks
                        (study_version, split, chunk_id, document_id, service, version,
                         source_type, content)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (study_version, split, chunk_id) DO NOTHING""",
                    (
                        STUDY_VERSION,
                        split,
                        chunk.chunk_id,
                        chunk.document_id,
                        chunk.service,
                        chunk.version,
                        chunk.source_type,
                        chunk.text,
                    ),
                )
            cursor.execute(
                "SELECT chunk_id, content FROM dur056.evidence_chunks "
                "WHERE study_version=%s AND split=%s",
                (STUDY_VERSION, split),
            )
            persisted = {str(row["chunk_id"]): str(row["content"]) for row in cursor.fetchall()}
        expected = {chunk.chunk_id: chunk.text for chunk in chunks}
        if any(persisted.get(chunk_id) != text for chunk_id, text in expected.items()):
            raise Dur056RetrievalError("persisted DUR-056 corpus differs from registered fixtures")
        if len(persisted) != len(expected):
            raise Dur056RetrievalError("unexpected rows exist in the DUR-056 split corpus")
    except psycopg.Error as error:
        raise Dur056RetrievalError(f"cannot seed DUR-056 corpus: {error}") from error
    return chunks


def development_query_rows(cases: Sequence[Dur056Case] | None = None) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for case in cases if cases is not None else build_cases("development"):
        if case.split != "development":
            raise Dur056RetrievalError("development query builder received held-out case data")
        for suffix, query in (("a", case.query_clean_a), ("b", case.query_clean_b)):
            rows.append(
                {
                    "query_id": f"dev-{case.case_id}-{suffix}",
                    "case_id": case.case_id,
                    "query": query,
                    "split": "development",
                }
            )
    return rows


def heldout_query_rows(cases: Sequence[Dur056Case]) -> list[dict[str, str]]:
    if not cases or any(case.split != "heldout" for case in cases):
        raise Dur056RetrievalError("held-out query builder requires only held-out fixtures")
    rows: list[dict[str, str]] = []
    for case in cases:
        for suffix, query in (("a", case.query_clean_a), ("b", case.query_clean_b)):
            rows.append(
                {
                    "query_id": f"hel-{case.case_id}-{suffix}",
                    "case_id": case.case_id,
                    "query": query,
                    "split": "heldout",
                }
            )
    return rows


class Dur056OpenAIEmbeddingProvider:
    """Bounded embedding client with an explicit pre-/post-Gate-A split scope."""

    def __init__(
        self,
        ledger: Dur056SpendLedger,
        *,
        split_scope: Literal["development", "heldout"],
        heldout_authorized: bool = False,
        timeout_seconds: float = 90,
    ) -> None:
        if split_scope == "heldout" and not heldout_authorized:
            raise Dur056RetrievalError("held-out embedding scope requires accepted Gate A receipt")
        if not os.environ.get("OPENAI_API_KEY"):
            raise Dur056RetrievalError("OPENAI_API_KEY is required for DUR-056 embeddings")
        self.ledger = ledger
        self.split_scope = split_scope
        self.timeout_seconds = timeout_seconds
        self.records: list[dict[str, Any]] = []

    def embed(
        self,
        inputs: Sequence[str],
        ids: Sequence[str],
        *,
        split: Literal["development", "heldout"],
        operation: str,
    ) -> list[list[float]]:
        if split != self.split_scope:
            raise Dur056RetrievalError(
                f"{self.split_scope} embedding provider refused {split} content"
            )
        if not inputs or len(inputs) != len(ids) or len(inputs) > EMBEDDING_BATCH_SIZE:
            raise Dur056RetrievalError("DUR-056 embedding batch is empty, misaligned, or too large")
        if any(not value.strip() for value in inputs):
            raise Dur056RetrievalError("DUR-056 embedding text cannot be empty")
        payload = {
            "model": EMBEDDING_MODEL,
            "input": list(inputs),
            "encoding_format": "float",
            "dimensions": EMBEDDING_DIMENSION,
        }
        body = canonical_json(payload).encode("utf-8")
        call_id = self.ledger.reserve(
            model=EMBEDDING_MODEL,
            operation=operation,
            request_bytes=len(body),
        )
        started = time.perf_counter()
        try:
            import urllib.error
            import urllib.request

            request = urllib.request.Request(
                "https://api.openai.com/v1/embeddings",
                data=body,
                headers={
                    "Authorization": f"Bearer {os.environ['OPENAI_API_KEY']}",
                    "Content-Type": "application/json",
                },
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                result = json.loads(response.read().decode("utf-8"))
        except Exception as error:
            uncertain = not isinstance(error, urllib.error.HTTPError)
            self.ledger.fail(call_id, outcome_uncertain=uncertain, detail=str(error))
            if isinstance(error, urllib.error.HTTPError):
                detail = error.read().decode("utf-8", errors="replace")[:400]
                raise Dur056RetrievalError(f"embedding API HTTP {error.code}: {detail}") from error
            raise Dur056RetrievalError(f"embedding API failed: {error}") from error
        latency_ms = (time.perf_counter() - started) * 1000
        try:
            response_rows = sorted(result["data"], key=lambda row: row["index"])
            vectors = [cast(list[float], row["embedding"]) for row in response_rows]
            if len(vectors) != len(inputs) or any(
                len(vector) != EMBEDDING_DIMENSION for vector in vectors
            ):
                raise ValueError("embedding vector count or dimension mismatch")
            tokens = int(result["usage"]["prompt_tokens"])
            cost = self.ledger.settle(
                call_id,
                input_tokens=tokens,
                response_id=str(result.get("id")) if result.get("id") else None,
                latency_ms=latency_ms,
            )
        except (KeyError, TypeError, ValueError, Dur056SpendError) as error:
            self.ledger.fail(call_id, outcome_uncertain=True, detail=f"bad response: {error}")
            raise Dur056RetrievalError(f"embedding response was invalid: {error}") from error
        self.records.append(
            {
                "operation": operation,
                "split": split,
                "ids": list(ids),
                "response_id": result.get("id"),
                "input_tokens": tokens,
                "latency_ms": round(latency_ms, 3),
                "cost_usd": f"{cost:.8f}",
            }
        )
        return vectors


def _vector_text(vector: Sequence[float]) -> str:
    if len(vector) != EMBEDDING_DIMENSION:
        raise Dur056RetrievalError("DUR-056 vector has the wrong dimension")
    return "[" + ",".join(format(float(value), ".9g") for value in vector) + "]"


def _existing_ids(
    connection: psycopg.Connection[Any],
    table: Literal["evidence_chunks", "query_embeddings"],
    split: str,
) -> set[str]:
    column = "chunk_id" if table == "evidence_chunks" else "query_id"
    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT {column} FROM dur056.{table} WHERE study_version=%s AND split=%s "
            + ("AND embedding IS NOT NULL" if table == "evidence_chunks" else ""),
            (STUDY_VERSION, split),
        )
        return {str(row[column]) for row in cursor.fetchall()}


def embed_missing(
    connection: psycopg.Connection[Any],
    provider: Dur056OpenAIEmbeddingProvider,
    chunks: Sequence[EvidenceChunk],
    queries: Sequence[dict[str, str]],
    *,
    split: Literal["development", "heldout"],
) -> None:
    if provider.split_scope != split:
        raise Dur056RetrievalError("embedding phase and requested split do not match")
    if any(row["split"] != split for row in queries):
        raise Dur056RetrievalError("DUR-056 query embedding input contains another split")
    chunk_done = _existing_ids(connection, "evidence_chunks", split)
    chunk_inputs = [chunk for chunk in chunks if chunk.chunk_id not in chunk_done]
    for offset in range(0, len(chunk_inputs), EMBEDDING_BATCH_SIZE):
        batch = chunk_inputs[offset : offset + EMBEDDING_BATCH_SIZE]
        vectors = provider.embed(
            [chunk.text for chunk in batch],
            [chunk.chunk_id for chunk in batch],
            split=split,
            operation=f"dur056-embed-{split}-corpus-{offset // EMBEDDING_BATCH_SIZE + 1}",
        )
        try:
            with connection.transaction(), connection.cursor() as cursor:
                for chunk, vector in zip(batch, vectors, strict=True):
                    cursor.execute(
                        """UPDATE dur056.evidence_chunks
                            SET embedding_model=%s, embedding_dimension=%s, embedding=%s::vector
                            WHERE study_version=%s AND split=%s AND chunk_id=%s
                              AND embedding IS NULL""",
                        (
                            EMBEDDING_MODEL,
                            EMBEDDING_DIMENSION,
                            _vector_text(vector),
                            STUDY_VERSION,
                            split,
                            chunk.chunk_id,
                        ),
                    )
                    if cursor.rowcount != 1:
                        raise Dur056RetrievalError(
                            f"refusing to overwrite embedding {chunk.chunk_id}"
                        )
        except psycopg.Error as error:
            raise Dur056RetrievalError(f"cannot persist DUR-056 corpus vectors: {error}") from error

    query_done = _existing_ids(connection, "query_embeddings", split)
    query_inputs = [row for row in queries if row["query_id"] not in query_done]
    for offset in range(0, len(query_inputs), EMBEDDING_BATCH_SIZE):
        query_batch = query_inputs[offset : offset + EMBEDDING_BATCH_SIZE]
        vectors = provider.embed(
            [row["query"] for row in query_batch],
            [row["query_id"] for row in query_batch],
            split=split,
            operation=f"dur056-embed-{split}-queries-{offset // EMBEDDING_BATCH_SIZE + 1}",
        )
        try:
            with connection.transaction(), connection.cursor() as cursor:
                for row, vector in zip(query_batch, vectors, strict=True):
                    cursor.execute(
                        """INSERT INTO dur056.query_embeddings
                            (study_version, split, query_id, embedding_model,
                             embedding_dimension, embedding)
                            VALUES (%s, %s, %s, %s, %s, %s::vector)
                            ON CONFLICT (study_version, split, query_id) DO NOTHING""",
                        (
                            STUDY_VERSION,
                            split,
                            row["query_id"],
                            EMBEDDING_MODEL,
                            EMBEDDING_DIMENSION,
                            _vector_text(vector),
                        ),
                    )
                    if cursor.rowcount != 1:
                        raise Dur056RetrievalError(
                            f"refusing to overwrite query vector {row['query_id']}"
                        )
        except psycopg.Error as error:
            raise Dur056RetrievalError(f"cannot persist DUR-056 query vectors: {error}") from error


def query_vector_map(
    connection: psycopg.Connection[Any], rows: Sequence[dict[str, str]]
) -> dict[str, tuple[str, str]]:
    result: dict[str, tuple[str, str]] = {}
    try:
        with connection.cursor() as cursor:
            for row in rows:
                split = str(row["split"])
                query_id = str(row["query_id"])
                cursor.execute(
                    "SELECT embedding::text AS vector FROM dur056.query_embeddings "
                    "WHERE study_version=%s AND split=%s AND query_id=%s",
                    (STUDY_VERSION, split, query_id),
                )
                found = cursor.fetchone()
                if found is None:
                    raise Dur056RetrievalError(f"DUR-056 query vector is missing: {query_id}")
                vector = str(found["vector"])
                if row["query"] in result and result[row["query"]] != (query_id, vector):
                    raise Dur056RetrievalError("duplicate DUR-056 query text has different IDs")
                result[str(row["query"])] = (query_id, vector)
    except psycopg.Error as error:
        raise Dur056RetrievalError(f"cannot read DUR-056 query vectors: {error}") from error
    return result


def _keyword_hits(
    connection: psycopg.Connection[Any],
    query: str,
    splits: Sequence[str],
    top_k: int,
) -> list[RetrievalHit]:
    tsquery = _disjunctive_tsquery(query)
    if not tsquery:
        return []
    with connection.cursor() as cursor:
        cursor.execute(
            """WITH q AS (SELECT to_tsquery('simple', %s) AS tsq)
            SELECT e.chunk_id, e.document_id, e.version, e.service,
                   ts_rank(e.search_vector, q.tsq) AS score,
                   left(e.content, 1200) AS snippet
            FROM dur056.evidence_chunks e CROSS JOIN q
            WHERE e.study_version=%s AND e.search_vector @@ q.tsq
              AND e.split = ANY(%s)
            ORDER BY score DESC, e.chunk_id
            LIMIT %s""",
            (tsquery, STUDY_VERSION, list(splits), top_k),
        )
        rows = cursor.fetchall()
    return [
        RetrievalHit(
            chunk_id=str(row["chunk_id"]),
            document_id=str(row["document_id"]),
            version=str(row["version"]),
            service=str(row["service"]),
            score=float(row["score"]),
            snippet=str(row["snippet"]),
        )
        for row in rows
    ]


def _disjunctive_tsquery(query: str) -> str:
    """Return sanitized OR terms for PostgreSQL simple-dictionary tsquery."""

    terms = list(dict.fromkeys(re.findall(r"[a-z0-9_]+", query.lower())))
    return " | ".join(terms)


def _dense_hits(
    connection: psycopg.Connection[Any],
    query_id: str,
    query_vector: str,
    query_split: str,
    top_k: int,
    ef_search: int,
    corpus_splits: Sequence[str],
) -> list[RetrievalHit]:
    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute("SELECT set_config('hnsw.ef_search', %s, true)", (str(ef_search),))
        cursor.execute(
            """SELECT e.chunk_id, e.document_id, e.version, e.service,
                          1 - (e.embedding <=> %s::vector) AS score,
                          left(e.content, 1200) AS snippet
                   FROM dur056.evidence_chunks e
                   WHERE e.study_version=%s AND e.embedding IS NOT NULL
                     AND e.split = ANY(%s)
                   ORDER BY e.embedding <=> %s::vector, e.chunk_id
                   LIMIT %s""",
            (query_vector, STUDY_VERSION, list(corpus_splits), query_vector, top_k),
        )
        rows = cursor.fetchall()
    if not query_id:
        raise Dur056RetrievalError("dense retrieval received an empty query vector ID")
    if query_split not in {"development", "heldout"}:
        raise Dur056RetrievalError("dense retrieval received an invalid query split")
    return [
        RetrievalHit(
            chunk_id=str(row["chunk_id"]),
            document_id=str(row["document_id"]),
            version=str(row["version"]),
            service=str(row["service"]),
            score=float(row["score"]),
            snippet=str(row["snippet"]),
        )
        for row in rows
    ]


def reciprocal_rank_fusion(
    keyword: Sequence[RetrievalHit], dense: Sequence[RetrievalHit], rrf_k: int, top_k: int
) -> tuple[RetrievalHit, ...]:
    scores: dict[str, float] = defaultdict(float)
    by_id: dict[str, RetrievalHit] = {}
    for result in (keyword, dense):
        for rank, hit in enumerate(result, start=1):
            scores[hit.chunk_id] += 1 / (rrf_k + rank)
            by_id[hit.chunk_id] = hit
    ranked = sorted(scores, key=lambda chunk_id: (-scores[chunk_id], chunk_id))[:top_k]
    return tuple(
        RetrievalHit(
            chunk_id=by_id[chunk_id].chunk_id,
            document_id=by_id[chunk_id].document_id,
            version=by_id[chunk_id].version,
            service=by_id[chunk_id].service,
            score=scores[chunk_id],
            snippet=by_id[chunk_id].snippet,
        )
        for chunk_id in ranked
    )


class PostgresRetrievalAdapter:
    """Bounded MCP search adapter backed only by the DUR-056 isolated schema."""

    def __init__(
        self,
        connection: psycopg.Connection[Any],
        config: dict[str, Any],
        query_vectors: dict[str, tuple[str, str]],
        query_splits: dict[str, str],
        allowed_query_splits: Sequence[str] = ("development",),
        corpus_splits: Sequence[str] = ("development",),
    ) -> None:
        self.connection = connection
        self.config = config
        self.query_vectors = query_vectors
        self.query_splits = query_splits
        self.allowed_query_splits = tuple(allowed_query_splits)
        self.corpus_splits = tuple(corpus_splits)

    def ranks(
        self, query: str
    ) -> tuple[list[RetrievalHit], list[RetrievalHit], tuple[RetrievalHit, ...]]:
        if query not in self.query_vectors:
            raise Dur056RetrievalError("query text has no frozen DUR-056 embedding")
        query_id, vector = self.query_vectors[query]
        split = self.query_splits[query]
        if split not in self.allowed_query_splits:
            raise Dur056RetrievalError(f"retrieval refused query split {split}")
        if not set(self.corpus_splits).issubset({"development", "heldout"}):
            raise Dur056RetrievalError("retrieval corpus split allowlist is invalid")
        top_k = int(self.config["ranking"]["top_k"])
        ef_search = int(self.config["dense"]["hnsw_ef_search"])
        keyword = _keyword_hits(self.connection, query, self.corpus_splits, top_k)
        dense = _dense_hits(
            self.connection,
            query_id,
            vector,
            split,
            top_k,
            ef_search,
            self.corpus_splits,
        )
        hybrid = reciprocal_rank_fusion(
            keyword,
            dense,
            int(self.config["hybrid"]["rrf_k"]),
            top_k,
        )
        return keyword, dense, hybrid

    def search(self, query: str, arm: RetrievalArm = "hybrid") -> RetrievalResponse:
        if arm not in {"keyword", "dense", "hybrid"}:
            raise ValueError("DUR-056 retrieval arm must be keyword, dense, or hybrid")
        keyword, dense, hybrid = self.ranks(query)
        keyword_ok = bool(keyword) and keyword[0].score >= float(
            self.config["keyword"]["threshold"]
        )
        dense_ok = bool(dense) and dense[0].score >= float(self.config["dense"]["threshold"])
        selected: Sequence[RetrievalHit]
        if arm == "keyword":
            selected, sufficient, reason = (
                (keyword, True, "keyword_threshold_pass")
                if keyword_ok
                else ([], False, "keyword_threshold_failed")
            )
        elif arm == "dense":
            selected, sufficient, reason = (
                (dense, True, "dense_threshold_pass")
                if dense_ok
                else ([], False, "dense_threshold_failed")
            )
        elif keyword_ok or dense_ok:
            selected, sufficient, reason = hybrid, bool(hybrid), "hybrid_constituent_pass"
        else:
            selected, sufficient, reason = [], False, "hybrid_constituents_failed"
        return RetrievalResponse(
            arm=arm,
            query=query,
            pre_gate_keyword=tuple(keyword),
            pre_gate_dense=tuple(dense),
            pre_gate_hybrid=hybrid,
            delivered=tuple(selected),
            sufficient=sufficient,
            reason=reason,
        )


class NoRetrievalAdapter:
    """Return empty evidence while the same model receives logs and metrics."""

    def search(self, query: str, arm: RetrievalArm = "hybrid") -> RetrievalResponse:
        return RetrievalResponse(arm, query, (), (), (), (), False, "no_retrieval_arm")


def _balanced_accuracy(labels: Sequence[bool], predicted: Sequence[bool]) -> float:
    positives = [prediction for label, prediction in zip(labels, predicted, strict=True) if label]
    negatives = [
        prediction for label, prediction in zip(labels, predicted, strict=True) if not label
    ]
    if not positives or not negatives:
        raise Dur056RetrievalError("development retrieval labels must include both classes")
    sensitivity = sum(positives) / len(positives)
    specificity = 1 - sum(negatives) / len(negatives)
    return (sensitivity + specificity) / 2


def _choose_threshold(scores: Sequence[float], labels: Sequence[bool]) -> dict[str, float]:
    best: tuple[float, float, float, float] | None = None
    chosen = 0.0
    for threshold in sorted({0.0, 1.0, *scores, *DEV_THRESHOLD_GRID}):
        predicted = [score >= threshold for score in scores]
        accuracy = _balanced_accuracy(labels, predicted)
        false_positive = sum(
            prediction for label, prediction in zip(labels, predicted, strict=True) if not label
        )
        true_positive = sum(
            prediction for label, prediction in zip(labels, predicted, strict=True) if label
        )
        key = (accuracy, -float(false_positive), float(true_positive), -threshold)
        if best is None or key > best:
            best = key
            chosen = threshold
    assert best is not None
    return {
        "threshold": chosen,
        "balanced_accuracy": best[0],
        "false_positive_count": -best[1],
    }


def tune_development(
    connection: psycopg.Connection[Any],
    cases: Sequence[Dur056Case],
    query_vectors: dict[str, tuple[str, str]],
    query_splits: dict[str, str],
) -> tuple[dict[str, Any], dict[str, Any]]:
    if len(cases) != 30 or any(case.split != "development" for case in cases):
        raise Dur056RetrievalError("DUR-056 retrieval tuning accepts only the 30 dev cases")
    query_cases = [
        (case, query) for case in cases for query in (case.query_clean_a, case.query_clean_b)
    ]
    labels = [case.expected_action is not None for case, _query in query_cases]
    best_config: dict[str, Any] | None = None
    best_results: dict[str, Any] | None = None
    best_key: tuple[float, float, float, int, int, int] | None = None
    candidate_grid: list[dict[str, Any]] = []
    for top_k in TOP_K_CANDIDATES:
        for ef_search in EF_SEARCH_CANDIDATES:
            raw: list[dict[str, Any]] = []
            for case, query in query_cases:
                query_id, vector = query_vectors[query]
                keyword = _keyword_hits(connection, query, ("development",), top_k)
                dense = _dense_hits(
                    connection,
                    query_id,
                    vector,
                    "development",
                    top_k,
                    ef_search,
                    ("development",),
                )
                raw.append(
                    {
                        "case": case,
                        "query": query,
                        "keyword": keyword,
                        "dense": dense,
                    }
                )
            keyword_scores = [rows["keyword"][0].score if rows["keyword"] else 0.0 for rows in raw]
            dense_scores = [rows["dense"][0].score if rows["dense"] else 0.0 for rows in raw]
            keyword_threshold = _choose_threshold(keyword_scores, labels)
            dense_threshold = _choose_threshold(dense_scores, labels)
            for rrf_k in RRF_K_CANDIDATES:
                per_arm: dict[str, list[bool]] = {"keyword": [], "dense": [], "hybrid": []}
                per_arm_recall: dict[str, list[float]] = {"keyword": [], "dense": [], "hybrid": []}
                rrf_rows: list[list[RetrievalHit]] = []
                for rows in raw:
                    hybrid = list(
                        reciprocal_rank_fusion(rows["keyword"], rows["dense"], rrf_k, top_k)
                    )
                    rrf_rows.append(hybrid)
                for index, rows in enumerate(raw):
                    keyword_ok = keyword_scores[index] >= keyword_threshold["threshold"]
                    dense_ok = dense_scores[index] >= dense_threshold["threshold"]
                    selected = {
                        "keyword": rows["keyword"] if keyword_ok else [],
                        "dense": rows["dense"] if dense_ok else [],
                        "hybrid": rrf_rows[index] if keyword_ok or dense_ok else [],
                    }
                    relevant = set(rows["case"].relevant_chunk_ids)
                    for arm in per_arm:
                        per_arm[arm].append(bool(selected[arm]))
                        if relevant:
                            per_arm_recall[arm].append(
                                len(relevant.intersection(hit.chunk_id for hit in selected[arm]))
                                / len(relevant)
                            )
                        else:
                            per_arm_recall[arm].append(0.0)
                accuracy = {arm: _balanced_accuracy(labels, per_arm[arm]) for arm in per_arm}
                false_positives = {
                    arm: sum(
                        prediction
                        for label, prediction in zip(labels, per_arm[arm], strict=True)
                        if not label
                    )
                    for arm in per_arm
                }
                mean_accuracy = statistics.mean(accuracy.values())
                total_false_positive = float(sum(false_positives.values()))
                mean_recall = statistics.mean(
                    statistics.mean(values) if values else 0.0 for values in per_arm_recall.values()
                )
                key = (
                    mean_accuracy,
                    -total_false_positive,
                    mean_recall,
                    -top_k,
                    -ef_search,
                    -abs(rrf_k - 60),
                )
                config = {
                    "schema": "dur056-retrieval-frozen-config.v2",
                    "study_version": STUDY_VERSION,
                    "embedding_model": EMBEDDING_MODEL,
                    "embedding_dimension": EMBEDDING_DIMENSION,
                    "embedding_normalization": "provider_default",
                    "index": {"type": "HNSW", "m": 16, "ef_construction": 64},
                    "ranking": {"top_k": top_k},
                    "keyword_query": {
                        "strategy": "sanitized_or_joined_to_tsquery_simple",
                        "rank": "ts_rank",
                        "token_pattern": "[a-z0-9_]+",
                    },
                    "dense": {
                        "hnsw_ef_search": ef_search,
                        **dense_threshold,
                    },
                    "keyword": keyword_threshold,
                    "hybrid": {"rrf_k": rrf_k},
                    "selection": {
                        "split": "development",
                        "objective": (
                            "mean balanced accuracy; then lower false positives, higher "
                            "delivered recall, lower top-k, lower ef_search, rrf_k nearest 60"
                        ),
                        "query_variants_per_case": ["clean-a", "clean-b"],
                        "query_count": len(query_cases),
                        "arm_balanced_accuracy": accuracy,
                        "arm_false_positive_count": false_positives,
                        "mean_delivered_recall": mean_recall,
                        "candidate_top_k": list(TOP_K_CANDIDATES),
                        "candidate_hnsw_ef_search": list(EF_SEARCH_CANDIDATES),
                        "candidate_rrf_k": list(RRF_K_CANDIDATES),
                    },
                }
                config["config_fingerprint"] = fingerprint(config)
                rows_payload = [
                    {
                        "case_id": rows["case"].case_id,
                        "query_variant": "clean-a"
                        if rows["query"] == rows["case"].query_clean_a
                        else "clean-b",
                        "query": rows["query"],
                        "answerable": labels[index],
                        "keyword": [hit.chunk_id for hit in rows["keyword"]],
                        "dense": [hit.chunk_id for hit in rows["dense"]],
                        "hybrid": [hit.chunk_id for hit in rrf_rows[index]],
                        "sufficient": {arm: per_arm[arm][index] for arm in per_arm},
                        "relevant_chunk_ids": list(rows["case"].relevant_chunk_ids),
                    }
                    for index, rows in enumerate(raw)
                ]
                results = {
                    "schema": "dur056-retrieval-development.v2",
                    "split": "development",
                    "case_count": len(cases),
                    "query_count": len(query_cases),
                    "frozen_candidate": config,
                    "rows": rows_payload,
                }
                candidate_grid.append(
                    {
                        "top_k": top_k,
                        "hnsw_ef_search": ef_search,
                        "rrf_k": rrf_k,
                        "keyword_threshold": keyword_threshold["threshold"],
                        "dense_threshold": dense_threshold["threshold"],
                        "balanced_accuracy_by_arm": accuracy,
                        "false_positive_count_by_arm": false_positives,
                        "mean_delivered_recall": mean_recall,
                        "selection_key": list(key),
                    }
                )
                if best_key is None or key > best_key:
                    best_key, best_config, best_results = key, config, results
    if best_config is None or best_results is None:
        raise Dur056RetrievalError("DUR-056 development retrieval tuning produced no candidate")
    best_results["candidate_grid"] = candidate_grid
    return best_config, best_results


def database_manifest(connection: psycopg.Connection[Any]) -> dict[str, str]:
    with connection.cursor() as cursor:
        cursor.execute("SELECT version() AS version")
        postgres_row = cursor.fetchone()
        if postgres_row is None:
            raise Dur056RetrievalError("PostgreSQL did not return its version")
        postgres = str(postgres_row["version"])
        cursor.execute("SELECT extversion FROM pg_extension WHERE extname='vector'")
        pgvector_row = cursor.fetchone()
        if pgvector_row is None:
            raise Dur056RetrievalError("pgvector extension is unavailable")
        pgvector = str(pgvector_row["extversion"])
    return {"postgres_version": postgres, "pgvector_version": pgvector}
