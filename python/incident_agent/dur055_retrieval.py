"""PostgreSQL and pgvector retrieval for the development phase of DUR-055.

This module deliberately constructs only development retrieval queries. Held-out
query text and labels are not loaded by any pre-freeze code path.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import statistics
import time
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from urllib.parse import quote

import psycopg
from psycopg.rows import dict_row

from incident_agent.dur055_budget import SpendLedger
from incident_agent.fixtures import (
    FAMILIES,
    SERVICES,
    TERMS,
    _detail_token,
    _query_marker,
    _target_document_index,
    build_corpus,
)
from incident_agent.models import EvidenceChunk, RetrievalArm, RetrievalHit, RetrievalResponse

STUDY_VERSION = "dur055-real-retrieval-v1"
CORPUS_VERSION = "dur055-corpus-v1"
EMBEDDING_MODEL = "text-embedding-3-small"
EMBEDDING_DIMENSION = 1536
CORPUS_BATCH_SIZE = 48
QUERY_BATCH_SIZE = 40
CANDIDATE_TOP_K = (3, 5, 8)
CANDIDATE_EF_SEARCH = (40, 80)
CANDIDATE_RRF_K = (30, 60, 90)


class RetrievalStudyError(RuntimeError):
    """Raised when the DUR-055 isolated retrieval study cannot proceed safely."""


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def fingerprint(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def development_retrieval_queries() -> tuple[dict[str, Any], ...]:
    """Build exactly the 40 development labels without materializing held-out rows."""

    rows: list[dict[str, Any]] = []
    for index in range(40):
        family_index = index % len(FAMILIES)
        family = FAMILIES[family_index]
        no_answer = index % 4 == 0
        service = SERVICES[family]
        words = TERMS[family]
        document_index = _target_document_index(index)
        target_document = f"doc-{family_index + 1:02d}-{document_index + 1:02d}"
        primary_detail = _detail_token(family_index, document_index)
        query = (
            f"{service} {words[index % len(words)]} unseen-detail-dev-{index + 1:03d}"
            if no_answer
            else (
                f"{service} {words[index % len(words)]} {primary_detail} "
                f"{_query_marker('development', index)}"
            )
        )
        distractor_document = f"doc-{family_index + 1:02d}-{document_index + 2:02d}"
        rows.append(
            {
                "query_id": f"dev-q-{index + 1:03d}",
                "split": "development",
                "query": query,
                "answerable": not no_answer,
                "relevant_chunk_ids": (() if no_answer else (f"{target_document}-chunk-01",)),
                "distractor_chunk_ids": (() if no_answer else (f"{distractor_document}-chunk-01",)),
            }
        )
    if len(rows) != 40 or any(row["split"] != "development" for row in rows):
        raise RetrievalStudyError("DUR-055 development query construction failed")
    return tuple(rows)


def development_incident_search_queries() -> tuple[dict[str, str], ...]:
    """Return only case IDs and retrieval prompts for the ten dev incident cases."""

    from incident_agent.fixtures import _case

    rows: list[dict[str, str]] = []
    for family in FAMILIES:
        for index in range(2):
            case = _case(
                f"dev-{family[:4]}-{index + 1:02d}",
                family,
                "development",
                index,
            )
            rows.append(
                {
                    "case_id": case.case_id,
                    "query": case.query,
                    "fallback_query": f"{case.query} runbook",
                }
            )
    return tuple(rows)


def corpus_hash(chunks: Sequence[EvidenceChunk] | None = None) -> str:
    rows = chunks if chunks is not None else build_corpus()
    return fingerprint(
        [
            {
                "chunk_id": item.chunk_id,
                "document_id": item.document_id,
                "version": item.version,
                "service": item.service,
                "text": item.text,
                "source_type": item.source_type,
            }
            for item in rows
        ]
    )


def heldout_ids_fingerprint() -> str:
    """Fingerprint the known held-out identifier pattern without loading labels."""

    # build_retrieval_queries uses split[:3], so "heldout" IDs start with "hel".
    return fingerprint([f"hel-q-{index:03d}" for index in range(1, 121)])


def connect(database_url: str | None = None) -> psycopg.Connection[Any]:
    dsn = database_url or os.environ.get("DUR055_DATABASE_URL")
    if not dsn:
        user = os.environ.get("POSTGRES_USER")
        database = os.environ.get("POSTGRES_DB")
        password = os.environ.get("POSTGRES_PASSWORD")
        if user and database and password:
            port = os.environ.get("DUR055_POSTGRES_PORT", "55432")
            dsn = (
                f"postgresql://{quote(user, safe='')}:{quote(password, safe='')}"
                f"@127.0.0.1:{port}/{quote(database, safe='')}"
            )
    if not dsn:
        raise RetrievalStudyError("DUR055_DATABASE_URL is required")
    try:
        return psycopg.connect(dsn, row_factory=dict_row, autocommit=True)
    except psycopg.Error as error:
        raise RetrievalStudyError(
            f"cannot connect to isolated DUR-055 PostgreSQL: {error}"
        ) from error


def _required_row(cursor: Any) -> Mapping[str, Any]:
    row = cursor.fetchone()
    if row is None:
        raise RetrievalStudyError("PostgreSQL returned no row for a required DUR-055 query")
    return cast(Mapping[str, Any], row)


def apply_migrations(connection: psycopg.Connection[Any], root: Path) -> None:
    """Apply only the bootstrap/source-store/study migrations to the isolated DB."""

    migrations = (
        (1, root / "migrations/000001_bootstrap.up.sql"),
        (14, root / "migrations/000014_m6_incident_source_corpus.up.sql"),
        (19, root / "migrations/dur055/000019_real_embeddings.up.sql"),
    )
    for version, path in migrations:
        if not path.is_file():
            raise RetrievalStudyError(f"required migration is missing: {path}")
        if version == 1:
            with connection.cursor() as cursor:
                cursor.execute("SELECT to_regclass('engine.schema_migrations') AS relation")
                exists = _required_row(cursor)["relation"] is not None
            applied = False
            if exists:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT EXISTS (SELECT 1 FROM engine.schema_migrations WHERE version=1)"
                    )
                    applied = bool(_required_row(cursor)["exists"])
        elif version == 19:
            with connection.cursor() as cursor:
                cursor.execute("SELECT to_regclass('dur055.schema_migrations') AS relation")
                exists = _required_row(cursor)["relation"] is not None
            applied = False
            if exists:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT EXISTS (SELECT 1 FROM dur055.schema_migrations WHERE version=19)"
                    )
                    applied = bool(_required_row(cursor)["exists"])
        else:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT EXISTS (SELECT 1 FROM engine.schema_migrations WHERE version = %s)",
                    (version,),
                )
                applied = bool(_required_row(cursor)["exists"])
        if not applied:
            try:
                with connection.cursor() as cursor:
                    cursor.execute(path.read_text(encoding="utf-8"))
            except psycopg.Error as error:
                connection.rollback()
                raise RetrievalStudyError(f"migration {version} failed: {error}") from error
            connection.commit()
    with connection.cursor() as cursor:
        cursor.execute("SELECT extversion FROM pg_extension WHERE extname = 'vector'")
        row = cursor.fetchone()
    if row is None:
        raise RetrievalStudyError("the isolated PostgreSQL instance does not have pgvector")


def seed_corpus(connection: psycopg.Connection[Any]) -> tuple[EvidenceChunk, ...]:
    """Insert the fixed corpus without updating any existing source record."""

    chunks = build_corpus()
    by_document: dict[tuple[str, str], list[EvidenceChunk]] = defaultdict(list)
    for chunk in chunks:
        by_document[(chunk.document_id, chunk.version)].append(chunk)
    try:
        with connection.transaction():
            for (document_id, version), items in sorted(by_document.items()):
                first = items[0]
                source_text = "\n".join(item.text for item in items)
                connection.execute(
                    """INSERT INTO source_corpus.documents
                       (document_id,version,source_type,service,title,source_text,
                        corpus_version)
                       VALUES (%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (document_id,version) DO NOTHING""",
                    (
                        document_id,
                        version,
                        first.source_type,
                        first.service,
                        document_id,
                        source_text,
                        CORPUS_VERSION,
                    ),
                )
                for ordinal, item in enumerate(items):
                    connection.execute(
                        """INSERT INTO source_corpus.chunks
                           (chunk_id,document_id,version,ordinal,chunk_text,corpus_version)
                           VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (chunk_id) DO NOTHING""",
                        (
                            item.chunk_id,
                            item.document_id,
                            item.version,
                            ordinal,
                            item.text,
                            CORPUS_VERSION,
                        ),
                    )
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT chunk_id,document_id,version,chunk_text FROM source_corpus.chunks "
                "ORDER BY chunk_id"
            )
            persisted = cursor.fetchall()
        expected = {item.chunk_id: item for item in chunks}
        actual = {str(row["chunk_id"]): row for row in persisted}
        if set(actual) != set(expected):
            raise RetrievalStudyError(
                f"source corpus IDs differ: expected {len(expected)}, found {len(actual)}"
            )
        for chunk_id, item in expected.items():
            row = actual[chunk_id]
            if (
                row["document_id"] != item.document_id
                or row["version"] != item.version
                or row["chunk_text"] != item.text
            ):
                raise RetrievalStudyError(f"persisted source content changed for {chunk_id}")
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) AS n FROM source_corpus.documents")
            document_count = int(_required_row(cursor)["n"])
        if document_count != 60:
            raise RetrievalStudyError(f"expected 60 source documents, found {document_count}")
        return chunks
    except psycopg.Error as error:
        raise RetrievalStudyError(f"cannot seed DUR-055 source corpus: {error}") from error


class OpenAIEmbeddingProvider:
    """Embeddings API client that reserves and settles each call in the spend ledger."""

    def __init__(self, ledger: SpendLedger, timeout_seconds: float = 90) -> None:
        self.ledger = ledger
        self.timeout_seconds = timeout_seconds
        self.records: list[dict[str, Any]] = []

    def embed(self, inputs: Sequence[str], ids: Sequence[str], operation: str) -> list[list[float]]:
        if len(inputs) != len(ids) or not inputs:
            raise RetrievalStudyError(
                "embedding inputs and identifiers must be non-empty and aligned"
            )
        if len(inputs) > 64:
            raise RetrievalStudyError("embedding batch exceeds the frozen 64-item request bound")
        if any(not value for value in inputs):
            raise RetrievalStudyError("embedding input cannot be empty")
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

            api_key = os.environ.get("OPENAI_API_KEY")
            if not api_key:
                self.ledger.fail(call_id, outcome_uncertain=False, detail="OPENAI_API_KEY missing")
                raise RetrievalStudyError("OPENAI_API_KEY is required for embedding requests")
            request = urllib.request.Request(
                "https://api.openai.com/v1/embeddings",
                data=body,
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                result = json.loads(response.read().decode("utf-8"))
        except Exception as error:
            uncertain = not isinstance(error, (RetrievalStudyError, urllib.error.HTTPError))
            self.ledger.fail(call_id, outcome_uncertain=uncertain, detail=str(error))
            if isinstance(error, RetrievalStudyError):
                raise
            if isinstance(error, urllib.error.HTTPError):
                raise RetrievalStudyError(
                    f"embeddings API HTTP {error.code}: "
                    f"{error.read().decode('utf-8', errors='replace')[:500]}"
                ) from error
            raise RetrievalStudyError(f"embeddings API failed: {error}") from error
        latency_ms = (time.perf_counter() - started) * 1000
        try:
            data = result["data"]
            vectors = [
                cast(list[float], row["embedding"])
                for row in sorted(data, key=lambda r: r["index"])
            ]
            if len(vectors) != len(inputs) or any(
                len(vector) != EMBEDDING_DIMENSION for vector in vectors
            ):
                raise ValueError("response vector count or dimension mismatch")
            usage = result.get("usage") or {}
            tokens = int(usage["prompt_tokens"])
            cost = self.ledger.settle(
                call_id,
                input_tokens=tokens,
                response_id=str(result.get("id")) if result.get("id") else None,
                latency_ms=latency_ms,
            )
        except (KeyError, TypeError, ValueError) as error:
            self.ledger.fail(call_id, outcome_uncertain=True, detail=f"bad response: {error}")
            raise RetrievalStudyError(f"embeddings response was malformed: {error}") from error
        self.records.append(
            {
                "operation": operation,
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
        raise RetrievalStudyError(f"expected {EMBEDDING_DIMENSION}-dimensional embedding")
    return "[" + ",".join(format(float(value), ".9g") for value in vector) + "]"


def _existing_ids(connection: psycopg.Connection[Any], table: str) -> set[str]:
    if table not in {"dur055_chunk_embeddings", "dur055_query_embeddings"}:
        raise RetrievalStudyError("invalid embedding table")
    column = "chunk_id" if table.endswith("chunk_embeddings") else "query_id"
    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT {column} FROM source_corpus.{table} WHERE study_version = %s",
            (STUDY_VERSION,),
        )
        return {str(row[column]) for row in cursor.fetchall()}


def ensure_embeddings(
    connection: psycopg.Connection[Any],
    provider: OpenAIEmbeddingProvider,
    chunks: Sequence[EvidenceChunk],
    queries: Sequence[dict[str, Any]],
    agent_queries: Sequence[dict[str, str]],
) -> None:
    """Insert missing corpus and development-query vectors, never replacing rows."""

    known_chunks = _existing_ids(connection, "dur055_chunk_embeddings")
    missing_chunks = [item for item in chunks if item.chunk_id not in known_chunks]
    for offset in range(0, len(missing_chunks), CORPUS_BATCH_SIZE):
        chunk_batch = missing_chunks[offset : offset + CORPUS_BATCH_SIZE]
        vectors = provider.embed(
            [item.text for item in chunk_batch],
            [item.chunk_id for item in chunk_batch],
            "dur055-corpus-embedding",
        )
        with connection.transaction():
            for item, vector in zip(chunk_batch, vectors, strict=True):
                connection.execute(
                    """INSERT INTO source_corpus.dur055_chunk_embeddings
                       (study_version,chunk_id,embedding_model,embedding_dimension,embedding)
                       VALUES (%s,%s,%s,%s,%s::vector) ON CONFLICT DO NOTHING""",
                    (
                        STUDY_VERSION,
                        item.chunk_id,
                        EMBEDDING_MODEL,
                        EMBEDDING_DIMENSION,
                        _vector_text(vector),
                    ),
                )

    all_dev_embeddings: list[tuple[str, str, str]] = [
        (str(row["query_id"]), str(row["query"]), "development") for row in queries
    ]
    all_dev_embeddings.extend(
        (f"agent-{row['case_id']}", row["query"], "development") for row in agent_queries
    )
    all_dev_embeddings.extend(
        (f"agent-{row['case_id']}-fallback", row["fallback_query"], "development")
        for row in agent_queries
    )
    known_queries = _existing_ids(connection, "dur055_query_embeddings")
    missing_queries = [row for row in all_dev_embeddings if row[0] not in known_queries]
    for offset in range(0, len(missing_queries), QUERY_BATCH_SIZE):
        query_batch = missing_queries[offset : offset + QUERY_BATCH_SIZE]
        vectors = provider.embed(
            [row[1] for row in query_batch],
            [row[0] for row in query_batch],
            "dur055-development-query-embedding",
        )
        with connection.transaction():
            for (query_id, _query, split), vector in zip(query_batch, vectors, strict=True):
                connection.execute(
                    """INSERT INTO source_corpus.dur055_query_embeddings
                       (study_version,query_id,split,embedding_model,embedding_dimension,embedding)
                       VALUES (%s,%s,%s,%s,%s,%s::vector) ON CONFLICT DO NOTHING""",
                    (
                        STUDY_VERSION,
                        query_id,
                        split,
                        EMBEDDING_MODEL,
                        EMBEDDING_DIMENSION,
                        _vector_text(vector),
                    ),
                )
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT count(*) AS n FROM source_corpus.dur055_chunk_embeddings "
            "WHERE study_version=%s",
            (STUDY_VERSION,),
        )
        if int(_required_row(cursor)["n"]) != len(chunks):
            raise RetrievalStudyError("DUR-055 corpus embedding count does not match the corpus")
        cursor.execute(
            "SELECT count(*) AS n FROM source_corpus.dur055_query_embeddings "
            "WHERE study_version=%s AND split='development'",
            (STUDY_VERSION,),
        )
        expected = len(queries) + 2 * len(agent_queries)
        if int(_required_row(cursor)["n"]) != expected:
            raise RetrievalStudyError("DUR-055 development query embedding count is inconsistent")


@dataclass(frozen=True)
class RankedHit:
    chunk_id: str
    document_id: str
    version: str
    service: str
    score: float
    snippet: str


def _keyword_hits(
    connection: psycopg.Connection[Any], query: str, top_k: int
) -> tuple[RankedHit, ...]:
    with connection.cursor() as cursor:
        cursor.execute(
            """SELECT c.chunk_id,c.document_id,c.version,d.service,
                      left(c.chunk_text,240) AS snippet,
                      ts_rank(c.search_vector, plainto_tsquery('simple', %s)) AS score
               FROM source_corpus.chunks AS c
               JOIN source_corpus.documents AS d USING (document_id,version)
               WHERE c.search_vector @@ plainto_tsquery('simple', %s)
               ORDER BY score DESC,c.chunk_id ASC LIMIT %s""",
            (query, query, top_k),
        )
        rows = cursor.fetchall()
    return tuple(
        RankedHit(
            str(row["chunk_id"]),
            str(row["document_id"]),
            str(row["version"]),
            str(row["service"]),
            float(row["score"]),
            str(row["snippet"]),
        )
        for row in rows
    )


def _dense_hits(
    connection: psycopg.Connection[Any], query_id: str, vector: str, top_k: int, ef_search: int
) -> tuple[RankedHit, ...]:
    if ef_search not in CANDIDATE_EF_SEARCH:
        raise RetrievalStudyError("hnsw.ef_search must be in the frozen candidate set")
    connection.execute(f"SET LOCAL hnsw.ef_search = {ef_search}")
    with connection.cursor() as cursor:
        cursor.execute(
            """SELECT c.chunk_id,c.document_id,c.version,d.service,
                      left(c.chunk_text,240) AS snippet,
                      1 - (e.embedding <=> %s::vector) AS score
               FROM source_corpus.dur055_chunk_embeddings AS e
               JOIN source_corpus.chunks AS c USING (chunk_id)
               JOIN source_corpus.documents AS d USING (document_id,version)
               WHERE e.study_version=%s AND e.chunk_id IS NOT NULL
               ORDER BY e.embedding <=> %s::vector,c.chunk_id ASC LIMIT %s""",
            (vector, STUDY_VERSION, vector, top_k),
        )
        rows = cursor.fetchall()
    if not rows:
        raise RetrievalStudyError(f"no dense results for development query {query_id}")
    return tuple(
        RankedHit(
            str(row["chunk_id"]),
            str(row["document_id"]),
            str(row["version"]),
            str(row["service"]),
            float(row["score"]),
            str(row["snippet"]),
        )
        for row in rows
    )


def _load_query_vector(connection: psycopg.Connection[Any], query_id: str) -> str:
    with connection.cursor() as cursor:
        cursor.execute(
            """SELECT embedding::text AS embedding FROM source_corpus.dur055_query_embeddings
               WHERE study_version=%s AND query_id=%s AND split='development'""",
            (STUDY_VERSION, query_id),
        )
        row = cursor.fetchone()
    if row is None:
        raise RetrievalStudyError(f"missing development embedding: {query_id}")
    return str(row["embedding"])


def _rrf(
    keyword: Sequence[RankedHit], dense: Sequence[RankedHit], rrf_k: int, top_k: int
) -> tuple[RankedHit, ...]:
    ranks: dict[str, float] = defaultdict(float)
    lookup: dict[str, RankedHit] = {}
    for ranking in (keyword, dense):
        for rank, hit in enumerate(ranking, 1):
            ranks[hit.chunk_id] += 1 / (rrf_k + rank)
            lookup[hit.chunk_id] = hit
    return tuple(lookup[key] for key in sorted(ranks, key=lambda key: (-ranks[key], key))[:top_k])


def _balanced_accuracy(labels: Sequence[bool], predictions: Sequence[bool]) -> float:
    positives = [predictions[i] for i, label in enumerate(labels) if label]
    negatives = [predictions[i] for i, label in enumerate(labels) if not label]
    if not positives or not negatives:
        raise RetrievalStudyError("development selection requires both answerable classes")
    return (
        sum(positives) / len(positives) + sum(not item for item in negatives) / len(negatives)
    ) / 2


def _fpr(labels: Sequence[bool], predictions: Sequence[bool]) -> float:
    negatives = [predictions[i] for i, label in enumerate(labels) if not label]
    return sum(negatives) / len(negatives) if negatives else 0.0


def _recall(
    rows: Sequence[dict[str, Any]],
    rankings: Sequence[Sequence[RankedHit]],
    predictions: Sequence[bool],
) -> float:
    values = []
    for row, ranking, delivered in zip(rows, rankings, predictions, strict=True):
        relevant = set(row["relevant_chunk_ids"])
        if row["answerable"]:
            hits = {hit.chunk_id for hit in ranking} & relevant if delivered else set()
            values.append(len(hits) / len(relevant))
    return statistics.mean(values) if values else 0.0


def _threshold_candidates(scores: Sequence[float]) -> tuple[float, ...]:
    distinct = sorted(set(scores))
    if not distinct:
        return (math.inf,)
    return tuple(distinct) + (math.nextafter(distinct[-1], math.inf),)


def _per_query_candidates(
    connection: psycopg.Connection[Any], queries: Sequence[dict[str, Any]]
) -> dict[tuple[int, int, int], list[dict[str, Any]]]:
    candidates: dict[tuple[int, int, int], list[dict[str, Any]]] = {}
    for top_k in CANDIDATE_TOP_K:
        for ef_search in CANDIDATE_EF_SEARCH:
            for rrf_k in CANDIDATE_RRF_K:
                trial: list[dict[str, Any]] = []
                for row in queries:
                    if row.get("split") != "development":
                        raise RetrievalStudyError("held-out row entered development tuning")
                    query_id = str(row["query_id"])
                    query_vector = _load_query_vector(connection, query_id)
                    with connection.transaction():
                        keyword = _keyword_hits(connection, str(row["query"]), top_k)
                        dense = _dense_hits(connection, query_id, query_vector, top_k, ef_search)
                    hybrid = _rrf(keyword, dense, rrf_k, top_k)
                    trial.append(
                        {
                            "query_id": query_id,
                            "answerable": bool(row["answerable"]),
                            "relevant_chunk_ids": list(row["relevant_chunk_ids"]),
                            "distractor_chunk_ids": list(row["distractor_chunk_ids"]),
                            "keyword": [asdict(hit) for hit in keyword],
                            "dense": [asdict(hit) for hit in dense],
                            "hybrid": [asdict(hit) for hit in hybrid],
                        }
                    )
                candidates[(top_k, ef_search, rrf_k)] = trial
    return candidates


def _select_thresholds(
    rows: Sequence[dict[str, Any]],
) -> tuple[float, float, dict[str, float], dict[str, float], dict[str, float]]:
    labels = [bool(row["answerable"]) for row in rows]
    keyword_rankings = [[RankedHit(**hit) for hit in row["keyword"]] for row in rows]
    dense_rankings = [[RankedHit(**hit) for hit in row["dense"]] for row in rows]
    hybrid_rankings = [[RankedHit(**hit) for hit in row["hybrid"]] for row in rows]
    keyword_scores = [hits[0].score if hits else 0.0 for hits in keyword_rankings]
    dense_scores = [hits[0].score if hits else 0.0 for hits in dense_rankings]
    best: (
        tuple[
            tuple[float, float, float],
            float,
            float,
            dict[str, float],
            dict[str, float],
            dict[str, float],
        ]
        | None
    ) = None
    for keyword_threshold in _threshold_candidates(keyword_scores):
        keyword_ok = [
            bool(keyword_rankings[index]) and score >= keyword_threshold
            for index, score in enumerate(keyword_scores)
        ]
        keyword_ba = _balanced_accuracy(labels, keyword_ok)
        keyword_fpr = _fpr(labels, keyword_ok)
        keyword_recall = _recall(rows, keyword_rankings, keyword_ok)
        for dense_threshold in _threshold_candidates(dense_scores):
            dense_ok = [score >= dense_threshold for score in dense_scores]
            hybrid_ok = [keyword_ok[i] or dense_ok[i] for i in range(len(rows))]
            dense_ba = _balanced_accuracy(labels, dense_ok)
            hybrid_ba = _balanced_accuracy(labels, hybrid_ok)
            balanced_mean = statistics.mean((keyword_ba, dense_ba, hybrid_ba))
            summed_fpr = sum(
                (
                    keyword_fpr,
                    _fpr(labels, dense_ok),
                    _fpr(labels, hybrid_ok),
                )
            )
            delivered_recall = statistics.mean(
                (
                    keyword_recall,
                    _recall(rows, dense_rankings, dense_ok),
                    _recall(rows, hybrid_rankings, hybrid_ok),
                )
            )
            key = (balanced_mean, -summed_fpr, delivered_recall)
            if best is None or key > best[0]:
                best = (
                    key,
                    keyword_threshold,
                    dense_threshold,
                    {"keyword": keyword_ba, "dense": dense_ba, "hybrid": hybrid_ba},
                    {
                        "keyword": keyword_fpr,
                        "dense": _fpr(labels, dense_ok),
                        "hybrid": _fpr(labels, hybrid_ok),
                    },
                    {
                        "keyword": keyword_recall,
                        "dense": _recall(rows, dense_rankings, dense_ok),
                        "hybrid": _recall(rows, hybrid_rankings, hybrid_ok),
                    },
                )
    assert best is not None
    return best[1], best[2], best[3], best[4], best[5]


def tune_development(
    connection: psycopg.Connection[Any], queries: Sequence[dict[str, Any]]
) -> tuple[dict[str, Any], dict[str, Any]]:
    if len(queries) != 40 or any(row.get("split") != "development" for row in queries):
        raise RetrievalStudyError("retrieval tuning requires exactly 40 development rows")
    trials = _per_query_candidates(connection, queries)
    scored: list[dict[str, Any]] = []
    for (top_k, ef_search, rrf_k), rows in trials.items():
        keyword_threshold, dense_threshold, accuracy, false_positive, recall = _select_thresholds(
            rows
        )
        scored.append(
            {
                "top_k": top_k,
                "ef_search": ef_search,
                "rrf_k": rrf_k,
                "keyword_threshold": keyword_threshold,
                "dense_threshold": dense_threshold,
                "balanced_accuracy": accuracy,
                "no_answer_false_positive_rate": false_positive,
                "delivered_recall_at_k": recall,
                "mean_balanced_accuracy": statistics.mean(accuracy.values()),
                "summed_false_positive_rate": sum(false_positive.values()),
                "mean_delivered_recall": statistics.mean(recall.values()),
            }
        )
    selected = max(
        scored,
        key=lambda trial: (
            trial["mean_balanced_accuracy"],
            -trial["summed_false_positive_rate"],
            trial["mean_delivered_recall"],
            -trial["top_k"],
            -trial["ef_search"],
            -abs(trial["rrf_k"] - 60),
        ),
    )
    raw_selected = trials[(selected["top_k"], selected["ef_search"], selected["rrf_k"])]
    agent_arm = max(
        ("hybrid", "dense", "keyword"),
        key=lambda arm: (
            selected["balanced_accuracy"][arm],
            -selected["no_answer_false_positive_rate"][arm],
            selected["delivered_recall_at_k"][arm],
        ),
    )
    query_fingerprint = fingerprint(
        [{"query_id": row["query_id"], "query": row["query"]} for row in queries]
    )
    content = {
        "schema": "dur055-retrieval-frozen-config.v1",
        "study_version": STUDY_VERSION,
        "corpus_version": CORPUS_VERSION,
        "corpus_fingerprint": corpus_hash(),
        "development_query_count": 40,
        "development_query_fingerprint": query_fingerprint,
        "heldout_query_count": 120,
        "heldout_query_ids_fingerprint": heldout_ids_fingerprint(),
        "heldout_scored": False,
        "keyword": {
            "engine": "PostgreSQL full-text search",
            "expression": "tsvector + plainto_tsquery('simple') + ts_rank",
            "index": "GIN source_corpus_chunks_fts",
            "threshold": selected["keyword_threshold"],
        },
        "dense": {
            "engine": "pgvector",
            "embedding_model": EMBEDDING_MODEL,
            "embedding_dimension": EMBEDDING_DIMENSION,
            "distance": "cosine",
            "threshold": selected["dense_threshold"],
            "index_type": "HNSW",
            "index": "source_corpus_dur055_embeddings_hnsw",
            "hnsw_m": 16,
            "hnsw_ef_construction": 64,
            "hnsw_ef_search": selected["ef_search"],
        },
        "hybrid": {
            "method": "reciprocal-rank fusion",
            "formula": "sum(1 / (rrf_k + rank))",
            "rrf_k": selected["rrf_k"],
            "sufficiency": "keyword_threshold_pass OR dense_threshold_pass",
        },
        "ranking": {"top_k": selected["top_k"], "tie_break": "chunk_id ascending"},
        "candidate_ranges": {
            "top_k": list(CANDIDATE_TOP_K),
            "hnsw_ef_search": list(CANDIDATE_EF_SEARCH),
            "rrf_k": list(CANDIDATE_RRF_K),
            "thresholds": (
                "all observed development top-score breakpoints and all-negative sentinel"
            ),
        },
        "selection_objective": [
            "maximize mean balanced accuracy across keyword, dense, and hybrid",
            "minimize summed no-answer false-positive rate",
            "maximize mean delivered Recall@K",
            "prefer lower top_k",
            "prefer lower hnsw.ef_search",
            "prefer rrf_k nearest to 60",
        ],
        "development_selection_metrics": {
            "balanced_accuracy": selected["balanced_accuracy"],
            "no_answer_false_positive_rate": selected["no_answer_false_positive_rate"],
            "delivered_recall_at_k": selected["delivered_recall_at_k"],
        },
        "agent_retrieval_arm": agent_arm,
        "agent_arm_selection_objective": [
            "highest development balanced accuracy",
            "lowest development no-answer false-positive rate",
            "highest development delivered Recall@K",
            "ties prefer hybrid, then dense, then keyword",
        ],
    }
    content["config_fingerprint"] = fingerprint(content)
    dev_results = {
        "schema": "dur055-retrieval-development.v1",
        "study_version": STUDY_VERSION,
        "config_fingerprint": content["config_fingerprint"],
        "query_count": 40,
        "candidate_trials": scored,
        "selected": selected,
        "rows": _render_rows(queries, raw_selected, selected),
        "heldout_scored": False,
    }
    return content, dev_results


def _render_rows(
    queries: Sequence[dict[str, Any]],
    raw_rows: Sequence[dict[str, Any]],
    selected: dict[str, Any],
) -> list[dict[str, Any]]:
    output = []
    for query, trial in zip(queries, raw_rows, strict=True):
        keyword = [RankedHit(**row) for row in trial["keyword"]]
        dense = [RankedHit(**row) for row in trial["dense"]]
        hybrid = [RankedHit(**row) for row in trial["hybrid"]]
        kw_ok = bool(keyword) and keyword[0].score >= selected["keyword_threshold"]
        dense_ok = bool(dense) and dense[0].score >= selected["dense_threshold"]
        relevant = set(query["relevant_chunk_ids"])
        distractors = set(query["distractor_chunk_ids"])
        arms: dict[str, Any] = {}
        for name, ranking, sufficient in (
            ("keyword", keyword, kw_ok),
            ("dense", dense, dense_ok),
            ("hybrid", hybrid, kw_ok or dense_ok),
        ):
            ids = [hit.chunk_id for hit in ranking]
            delivered = ids if sufficient else []
            relevant_ranks = [index for index, item in enumerate(ids, 1) if item in relevant]
            arms[name] = {
                "ranking": [{**asdict(hit), "score": round(hit.score, 10)} for hit in ranking],
                "delivered_chunk_ids": delivered,
                "sufficient": sufficient,
                "ranking_recall_at_k": (len(relevant & set(ids)) / len(relevant))
                if relevant
                else None,
                "delivered_recall_at_k": (len(relevant & set(delivered)) / len(relevant))
                if relevant
                else None,
                "reciprocal_rank": 1 / min(relevant_ranks) if relevant_ranks else 0.0,
                "distractor_hits": len(distractors & set(ids)),
            }
        output.append(
            {
                "query_id": query["query_id"],
                "split": "development",
                "answerable": query["answerable"],
                "relevant_chunk_ids": list(query["relevant_chunk_ids"]),
                "arms": arms,
            }
        )
    return output


class PostgresRetrievalAdapter:
    """Existing bounded-tool retrieval interface backed by DUR-055 PostgreSQL."""

    def __init__(
        self,
        connection: psycopg.Connection[Any],
        config: dict[str, Any],
        query_vectors: dict[str, tuple[str, str]],
    ) -> None:
        self.connection = connection
        self.config = config
        self.query_vectors = query_vectors

    def search(self, query: str, arm: RetrievalArm = "hybrid") -> RetrievalResponse:
        if arm not in {"keyword", "dense", "hybrid"}:
            raise ValueError("arm must be keyword, dense, or hybrid")
        top_k = int(self.config["ranking"]["top_k"])
        ef_search = int(self.config["dense"]["hnsw_ef_search"])
        rrf_k = int(self.config["hybrid"]["rrf_k"])
        query_embedding = self.query_vectors.get(query)
        if query_embedding is None:
            raise RetrievalStudyError("query has no frozen DUR-055 development embedding")
        query_id, query_vector = query_embedding
        with self.connection.transaction():
            keyword = _keyword_hits(self.connection, query, top_k)
            dense = _dense_hits(self.connection, query_id, query_vector, top_k, ef_search)
        hybrid = _rrf(keyword, dense, rrf_k, top_k)
        keyword_threshold = float(self.config["keyword"]["threshold"])
        dense_threshold = float(self.config["dense"]["threshold"])
        keyword_ok = bool(keyword) and keyword[0].score >= keyword_threshold
        dense_ok = bool(dense) and dense[0].score >= dense_threshold
        delivered_rank: Sequence[RankedHit]
        if arm == "keyword":
            delivered_rank = keyword if keyword_ok else ()
            reason = "keyword_threshold_pass" if keyword_ok else "keyword_threshold_failed"
        elif arm == "dense":
            delivered_rank = dense if dense_ok else ()
            reason = "dense_threshold_pass" if dense_ok else "dense_threshold_failed"
        elif keyword_ok or dense_ok:
            delivered_rank = hybrid
            reason = "hybrid_or_constituent_pass"
        else:
            delivered_rank = ()
            reason = "hybrid_constituent_thresholds_failed"

        def render(rows: Sequence[RankedHit]) -> tuple[RetrievalHit, ...]:
            return tuple(
                RetrievalHit(
                    row.chunk_id,
                    row.document_id,
                    row.version,
                    row.service,
                    round(row.score, 8),
                    row.snippet,
                )
                for row in rows
            )

        return RetrievalResponse(
            arm=arm,
            query=query,
            pre_gate_keyword=render(keyword),
            pre_gate_dense=render(dense),
            pre_gate_hybrid=render(hybrid),
            delivered=render(delivered_rank),
            sufficient=bool(delivered_rank),
            reason=reason,
        )


def query_vector_map(
    connection: psycopg.Connection[Any], case_queries: Sequence[dict[str, str]]
) -> dict[str, tuple[str, str]]:
    result = {}
    for row in case_queries:
        query_id = f"agent-{row['case_id']}"
        result[row["query"]] = (query_id, _load_query_vector(connection, query_id))
        fallback_id = f"{query_id}-fallback"
        result[row["fallback_query"]] = (
            fallback_id,
            _load_query_vector(connection, fallback_id),
        )
    return result


def database_manifest(connection: psycopg.Connection[Any]) -> dict[str, str]:
    with connection.cursor() as cursor:
        cursor.execute("SELECT version() AS v")
        version = str(_required_row(cursor)["v"])
        cursor.execute("SELECT extversion FROM pg_extension WHERE extname='vector'")
        pgvector = str(_required_row(cursor)["extversion"])
    return {"postgres_version": version, "pgvector_version": pgvector}


def write_once(directory: Path, stem: str, value: Any) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    path = directory / f"{stem}-{stamp}.json"
    if path.exists():
        raise RetrievalStudyError(f"refusing to overwrite versioned artifact: {path}")
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8", newline="\n"
    )
    return path


def retrieval_artifact_bundle(
    connection: psycopg.Connection[Any],
    provider: OpenAIEmbeddingProvider,
    config: dict[str, Any],
    results: dict[str, Any],
    directory: Path,
) -> tuple[Path, Path, Path]:
    db = database_manifest(connection)
    config["database"] = db
    core = {key: value for key, value in config.items() if key != "config_fingerprint"}
    config["config_fingerprint"] = fingerprint(core)
    results["config_fingerprint"] = config["config_fingerprint"]
    config_path = write_once(directory, "retrieval-frozen-config", config)
    results_path = write_once(directory, "retrieval-development", results)
    manifest = {
        "schema": "dur055-study-manifest.v1",
        "study_version": STUDY_VERSION,
        "corpus_count": 300,
        "corpus_fingerprint": config["corpus_fingerprint"],
        "development_query_count": 40,
        "development_query_fingerprint": config["development_query_fingerprint"],
        "heldout_query_count": 120,
        "heldout_query_ids_fingerprint": config["heldout_query_ids_fingerprint"],
        "heldout_queries_read": False,
        "heldout_queries_embedded": False,
        "heldout_queries_scored": False,
        "embedding_model": EMBEDDING_MODEL,
        "embedding_dimension": EMBEDDING_DIMENSION,
        "embedding_requests": provider.records,
        "frozen_config_path": config_path.name,
        "development_results_path": results_path.name,
    }
    manifest_path = write_once(directory, "study-manifest", manifest)
    return config_path, results_path, manifest_path
