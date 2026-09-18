"""Deterministic keyword, dense, and hybrid runbook retrieval."""

from __future__ import annotations

import hashlib
import json
import math
import re
import time
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from typing import Any, cast

from incident_agent.models import EvidenceChunk, RetrievalArm, RetrievalHit, RetrievalResponse

TOKEN_RE = re.compile(r"[a-z0-9_]+")


@dataclass(frozen=True)
class RetrievalConfig:
    corpus_version: str = "m6-corpus-v1"
    embedding_model: str = "durable-hash-embed-v1"
    embedding_revision: str = "sha256:incident-agent-local-64d-v1"
    embedding_dimension: int = 64
    top_k: int = 5
    keyword_threshold: float = 0.75
    dense_threshold: float = 0.45
    rrf_k: int = 60

    def fingerprint(self) -> str:
        payload = json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))
        return "sha256:" + hashlib.sha256(payload.encode()).hexdigest()


def _tokens(value: str) -> tuple[str, ...]:
    return tuple(TOKEN_RE.findall(value.lower()))


def _vector(value: str, dimension: int) -> tuple[float, ...]:
    vector = [0.0] * dimension
    for token in _tokens(value):
        digest = hashlib.sha256(token.encode()).digest()
        index = int.from_bytes(digest[:4], "big") % dimension
        sign = 1.0 if digest[4] & 1 else -1.0
        vector[index] += sign
    norm = math.sqrt(sum(item * item for item in vector)) or 1.0
    return tuple(item / norm for item in vector)


def _cosine(left: tuple[float, ...], right: tuple[float, ...]) -> float:
    return sum(a * b for a, b in zip(left, right, strict=True))


class RetrievalIndex:
    def __init__(
        self, chunks: Iterable[EvidenceChunk], config: RetrievalConfig | None = None
    ) -> None:
        self.config = config or RetrievalConfig()
        self.chunks = tuple(chunks)
        self._vectors = tuple(
            _vector(chunk.text, self.config.embedding_dimension) for chunk in self.chunks
        )

    def _keyword_score(self, query_tokens: tuple[str, ...], chunk: EvidenceChunk) -> float:
        if not query_tokens:
            return 0.0
        chunk_tokens = set(_tokens(chunk.text))
        return sum(token in chunk_tokens for token in query_tokens) / len(set(query_tokens))

    def _hits(self, query: str, dense: bool) -> tuple[RetrievalHit, ...]:
        query_vector = _vector(query, self.config.embedding_dimension)
        query_tokens = _tokens(query)
        scored: list[tuple[float, EvidenceChunk]] = []
        for index, chunk in enumerate(self.chunks):
            score = (
                _cosine(query_vector, self._vectors[index])
                if dense
                else self._keyword_score(query_tokens, chunk)
            )
            scored.append((score, chunk))
        scored.sort(key=lambda item: (-item[0], item[1].chunk_id))
        return tuple(
            RetrievalHit(
                chunk_id=chunk.chunk_id,
                document_id=chunk.document_id,
                version=chunk.version,
                service=chunk.service,
                score=round(score, 8),
                snippet=chunk.text[:240],
            )
            for score, chunk in scored[: self.config.top_k]
        )

    def search(self, query: str, arm: RetrievalArm = "hybrid") -> RetrievalResponse:
        if arm not in {"keyword", "dense", "hybrid"}:
            raise ValueError("arm must be keyword, dense, or hybrid")
        started = time.perf_counter()
        keyword = self._hits(query, dense=False)
        dense = self._hits(query, dense=True)
        keyword_ok = bool(keyword) and keyword[0].score >= self.config.keyword_threshold
        dense_ok = bool(dense) and dense[0].score >= self.config.dense_threshold
        ranks: dict[str, float] = {}
        by_id = {hit.chunk_id: hit for hit in (*keyword, *dense)}
        for rank, hit in enumerate(keyword, 1):
            ranks[hit.chunk_id] = ranks.get(hit.chunk_id, 0.0) + 1.0 / (self.config.rrf_k + rank)
        for rank, hit in enumerate(dense, 1):
            ranks[hit.chunk_id] = ranks.get(hit.chunk_id, 0.0) + 1.0 / (self.config.rrf_k + rank)
        ordered = sorted(ranks.items(), key=lambda item: (-item[1], item[0]))[: self.config.top_k]
        hybrid = tuple(
            RetrievalHit(
                chunk_id=chunk_id,
                document_id=by_id[chunk_id].document_id,
                version=by_id[chunk_id].version,
                service=by_id[chunk_id].service,
                score=round(score, 8),
                snippet=by_id[chunk_id].snippet,
            )
            for chunk_id, score in ordered
        )
        if arm == "keyword":
            delivered = keyword if keyword_ok else ()
            reason = "keyword_threshold_pass" if keyword_ok else "keyword_threshold_failed"
        elif arm == "dense":
            delivered = dense if dense_ok else ()
            reason = "dense_threshold_pass" if dense_ok else "dense_threshold_failed"
        else:
            if not keyword_ok and not dense_ok:
                delivered = ()
                reason = "hybrid_constituent_thresholds_failed"
            else:
                delivered = hybrid
                reason = "hybrid_or_constituent_pass"
        _ = time.perf_counter() - started
        return RetrievalResponse(
            arm=arm,
            query=query,
            pre_gate_keyword=keyword,
            pre_gate_dense=dense,
            pre_gate_hybrid=hybrid,
            delivered=delivered,
            sufficient=bool(delivered),
            reason=reason,
        )

    def benchmark(
        self, queries: Iterable[dict[str, Any]], arm: RetrievalArm
    ) -> dict[str, float | int | str]:
        rows = list(queries)
        answerable = [row for row in rows if bool(row["answerable"])]
        no_answer = [row for row in rows if not bool(row["answerable"])]
        ranking_relevant = 0
        delivered_hits = 0
        reciprocal = 0.0
        false_positive = 0
        for row in rows:
            response = self.search(str(row["query"]), arm)
            relevant = set(cast(tuple[str, ...], row["relevant_chunk_ids"]))
            ranking = {
                "keyword": response.pre_gate_keyword,
                "dense": response.pre_gate_dense,
                "hybrid": response.pre_gate_hybrid,
            }[arm]
            ranking_ids = [hit.chunk_id for hit in ranking]
            delivered = [hit.chunk_id for hit in response.delivered]
            if relevant and any(item in relevant for item in ranking_ids):
                ranking_relevant += 1
                reciprocal += 1.0 / (
                    ranking_ids.index(next(item for item in ranking_ids if item in relevant)) + 1
                )
            if relevant and any(item in relevant for item in delivered):
                delivered_hits += 1
            if not relevant and response.sufficient:
                false_positive += 1
        answerable_count = len(answerable) or 1
        no_answer_count = len(no_answer) or 1
        return {
            "arm": arm,
            "queries": len(rows),
            "answerable_queries": len(answerable),
            "ranking_recall_at_k": ranking_relevant / answerable_count,
            "delivered_recall_at_k": delivered_hits / answerable_count,
            "ranking_mrr": reciprocal / answerable_count,
            "no_answer_false_positive_rate": false_positive / no_answer_count,
        }
