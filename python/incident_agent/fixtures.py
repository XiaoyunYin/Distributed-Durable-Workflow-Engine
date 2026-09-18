"""Deterministic incident and retrieval fixtures.

The generated catalog is intentionally boring and reproducible: 60 versioned
documents, five chunks each, five balanced incident families, and separate
development/held-out query sets.  It is a fixture generator, not a claim that
these synthetic cases represent production incidents.
"""

from __future__ import annotations

from dataclasses import asdict
from typing import Any, Literal

from incident_agent.models import EvidenceChunk, IncidentCase

FAMILIES = (
    "bad_configuration",
    "connection_pool",
    "downstream_latency",
    "disk_pressure",
    "insufficient_evidence",
)
SERVICES = {
    "bad_configuration": "checkout",
    "connection_pool": "payments",
    "downstream_latency": "catalog",
    "disk_pressure": "search",
    "insufficient_evidence": "notifications",
}
TERMS = {
    "bad_configuration": ("revision", "deployment", "config", "rollback", "flag"),
    "connection_pool": ("pool", "connection", "timeout", "wait", "database"),
    "downstream_latency": ("latency", "upstream", "deadline", "errors", "dependency"),
    "disk_pressure": ("disk", "pressure", "inode", "retention", "volume"),
    "insufficient_evidence": ("transient", "sample", "unknown", "observe", "restraint"),
}
DIAGNOSES = {
    "bad_configuration": "The checkout deployment is using a bad configuration revision.",
    "connection_pool": "The payments service is exhausting its database connection pool.",
    "downstream_latency": "The catalog dependency is exceeding the request deadline.",
    "disk_pressure": "The search volume is under disk pressure and needs retention relief.",
    "insufficient_evidence": "Evidence is insufficient to justify a remediation action.",
}


def build_corpus() -> tuple[EvidenceChunk, ...]:
    """Build 60 documents and exactly 300 stable chunks."""

    chunks: list[EvidenceChunk] = []
    for family_index, family in enumerate(FAMILIES):
        service = SERVICES[family]
        terms = TERMS[family]
        for document_index in range(12):
            document_id = f"doc-{family_index + 1:02d}-{document_index + 1:02d}"
            version = f"v{(document_index % 3) + 1}"
            source_type: Literal["runbook", "postmortem"] = (
                "runbook" if document_index % 2 == 0 else "postmortem"
            )
            for chunk_index in range(5):
                chunk_id = f"{document_id}-chunk-{chunk_index + 1:02d}"
                lead = terms[chunk_index]
                distractor = TERMS[FAMILIES[(family_index + 1) % len(FAMILIES)]][chunk_index]
                text = (
                    f"{service} {version} {source_type} section {chunk_index + 1}: "
                    f"{lead} guidance for {family.replace('_', ' ')}. "
                    f"Check {terms[(chunk_index + 1) % len(terms)]} before changing service state. "
                    f"The distractor term {distractor} must not be treated as proof."
                )
                chunks.append(
                    EvidenceChunk(
                        chunk_id=chunk_id,
                        document_id=document_id,
                        version=version,
                        service=service,
                        text=text,
                        source_type=source_type,
                        relevant_families=(family,),
                    )
                )
    return tuple(chunks)


def _case(case_id: str, family: str, split: str, index: int) -> IncidentCase:
    service = SERVICES[family]
    terms = TERMS[family]
    answerable = family != "insufficient_evidence"
    doc_dependent = answerable and index % 2 == 0
    document_id = f"doc-{FAMILIES.index(family) + 1:02d}-{(index % 12) + 1:02d}"
    relevant = tuple(f"{document_id}-chunk-{i:02d}" for i in (1, 2)) if doc_dependent else ()
    logs = (
        {
            "timestamp": f"2026-09-18T00:{index:02d}:00Z",
            "service": service,
            "level": "WARN" if answerable else "INFO",
            "message": f"{terms[0]} signal observed; inspect bounded evidence",
        },
        {
            "timestamp": f"2026-09-18T00:{index:02d}:05Z",
            "service": service,
            "level": "INFO",
            "message": "request sample retained for investigation",
        },
    )
    metrics = (
        {
            "timestamp": f"2026-09-18T00:{index:02d}:00Z",
            "service": service,
            "name": "incident_signal",
            "value": 1.0 if answerable else 0.2,
        },
    )
    expected_action: dict[str, Any] | None = None
    if family == "bad_configuration":
        expected_action = {"action": "rollback", "revision": "v1"}
    elif family == "connection_pool":
        expected_action = {"action": "restart_pool", "replicas": 1}
    elif family == "downstream_latency":
        expected_action = {"action": "enable_circuit_breaker", "service": service}
    elif family == "disk_pressure":
        expected_action = {"action": "compact_volume", "service": service}
    return IncidentCase(
        case_id=case_id,
        family=family,
        split=split,  # type: ignore[arg-type]
        service=service,
        query=f"Investigate {service}: {' '.join(terms[:3])} and recent symptoms",
        answerable=answerable,
        document_dependent=doc_dependent,
        relevant_chunk_ids=relevant,
        logs=logs,
        metrics=metrics,
        expected_diagnosis=DIAGNOSES[family],
        expected_action=expected_action,
    )


def build_incident_cases() -> tuple[IncidentCase, ...]:
    cases: list[IncidentCase] = []
    for family in FAMILIES:
        for index in range(6):
            split = "development" if index < 2 else "heldout"
            cases.append(_case(f"{split[:3]}-{family[:4]}-{index + 1:02d}", family, split, index))
    return tuple(cases)


def build_retrieval_queries() -> tuple[dict[str, Any], ...]:
    """Return 40 development and 120 held-out labeled queries.

    The query labels are evaluator-only and never flow through MCP responses.
    """

    queries: list[dict[str, Any]] = []
    family_chunks = {
        family: tuple(
            chunk.chunk_id for chunk in build_corpus() if family in chunk.relevant_families
        )
        for family in FAMILIES
    }
    for split, count in (("development", 40), ("heldout", 120)):
        for index in range(count):
            family = FAMILIES[index % len(FAMILIES)]
            no_answer = index % 4 == 0
            service = SERVICES[family]
            words = TERMS[family]
            query = (
                "unrelated mystery signal never-seen"
                if no_answer
                else f"{service} {words[index % len(words)]} {words[(index + 1) % len(words)]}"
            )
            relevant = () if no_answer else family_chunks[family]
            queries.append(
                {
                    "query_id": f"{split[:3]}-q-{index + 1:03d}",
                    "split": split,
                    "query": query,
                    "answerable": not no_answer,
                    "relevant_chunk_ids": relevant,
                    "near_duplicate_distractor": index % 3 == 0,
                }
            )
    return tuple(queries)


def corpus_manifest() -> dict[str, Any]:
    corpus = build_corpus()
    cases = build_incident_cases()
    queries = build_retrieval_queries()
    return {
        "schema": "incident-fixtures.v1",
        "families": list(FAMILIES),
        "documents": len({chunk.document_id for chunk in corpus}),
        "chunks": len(corpus),
        "incident_cases": len(cases),
        "development_cases": sum(case.split == "development" for case in cases),
        "heldout_cases": sum(case.split == "heldout" for case in cases),
        "development_queries": sum(query["split"] == "development" for query in queries),
        "heldout_queries": sum(query["split"] == "heldout" for query in queries),
        "no_answer_development_queries": sum(
            query["split"] == "development" and not query["answerable"] for query in queries
        ),
        "no_answer_heldout_queries": sum(
            query["split"] == "heldout" and not query["answerable"] for query in queries
        ),
    }


def as_jsonable_case(case: IncidentCase) -> dict[str, Any]:
    return asdict(case)
