"""Portable source-store adapter shared by fixtures and the PostgreSQL contract.

The production migration uses PostgreSQL schemas and indexes.  M6's local
evidence runner uses the same table boundary through an attached SQLite
database so retrieval reads persisted source rows rather than bypassing the
declared source store.  Workflow state remains in a separate database.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from typing import Any

from incident_agent.models import EvidenceChunk, IncidentCase

SOURCE_SCHEMA = "source_corpus"
SOURCE_TABLES = ("documents", "chunks", "fixture_cases")
FULL_TEXT_COLUMN = "search_vector"
EMBEDDING_COLUMNS = ("embedding", "embedding_json")


def source_corpus_contract() -> dict[str, Any]:
    return {
        "schema": SOURCE_SCHEMA,
        "tables": list(SOURCE_TABLES),
        "full_text_column": FULL_TEXT_COLUMN,
        "embedding_columns": list(EMBEDDING_COLUMNS),
        "boundary": "raw source data is not workflow state",
    }


class SourceCorpusStore:
    """Persist and reload source fixtures behind the declared source boundary."""

    def __init__(self) -> None:
        self.connection = sqlite3.connect(":memory:")
        self.connection.execute("ATTACH DATABASE ':memory:' AS source_corpus")
        self.connection.executescript(
            """
            CREATE TABLE source_corpus.documents (
                document_id TEXT PRIMARY KEY,
                version TEXT NOT NULL,
                service TEXT NOT NULL,
                source_type TEXT NOT NULL
            );
            CREATE TABLE source_corpus.chunks (
                chunk_id TEXT PRIMARY KEY,
                document_id TEXT NOT NULL,
                chunk_index INTEGER NOT NULL,
                text TEXT NOT NULL,
                search_vector TEXT NOT NULL,
                embedding_json TEXT,
                near_duplicate_group TEXT,
                decisive_detail TEXT NOT NULL,
                relevant_families TEXT NOT NULL,
                canary INTEGER NOT NULL,
                FOREIGN KEY (document_id) REFERENCES documents(document_id)
            );
            CREATE TABLE source_corpus.fixture_cases (
                case_id TEXT PRIMARY KEY,
                split TEXT NOT NULL,
                payload TEXT NOT NULL
            );
            """
        )

    @classmethod
    def from_fixtures(
        cls, chunks: tuple[EvidenceChunk, ...], cases: tuple[IncidentCase, ...] = ()
    ) -> SourceCorpusStore:
        store = cls()
        documents = {
            chunk.document_id: (chunk.version, chunk.service, chunk.source_type) for chunk in chunks
        }
        store.connection.executemany(
            "INSERT INTO source_corpus.documents VALUES (?, ?, ?, ?)",
            [(document_id, *values) for document_id, values in documents.items()],
        )
        store.connection.executemany(
            "INSERT INTO source_corpus.chunks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    chunk.chunk_id,
                    chunk.document_id,
                    int(chunk.chunk_id.rsplit("-", 1)[-1]),
                    chunk.text,
                    chunk.text,
                    None,
                    chunk.near_duplicate_group,
                    chunk.decisive_detail,
                    json.dumps(chunk.relevant_families),
                    int(chunk.canary),
                )
                for chunk in chunks
            ],
        )
        store.connection.executemany(
            "INSERT INTO source_corpus.fixture_cases VALUES (?, ?, ?)",
            [
                (case.case_id, case.split, json.dumps(asdict(case), sort_keys=True))
                for case in cases
            ],
        )
        store.connection.commit()
        return store

    def chunks(self) -> tuple[EvidenceChunk, ...]:
        rows = self.connection.execute(
            "SELECT c.chunk_id, c.document_id, d.version, d.service, c.text, "
            "d.source_type, c.relevant_families, c.canary, c.near_duplicate_group, "
            "c.decisive_detail FROM source_corpus.chunks c "
            "JOIN source_corpus.documents d ON d.document_id = c.document_id "
            "ORDER BY c.chunk_id"
        ).fetchall()
        return tuple(
            EvidenceChunk(
                chunk_id=str(row[0]),
                document_id=str(row[1]),
                version=str(row[2]),
                service=str(row[3]),
                text=str(row[4]),
                source_type=str(row[5]),  # type: ignore[arg-type]
                relevant_families=tuple(json.loads(row[6])),
                canary=bool(row[7]),
                near_duplicate_group=None if row[8] is None else str(row[8]),
                decisive_detail=str(row[9]),
            )
            for row in rows
        )

    def counts(self) -> dict[str, int]:
        return {
            table: int(
                self.connection.execute(f"SELECT COUNT(*) FROM source_corpus.{table}").fetchone()[0]
            )
            for table in SOURCE_TABLES
        }

    def close(self) -> None:
        self.connection.close()
