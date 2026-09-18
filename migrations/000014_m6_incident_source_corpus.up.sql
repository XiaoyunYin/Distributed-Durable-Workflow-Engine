BEGIN;

-- M6 source-store boundary. Raw runbooks/postmortems are intentionally
-- separate from engine state even though development uses one PostgreSQL
-- server. pgvector is optional in the pinned local image; the JSON embedding
-- column is the deterministic fallback used by the Python fixture index.
CREATE SCHEMA IF NOT EXISTS source_corpus;

CREATE TABLE IF NOT EXISTS source_corpus.documents (
    document_id text NOT NULL,
    version text NOT NULL,
    source_type text NOT NULL CHECK (source_type IN ('runbook', 'postmortem')),
    service text NOT NULL,
    title text NOT NULL,
    source_text text NOT NULL,
    corpus_version text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (document_id, version)
);

CREATE TABLE IF NOT EXISTS source_corpus.chunks (
    chunk_id text PRIMARY KEY,
    document_id text NOT NULL,
    version text NOT NULL,
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    chunk_text text NOT NULL,
    search_vector tsvector GENERATED ALWAYS AS (to_tsvector('simple', chunk_text)) STORED,
    embedding_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    embedding_model text NOT NULL DEFAULT 'durable-hash-embed-v1',
    corpus_version text NOT NULL,
    FOREIGN KEY (document_id, version)
        REFERENCES source_corpus.documents (document_id, version)
        ON DELETE CASCADE,
    UNIQUE (document_id, version, ordinal)
);

CREATE INDEX IF NOT EXISTS source_corpus_chunks_fts
    ON source_corpus.chunks USING gin (search_vector);
CREATE INDEX IF NOT EXISTS source_corpus_chunks_document
    ON source_corpus.chunks (document_id, version, ordinal);

-- The pinned development image may not package pgvector.  When it is
-- available, keep the same corpus contract and add the native vector index;
-- otherwise embedding_json remains the explicit deterministic fallback.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'vector') THEN
        EXECUTE 'CREATE EXTENSION IF NOT EXISTS vector';
        EXECUTE 'ALTER TABLE source_corpus.chunks ADD COLUMN IF NOT EXISTS embedding vector(64)';
        EXECUTE 'CREATE INDEX IF NOT EXISTS source_corpus_chunks_vector ON source_corpus.chunks USING hnsw (embedding vector_cosine_ops)';
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS source_corpus.fixture_cases (
    case_id text PRIMARY KEY,
    split text NOT NULL CHECK (split IN ('development', 'heldout')),
    family text NOT NULL,
    service text NOT NULL,
    answerable boolean NOT NULL,
    document_dependent boolean NOT NULL,
    expected_diagnosis text NOT NULL,
    expected_action jsonb
);

INSERT INTO engine.schema_migrations (version) VALUES (14)
ON CONFLICT (version) DO NOTHING;

COMMIT;
