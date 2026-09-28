BEGIN;

CREATE EXTENSION IF NOT EXISTS vector;
CREATE SCHEMA IF NOT EXISTS dur056;

CREATE TABLE IF NOT EXISTS dur056.schema_migrations (
    version bigint PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS dur056.evidence_chunks (
    study_version text NOT NULL CHECK (study_version = 'dur056-solvable-evidence-v1'),
    split text NOT NULL CHECK (split IN ('development', 'heldout')),
    chunk_id text NOT NULL,
    document_id text NOT NULL,
    service text NOT NULL,
    version text NOT NULL,
    source_type text NOT NULL CHECK (source_type IN ('runbook', 'postmortem')),
    content text NOT NULL,
    search_vector tsvector GENERATED ALWAYS AS
        (to_tsvector('simple'::regconfig, content)) STORED,
    embedding_model text,
    embedding_dimension integer,
    embedding vector(1536),
    PRIMARY KEY (study_version, split, chunk_id),
    CHECK (embedding IS NULL OR embedding_dimension = 1536),
    CHECK (embedding IS NULL OR embedding_model = 'text-embedding-3-small')
);

CREATE INDEX IF NOT EXISTS dur056_evidence_search_gin
    ON dur056.evidence_chunks USING gin (search_vector);
CREATE INDEX IF NOT EXISTS dur056_evidence_embedding_hnsw
    ON dur056.evidence_chunks USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64)
    WHERE embedding IS NOT NULL;

CREATE TABLE IF NOT EXISTS dur056.query_embeddings (
    study_version text NOT NULL CHECK (study_version = 'dur056-solvable-evidence-v1'),
    split text NOT NULL CHECK (split IN ('development', 'heldout')),
    query_id text NOT NULL,
    embedding_model text NOT NULL CHECK (embedding_model = 'text-embedding-3-small'),
    embedding_dimension integer NOT NULL CHECK (embedding_dimension = 1536),
    embedding vector(1536) NOT NULL,
    PRIMARY KEY (study_version, split, query_id),
    CHECK (vector_dims(embedding) = embedding_dimension)
);

INSERT INTO dur056.schema_migrations (version) VALUES (56)
ON CONFLICT (version) DO NOTHING;

COMMIT;
