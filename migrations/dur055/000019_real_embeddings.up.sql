BEGIN;

-- DUR-055 gets a separate versioned embedding table. The existing M6/DUR-029
-- 64-dimensional deterministic vectors remain unchanged in source_corpus.chunks.
CREATE EXTENSION IF NOT EXISTS vector;
CREATE SCHEMA IF NOT EXISTS dur055;

CREATE TABLE IF NOT EXISTS dur055.schema_migrations (
    version bigint PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS source_corpus.dur055_chunk_embeddings (
    study_version text NOT NULL CHECK (study_version = 'dur055-real-retrieval-v1'),
    chunk_id text NOT NULL REFERENCES source_corpus.chunks (chunk_id) ON DELETE CASCADE,
    embedding_model text NOT NULL CHECK (embedding_model = 'text-embedding-3-small'),
    embedding_dimension integer NOT NULL CHECK (embedding_dimension = 1536),
    embedding vector(1536) NOT NULL,
    PRIMARY KEY (study_version, chunk_id),
    CHECK (vector_dims(embedding) = embedding_dimension)
);

CREATE INDEX IF NOT EXISTS source_corpus_dur055_embeddings_hnsw
    ON source_corpus.dur055_chunk_embeddings
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

CREATE TABLE IF NOT EXISTS source_corpus.dur055_query_embeddings (
    study_version text NOT NULL CHECK (study_version = 'dur055-real-retrieval-v1'),
    query_id text NOT NULL,
    split text NOT NULL CHECK (split IN ('development', 'heldout')),
    embedding_model text NOT NULL CHECK (embedding_model = 'text-embedding-3-small'),
    embedding_dimension integer NOT NULL CHECK (embedding_dimension = 1536),
    embedding vector(1536) NOT NULL,
    PRIMARY KEY (study_version, query_id),
    CHECK (vector_dims(embedding) = embedding_dimension)
);

INSERT INTO dur055.schema_migrations (version) VALUES (19)
ON CONFLICT (version) DO NOTHING;

COMMIT;
