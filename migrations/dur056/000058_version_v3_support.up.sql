BEGIN;

ALTER TABLE dur056.evidence_chunks
    DROP CONSTRAINT IF EXISTS evidence_chunks_study_version_check;
ALTER TABLE dur056.evidence_chunks
    ADD CONSTRAINT evidence_chunks_study_version_check
    CHECK (study_version IN (
        'dur056-solvable-evidence-v1',
        'dur056-solvable-evidence-v2',
        'dur056-solvable-evidence-v3'
    ));

ALTER TABLE dur056.query_embeddings
    DROP CONSTRAINT IF EXISTS query_embeddings_study_version_check;
ALTER TABLE dur056.query_embeddings
    ADD CONSTRAINT query_embeddings_study_version_check
    CHECK (study_version IN (
        'dur056-solvable-evidence-v1',
        'dur056-solvable-evidence-v2',
        'dur056-solvable-evidence-v3'
    ));

INSERT INTO dur056.schema_migrations (version) VALUES (58)
ON CONFLICT (version) DO NOTHING;

COMMIT;
