BEGIN;

ALTER TABLE engine.workflow_definitions
    ADD COLUMN IF NOT EXISTS effect_classes jsonb NOT NULL DEFAULT '{}'::jsonb;

DO $$
DECLARE
    existing_constraint record;
BEGIN
    -- PostgreSQL truncates generated names at 63 bytes, so the name emitted
    -- by 000002 is not stable enough for a literal DROP CONSTRAINT.
    FOR existing_constraint IN
        SELECT c.conname
        FROM pg_constraint AS c
        WHERE c.conrelid = 'engine.attempt_result_evidence'::regclass
          AND c.contype = 'f'
          AND pg_get_constraintdef(c.oid) LIKE 'FOREIGN KEY (workflow_id, node_id, iteration, attempt_number)%'
          AND pg_get_constraintdef(c.oid) NOT LIKE '%ON DELETE CASCADE%'
    LOOP
        EXECUTE format(
            'ALTER TABLE engine.attempt_result_evidence DROP CONSTRAINT %I',
            existing_constraint.conname
        );
    END LOOP;
END $$;

ALTER TABLE engine.attempt_result_evidence
    ADD CONSTRAINT attempt_result_evidence_attempt_fk
    FOREIGN KEY (workflow_id, node_id, iteration, attempt_number)
    REFERENCES engine.activity_attempts (workflow_id, node_id, iteration, attempt_number)
    ON DELETE CASCADE;

INSERT INTO engine.schema_migrations (version) VALUES (3)
ON CONFLICT (version) DO NOTHING;

COMMIT;
