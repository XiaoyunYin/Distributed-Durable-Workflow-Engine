BEGIN;

DO $$
DECLARE
    existing_constraint record;
BEGIN
    FOR existing_constraint IN
        SELECT c.conname
        FROM pg_constraint AS c
        WHERE c.conrelid = 'engine.timers'::regclass
          AND c.contype = 'c'
          AND pg_get_constraintdef(c.oid) LIKE '%purpose%RETRY_BACKOFF%'
    LOOP
        EXECUTE format(
            'ALTER TABLE engine.timers DROP CONSTRAINT %I',
            existing_constraint.conname
        );
    END LOOP;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'engine.timers'::regclass
          AND conname = 'timers_purpose_check'
    ) THEN
        ALTER TABLE engine.timers
            ADD CONSTRAINT timers_purpose_check
            CHECK (purpose IN ('RETRY_BACKOFF', 'WORKFLOW_TIMER'));
    END IF;
END $$;

INSERT INTO engine.schema_migrations (version) VALUES (4)
ON CONFLICT (version) DO NOTHING;

COMMIT;
