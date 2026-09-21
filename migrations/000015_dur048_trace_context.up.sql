BEGIN;

-- Correlation metadata only; it never participates in authority or workflow
-- transition decisions. It links the async outbox/Kafka path to submission.
ALTER TABLE engine.workflow_executions
    ADD COLUMN IF NOT EXISTS traceparent text NOT NULL DEFAULT '';

ALTER TABLE engine.outbox
    ADD COLUMN IF NOT EXISTS traceparent text NOT NULL DEFAULT '';

INSERT INTO engine.schema_migrations (version) VALUES (15)
ON CONFLICT (version) DO NOTHING;

COMMIT;
