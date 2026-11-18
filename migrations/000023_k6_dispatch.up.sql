BEGIN;

ALTER TABLE engine.approval_action_intents ADD COLUMN k6_logical_key text;
CREATE UNIQUE INDEX approval_k6_logical_key ON engine.approval_action_intents (k6_logical_key)
    WHERE k6_logical_key IS NOT NULL;

ALTER TABLE engine.activity_attempts
    ADD COLUMN k6_fence bigint CHECK (k6_fence > 0),
    ADD COLUMN k6_proof jsonb;

INSERT INTO engine.schema_migrations (version) VALUES (23) ON CONFLICT DO NOTHING;
COMMIT;
