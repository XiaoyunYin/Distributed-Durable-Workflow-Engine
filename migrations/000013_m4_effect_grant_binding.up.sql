BEGIN;

-- Preserve the approval identity on every cooperating-sink receipt. These
-- columns are intentionally nullable for pre-000013 unknown-effect evidence;
-- newly applied effects always populate both values.
ALTER TABLE effects.effect_records
    ADD COLUMN IF NOT EXISTS intent_id uuid,
    ADD COLUMN IF NOT EXISTS resource_id text;

CREATE INDEX IF NOT EXISTS effect_records_intent_lookup
    ON effects.effect_records (intent_id, workflow_id, logical_effect_key);

INSERT INTO engine.schema_migrations (version) VALUES (13)
ON CONFLICT (version) DO NOTHING;

COMMIT;
