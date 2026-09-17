BEGIN;

ALTER TABLE engine.node_instances
    ADD COLUMN IF NOT EXISTS timer_fired boolean NOT NULL DEFAULT false;

INSERT INTO engine.schema_migrations (version) VALUES (5)
ON CONFLICT (version) DO NOTHING;

COMMIT;
