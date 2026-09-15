BEGIN;

CREATE SCHEMA IF NOT EXISTS engine;
CREATE TABLE IF NOT EXISTS engine.schema_migrations (
    version bigint PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO engine.schema_migrations (version) VALUES (1) ON CONFLICT DO NOTHING;

COMMIT;
