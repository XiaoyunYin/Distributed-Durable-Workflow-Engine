BEGIN;

CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

INSERT INTO engine.schema_migrations (version) VALUES (18)
ON CONFLICT (version) DO NOTHING;

COMMIT;
