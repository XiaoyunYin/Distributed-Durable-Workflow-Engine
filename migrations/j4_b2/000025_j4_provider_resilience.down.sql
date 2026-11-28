BEGIN;
DROP TABLE engine.j4_provider_attempts;
DELETE FROM engine.schema_migrations WHERE version = 25;
COMMIT;
