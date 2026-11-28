BEGIN;
DROP TABLE engine.j12_workflow_requesters;
DELETE FROM engine.schema_migrations WHERE version = 26;
COMMIT;
