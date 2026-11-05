BEGIN;
DROP INDEX IF EXISTS engine.activity_attempts_demand;
DROP INDEX IF EXISTS engine.workflow_executions_demand;
DELETE FROM engine.schema_migrations WHERE version = 19;
COMMIT;
