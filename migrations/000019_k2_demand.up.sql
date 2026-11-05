BEGIN;
CREATE INDEX IF NOT EXISTS activity_attempts_demand
    ON engine.activity_attempts (workflow_id, node_id, iteration, attempt_number)
    INCLUDE (state, created_at, heartbeat_deadline)
    WHERE is_current AND state IN ('DISPATCHABLE', 'CLAIMED');
CREATE INDEX IF NOT EXISTS workflow_executions_demand
    ON engine.workflow_executions (namespace, workflow_id)
    WHERE state = 'WAITING_ACTIVITY';
INSERT INTO engine.schema_migrations (version) VALUES (19)
ON CONFLICT (version) DO NOTHING;
COMMIT;
