BEGIN;

CREATE TABLE IF NOT EXISTS engine.dur050_admission_slots (
    workflow_id text PRIMARY KEY,
    namespace text NOT NULL CHECK (namespace LIKE 'dur050-%'),
    admitted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (workflow_id) REFERENCES engine.workflow_executions (workflow_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS workflow_executions_namespace_workflow
    ON engine.workflow_executions (namespace, workflow_id);

CREATE INDEX IF NOT EXISTS outbox_dur050_pending_workflow
    ON engine.outbox (workflow_id)
    WHERE publish_state IN ('PENDING', 'CLAIMED');

CREATE INDEX IF NOT EXISTS dur050_admission_slots_active
    ON engine.dur050_admission_slots (namespace, admitted_at);

INSERT INTO engine.schema_migrations (version) VALUES (17)
ON CONFLICT (version) DO NOTHING;

COMMIT;
