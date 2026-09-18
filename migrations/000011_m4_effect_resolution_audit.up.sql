BEGIN;

CREATE TABLE IF NOT EXISTS engine.effect_resolution_audit (
    resolution_id uuid PRIMARY KEY,
    workflow_id text NOT NULL,
    logical_effect_key text NOT NULL,
    argument_hash text NOT NULL,
    actor_id text NOT NULL CHECK (actor_id <> ''),
    disposition text NOT NULL CHECK (disposition IN ('CONFIRMED_APPLIED', 'ABANDONED_UNKNOWN')),
    receipt jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (workflow_id) REFERENCES engine.workflow_executions (workflow_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS effect_resolution_audit_lookup
    ON engine.effect_resolution_audit (workflow_id, logical_effect_key, created_at);

INSERT INTO engine.schema_migrations (version) VALUES (11)
ON CONFLICT (version) DO NOTHING;

COMMIT;
