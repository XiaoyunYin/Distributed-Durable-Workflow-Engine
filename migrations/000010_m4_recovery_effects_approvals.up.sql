BEGIN;

-- M4 retry policy is workflow/node data, not process-local configuration.
CREATE TABLE IF NOT EXISTS engine.retry_policies (
    workflow_id text NOT NULL,
    node_id text NOT NULL,
    iteration integer NOT NULL CHECK (iteration >= 0),
    max_retries integer NOT NULL CHECK (max_retries >= 0),
    retries_used integer NOT NULL DEFAULT 0 CHECK (retries_used >= 0),
    total_deadline_at timestamptz,
    checkpoint_schema_version integer NOT NULL DEFAULT 1 CHECK (checkpoint_schema_version > 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (workflow_id, node_id, iteration),
    FOREIGN KEY (workflow_id, node_id, iteration)
        REFERENCES engine.node_instances (workflow_id, node_id, iteration)
        ON DELETE CASCADE,
    CHECK (retries_used <= max_retries)
);

CREATE TABLE IF NOT EXISTS engine.effect_call_attempts (
    call_id uuid PRIMARY KEY,
    workflow_id text NOT NULL,
    logical_effect_key text NOT NULL,
    argument_hash text NOT NULL,
    attempt_number bigint NOT NULL CHECK (attempt_number > 0),
    request_id text NOT NULL,
    outcome text NOT NULL CHECK (outcome IN ('APPLIED', 'DUPLICATE', 'CONFLICT', 'REJECTED', 'UNKNOWN')),
    receipt jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (workflow_id) REFERENCES engine.workflow_executions (workflow_id)
        ON DELETE CASCADE,
    UNIQUE (workflow_id, request_id)
);

CREATE TABLE IF NOT EXISTS engine.effect_resource_fences (
    resource_id text PRIMARY KEY,
    next_token bigint NOT NULL CHECK (next_token >= 0),
    observed_token bigint NOT NULL CHECK (observed_token >= 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (observed_token <= next_token)
);

CREATE TABLE IF NOT EXISTS engine.sandbox_effect_state (
    resource_id text PRIMARY KEY,
    state jsonb NOT NULL,
    resource_revision bigint NOT NULL DEFAULT 0 CHECK (resource_revision >= 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

ALTER TABLE engine.approval_action_intents
    ADD COLUMN IF NOT EXISTS proposal_signature text,
    ADD COLUMN IF NOT EXISTS grant_scope_hash text,
    ADD COLUMN IF NOT EXISTS grant_token uuid,
    ADD COLUMN IF NOT EXISTS grant_expires_at timestamptz,
    ADD COLUMN IF NOT EXISTS decision_reason text;

CREATE UNIQUE INDEX IF NOT EXISTS approval_one_pending_intent
    ON engine.approval_action_intents (workflow_id, node_id, iteration)
    WHERE decision = 'PENDING' AND dispatch_status = 'NOT_GRANTED';

CREATE INDEX IF NOT EXISTS effect_call_attempts_lookup
    ON engine.effect_call_attempts (workflow_id, logical_effect_key, created_at);

INSERT INTO engine.schema_migrations (version) VALUES (10)
ON CONFLICT (version) DO NOTHING;

COMMIT;
