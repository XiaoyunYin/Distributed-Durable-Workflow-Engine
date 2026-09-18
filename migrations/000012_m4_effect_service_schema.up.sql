BEGIN;

-- The cooperating effect service owns this ledger separately from the engine
-- workflow tables.  It intentionally has no foreign keys into engine: an
-- effect may be committed by the service while the engine transaction is
-- unavailable, and the engine reconciles that evidence later.
CREATE SCHEMA IF NOT EXISTS effects;

CREATE TABLE IF NOT EXISTS effects.effect_records (
    workflow_id text NOT NULL,
    logical_effect_key text NOT NULL,
    argument_hash text NOT NULL,
    attempt_number bigint NOT NULL CHECK (attempt_number > 0),
    grant_scope_hash text,
    outcome text NOT NULL CHECK (outcome IN ('APPLIED', 'OUTCOME_UNKNOWN')),
    receipt jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (workflow_id, logical_effect_key)
);

CREATE TABLE IF NOT EXISTS effects.effect_call_attempts (
    call_id uuid PRIMARY KEY,
    workflow_id text NOT NULL,
    logical_effect_key text NOT NULL,
    argument_hash text NOT NULL,
    attempt_number bigint NOT NULL CHECK (attempt_number > 0),
    request_id text NOT NULL,
    outcome text NOT NULL CHECK (outcome IN ('APPLIED', 'DUPLICATE', 'CONFLICT', 'REJECTED', 'UNKNOWN')),
    receipt jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (workflow_id, request_id)
);

CREATE TABLE IF NOT EXISTS effects.effect_resource_fences (
    resource_id text PRIMARY KEY,
    next_token bigint NOT NULL CHECK (next_token >= 0),
    observed_token bigint NOT NULL CHECK (observed_token >= 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (observed_token <= next_token)
);

CREATE TABLE IF NOT EXISTS effects.sandbox_effect_state (
    resource_id text PRIMARY KEY,
    state jsonb NOT NULL,
    resource_revision bigint NOT NULL DEFAULT 0 CHECK (resource_revision >= 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS effects.effect_resolution_audit (
    resolution_id uuid PRIMARY KEY,
    workflow_id text NOT NULL,
    logical_effect_key text NOT NULL,
    argument_hash text NOT NULL,
    actor_id text NOT NULL CHECK (actor_id <> ''),
    disposition text NOT NULL CHECK (disposition IN ('CONFIRMED_APPLIED', 'ABANDONED_UNKNOWN')),
    receipt jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS effect_call_attempts_lookup
    ON effects.effect_call_attempts (workflow_id, logical_effect_key, created_at);

CREATE INDEX IF NOT EXISTS effect_resolution_audit_lookup
    ON effects.effect_resolution_audit (workflow_id, logical_effect_key, created_at);

INSERT INTO engine.schema_migrations (version) VALUES (12)
ON CONFLICT (version) DO NOTHING;

COMMIT;
