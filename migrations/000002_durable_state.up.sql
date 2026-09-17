BEGIN;

CREATE TABLE IF NOT EXISTS engine.workflow_definitions (
    definition_id text NOT NULL,
    version integer NOT NULL CHECK (version > 0),
    definition_hash text NOT NULL,
    graph jsonb NOT NULL,
    activity_versions jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (definition_id, version)
);

CREATE TABLE IF NOT EXISTS engine.workflow_executions (
    workflow_id text PRIMARY KEY CHECK (workflow_id <> ''),
    namespace text NOT NULL DEFAULT 'default' CHECK (namespace <> ''),
    submission_key text NOT NULL CHECK (submission_key <> ''),
    submission_payload_hash text NOT NULL,
    definition_id text NOT NULL,
    definition_version integer NOT NULL CHECK (definition_version > 0),
    partition_id smallint NOT NULL CHECK (partition_id >= 0 AND partition_id < 16),
    state text NOT NULL DEFAULT 'RUNNABLE' CHECK (
        state IN (
            'RUNNABLE', 'WAITING_ACTIVITY', 'WAITING_TIMER',
            'WAITING_APPROVAL', 'PAUSED_UNSUPPORTED_VERSION',
            'RECONCILIATION_REQUIRED', 'SUCCEEDED', 'FAILED',
            'REJECTED', 'CANCELED', 'ABANDONED'
        )
    ),
    revision bigint NOT NULL DEFAULT 0 CHECK (revision >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (namespace, submission_key),
    FOREIGN KEY (definition_id, definition_version)
        REFERENCES engine.workflow_definitions (definition_id, version)
);

CREATE TABLE IF NOT EXISTS engine.node_instances (
    workflow_id text NOT NULL,
    node_id text NOT NULL CHECK (node_id <> ''),
    iteration integer NOT NULL DEFAULT 0 CHECK (iteration >= 0),
    state text NOT NULL CHECK (
        state IN (
            'RUNNABLE', 'WAITING_ACTIVITY', 'WAITING_TIMER',
            'WAITING_APPROVAL', 'PAUSED_UNSUPPORTED_VERSION',
            'RECONCILIATION_REQUIRED', 'SUCCEEDED', 'FAILED',
            'REJECTED', 'CANCELED', 'ABANDONED'
        )
    ),
    dependencies jsonb NOT NULL DEFAULT '[]'::jsonb,
    input jsonb NOT NULL DEFAULT '{}'::jsonb,
    accepted_result jsonb,
    current_attempt_number bigint,
    retry_count integer NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    deadline_at timestamptz,
    revision bigint NOT NULL DEFAULT 0 CHECK (revision >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (workflow_id, node_id, iteration),
    FOREIGN KEY (workflow_id) REFERENCES engine.workflow_executions (workflow_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS engine.activity_attempts (
    workflow_id text NOT NULL,
    node_id text NOT NULL,
    iteration integer NOT NULL CHECK (iteration >= 0),
    attempt_number bigint NOT NULL CHECK (attempt_number > 0),
    state text NOT NULL CHECK (
        state IN (
            'CREATED', 'DISPATCHABLE', 'CLAIMED', 'SUCCEEDED',
            'FAILED_RETRYABLE', 'FAILED_FINAL', 'TIMED_OUT',
            'REPLACED', 'OUTCOME_UNKNOWN', 'CANCELED'
        )
    ),
    effect_class text NOT NULL CHECK (
        effect_class IN ('PURE_ACTIVITY', 'COOPERATING_EFFECT', 'NON_COOPERATING_EFFECT')
    ),
    claim_token uuid,
    worker_id text,
    worker_request_id text,
    heartbeat_deadline timestamptz,
    logical_effect_key text,
    grant_scope_hash text,
    outcome_disposition text NOT NULL DEFAULT 'NONE' CHECK (
        outcome_disposition IN ('NONE', 'KNOWN_SUCCESS', 'OUTCOME_UNKNOWN')
    ),
    result jsonb,
    is_current boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (workflow_id, node_id, iteration, attempt_number),
    FOREIGN KEY (workflow_id, node_id, iteration)
        REFERENCES engine.node_instances (workflow_id, node_id, iteration)
        ON DELETE CASCADE,
    CHECK (
        effect_class <> 'COOPERATING_EFFECT' OR logical_effect_key IS NOT NULL
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS activity_attempts_one_current
    ON engine.activity_attempts (workflow_id, node_id, iteration)
    WHERE is_current;

CREATE UNIQUE INDEX IF NOT EXISTS activity_attempts_worker_request
    ON engine.activity_attempts (worker_request_id)
    WHERE worker_request_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS engine.partition_leases (
    partition_id smallint PRIMARY KEY CHECK (partition_id >= 0 AND partition_id < 16),
    owner_id uuid,
    epoch bigint NOT NULL DEFAULT 0 CHECK (epoch >= 0),
    lease_expires_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((owner_id IS NULL) = (lease_expires_at IS NULL))
);

INSERT INTO engine.partition_leases (partition_id)
SELECT partition_id::smallint
FROM generate_series(0, 15) AS partition_id
ON CONFLICT (partition_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS engine.timers (
    timer_id uuid PRIMARY KEY,
    workflow_id text NOT NULL,
    node_id text NOT NULL,
    iteration integer NOT NULL CHECK (iteration >= 0),
    due_at timestamptz NOT NULL,
    purpose text NOT NULL CHECK (purpose = 'RETRY_BACKOFF'),
    consumed_at timestamptz,
    owning_revision bigint NOT NULL CHECK (owning_revision >= 0),
    FOREIGN KEY (workflow_id, node_id, iteration)
        REFERENCES engine.node_instances (workflow_id, node_id, iteration)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS timers_due ON engine.timers (due_at)
    WHERE consumed_at IS NULL;

CREATE TABLE IF NOT EXISTS engine.checkpoints (
    workflow_id text NOT NULL,
    node_id text NOT NULL,
    iteration integer NOT NULL CHECK (iteration >= 0),
    sequence bigint NOT NULL CHECK (sequence >= 0),
    schema_version integer NOT NULL CHECK (schema_version > 0),
    source_attempt_number bigint NOT NULL CHECK (source_attempt_number > 0),
    payload jsonb NOT NULL,
    payload_hash text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (workflow_id, node_id, iteration, sequence),
    FOREIGN KEY (workflow_id, node_id, iteration)
        REFERENCES engine.node_instances (workflow_id, node_id, iteration)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS engine.transition_history (
    transition_id uuid PRIMARY KEY,
    workflow_id text NOT NULL,
    revision bigint NOT NULL CHECK (revision > 0),
    actor_kind text NOT NULL CHECK (actor_kind <> ''),
    actor_id text NOT NULL CHECK (actor_id <> ''),
    scheduler_epoch bigint CHECK (scheduler_epoch IS NULL OR scheduler_epoch >= 0),
    node_id text,
    iteration integer CHECK (iteration IS NULL OR iteration >= 0),
    attempt_number bigint CHECK (attempt_number IS NULL OR attempt_number > 0),
    old_state text,
    new_state text NOT NULL,
    reason text NOT NULL CHECK (reason <> ''),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (workflow_id, revision),
    FOREIGN KEY (workflow_id) REFERENCES engine.workflow_executions (workflow_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS engine.outbox (
    event_id uuid PRIMARY KEY,
    workflow_id text NOT NULL,
    aggregate_revision bigint NOT NULL CHECK (aggregate_revision >= 0),
    event_type text NOT NULL CHECK (event_type <> ''),
    payload jsonb NOT NULL,
    publish_state text NOT NULL DEFAULT 'PENDING' CHECK (
        publish_state IN ('PENDING', 'CLAIMED', 'PUBLISHED', 'QUARANTINED')
    ),
    relay_attempts integer NOT NULL DEFAULT 0 CHECK (relay_attempts >= 0),
    claim_owner uuid,
    claim_expires_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    published_at timestamptz,
    FOREIGN KEY (workflow_id) REFERENCES engine.workflow_executions (workflow_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS outbox_pending ON engine.outbox (created_at)
    WHERE publish_state IN ('PENDING', 'CLAIMED');

CREATE TABLE IF NOT EXISTS engine.event_inbox (
    consumer_id text NOT NULL CHECK (consumer_id <> ''),
    event_id uuid NOT NULL,
    disposition text NOT NULL CHECK (
        disposition IN ('ACCEPTED', 'ALREADY_HANDLED', 'STALE', 'QUARANTINED')
    ),
    reconciliation_reference text,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (consumer_id, event_id),
    FOREIGN KEY (event_id) REFERENCES engine.outbox (event_id)
        ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS engine.approval_action_intents (
    intent_id uuid PRIMARY KEY,
    workflow_id text NOT NULL,
    node_id text NOT NULL,
    iteration integer NOT NULL CHECK (iteration >= 0),
    proposal_hash text NOT NULL,
    target text NOT NULL,
    canonical_arguments jsonb NOT NULL,
    expected_resource_revision text,
    decision text NOT NULL DEFAULT 'PENDING' CHECK (
        decision IN ('PENDING', 'APPROVED', 'REJECTED')
    ),
    approver_id text,
    valid_until timestamptz NOT NULL,
    dispatch_status text NOT NULL DEFAULT 'NOT_GRANTED' CHECK (
        dispatch_status IN ('NOT_GRANTED', 'GRANTED', 'DISPATCHED', 'CANCELED')
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    decided_at timestamptz,
    FOREIGN KEY (workflow_id, node_id, iteration)
        REFERENCES engine.node_instances (workflow_id, node_id, iteration)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS engine.cancellation_requests (
    request_id uuid PRIMARY KEY,
    workflow_id text NOT NULL,
    client_key text NOT NULL,
    observed_revision bigint NOT NULL CHECK (observed_revision >= 0),
    status text NOT NULL DEFAULT 'PENDING' CHECK (
        status IN ('PENDING', 'APPLIED', 'BEST_EFFORT', 'REJECTED')
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    applied_at timestamptz,
    UNIQUE (workflow_id, client_key),
    FOREIGN KEY (workflow_id) REFERENCES engine.workflow_executions (workflow_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS engine.effect_records (
    workflow_id text NOT NULL,
    logical_effect_key text NOT NULL,
    argument_hash text NOT NULL,
    attempt_number bigint NOT NULL CHECK (attempt_number > 0),
    grant_scope_hash text,
    outcome text NOT NULL CHECK (outcome IN ('APPLIED', 'OUTCOME_UNKNOWN')),
    receipt jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (workflow_id, logical_effect_key),
    FOREIGN KEY (workflow_id) REFERENCES engine.workflow_executions (workflow_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS engine.attempt_result_evidence (
    evidence_id uuid PRIMARY KEY,
    workflow_id text NOT NULL,
    node_id text NOT NULL,
    iteration integer NOT NULL CHECK (iteration >= 0),
    attempt_number bigint NOT NULL CHECK (attempt_number > 0),
    claim_token uuid,
    reconciliation_reference text NOT NULL CHECK (reconciliation_reference <> ''),
    payload jsonb NOT NULL,
    disposition text NOT NULL CHECK (disposition = 'RECORDED_AS_EVIDENCE'),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (workflow_id, node_id, iteration, attempt_number)
        REFERENCES engine.activity_attempts (workflow_id, node_id, iteration, attempt_number)
        ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS attempt_result_evidence_reconciliation
    ON engine.attempt_result_evidence (workflow_id, reconciliation_reference);

INSERT INTO engine.schema_migrations (version) VALUES (2)
ON CONFLICT (version) DO NOTHING;

COMMIT;
