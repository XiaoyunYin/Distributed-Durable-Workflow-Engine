BEGIN;

ALTER TABLE engine.outbox
    ADD COLUMN IF NOT EXISTS topic text NOT NULL DEFAULT 'durable-agent.events.v1',
    ADD COLUMN IF NOT EXISTS schema_version integer NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    ADD COLUMN IF NOT EXISTS last_error text;

CREATE INDEX IF NOT EXISTS outbox_ready
    ON engine.outbox (next_attempt_at, created_at)
    WHERE publish_state IN ('PENDING', 'CLAIMED');

CREATE TABLE IF NOT EXISTS engine.outbox_publications (
    publication_id uuid PRIMARY KEY,
    event_id uuid NOT NULL,
    relay_owner uuid NOT NULL,
    relay_attempt integer NOT NULL CHECK (relay_attempt > 0),
    broker_acknowledged boolean NOT NULL,
    published_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    error text,
    FOREIGN KEY (event_id) REFERENCES engine.outbox (event_id) ON DELETE CASCADE,
    UNIQUE (event_id, relay_attempt)
);

CREATE TABLE IF NOT EXISTS engine.consumer_offsets (
    consumer_id text NOT NULL CHECK (consumer_id <> ''),
    topic text NOT NULL CHECK (topic <> ''),
    kafka_partition integer NOT NULL CHECK (kafka_partition >= 0),
    next_offset bigint NOT NULL CHECK (next_offset >= 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (consumer_id, topic, kafka_partition)
);

ALTER TABLE engine.event_inbox
    ADD COLUMN IF NOT EXISTS topic text,
    ADD COLUMN IF NOT EXISTS kafka_partition integer,
    ADD COLUMN IF NOT EXISTS kafka_offset bigint;

CREATE INDEX IF NOT EXISTS event_inbox_offsets
    ON engine.event_inbox (consumer_id, topic, kafka_partition, kafka_offset)
    WHERE topic IS NOT NULL AND kafka_partition IS NOT NULL AND kafka_offset IS NOT NULL;

CREATE TABLE IF NOT EXISTS engine.scheduler_wakeups (
    wakeup_id uuid PRIMARY KEY,
    workflow_id text NOT NULL,
    partition_id smallint NOT NULL CHECK (partition_id >= 0 AND partition_id < 16),
    event_id uuid NOT NULL,
    reason text NOT NULL CHECK (reason <> ''),
    state text NOT NULL DEFAULT 'PENDING' CHECK (state IN ('PENDING', 'CLAIMED', 'CONSUMED')),
    claim_owner uuid,
    claim_epoch bigint,
    claim_expires_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    consumed_at timestamptz,
    FOREIGN KEY (workflow_id) REFERENCES engine.workflow_executions (workflow_id) ON DELETE CASCADE,
    FOREIGN KEY (event_id) REFERENCES engine.outbox (event_id) ON DELETE CASCADE,
    UNIQUE (event_id)
);

CREATE INDEX IF NOT EXISTS scheduler_wakeups_pending
    ON engine.scheduler_wakeups (partition_id, created_at)
    WHERE state IN ('PENDING', 'CLAIMED');

CREATE TABLE IF NOT EXISTS engine.reconciliation_items (
    item_id uuid PRIMARY KEY,
    workflow_id text NOT NULL,
    partition_id smallint NOT NULL CHECK (partition_id >= 0 AND partition_id < 16),
    node_id text,
    iteration integer CHECK (iteration IS NULL OR iteration >= 0),
    attempt_number bigint CHECK (attempt_number IS NULL OR attempt_number > 0),
    kind text NOT NULL CHECK (kind IN ('EXPIRED_ATTEMPT', 'PENDING_OUTBOX', 'LOST_WAKEUP', 'DUE_TIMER', 'POISON_RECORD')),
    reference text NOT NULL CHECK (reference <> ''),
    status text NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'RESOLVED', 'ABANDONED')),
    detail jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    resolved_at timestamptz,
    FOREIGN KEY (workflow_id) REFERENCES engine.workflow_executions (workflow_id) ON DELETE CASCADE,
    UNIQUE (kind, reference)
);

CREATE INDEX IF NOT EXISTS reconciliation_items_open
    ON engine.reconciliation_items (partition_id, created_at)
    WHERE status = 'OPEN';

INSERT INTO engine.schema_migrations (version) VALUES (6)
ON CONFLICT (version) DO NOTHING;

COMMIT;
