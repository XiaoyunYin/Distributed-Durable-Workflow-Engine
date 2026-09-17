BEGIN;

-- A malformed or unknown broker record has no trustworthy engine event
-- foreign key. Preserve it as a durable, offset-addressed poison record so
-- the consumer can acknowledge it without losing the evidence needed for
-- operator reconciliation.
CREATE TABLE IF NOT EXISTS engine.transport_quarantine (
    quarantine_id uuid PRIMARY KEY,
    consumer_id text NOT NULL CHECK (consumer_id <> ''),
    event_id text,
    topic text NOT NULL CHECK (topic <> ''),
    kafka_partition integer NOT NULL CHECK (kafka_partition >= 0),
    kafka_offset bigint NOT NULL CHECK (kafka_offset >= 0),
    event_type text,
    payload bytea NOT NULL,
    reason text NOT NULL CHECK (reason <> ''),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (consumer_id, topic, kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS transport_quarantine_open
    ON engine.transport_quarantine (consumer_id, topic, kafka_partition, kafka_offset);

INSERT INTO engine.schema_migrations (version) VALUES (8)
ON CONFLICT (version) DO NOTHING;

COMMIT;
