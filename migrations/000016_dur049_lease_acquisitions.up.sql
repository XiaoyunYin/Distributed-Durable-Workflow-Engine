BEGIN;

-- Append-only ownership observations for bounded recovery campaigns. The
-- mutable partition_leases row answers current ownership; this ledger records
-- each successful AcquireLease commit so takeover timing can be reconstructed
-- after later scheduler passes have advanced the epoch.
CREATE TABLE IF NOT EXISTS engine.lease_acquisitions (
    acquisition_id uuid PRIMARY KEY,
    partition_id smallint NOT NULL CHECK (partition_id >= 0 AND partition_id < 16),
    owner_id uuid NOT NULL,
    epoch bigint NOT NULL CHECK (epoch >= 0),
    lease_expires_at timestamptz NOT NULL,
    acquired_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS lease_acquisitions_partition_time
    ON engine.lease_acquisitions (partition_id, acquired_at, epoch);

INSERT INTO engine.schema_migrations (version) VALUES (16)
ON CONFLICT (version) DO NOTHING;

COMMIT;
