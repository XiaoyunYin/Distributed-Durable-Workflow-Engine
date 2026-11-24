BEGIN;

ALTER TABLE k6_sink.journal ADD COLUMN transaction_id text;
ALTER TABLE k6_sink.journal ALTER COLUMN transaction_id SET DEFAULT pg_current_xact_id()::text;

ALTER TABLE k6_sink.calls
    ADD COLUMN transaction_id text,
    ADD COLUMN committed_at timestamptz,
    ADD COLUMN grant_read_at timestamptz,
    ADD COLUMN grant_hash text,
    ADD COLUMN node_id text,
    ADD COLUMN iteration integer,
    ADD COLUMN scope_hash text,
    ADD COLUMN expected_revision text,
    ADD COLUMN arguments jsonb,
    ADD COLUMN fence bigint;
ALTER TABLE k6_sink.calls ALTER COLUMN transaction_id SET DEFAULT pg_current_xact_id()::text;

GRANT INSERT (committed_at, grant_read_at, grant_hash, node_id, iteration, scope_hash,
    expected_revision, arguments, fence) ON k6_sink.calls TO k6_sink_service;
GRANT SELECT (transaction_id) ON k6_sink.journal TO k6_sink_exporter;
GRANT SELECT (transaction_id, committed_at, grant_read_at, grant_hash, node_id, iteration,
    scope_hash, expected_revision, arguments, fence) ON k6_sink.calls TO k6_sink_exporter;

COMMIT;
