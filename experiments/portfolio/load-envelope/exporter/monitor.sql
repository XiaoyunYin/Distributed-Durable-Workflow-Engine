-- The namespace is fixed at installation. The login can only execute this projection.
-- Supply namespace and password as psql variables on a dedicated experiment database.
BEGIN;
CREATE ROLE j2_exporter LOGIN PASSWORD :'password' CONNECTION LIMIT 1;
ALTER ROLE j2_exporter SET statement_timeout = '2s';
ALTER ROLE j2_exporter SET default_transaction_read_only = on;

CREATE SCHEMA j2_monitor;
CREATE TABLE j2_monitor.scope (namespace text NOT NULL);
INSERT INTO j2_monitor.scope VALUES (:'namespace');

CREATE FUNCTION j2_monitor.sample()
RETURNS TABLE (sampled_at timestamptz, outstanding bigint, unpublished bigint,
    oldest_claimable_seconds double precision, admissions bigint, terminals bigint,
    wait_lock bigint, wait_lwlock bigint, wait_io bigint, wait_ipc bigint,
    wait_client bigint, wait_timeout bigint, wait_other bigint)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
WITH workflows AS MATERIALIZED (
    SELECT w.workflow_id, w.state FROM engine.workflow_executions w
    JOIN j2_monitor.scope s ON s.namespace = w.namespace
), counts AS (
    SELECT count(*) AS admitted,
        count(*) FILTER (WHERE state IN ('SUCCEEDED','FAILED','CANCELED')) AS terminal
    FROM workflows
), waits AS (
    SELECT a.wait_event_type AS kind FROM pg_catalog.pg_stat_activity a
    WHERE a.datname = current_database() AND a.backend_type = 'client backend'
      AND a.pid <> pg_backend_pid() AND a.state = 'active' AND a.wait_event_type IS NOT NULL
)
SELECT statement_timestamp(), admitted - terminal,
    (SELECT count(*) FROM engine.outbox o JOIN workflows w USING (workflow_id)
        WHERE o.published_at IS NULL),
    (SELECT coalesce(greatest(0, extract(epoch FROM statement_timestamp() - min(a.created_at))), 0)::float8
        FROM engine.activity_attempts a JOIN workflows w USING (workflow_id)
        JOIN engine.node_instances n USING (workflow_id, node_id, iteration)
        WHERE a.is_current AND a.state = 'DISPATCHABLE' AND w.state = 'WAITING_ACTIVITY'
          AND n.current_attempt_number = a.attempt_number),
    admitted, terminal,
    count(*) FILTER (WHERE kind = 'Lock'), count(*) FILTER (WHERE kind = 'LWLock'),
    count(*) FILTER (WHERE kind = 'IO'), count(*) FILTER (WHERE kind = 'IPC'),
    count(*) FILTER (WHERE kind = 'Client'), count(*) FILTER (WHERE kind = 'Timeout'),
    count(*) FILTER (WHERE kind NOT IN ('Lock','LWLock','IO','IPC','Client','Timeout'))
FROM counts LEFT JOIN waits ON true GROUP BY admitted, terminal;
$$;
REVOKE ALL ON FUNCTION j2_monitor.sample() FROM PUBLIC;
GRANT USAGE ON SCHEMA j2_monitor TO j2_exporter;
GRANT EXECUTE ON FUNCTION j2_monitor.sample() TO j2_exporter;
COMMIT;
