-- Install as the owner before loading the immutable schedule or starting the exporter.
BEGIN;
CREATE ROLE j3_exporter LOGIN PASSWORD :'password' CONNECTION LIMIT 1;
ALTER ROLE j3_exporter SET statement_timeout = '2s';
ALTER ROLE j3_exporter SET default_transaction_read_only = on;
CREATE SCHEMA j2_monitor;
REVOKE ALL ON SCHEMA j2_monitor FROM PUBLIC;
CREATE TABLE j2_monitor.scope (singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
                              namespace text NOT NULL CHECK (namespace <> ''));
INSERT INTO j2_monitor.scope (namespace) VALUES (:'namespace');
CREATE TABLE j2_monitor.j3_clock (singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
                                 u_ns bigint NOT NULL CHECK (u_ns >= 0));
INSERT INTO j2_monitor.j3_clock (u_ns) VALUES (:'u_ns');
CREATE TABLE j2_monitor.j3_plan (
    namespace text NOT NULL, submission_key text NOT NULL, scheduled_ns bigint NOT NULL,
    PRIMARY KEY (namespace, submission_key)
);

CREATE FUNCTION j2_monitor.j3_sample() RETURNS json
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
WITH clock AS (
    SELECT (extract(epoch FROM statement_timestamp()) * 1000000000)::bigint AS t, u_ns
    FROM j2_monitor.j3_clock
), admissions AS MATERIALIZED (
    SELECT w.workflow_id, w.submission_key, w.state, w.revision, p.scheduled_ns
    FROM j2_monitor.scope s
    JOIN engine.workflow_executions w ON w.namespace = s.namespace
    LEFT JOIN j2_monitor.j3_plan p ON (p.namespace, p.submission_key) = (w.namespace, w.submission_key)
), cohort AS (
    SELECT w.workflow_id, w.revision, scheduled_ns + 2854000000 AS deadline_ns,
           w.state = 'SUCCEEDED' AND h.new_state = 'SUCCEEDED' AS visible_success,
           w.state = 'SUCCEEDED' AND h.new_state = 'SUCCEEDED'
             AND extract(epoch FROM h.created_at) * 1000000000 + c.u_ns
                 <= scheduled_ns + 2854000000 AS on_time
    FROM admissions w CROSS JOIN clock c
    LEFT JOIN engine.transition_history h
      ON w.state = 'SUCCEEDED' AND (h.workflow_id, h.revision) = (w.workflow_id, w.revision)
), counts AS (
    SELECT count(*) FILTER (WHERE matured AND short_window) AS eligible_short,
           count(*) FILTER (WHERE matured AND short_window AND bad) AS bad_short,
           count(*) FILTER (WHERE matured AND long_window) AS eligible_long,
           count(*) FILTER (WHERE matured AND long_window AND bad) AS bad_long,
           count(*) FILTER (WHERE matured) AS eligible_all,
           count(*) FILTER (WHERE matured AND bad) AS bad_all,
           coalesce(json_object_agg(workflow_id, revision) FILTER (WHERE visible_success), '{}')
               AS successes
    FROM (
        SELECT q.*, q.deadline_ns <= c.t AS matured, q.on_time IS NOT TRUE AS bad,
               q.deadline_ns > c.t - 120000000000 AS short_window,
               q.deadline_ns > c.t - 600000000000 AS long_window
        FROM cohort q CROSS JOIN clock c
    ) window_input
)
SELECT json_build_object(
    'sample_ns', c.t, 'u_ns', c.u_ns,
    'windows', json_build_object(
        '2m', json_build_object('eligible', eligible_short, 'bad', bad_short),
        '10m', json_build_object('eligible', eligible_long, 'bad', bad_long),
        'all', json_build_object('eligible', eligible_all, 'bad', bad_all)),
    'admissions', (SELECT coalesce(json_object_agg(workflow_id, submission_key), '{}') FROM admissions),
    'successes', counts.successes,
    'outstanding', (SELECT count(*) FROM admissions WHERE state NOT IN ('SUCCEEDED','FAILED','CANCELED')),
    'unpublished', (SELECT count(*) FROM engine.outbox JOIN admissions USING (workflow_id) WHERE published_at IS NULL),
    'sql_oldest_claimable_seconds', (
        SELECT coalesce(greatest(0, extract(epoch FROM statement_timestamp() - min(a.created_at))), 0)
        FROM engine.activity_attempts a JOIN admissions w USING (workflow_id)
        JOIN engine.node_instances n USING (workflow_id, node_id, iteration)
        WHERE a.is_current AND a.state = 'DISPATCHABLE' AND w.state = 'WAITING_ACTIVITY'
          AND n.current_attempt_number = a.attempt_number
    )
) FROM clock c CROSS JOIN counts;
$$;
REVOKE ALL ON FUNCTION j2_monitor.j3_sample() FROM PUBLIC;
GRANT USAGE ON SCHEMA j2_monitor TO j3_exporter;
GRANT EXECUTE ON FUNCTION j2_monitor.j3_sample() TO j3_exporter;
COMMIT;
