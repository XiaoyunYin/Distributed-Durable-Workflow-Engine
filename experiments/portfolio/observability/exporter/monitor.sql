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

CREATE FUNCTION j2_monitor.j3_sample() RETURNS jsonb
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
WITH clock AS (
    SELECT (extract(epoch FROM statement_timestamp()) * 1000000000)::bigint AS t, u_ns
    FROM j2_monitor.j3_clock
), admissions AS MATERIALIZED (
    SELECT w.workflow_id, w.submission_key, w.state, w.revision, p.scheduled_ns
    FROM j2_monitor.scope s
    JOIN engine.workflow_executions w ON w.namespace = s.namespace
    LEFT JOIN j2_monitor.j3_plan p ON (p.namespace, p.submission_key) = (w.namespace, w.submission_key)
), cohort AS MATERIALIZED (
    SELECT w.*, scheduled_ns + 2854000000 AS deadline_ns,
           w.state = 'SUCCEEDED' AND h.new_state = 'SUCCEEDED' AS visible_success,
           w.state = 'SUCCEEDED' AND h.new_state = 'SUCCEEDED'
             AND extract(epoch FROM h.created_at) * 1000000000 + c.u_ns
                 <= scheduled_ns + 2854000000 AS on_time
    FROM admissions w CROSS JOIN clock c
    LEFT JOIN engine.transition_history h
      ON (h.workflow_id, h.revision) = (w.workflow_id, w.revision)
), windows(label, width_ns) AS (
    VALUES ('2m', 120000000000::bigint), ('10m', 600000000000::bigint), ('all', NULL::bigint)
), counts AS (
    SELECT v.label, count(q.deadline_ns) AS eligible,
           count(q.deadline_ns) FILTER (WHERE q.on_time IS NOT TRUE) AS bad
    FROM windows v CROSS JOIN clock c
    LEFT JOIN cohort q ON q.deadline_ns <= c.t
        AND (v.width_ns IS NULL OR q.deadline_ns > c.t - v.width_ns)
    GROUP BY v.label
)
SELECT jsonb_build_object(
    'sample_ns', c.t, 'u_ns', c.u_ns,
    'windows', (SELECT jsonb_object_agg(label, jsonb_build_object('eligible', eligible, 'bad', bad)) FROM counts),
    'admissions', (SELECT coalesce(jsonb_object_agg(workflow_id, submission_key), '{}') FROM admissions),
    'successes', (SELECT coalesce(jsonb_object_agg(workflow_id, revision), '{}') FROM cohort WHERE visible_success),
    'outstanding', (SELECT count(*) FROM admissions WHERE state NOT IN ('SUCCEEDED','FAILED','CANCELED')),
    'unpublished', (SELECT count(*) FROM engine.outbox JOIN admissions USING (workflow_id) WHERE published_at IS NULL),
    'sql_oldest_claimable_seconds', (
        SELECT coalesce(greatest(0, extract(epoch FROM statement_timestamp() - min(a.created_at))), 0)
        FROM engine.activity_attempts a JOIN admissions w USING (workflow_id)
        JOIN engine.node_instances n USING (workflow_id, node_id, iteration)
        WHERE a.is_current AND a.state = 'DISPATCHABLE' AND w.state = 'WAITING_ACTIVITY'
          AND n.current_attempt_number = a.attempt_number
    )
) FROM clock c;
$$;
REVOKE ALL ON FUNCTION j2_monitor.j3_sample() FROM PUBLIC;
GRANT USAGE ON SCHEMA j2_monitor TO j3_exporter;
GRANT EXECUTE ON FUNCTION j2_monitor.j3_sample() TO j3_exporter;
COMMIT;
