-- Synthetic J3 population. Install monitor.sql and foreign.sql first.
-- The same immutable plan supports both the preloaded and concurrent rows.
INSERT INTO j2_monitor.j3_plan
SELECT 'target', 'dur053-j3-sqlbench-530001-' || lpad(i::text, 6, '0'),
       (extract(epoch FROM statement_timestamp()) * 1e9)::bigint
           + ((i - 20000) * 1e9 / 7.136100570066256)::bigint
FROM generate_series(1, 22000) i;

INSERT INTO j2_monitor.j3_plan
SELECT 'foreign', 'foreign-' || i,
       (extract(epoch FROM statement_timestamp()) * 1e9)::bigint - 60000000000
FROM generate_series(1, 4141) i;

CREATE SCHEMA j3_bench;
CREATE FUNCTION j3_bench.add_targets(first_id integer, last_id integer) RETURNS void
LANGUAGE sql AS $$
WITH cohort AS MATERIALIZED (
    SELECT p.submission_key AS id, (i % 16)::smallint AS partition_id,
           i % 8 <> 0 AS finished,
           to_timestamp(p.scheduled_ns / 1e9) AS scheduled,
           CASE WHEN i % 13 = 0 THEN interval '4 s' ELSE interval '100 ms' END AS latency
    FROM generate_series(first_id, last_id) i
    JOIN j2_monitor.j3_plan p ON p.namespace = 'target'
      AND p.submission_key = 'dur053-j3-sqlbench-530001-' || lpad(i::text, 6, '0')
), wf AS (
    INSERT INTO engine.workflow_executions
        (workflow_id, namespace, submission_key, submission_payload_hash,
         definition_id, definition_version, partition_id, state, revision, created_at, updated_at)
    SELECT id, 'target', id, repeat('a', 64), 'j3-foreign-echo', 1, partition_id,
           CASE WHEN finished THEN 'SUCCEEDED' ELSE 'WAITING_ACTIVITY' END,
           CASE WHEN finished THEN 4 ELSE 2 END,
           scheduled, CASE WHEN finished THEN scheduled + latency ELSE scheduled END
    FROM cohort
    RETURNING workflow_id
), nodes AS (
    INSERT INTO engine.node_instances
        (workflow_id, node_id, state, current_attempt_number, revision, created_at, updated_at)
    SELECT id, 'pure.echo', CASE WHEN finished THEN 'SUCCEEDED' ELSE 'WAITING_ACTIVITY' END,
           1, 1, scheduled, scheduled
    FROM cohort JOIN wf ON workflow_id = id
    RETURNING workflow_id
), attempts AS (
    INSERT INTO engine.activity_attempts
        (workflow_id, node_id, iteration, attempt_number, state, effect_class, created_at, updated_at)
    SELECT id, 'pure.echo', 0, 1, CASE WHEN finished THEN 'SUCCEEDED' ELSE 'DISPATCHABLE' END,
           'PURE_ACTIVITY', scheduled, scheduled
    FROM cohort JOIN nodes ON workflow_id = id
), history AS (
    INSERT INTO engine.transition_history
        (transition_id, workflow_id, revision, actor_kind, actor_id, new_state, reason, created_at)
    SELECT gen_random_uuid(), id, revision, 'fixture', 'j3-benchmark',
           CASE revision WHEN 1 THEN 'RUNNABLE' WHEN 4 THEN 'SUCCEEDED' ELSE 'WAITING_ACTIVITY' END,
           'J3_BENCH', scheduled + CASE WHEN revision = 4 THEN latency ELSE interval '0' END
    FROM cohort JOIN wf ON workflow_id = id
    CROSS JOIN LATERAL generate_series(1, CASE WHEN finished THEN 4 ELSE 2 END) revision
)
INSERT INTO engine.outbox
    (event_id, workflow_id, aggregate_revision, event_type, payload,
     publish_state, created_at, published_at)
SELECT gen_random_uuid(), id, revision, 'attempt.dispatch',
       jsonb_build_object('workflow_id', id, 'definition_id', 'j3-foreign-echo'),
       CASE WHEN finished OR revision = 1 THEN 'PUBLISHED' ELSE 'PENDING' END,
       scheduled, CASE WHEN finished OR revision = 1 THEN scheduled END
FROM cohort JOIN wf ON workflow_id = id CROSS JOIN generate_series(1, 3) revision;
$$;
