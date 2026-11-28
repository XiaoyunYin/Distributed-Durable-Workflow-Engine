-- Run with a read-only transaction after the seed writes have been witnessed.
-- Parameters are the foreign namespace, the target exporter sample time in ns,
-- and the fixed setup clock width. The closeout must prove the fixture stayed
-- unchanged and the seed receipts must place every counted row before the sample.
BEGIN READ ONLY;
WITH parameters AS (
    SELECT :'foreign'::text AS ns, :'sample_ns'::bigint AS t, :'u_ns'::bigint AS u
), admissions AS (
    SELECT w.*, schedule.scheduled_ns + 2854000000 AS deadline_ns
    FROM engine.workflow_executions w
    JOIN j2_monitor.j3_plan schedule USING (namespace, submission_key)
    CROSS JOIN parameters p
    WHERE w.namespace = p.ns
      AND extract(epoch FROM pg_xact_commit_timestamp(w.xmin)) * 1000000000 <= p.t
), claimable AS (
    SELECT extract(epoch FROM a.created_at) * 1000000000 AS created_ns
    FROM engine.activity_attempts a
    JOIN admissions w USING (workflow_id)
    JOIN engine.node_instances n USING (workflow_id, node_id, iteration)
    CROSS JOIN parameters p
    WHERE w.state = 'WAITING_ACTIVITY' AND a.state = 'DISPATCHABLE' AND a.is_current
      AND n.current_attempt_number = a.attempt_number
      AND extract(epoch FROM a.created_at) * 1000000000 <= p.t
), deadline_counts AS (
    SELECT v.name, count(w.workflow_id) AS eligible,
           count(w.workflow_id) FILTER (WHERE NOT EXISTS (
               SELECT 1 FROM engine.transition_history h
               WHERE h.workflow_id = w.workflow_id AND h.revision = w.revision
                 AND w.state = 'SUCCEEDED' AND h.new_state = 'SUCCEEDED'
                 AND extract(epoch FROM h.created_at) * 1000000000 + p.u <= w.deadline_ns
           )) AS bad
    FROM (VALUES ('2m', 120000000000::bigint), ('10m', 600000000000::bigint)) v(name, width)
    CROSS JOIN parameters p
    LEFT JOIN admissions w ON w.deadline_ns > p.t - v.width AND w.deadline_ns <= p.t
    GROUP BY v.name
)
SELECT jsonb_build_object(
    'namespace', p.ns, 'sample_ns', p.t,
    'admission_ids', (SELECT coalesce(jsonb_agg(workflow_id ORDER BY workflow_id), '[]') FROM admissions),
    'claimable_attempts', (SELECT count(*) FROM claimable),
    'oldest_claimable_age_ns', (SELECT coalesce(p.t - min(created_ns), 0) FROM claimable),
    'windows', (SELECT jsonb_object_agg(name, to_jsonb(d) - 'name') FROM deadline_counts d)
) FROM parameters p;
COMMIT;
