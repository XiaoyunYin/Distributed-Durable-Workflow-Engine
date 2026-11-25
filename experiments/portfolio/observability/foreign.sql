-- Installer supplies password and a distinct foreign workflow namespace.
BEGIN;
CREATE ROLE j3_fixture LOGIN PASSWORD :'password' CONNECTION LIMIT 1;
ALTER ROLE j3_fixture SET statement_timeout = '2s';
CREATE TABLE j2_monitor.j3_foreign (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton), namespace text NOT NULL
);
INSERT INTO j2_monitor.j3_foreign (namespace)
SELECT :'foreign' FROM j2_monitor.scope WHERE namespace <> :'foreign';

-- pure.echo@v1 is in the registered worker and runtime registries.
INSERT INTO engine.workflow_definitions
    (definition_id, version, definition_hash, graph, activity_versions, effect_classes)
VALUES ('j3-foreign-echo', 1, 'j3-foreign-echo-v1',
    '{"entry":"pure.echo","nodes":[{"id":"pure.echo","kind":"activity","next":"done"},{"id":"done","kind":"success"}]}',
    '{"pure.echo":"v1"}', '{"pure.echo":"PURE_ACTIVITY"}');

CREATE FUNCTION j2_monitor.j3_seed(key text) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    plan j2_monitor.j3_plan%ROWTYPE;
    id text;
    seeded_at timestamptz := clock_timestamp();
BEGIN
    SELECT p.* INTO STRICT plan
    FROM j2_monitor.j3_plan p JOIN j2_monitor.j3_foreign f USING (namespace)
    WHERE p.submission_key = key
      AND p.scheduled_ns + 2854000000 <= extract(epoch FROM seeded_at) * 1000000000;
    id := 'j3-fixture-' || key;
    INSERT INTO engine.workflow_executions
        (workflow_id, namespace, submission_key, submission_payload_hash, definition_id,
         definition_version, partition_id, state, revision, created_at, updated_at)
    VALUES (id, plan.namespace, key, encode(sha256(convert_to(id, 'UTF8')), 'hex'),
        'j3-foreign-echo', 1, get_byte(sha256(convert_to(id, 'UTF8')), 7) % 16,
        'WAITING_ACTIVITY', 1, seeded_at, seeded_at);
    INSERT INTO engine.node_instances
        (workflow_id, node_id, state, current_attempt_number, revision, created_at, updated_at)
    VALUES (id, 'pure.echo', 'WAITING_ACTIVITY', 1, 1, seeded_at, seeded_at);
    INSERT INTO engine.activity_attempts
        (workflow_id, node_id, iteration, attempt_number, state, effect_class,
         is_current, heartbeat_deadline, worker_id, claim_token, created_at, updated_at)
    VALUES (id, 'pure.echo', 0, 1, 'DISPATCHABLE', 'PURE_ACTIVITY', true, NULL, NULL, NULL,
        seeded_at, seeded_at);
    INSERT INTO engine.transition_history
        (transition_id, workflow_id, revision, actor_kind, actor_id, node_id, iteration,
         attempt_number, new_state, reason, created_at)
    VALUES (gen_random_uuid(), id, 1, 'fixture', 'j3-fixture', 'pure.echo', 0, 1,
        'WAITING_ACTIVITY', 'J3_FOREIGN_SEED', seeded_at);
    RETURN jsonb_build_object('workflow_id', id, 'revision', 1, 'seeded_at', seeded_at);
END;
$$;
REVOKE ALL ON FUNCTION j2_monitor.j3_seed(text) FROM PUBLIC;
GRANT USAGE ON SCHEMA j2_monitor TO j3_fixture;
GRANT EXECUTE ON FUNCTION j2_monitor.j3_seed(text) TO j3_fixture;
COMMIT;
