BEGIN;

DO $roles$
BEGIN
    BEGIN
        CREATE ROLE k6_grant_function_owner NOLOGIN NOINHERIT;
    EXCEPTION WHEN duplicate_object THEN NULL;
    END;
    BEGIN
        CREATE ROLE k6_decision_function_owner NOLOGIN NOINHERIT;
    EXCEPTION WHEN duplicate_object THEN NULL;
    END;
    IF EXISTS (
        SELECT FROM pg_catalog.pg_roles AS r
        WHERE r.rolname IN ('k6_grant_function_owner', 'k6_decision_function_owner')
          AND (r.rolcanlogin OR r.rolinherit OR r.rolsuper OR r.rolcreatedb OR r.rolcreaterole
               OR r.rolreplication OR r.rolbypassrls OR EXISTS (
                   SELECT FROM pg_catalog.pg_auth_members AS m WHERE m.roleid = r.oid OR m.member = r.oid))
    ) THEN
        RAISE EXCEPTION 'K6 function owners must be isolated NOLOGIN roles';
    END IF;
END
$roles$;

CREATE SCHEMA IF NOT EXISTS k6_auth;
REVOKE ALL ON SCHEMA k6_auth FROM PUBLIC;
GRANT USAGE ON SCHEMA k6_auth TO k6_grant_function_owner, k6_decision_function_owner;

CREATE TABLE IF NOT EXISTS k6_auth.episode (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    namespace text NOT NULL CHECK (namespace <> ''),
    approver_subject text NOT NULL CHECK (approver_subject = 'system:serviceaccount:' || namespace || ':k6-approver'),
    valid_until timestamptz NOT NULL CHECK (pg_catalog.isfinite(valid_until)),
    uncertainty_us bigint NOT NULL CHECK (uncertainty_us >= 0)
);
REVOKE ALL ON k6_auth.episode FROM PUBLIC;

CREATE OR REPLACE VIEW k6_auth.episode_grants WITH (security_barrier = true) AS
SELECT w.namespace, i.intent_id, i.workflow_id, i.node_id, i.iteration,
       i.grant_token, i.proposal_hash, i.target, i.canonical_arguments,
       i.expected_resource_revision, i.decision, i.dispatch_status,
       i.grant_scope_hash, i.grant_expires_at
FROM engine.approval_action_intents AS i
JOIN engine.workflow_executions AS w ON w.workflow_id = i.workflow_id
JOIN k6_auth.episode AS e ON w.namespace = e.namespace;
REVOKE ALL ON k6_auth.episode_grants FROM PUBLIC;
GRANT SELECT (intent_id, workflow_id, node_id, iteration, grant_token, proposal_hash, target,
              canonical_arguments, expected_resource_revision, decision, dispatch_status,
              grant_scope_hash, grant_expires_at)
    ON k6_auth.episode_grants TO k6_grant_function_owner;

CREATE OR REPLACE FUNCTION k6_auth.grant_is_live(sampled timestamptz, expires_at timestamptz)
RETURNS boolean LANGUAGE sql IMMUTABLE STRICT SET search_path = pg_catalog, pg_temp
RETURN sampled < expires_at;
REVOKE ALL ON FUNCTION k6_auth.grant_is_live(timestamptz, timestamptz) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION k6_auth.grant_is_live(timestamptz, timestamptz) TO k6_grant_function_owner;

CREATE OR REPLACE FUNCTION k6_auth.read_exact_grant(
    p_intent uuid, p_token text, p_workflow text, p_node text,
    p_iteration integer, p_key text, p_scope text, p_target text
) RETURNS TABLE (
    intent_id uuid, workflow_id text, node_id text, iteration integer,
    proposal_hash text, target text, canonical_arguments jsonb, expected_resource_revision text,
    decision text, dispatch_status text, grant_scope_hash text, grant_expires_at timestamptz,
    db_time timestamptz
)
LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $read$
    WITH sampled AS MATERIALIZED (SELECT pg_catalog.clock_timestamp() AS db_time)
    SELECT g.intent_id, g.workflow_id, g.node_id, g.iteration, g.proposal_hash, g.target,
           g.canonical_arguments, COALESCE(g.expected_resource_revision, ''),
           g.decision, g.dispatch_status, g.grant_scope_hash, g.grant_expires_at, sampled.db_time
    FROM k6_auth.episode_grants AS g CROSS JOIN sampled
    WHERE g.intent_id = p_intent AND g.grant_token::text = p_token
      AND g.workflow_id = p_workflow AND g.node_id = p_node AND g.iteration = p_iteration
      AND p_workflow <> '' AND p_node <> '' AND p_key <> '' AND p_scope <> '' AND p_target <> ''
      AND g.target = p_target AND g.grant_scope_hash = p_scope
      AND g.decision = 'APPROVED' AND g.dispatch_status IN ('GRANTED', 'DISPATCHED')
      AND k6_auth.grant_is_live(sampled.db_time, g.grant_expires_at)
      AND g.grant_scope_hash = pg_catalog.encode(pg_catalog.sha256(
          pg_catalog.convert_to(g.proposal_hash, 'UTF8') || pg_catalog.decode('00', 'hex') ||
          pg_catalog.convert_to(COALESCE(g.expected_resource_revision, ''), 'UTF8') || pg_catalog.decode('00', 'hex') ||
          pg_catalog.convert_to(p_key, 'UTF8') || pg_catalog.decode('00', 'hex') ||
          pg_catalog.convert_to(p_target, 'UTF8')), 'hex');
$read$;
REVOKE ALL ON FUNCTION k6_auth.read_exact_grant(uuid, text, text, text, integer, text, text, text) FROM PUBLIC;
ALTER FUNCTION k6_auth.read_exact_grant(uuid, text, text, text, integer, text, text, text) OWNER TO k6_grant_function_owner;

GRANT USAGE ON SCHEMA engine TO k6_decision_function_owner;
GRANT SELECT (namespace, approver_subject, valid_until) ON k6_auth.episode TO k6_decision_function_owner;
GRANT SELECT (workflow_id, namespace) ON engine.workflow_executions TO k6_decision_function_owner;
GRANT SELECT (intent_id, workflow_id, node_id, iteration, proposal_hash, decision, approver_id,
              decided_at, valid_until, dispatch_status), UPDATE (decision, approver_id, decided_at)
    ON engine.approval_action_intents TO k6_decision_function_owner;

-- NULL p_decision requests the pending identity for the exact plan tuple and
-- hash. Recording also requires the UUID returned by that lookup.
CREATE OR REPLACE FUNCTION k6_auth.approval_decision(
    p_intent uuid, p_workflow text, p_node text, p_iteration integer,
    p_hash text, p_subject text, p_decision text
) RETURNS TABLE (code text, intent_id uuid, proposal_hash text, decision text, subject text, decided_at timestamptz)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $decision$
DECLARE
    installation record;
    existing record;
BEGIN
    SELECT e.namespace, e.approver_subject, e.valid_until INTO installation FROM k6_auth.episode AS e;
    IF NOT FOUND THEN RETURN; END IF;
    IF pg_catalog.clock_timestamp() >= installation.valid_until THEN
        RETURN QUERY SELECT 'EXPIRED'::text, NULL::uuid, NULL::text, NULL::text, NULL::text, NULL::timestamptz;
        RETURN;
    END IF;
    IF p_subject IS DISTINCT FROM installation.approver_subject OR p_hash IS NULL OR p_hash = ''
       OR (p_decision IS NOT NULL AND (p_intent IS NULL OR p_decision NOT IN ('APPROVED', 'REJECTED'))) THEN RETURN; END IF;

    -- Do not queue an approver behind an engine transaction through cutoff.
    SELECT i.intent_id, i.proposal_hash, i.decision, i.approver_id, i.decided_at, i.valid_until, i.dispatch_status
    INTO STRICT existing
    FROM engine.approval_action_intents AS i
    JOIN engine.workflow_executions AS w ON w.workflow_id = i.workflow_id
    WHERE w.namespace = installation.namespace AND (p_intent IS NULL OR i.intent_id = p_intent)
      AND i.workflow_id = p_workflow AND i.node_id = p_node AND i.iteration = p_iteration
      AND i.proposal_hash = p_hash AND i.valid_until = installation.valid_until
    FOR UPDATE OF i NOWAIT;
    IF pg_catalog.clock_timestamp() >= existing.valid_until THEN
        RETURN QUERY SELECT 'EXPIRED'::text, NULL::uuid, NULL::text, NULL::text, NULL::text, NULL::timestamptz;
        RETURN;
    END IF;
    IF p_decision IS NULL THEN
        IF existing.decision = 'PENDING' AND existing.dispatch_status = 'NOT_GRANTED' THEN
            RETURN QUERY SELECT 'OK'::text, existing.intent_id, existing.proposal_hash,
                existing.decision, NULL::text, NULL::timestamptz;
        END IF;
        RETURN;
    END IF;
    IF existing.decision <> 'PENDING' THEN
        IF existing.decision = p_decision AND existing.approver_id = installation.approver_subject THEN
            RETURN QUERY SELECT 'OK'::text, existing.intent_id, existing.proposal_hash,
                existing.decision, existing.approver_id, existing.decided_at;
        END IF;
        RETURN;
    END IF;
    IF existing.dispatch_status <> 'NOT_GRANTED' THEN RETURN; END IF;
    RETURN QUERY
        UPDATE engine.approval_action_intents AS i
        SET decision = p_decision, approver_id = installation.approver_subject, decided_at = pg_catalog.clock_timestamp()
        WHERE i.intent_id = existing.intent_id AND pg_catalog.clock_timestamp() < existing.valid_until
        RETURNING 'OK'::text, i.intent_id, i.proposal_hash, i.decision, i.approver_id, i.decided_at;
    IF NOT FOUND THEN
        RETURN QUERY SELECT 'EXPIRED'::text, NULL::uuid, NULL::text, NULL::text, NULL::text, NULL::timestamptz;
    END IF;
EXCEPTION WHEN lock_not_available OR no_data_found OR too_many_rows THEN
    IF pg_catalog.clock_timestamp() >= installation.valid_until THEN
        RETURN QUERY SELECT 'EXPIRED'::text, NULL::uuid, NULL::text, NULL::text, NULL::text, NULL::timestamptz;
    END IF;
    RETURN;
END;
$decision$;
REVOKE ALL ON FUNCTION k6_auth.approval_decision(uuid, text, text, integer, text, text, text) FROM PUBLIC;
ALTER FUNCTION k6_auth.approval_decision(uuid, text, text, integer, text, text, text) OWNER TO k6_decision_function_owner;

INSERT INTO engine.schema_migrations (version) VALUES (22) ON CONFLICT DO NOTHING;
COMMIT;
