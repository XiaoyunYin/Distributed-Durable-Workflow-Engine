BEGIN;

-- Additive to 000020: capture the durable claim even if its HTTP response dies.
ALTER TABLE engine.k4_audit_queue DROP CONSTRAINT k4_audit_queue_kind_check;
ALTER TABLE engine.k4_audit_queue ADD CHECK (kind IN ('acquire','release','history','claim'));
ALTER TABLE engine.k4_audit DROP CONSTRAINT k4_audit_kind_check;
ALTER TABLE engine.k4_audit ADD CHECK (kind IN ('acquire','release','history','claim'));
ALTER TABLE engine.k4_audit_queue ADD COLUMN claim_identity jsonb;
ALTER TABLE engine.k4_audit ADD COLUMN claim_identity jsonb;
ALTER TABLE engine.k4_audit ADD COLUMN finalized_at timestamptz;

CREATE OR REPLACE FUNCTION engine.k4_flush_audit() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    -- Still deferred: never acquire the serialization lock in business statements.
    -- Only audit rows are touched here, no attempt/workflow/lease reads or locks.
    PERFORM pg_advisory_xact_lock(530004, 1);
    INSERT INTO engine.k4_audit
        (at, kind, partition_id, owner_id, epoch, session_id, pod_uid,
         container_id, transaction_id, transition_id, claim_identity, finalized_at)
    VALUES (NEW.at, NEW.kind, NEW.partition_id, NEW.owner_id, NEW.epoch,
            NEW.session_id, NEW.pod_uid, NEW.container_id, NEW.transaction_id,
            NEW.transition_id, NEW.claim_identity, clock_timestamp());
    DELETE FROM engine.k4_audit_queue WHERE id = NEW.id;
    RETURN NULL;
END $$;

CREATE FUNCTION engine.k4_capture_claim() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO engine.k4_audit_queue
        (kind, partition_id, owner_id, epoch, session_id, pod_uid, container_id, claim_identity)
    SELECT 'claim', w.partition_id, '', 0,
           current_setting('durable.k4_session'), current_setting('durable.k4_pod_uid'),
           current_setting('durable.k4_container_id'),
           jsonb_build_object('workflow_id', NEW.workflow_id, 'node_id', NEW.node_id,
               'iteration', NEW.iteration, 'attempt_number', NEW.attempt_number,
               'token_hash', encode(sha256(convert_to(NEW.claim_token::text,'UTF8')),'hex'),
               'worker_id', NEW.worker_id, 'worker_request_id', NEW.worker_request_id)
    FROM engine.workflow_executions w WHERE w.workflow_id = NEW.workflow_id;
    RETURN NULL;
END $$;
-- AFTER runs in ClaimAttempt's transaction, before COMMIT. Idempotent reclaims
-- do not UPDATE and cannot duplicate the immutable claim record.
CREATE TRIGGER k4_capture_claim AFTER UPDATE ON engine.activity_attempts
    FOR EACH ROW WHEN (current_setting('durable.k4_campaign', true) = 'on'
                      AND NEW.state = 'CLAIMED' AND OLD.state = 'DISPATCHABLE')
    EXECUTE FUNCTION engine.k4_capture_claim();
CREATE VIEW engine.k4_claim_audit AS
SELECT event_order AS "order", at, finalized_at, session_id AS session,
       pod_uid, container_id, transaction_id AS "transaction",
       claim_identity->>'workflow_id' AS workflow_id,
       claim_identity->>'node_id' AS node_id,
       (claim_identity->>'iteration')::integer AS iteration,
       (claim_identity->>'attempt_number')::bigint AS attempt_number,
       claim_identity->>'token_hash' AS token_hash,
       claim_identity->>'worker_id' AS worker_id,
       claim_identity->>'worker_request_id' AS worker_request_id
FROM engine.k4_audit WHERE kind = 'claim';

INSERT INTO engine.schema_migrations (version) VALUES (21);
COMMIT;
