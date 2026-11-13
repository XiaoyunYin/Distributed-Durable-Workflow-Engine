BEGIN;

-- K4 only: final rows are immutable. The queue is transaction-local in effect:
-- its deferred trigger drains it before that same transaction can commit.
CREATE SEQUENCE engine.k4_commit_order;
CREATE TABLE engine.k4_audit_queue (
    id bigserial PRIMARY KEY,
    at timestamptz NOT NULL DEFAULT clock_timestamp(),
    kind text NOT NULL CHECK (kind IN ('acquire', 'release', 'history')),
    partition_id smallint NOT NULL,
    owner_id text NOT NULL,
    epoch bigint NOT NULL,
    session_id text NOT NULL,
    pod_uid text NOT NULL,
    container_id text NOT NULL,
    transaction_id text NOT NULL DEFAULT pg_current_xact_id()::text,
    transition_id uuid
);
CREATE TABLE engine.k4_audit (
    event_order bigint PRIMARY KEY DEFAULT nextval('engine.k4_commit_order'),
    at timestamptz NOT NULL,
    kind text NOT NULL CHECK (kind IN ('acquire', 'release', 'history')),
    partition_id smallint NOT NULL,
    owner_id text NOT NULL,
    epoch bigint NOT NULL,
    session_id text NOT NULL,
    pod_uid text NOT NULL,
    container_id text NOT NULL,
    transaction_id text NOT NULL,
    transition_id uuid UNIQUE
);

CREATE FUNCTION engine.k4_flush_audit() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    -- Finalization happens after business statements, at deferred-trigger time.
    -- Every audit writer shares this lock and holds it through COMMIT/ROLLBACK.
    -- nextval alone is NOT commit ordered. No business rows are locked here.
    PERFORM pg_advisory_xact_lock(530004, 1);
    INSERT INTO engine.k4_audit
        (at, kind, partition_id, owner_id, epoch, session_id, pod_uid,
         container_id, transaction_id, transition_id)
    VALUES (NEW.at, NEW.kind, NEW.partition_id, NEW.owner_id, NEW.epoch,
            NEW.session_id, NEW.pod_uid, NEW.container_id, NEW.transaction_id,
            NEW.transition_id);
    DELETE FROM engine.k4_audit_queue WHERE id = NEW.id;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER k4_finalize_audit
    AFTER INSERT ON engine.k4_audit_queue DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION engine.k4_flush_audit();

CREATE FUNCTION engine.k4_history_audit() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO engine.k4_audit_queue
        (at, kind, partition_id, owner_id, epoch, session_id, pod_uid,
         container_id, transition_id)
    SELECT NEW.created_at, 'history', w.partition_id,
           CASE WHEN NEW.scheduler_epoch IS NULL THEN ''
                ELSE COALESCE(current_setting('durable.k4_owner', true), '') END,
           COALESCE(NEW.scheduler_epoch, 0),
           current_setting('durable.k4_session'), current_setting('durable.k4_pod_uid'),
           current_setting('durable.k4_container_id'), NEW.transition_id
    FROM engine.workflow_executions w WHERE w.workflow_id = NEW.workflow_id;
    RETURN NULL;
END $$;
CREATE TRIGGER k4_capture_history AFTER INSERT ON engine.transition_history
    FOR EACH ROW WHEN (current_setting('durable.k4_campaign', true) = 'on')
    EXECUTE FUNCTION engine.k4_history_audit();

CREATE FUNCTION engine.k4_audit_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'K4 audit is append-only';
END $$;
CREATE TRIGGER k4_audit_no_mutation BEFORE UPDATE OR DELETE ON engine.k4_audit
    FOR EACH ROW EXECUTE FUNCTION engine.k4_audit_immutable();
CREATE TRIGGER k4_audit_no_truncate BEFORE TRUNCATE ON engine.k4_audit
    FOR EACH STATEMENT EXECUTE FUNCTION engine.k4_audit_immutable();

-- Read-only B2 interface. at is a DB timestamp, NOT a measured commit instant.
-- B2 maps at into its observer clock domain using retained clock brackets.
CREATE VIEW engine.k4_lease_audit AS
SELECT 'dur053.k4-lease-audit.v1'::text AS schema, event_order AS "order",
       at, kind, partition_id AS partition, owner_id AS owner, epoch,
       session_id AS session, transaction_id AS "transaction"
FROM engine.k4_audit WHERE kind IN ('acquire', 'release');
CREATE VIEW engine.k4_history_order AS
SELECT transition_id, event_order AS "order", at, partition_id AS partition,
       owner_id AS owner, epoch, session_id AS session, transaction_id AS "transaction"
FROM engine.k4_audit WHERE kind = 'history';

INSERT INTO engine.schema_migrations (version) VALUES (20);
COMMIT;
