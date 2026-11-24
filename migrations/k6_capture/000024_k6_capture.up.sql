BEGIN;

-- Installed only in instrumented K6 episode databases.
CREATE TABLE IF NOT EXISTS engine.k6_write_capture (
    event_order bigint PRIMARY KEY,
    transaction_id text NOT NULL,
    subject text NOT NULL,
    operation text NOT NULL,
    row_key jsonb NOT NULL,
    workflow_id text,
    partition_id smallint,
    before_image jsonb,
    after_image jsonb
);
CREATE TABLE IF NOT EXISTS engine.k6_capture_queue (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transaction_id text NOT NULL DEFAULT pg_current_xact_id()::text,
    subject text NOT NULL,
    operation text NOT NULL,
    row_key jsonb NOT NULL,
    workflow_id text,
    partition_id smallint,
    before_image jsonb,
    after_image jsonb,
    owner_partition smallint,
    owner_id text,
    epoch bigint
);
CREATE TABLE IF NOT EXISTS engine.k6_owner_write_transactions (
    event_order bigint PRIMARY KEY REFERENCES engine.k6_write_capture,
    transaction_id text NOT NULL,
    subject text NOT NULL,
    operation text NOT NULL,
    partition_id smallint NOT NULL,
    owner_id text NOT NULL,
    epoch bigint NOT NULL
);

CREATE OR REPLACE FUNCTION engine.k6_finalize_capture() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    position bigint;
BEGIN
    PERFORM pg_advisory_xact_lock(530004, 1);
    position := nextval('engine.k4_commit_order');
    INSERT INTO engine.k6_write_capture
    VALUES (position, NEW.transaction_id, NEW.subject, NEW.operation, NEW.row_key,
            NEW.workflow_id, NEW.partition_id, NEW.before_image, NEW.after_image);
    IF NEW.owner_id IS NOT NULL THEN
        INSERT INTO engine.k6_owner_write_transactions
        VALUES (position, NEW.transaction_id, NEW.subject, NEW.operation,
                NEW.owner_partition, NEW.owner_id, NEW.epoch);
    END IF;
    DELETE FROM engine.k6_capture_queue WHERE id = NEW.id;
    RETURN NULL;
END $$;
DROP TRIGGER IF EXISTS k6_finalize_capture ON engine.k6_capture_queue;
CREATE CONSTRAINT TRIGGER k6_finalize_capture AFTER INSERT ON engine.k6_capture_queue
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION engine.k6_finalize_capture();

-- Triggers retain a closed scalar projection, never a raw token, payload or error.
CREATE OR REPLACE FUNCTION engine.k6_capture_fields(row_data jsonb, fields text) RETURNS jsonb
LANGUAGE sql IMMUTABLE STRICT SET search_path = pg_catalog, pg_temp AS $$
    SELECT COALESCE(jsonb_object_agg(field, row_data->field), '{}'::jsonb)
    FROM unnest(string_to_array(fields, ' ')) AS f(field)
$$;

CREATE OR REPLACE FUNCTION engine.k6_capture_row() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp SET TimeZone = 'UTC' AS $$
DECLARE
    b jsonb;
    a jsonb;
    r jsonb;
    k jsonb;
    before_image jsonb;
    after_image jsonb;
    workflow text;
    partition smallint;
    owner_partition smallint := NULLIF(current_setting('durable.k6_owner_partition', true), '')::smallint;
    owner text;
    owner_epoch bigint;
    operation text := lower(TG_OP);
BEGIN
    IF TG_OP <> 'INSERT' THEN b := to_jsonb(OLD); END IF;
    IF TG_OP <> 'DELETE' THEN a := to_jsonb(NEW); END IF;
    r := COALESCE(a, b);
    SELECT jsonb_agg(r->field ORDER BY ord) INTO k
        FROM unnest(string_to_array(TG_ARGV[0], ' ')) WITH ORDINALITY AS f(field, ord);
    before_image := engine.k6_capture_fields(b, TG_ARGV[1]);
    after_image := engine.k6_capture_fields(a, TG_ARGV[1]);
    workflow := r->>'workflow_id';
    partition := (r->>'partition_id')::smallint;
    IF workflow IS NOT NULL AND partition IS NULL THEN
        SELECT partition_id INTO partition FROM engine.workflow_executions WHERE workflow_id = workflow;
    END IF;
    IF owner_partition IS NOT NULL THEN
        owner := NULLIF(current_setting('durable.k4_owner', true), '');
        owner_epoch := NULLIF(current_setting('durable.k6_owner_epoch', true), '')::bigint;
    END IF;
    CASE TG_TABLE_NAME
    WHEN 'node_instances' THEN
        IF b IS NOT NULL THEN
            before_image := before_image || jsonb_build_object('image_hash',
                encode(sha256(convert_to(b::text, 'UTF8')), 'hex'));
        END IF;
        IF a IS NOT NULL THEN
            after_image := after_image || jsonb_build_object('image_hash',
                encode(sha256(convert_to(a::text, 'UTF8')), 'hex'));
        END IF;
    WHEN 'activity_attempts' THEN
        IF b IS NOT NULL THEN
            b := (b - 'claim_token') || jsonb_build_object('claim_token_hash',
                encode(sha256(convert_to(b->>'claim_token', 'UTF8')), 'hex'));
            before_image := before_image || jsonb_build_object('claim_token_hash', b->'claim_token_hash',
                'receipt_id', b#>>'{k6_proof,receipt,application_id}',
                'image_hash', encode(sha256(convert_to(b::text, 'UTF8')), 'hex'));
        END IF;
        IF a IS NOT NULL THEN
            a := (a - 'claim_token') || jsonb_build_object('claim_token_hash',
                encode(sha256(convert_to(a->>'claim_token', 'UTF8')), 'hex'));
            after_image := after_image || jsonb_build_object('claim_token_hash', a->'claim_token_hash',
                'receipt_id', a#>>'{k6_proof,receipt,application_id}',
                'image_hash', encode(sha256(convert_to(a::text, 'UTF8')), 'hex'));
        END IF;
    WHEN 'partition_leases' THEN
        owner_partition := partition;
        owner := COALESCE(a->>'owner_id', b->>'owner_id');
        owner_epoch := (r->>'epoch')::bigint;
        operation := CASE WHEN a->>'owner_id' IS NULL THEN 'release'
                          WHEN a->>'epoch' IS DISTINCT FROM b->>'epoch' AND b->>'owner_id' IS NOT NULL THEN 'takeover'
                          WHEN a->>'epoch' IS DISTINCT FROM b->>'epoch'
                               OR NULLIF(current_setting('durable.k6_owner_epoch', true), '') IS NULL THEN 'acquire'
                          ELSE 'renewal' END;
    WHEN 'lease_acquisitions' THEN
        owner_partition := partition;
        owner := r->>'owner_id';
        owner_epoch := (r->>'epoch')::bigint;
    WHEN 'outbox' THEN
        -- Kafka uses the event ID as both the record key and the outbox PK.
        IF b IS NOT NULL THEN
            before_image := before_image || jsonb_build_object('key', b->'event_id',
                'payload_hash', encode(sha256(convert_to(b->>'payload', 'UTF8')), 'hex'));
        END IF;
        IF a IS NOT NULL THEN
            after_image := after_image || jsonb_build_object('key', a->'event_id',
                'payload_hash', encode(sha256(convert_to(a->>'payload', 'UTF8')), 'hex'));
        END IF;
    WHEN 'approval_action_intents' THEN
        IF b IS NOT NULL THEN
            before_image := before_image || jsonb_build_object('grant_token_hash',
                encode(sha256(convert_to(b->>'grant_token', 'UTF8')), 'hex'));
        END IF;
        IF a IS NOT NULL THEN
            after_image := after_image || jsonb_build_object('grant_token_hash',
                encode(sha256(convert_to(a->>'grant_token', 'UTF8')), 'hex'));
        END IF;
    ELSE
        NULL;
    END CASE;
    INSERT INTO engine.k6_capture_queue
        (subject, operation, row_key, workflow_id, partition_id, before_image, after_image,
         owner_partition, owner_id, epoch)
    VALUES (TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME, operation, k, workflow, partition,
            before_image, after_image, owner_partition, owner, owner_epoch);
    RETURN NULL;
END $$;

-- Argument 1 is the durable row key. Argument 2 is the public scalar image.
DO $triggers$
DECLARE
    spec record;
BEGIN
    FOR spec IN SELECT * FROM (VALUES
        ('engine.workflow_executions', 'workflow_id', 'state revision'),
        ('engine.transition_history', 'transition_id', 'revision scheduler_epoch reason'),
        ('engine.node_instances', 'workflow_id node_id iteration', 'state revision current_attempt_number retry_count timer_fired updated_at'),
        ('engine.activity_attempts', 'workflow_id node_id iteration attempt_number', 'state worker_id worker_request_id is_current effect_class outcome_disposition k6_fence heartbeat_deadline updated_at'),
        ('engine.attempt_result_evidence', 'evidence_id', 'node_id iteration attempt_number disposition'),
        ('engine.partition_leases', 'partition_id', 'owner_id epoch lease_expires_at updated_at'),
        ('engine.lease_acquisitions', 'acquisition_id', 'owner_id epoch'),
        ('engine.retry_policies', 'workflow_id node_id iteration', 'retries_used max_retries'),
        ('engine.dur050_admission_slots', 'workflow_id', ''),
        ('engine.timers', 'timer_id', 'node_id iteration purpose owning_revision consumed_at'),
        ('engine.cancellation_requests', 'request_id', 'status'),
        ('engine.approval_action_intents', 'intent_id', 'decision dispatch_status grant_scope_hash node_id iteration grant_expires_at proposal_hash approver_id decided_at'),
        ('engine.outbox', 'event_id', 'aggregate_revision topic event_type publish_state relay_attempts claim_owner'),
        ('engine.outbox_publications', 'publication_id', 'event_id relay_owner relay_attempt broker_acknowledged'),
        ('engine.event_inbox', 'consumer_id event_id', 'topic kafka_partition kafka_offset disposition'),
        ('engine.consumer_offsets', 'consumer_id topic kafka_partition', 'next_offset'),
        ('engine.scheduler_wakeups', 'wakeup_id', 'event_id state claim_owner claim_epoch'),
        ('engine.transport_quarantine', 'quarantine_id', 'consumer_id event_id topic kafka_partition kafka_offset'),
        ('engine.reconciliation_items', 'item_id', 'kind reference status node_id iteration attempt_number'),
        ('engine.effect_records', 'workflow_id logical_effect_key', 'outcome attempt_number'),
        ('effects.effect_records', 'workflow_id logical_effect_key', 'outcome attempt_number'),
        ('engine.effect_resolution_audit', 'resolution_id', 'logical_effect_key disposition'),
        ('effects.effect_resolution_audit', 'resolution_id', 'logical_effect_key disposition')
    ) AS t(subject, keys, fields) LOOP
        EXECUTE format('CREATE OR REPLACE TRIGGER k6_capture_row AFTER INSERT OR UPDATE OR DELETE ON %s
            FOR EACH ROW WHEN (current_setting(''durable.k4_campaign'', true) = ''on''
                AND current_setting(''durable.k6_capture'', true) = ''on'')
            EXECUTE FUNCTION engine.k6_capture_row(%L, %L)', spec.subject, spec.keys, spec.fields);
    END LOOP;
END $triggers$;

DROP TRIGGER IF EXISTS k6_write_immutable ON engine.k6_write_capture;
DROP TRIGGER IF EXISTS k6_owner_immutable ON engine.k6_owner_write_transactions;
CREATE TRIGGER k6_write_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON engine.k6_write_capture
    FOR EACH STATEMENT EXECUTE FUNCTION engine.k4_audit_immutable();
CREATE TRIGGER k6_owner_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON engine.k6_owner_write_transactions
    FOR EACH STATEMENT EXECUTE FUNCTION engine.k4_audit_immutable();

INSERT INTO engine.schema_migrations (version) VALUES (24) ON CONFLICT DO NOTHING;
COMMIT;
