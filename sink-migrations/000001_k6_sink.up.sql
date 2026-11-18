BEGIN;

DO $roles$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'k6_sink_service') THEN
        CREATE ROLE k6_sink_service NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'k6_sink_exporter') THEN
        CREATE ROLE k6_sink_exporter NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'k6_sink_journal_owner') THEN
        CREATE ROLE k6_sink_journal_owner NOLOGIN;
    END IF;
    EXECUTE pg_catalog.format('REVOKE CONNECT, TEMPORARY ON DATABASE %I FROM PUBLIC', current_database());
    EXECUTE pg_catalog.format('GRANT CONNECT ON DATABASE %I TO k6_sink_service, k6_sink_exporter', current_database());
END
$roles$;

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
CREATE SCHEMA k6_sink;
REVOKE ALL ON SCHEMA k6_sink FROM PUBLIC;
GRANT USAGE ON SCHEMA k6_sink TO k6_sink_service, k6_sink_exporter, k6_sink_journal_owner;

CREATE TABLE k6_sink.resources (
    resource_id text PRIMARY KEY,
    state jsonb NOT NULL DEFAULT '{}'::jsonb,
    revision bigint NOT NULL DEFAULT 0 CHECK (revision >= 0),
    latest_fence bigint NOT NULL DEFAULT 0 CHECK (latest_fence >= 0),
    logical_key text,
    intent_id uuid
);

CREATE TABLE k6_sink.allocations (
    intent_id uuid NOT NULL,
    grant_hash text NOT NULL,
    attempt bigint NOT NULL CHECK (attempt > 0),
    resource_id text NOT NULL REFERENCES k6_sink.resources,
    fence bigint NOT NULL CHECK (fence > 0),
    PRIMARY KEY (intent_id, grant_hash, attempt),
    UNIQUE (resource_id, fence)
);

CREATE TABLE k6_sink.journal (
    resource_id text NOT NULL REFERENCES k6_sink.resources,
    old_revision bigint NOT NULL CHECK (old_revision >= 0),
    revision bigint NOT NULL CHECK (revision = old_revision + 1),
    old_state_hash text NOT NULL,
    state_hash text NOT NULL,
    logical_key text NOT NULL,
    intent_id uuid NOT NULL,
    PRIMARY KEY (resource_id, revision)
);

CREATE FUNCTION k6_sink.journal_resource_write() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $journal$
BEGIN
    IF NEW.latest_fence < OLD.latest_fence OR NEW.latest_fence - OLD.latest_fence > 1 THEN
        RAISE EXCEPTION 'invalid resource fence advance';
    END IF;
    IF (NEW.state, NEW.revision, NEW.logical_key, NEW.intent_id)
        IS DISTINCT FROM (OLD.state, OLD.revision, OLD.logical_key, OLD.intent_id) THEN
        INSERT INTO k6_sink.journal
            (resource_id, old_revision, revision, old_state_hash, state_hash, logical_key, intent_id)
        VALUES (NEW.resource_id, OLD.revision, NEW.revision,
            encode(sha256(convert_to(OLD.state::text, 'UTF8')), 'hex'),
            encode(sha256(convert_to(NEW.state::text, 'UTF8')), 'hex'),
            NEW.logical_key, NEW.intent_id);
    END IF;
    RETURN NEW;
END;
$journal$;
CREATE TRIGGER journal_resource_write AFTER UPDATE ON k6_sink.resources
FOR EACH ROW EXECUTE FUNCTION k6_sink.journal_resource_write();
REVOKE ALL ON FUNCTION k6_sink.journal_resource_write() FROM PUBLIC;
GRANT INSERT ON k6_sink.journal TO k6_sink_journal_owner;
ALTER FUNCTION k6_sink.journal_resource_write() OWNER TO k6_sink_journal_owner;

CREATE TABLE k6_sink.receipts (
    workflow_id text NOT NULL,
    logical_key text NOT NULL,
    resource_id text NOT NULL REFERENCES k6_sink.resources,
    canonical_hash text NOT NULL,
    scope_hash text NOT NULL,
    intent_id uuid NOT NULL,
    expected_revision bigint NOT NULL CHECK (expected_revision >= 0),
    applied_revision bigint NOT NULL CHECK (applied_revision = expected_revision + 1),
    application_id uuid NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    PRIMARY KEY (workflow_id, logical_key)
);

CREATE TABLE k6_sink.request_bindings (
    request_id text PRIMARY KEY,
    body_hash text NOT NULL
);

CREATE TABLE k6_sink.calls (
    call_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    operation text NOT NULL CHECK (operation IN ('apply', 'lookup', 'allocate')),
    workflow_id text NOT NULL,
    logical_key text NOT NULL,
    intent_id uuid NOT NULL,
    resource_id text NOT NULL,
    message_id text NOT NULL,
    attempt bigint NOT NULL CHECK (attempt > 0),
    request_id text NOT NULL REFERENCES k6_sink.request_bindings,
    body_hash text NOT NULL,
    connection_hash text NOT NULL,
    outcome text NOT NULL CHECK (outcome IN
        ('APPLIED', 'DUPLICATE', 'FOUND', 'MISSING', 'STALE_FENCE', 'REVISION_CONFLICT',
         'KEY_CONFLICT', 'REQUEST_CONFLICT', 'ALLOCATED')),
    application_id uuid REFERENCES k6_sink.receipts (application_id),
    response_hash text,
    observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((outcome IN ('APPLIED', 'DUPLICATE', 'FOUND')) = (application_id IS NOT NULL)),
    CHECK ((application_id IS NOT NULL) = (response_hash IS NOT NULL))
);

-- The service may create only revision-zero rows and can neither write the
-- journal directly nor change a recorded receipt, allocation, binding or call.
GRANT SELECT, INSERT (resource_id),
    UPDATE (state, revision, latest_fence, logical_key, intent_id)
    ON k6_sink.resources TO k6_sink_service;
GRANT SELECT, INSERT ON k6_sink.allocations, k6_sink.request_bindings TO k6_sink_service;
GRANT SELECT, INSERT (workflow_id, logical_key, resource_id, canonical_hash, scope_hash, intent_id,
    expected_revision, applied_revision) ON k6_sink.receipts TO k6_sink_service;
-- SELECT FOR UPDATE on immutable receipts is unnecessary: the resource and a
-- transaction-scoped stable-key lock serialize all receipt writers.
GRANT INSERT (operation, workflow_id, logical_key, intent_id, resource_id, message_id,
    attempt, request_id, body_hash, connection_hash, outcome, application_id, response_hash)
    ON k6_sink.calls TO k6_sink_service;
CREATE VIEW k6_sink.resource_states AS
SELECT resource_id, revision, latest_fence,
    encode(sha256(convert_to('{}'::jsonb::text, 'UTF8')), 'hex') AS initial_state_hash,
    encode(sha256(convert_to(state::text, 'UTF8')), 'hex') AS final_state_hash
FROM k6_sink.resources;
GRANT SELECT ON k6_sink.resource_states TO k6_sink_exporter;
GRANT SELECT ON k6_sink.journal, k6_sink.receipts, k6_sink.calls, k6_sink.request_bindings,
    k6_sink.allocations TO k6_sink_exporter;

COMMIT;
