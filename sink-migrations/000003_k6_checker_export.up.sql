BEGIN;

-- Extend the safe view without granting access to raw resource state.
CREATE OR REPLACE VIEW k6_sink.resource_states AS
SELECT resource_id, revision, latest_fence,
    encode(sha256(convert_to('{}'::jsonb::text, 'UTF8')), 'hex') AS initial_state_hash,
    encode(sha256(convert_to(state::text, 'UTF8')), 'hex') AS final_state_hash,
    logical_key, intent_id
FROM k6_sink.resources;

-- grant_hash is already a SHA-256 digest.
GRANT SELECT (intent_id, grant_hash, attempt) ON k6_sink.lookup_misses TO k6_sink_exporter;
GRANT SELECT (resource_id) ON k6_sink.resources TO k6_sink_exporter;

COMMIT;
