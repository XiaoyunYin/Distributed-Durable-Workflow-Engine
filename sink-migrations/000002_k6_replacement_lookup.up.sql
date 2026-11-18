BEGIN;

-- One missing lookup under the current allocation permits a replacement
-- application. The original call remains in the independently exported log.
CREATE TABLE k6_sink.lookup_misses (
    intent_id uuid NOT NULL,
    grant_hash text NOT NULL,
    attempt bigint NOT NULL CHECK (attempt > 1),
    PRIMARY KEY (intent_id, grant_hash, attempt),
    FOREIGN KEY (intent_id, grant_hash, attempt)
        REFERENCES k6_sink.allocations (intent_id, grant_hash, attempt)
);
REVOKE ALL ON k6_sink.lookup_misses FROM PUBLIC;
GRANT SELECT, INSERT ON k6_sink.lookup_misses TO k6_sink_service;

COMMIT;
