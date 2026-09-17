BEGIN;

ALTER TABLE engine.event_inbox
    DROP CONSTRAINT IF EXISTS event_inbox_event_id_fkey;

ALTER TABLE engine.event_inbox
    ADD CONSTRAINT event_inbox_event_id_fkey
    FOREIGN KEY (event_id) REFERENCES engine.outbox (event_id)
    ON DELETE CASCADE;

INSERT INTO engine.schema_migrations (version) VALUES (7)
ON CONFLICT (version) DO NOTHING;

COMMIT;
