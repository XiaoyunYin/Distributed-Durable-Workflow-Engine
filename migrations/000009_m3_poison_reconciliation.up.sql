BEGIN;

-- Keep a poison delivery linked to its workflow when the event ID is known.
-- Unknown IDs remain global evidence and are exposed by the transport poison
-- scan; they cannot safely be attached to an arbitrary workflow.
ALTER TABLE engine.transport_quarantine
    ADD COLUMN IF NOT EXISTS workflow_id text,
    ADD COLUMN IF NOT EXISTS partition_id smallint;

-- A broker record with no trustworthy event ID has no workflow or partition.
-- Keep its POISON_RECORD obligation global instead of attaching it to guessed
-- work. Existing workflow-scoped items retain their foreign-key protection.
ALTER TABLE engine.reconciliation_items
    ALTER COLUMN workflow_id DROP NOT NULL,
    ALTER COLUMN partition_id DROP NOT NULL;

CREATE INDEX IF NOT EXISTS transport_quarantine_workflow
    ON engine.transport_quarantine (workflow_id, partition_id, recorded_at)
    WHERE workflow_id IS NOT NULL;

INSERT INTO engine.schema_migrations (version) VALUES (9)
ON CONFLICT (version) DO NOTHING;

COMMIT;
