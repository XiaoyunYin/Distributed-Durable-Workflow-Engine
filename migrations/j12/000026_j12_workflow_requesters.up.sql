BEGIN;

CREATE TABLE engine.j12_workflow_requesters (
    workflow_id text PRIMARY KEY REFERENCES engine.workflow_executions (workflow_id) ON DELETE CASCADE,
    issuer text NOT NULL CHECK (length(issuer) BETWEEN 1 AND 255),
    subject text NOT NULL CHECK (length(subject) BETWEEN 1 AND 255)
);

INSERT INTO engine.schema_migrations (version) VALUES (26);
COMMIT;
