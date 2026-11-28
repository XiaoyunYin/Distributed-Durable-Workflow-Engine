BEGIN;

CREATE TABLE engine.j4_provider_attempts (
    workflow_id text NOT NULL,
    node_id text NOT NULL CHECK (node_id = 'j4.recorded-llm'),
    iteration integer NOT NULL CHECK (iteration = 0),
    sequence integer NOT NULL CHECK (sequence BETWEEN 1 AND 4),
    attempt_number bigint NOT NULL,
    reserve_request_id text NOT NULL CHECK (reserve_request_id <> ''),
    call_id text NOT NULL UNIQUE,
    boot_id text NOT NULL CHECK (boot_id <> ''),
    issued_ns bigint NOT NULL CHECK (issued_ns > 0),
    deadline_ns bigint NOT NULL CHECK (deadline_ns - issued_ns = 2000000000),
    outcome text CHECK (outcome IN ('rate_limited', 'timeout', 'ambiguous', 'success', 'mismatch')),
    observed_ns bigint,
    retry_after_seconds integer NOT NULL DEFAULT 0 CHECK (retry_after_seconds >= 0),
    eligible_ns bigint,
    terminal_state text CHECK (terminal_state IN ('SUCCEEDED', 'FAILED_FINAL')),
    terminal_payload jsonb,
    PRIMARY KEY (workflow_id, node_id, iteration, sequence),
    UNIQUE (workflow_id, node_id, iteration, attempt_number, reserve_request_id),
    FOREIGN KEY (workflow_id, node_id, iteration, attempt_number)
        REFERENCES engine.activity_attempts (workflow_id, node_id, iteration, attempt_number)
        ON DELETE CASCADE,
    CHECK ((outcome IS NULL) = (observed_ns IS NULL)),
    CHECK (observed_ns IS NULL OR observed_ns >= issued_ns),
    CHECK ((terminal_state IS NULL) = (terminal_payload IS NULL)),
    CHECK (outcome IS NOT NULL OR (eligible_ns IS NULL AND terminal_state IS NULL)),
    CHECK (outcome IS NULL OR CASE
        WHEN outcome IN ('success', 'mismatch') THEN eligible_ns IS NULL AND terminal_state IS NOT NULL
        ELSE eligible_ns IS NOT NULL AND ((sequence = 4) = (terminal_state IS NOT NULL))
    END)
);

INSERT INTO engine.schema_migrations (version) VALUES (25);
COMMIT;
