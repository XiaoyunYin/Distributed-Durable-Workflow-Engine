\set ON_ERROR_STOP on

SELECT format('CREATE ROLE dur050_observer LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD %L', :'observer_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dur050_observer')
\gexec
SELECT format('ALTER ROLE dur050_observer LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 2 PASSWORD %L', :'observer_password')
\gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO dur050_observer', :'postgres_db')
\gexec

ALTER ROLE dur050_observer SET default_transaction_read_only = on;
GRANT USAGE ON SCHEMA engine TO dur050_observer;
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA engine FROM dur050_observer;
GRANT SELECT (workflow_id, namespace, state, created_at, updated_at)
    ON engine.workflow_executions TO dur050_observer;
GRANT SELECT (workflow_id, revision, new_state, created_at, reason)
    ON engine.transition_history TO dur050_observer;
