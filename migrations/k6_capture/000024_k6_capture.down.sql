BEGIN;
-- Dropping the capture function removes its episode-only triggers.
DROP FUNCTION IF EXISTS engine.k6_capture_row() CASCADE;
DROP FUNCTION IF EXISTS engine.k6_capture_fields(jsonb, text);
DROP TABLE IF EXISTS engine.k6_capture_queue, engine.k6_owner_write_transactions, engine.k6_write_capture;
DROP FUNCTION IF EXISTS engine.k6_finalize_capture();
DELETE FROM engine.schema_migrations WHERE version = 24;
COMMIT;
