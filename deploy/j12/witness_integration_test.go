package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestJ12WitnessSQLOnPostgres(t *testing.T) {
	databaseURL := os.Getenv("J12_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("requires a disposable base-only J12 PostgreSQL database")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(context.Background())
	var base bool
	if err := admin.QueryRow(ctx, `SELECT (SELECT array_agg(version ORDER BY version) FROM engine.schema_migrations)=ARRAY(SELECT generate_series(1,23)::bigint)
		AND NOT EXISTS (SELECT 1 FROM engine.workflow_executions)
		AND NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='j12_witness')`).Scan(&base); err != nil || !base {
		t.Fatalf("J12 tooling database was not fresh: %v", err)
	}
	execFile := func(ctx context.Context, name string) error {
		raw, err := os.ReadFile(filepath.Join("../..", name))
		if err != nil {
			return err
		}
		_, err = admin.Exec(ctx, string(raw))
		return err
	}
	if err := execFile(ctx, requesterMigration); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := admin.Exec(cleanup, `DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='j12_witness') THEN
			DROP OWNED BY j12_witness; DROP ROLE j12_witness; END IF; END $$`); err != nil {
			t.Errorf("remove displaced tooling observer: %v", err)
		}
		if err := execFile(cleanup, "migrations/j12/000026_j12_workflow_requesters.down.sql"); err != nil {
			t.Errorf("remove J12 tooling migration: %v", err)
		}
	}()
	observer, err := openObserver(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close(context.Background())
	var identity, mode string
	if err := observer.QueryRow(ctx, `SELECT current_user,current_setting('transaction_read_only')`).Scan(&identity, &mode); err != nil || identity != "j12_witness" || mode != "on" {
		t.Fatalf("wrong tooling observer identity or mode: %s %s %v", identity, mode, err)
	}
	s, raw, err := readSnapshot(ctx, observer)
	if err != nil || len(raw) == 0 || !s.noEffects() || len(s.Requesters) != 0 || len(s.Approvals) != 0 {
		t.Fatalf("base J12 SQL witness had unexpected state: %v", err)
	}
	if _, err := observer.Exec(ctx, `DELETE FROM engine.j12_workflow_requesters WHERE false`); err == nil {
		t.Fatal("observer could write to requester table")
	}
}
