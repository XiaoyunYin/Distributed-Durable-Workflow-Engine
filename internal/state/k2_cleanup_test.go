package state

import (
	"context"
	"github.com/jackc/pgx/v5"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// Reopening is deferred before the deliberate failure so fixture t.Cleanup
// callbacks can delete their rows on both the passing and mutated paths.
func reopenK2FixturePool(t *testing.T, ctx context.Context, store *Store) {
	t.Helper()
	cleanupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	replacement, err := NewFromURL(cleanupCtx, integrationDatabaseURL(t))
	if err != nil {
		t.Error(err)
		return
	}
	store.pool = replacement.pool
}

func TestK2cClosedPoolCleanup(t *testing.T) {
	if namespace := os.Getenv("DUR053_CLEANUP_CHILD_NAMESPACE"); namespace != "" {
		store, ctx := k2Store(t)
		k2DemandFixture(t, store, namespace, 1)
		defer reopenK2FixturePool(t, ctx, store)
		store.pool.Close()
		t.Fatal("injected closed-pool assertion failure")
	}
	store, ctx := k2Store(t)
	namespace := "k2c-cleanup-" + NewID()
	// The parent independently removes a deliberately leaked mutant fixture,
	// so an expected failed child cannot contaminate the restored baseline.
	t.Cleanup(func() {
		rows, err := store.pool.Query(ctx, `SELECT definition_id FROM engine.workflow_executions WHERE namespace=$1`, namespace)
		if err != nil {
			t.Error(err)
			return
		}
		ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			t.Error(err)
			return
		}
		if _, err := store.pool.Exec(ctx, `DELETE FROM engine.workflow_executions WHERE namespace=$1`, namespace); err != nil {
			t.Error(err)
		}
		if _, err := store.pool.Exec(ctx, `DELETE FROM engine.workflow_definitions WHERE definition_id=ANY($1::text[])`, ids); err != nil {
			t.Error(err)
		}
	})
	command := exec.Command(os.Args[0], "-test.run=^TestK2cClosedPoolCleanup$", "-test.v")
	command.Env = append(os.Environ(), "DUR053_CLEANUP_CHILD_NAMESPACE="+namespace)
	output, err := command.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "injected closed-pool assertion failure") {
		t.Fatalf("cleanup failure control did not execute: %v %s", err, output)
	}
	var count int
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM engine.workflow_executions WHERE namespace=$1`, namespace).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("closed-pool cleanup leaked fixture: %d rows", count)
	}
}
