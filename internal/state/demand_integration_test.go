package state

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/telemetry"
)

func k2Store(t *testing.T) (*Store, context.Context) {
	t.Helper()
	url := integrationDatabaseURL(t)
	store, err := NewFromURL(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	return store, context.Background()
}

// Fixture deliberately writes isolated rows directly so its expected count does
// not depend on transition validators or the production demand predicates.
func k2DemandFixture(t *testing.T, store *Store, namespace string, age int) string {
	t.Helper()
	ctx := context.Background()
	id := "k2-demand-" + NewID()
	mapped, _ := partition.ID(id)
	err := store.CreateDefinition(ctx, DefinitionInput{DefinitionID: id, Version: 1, DefinitionHash: id,
		Graph:            []byte(`{"entry":"pure.echo","nodes":[{"id":"pure.echo","kind":"activity"}]}`),
		ActivityVersions: []byte(`{"pure.echo":"v1"}`), EffectClasses: []byte(`{"pure.echo":"PURE_ACTIVITY"}`)})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.CreateWorkflow(ctx, CreateWorkflowInput{WorkflowID: id, Namespace: namespace, SubmissionKey: id, SubmissionPayloadHash: id,
		DefinitionID: id, DefinitionVersion: 1, PartitionID: int16(mapped), InitialNodeID: "pure.echo"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := store.pool.Exec(ctx, `DELETE FROM engine.workflow_executions WHERE workflow_id=$1`, id); err != nil {
			t.Error(err)
		}
		if _, err := store.pool.Exec(ctx, `DELETE FROM engine.workflow_definitions WHERE definition_id=$1`, id); err != nil {
			t.Error(err)
		}
	})
	for _, q := range []string{
		`UPDATE engine.workflow_executions SET state='WAITING_ACTIVITY' WHERE workflow_id=$1`,
		`UPDATE engine.node_instances SET state='WAITING_ACTIVITY',current_attempt_number=1 WHERE workflow_id=$1`,
		`INSERT INTO engine.activity_attempts(workflow_id,node_id,iteration,attempt_number,state,effect_class,created_at,heartbeat_deadline)
          VALUES($1,'pure.echo',0,1,'DISPATCHABLE','PURE_ACTIVITY',clock_timestamp()-($2::int * interval '1 second'),clock_timestamp()+interval '1 minute')`,
	} {
		var err error
		if strings.Contains(q, "$2") {
			_, err = store.pool.Exec(ctx, q, id, age)
		} else {
			_, err = store.pool.Exec(ctx, q, id)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	return id
}

func TestK2DurableDemand(t *testing.T) {
	store, ctx := k2Store(t)
	ns := "k2-demand-" + NewID()
	store.SetTelemetry(telemetry.New("test"))
	a := k2DemandFixture(t, store, ns, 120)
	var b string
	for {
		b = k2DemandFixture(t, store, ns, 20)
		pa, _ := partition.ID(a)
		pb, _ := partition.ID(b)
		if pa != pb {
			break
		}
		// Keep any same-partition fixture out of the namespace being measured.
		_, err := store.pool.Exec(ctx, `UPDATE engine.workflow_executions SET namespace=namespace||'-excluded' WHERE workflow_id=$1`, b)
		if err != nil {
			t.Fatal(err)
		}
	}
	assert := func(active, claimable int64, minAge, maxAge float64) {
		t.Helper()
		got, err := store.DurableDemand(ctx, ns)
		if err != nil || got.Namespace != ns || got.SampledAt.IsZero() || got.ActiveClaims != active || got.ClaimableAttempts != claimable || got.Demand != active+claimable || got.OldestClaimableAgeSeconds < minAge || got.OldestClaimableAgeSeconds > maxAge {
			t.Fatalf("demand contract violated: %+v err=%v want active=%d claimable=%d age=%f..%f", got, err, active, claimable, minAge, maxAge)
		}
	}
	assert(0, 2, 120, 125)
	canceled, stop := context.WithCancel(ctx)
	stop()
	if _, err := store.DurableDemand(canceled, ns); err == nil {
		t.Fatal("demand database error swallowed")
	}
	// Partition reads in either order cannot publish a last-partition gauge.
	for _, id := range []string{a, b, b, a} {
		p, _ := partition.ID(id)
		if _, err := store.GetBacklog(ctx, int16(p)); err != nil {
			t.Fatal(err)
		}
	}
	if strings.Contains(store.Telemetry().Render(), "durable_reconciliation_oldest_age_seconds") {
		t.Fatal("misleading backlog gauge retained")
	}
	assert(0, 2, 120, 125)
	// Many pending/published redispatch notifications must not inflate demand.
	for i := 0; i < 4; i++ {
		_, err := store.pool.Exec(ctx, `INSERT INTO engine.outbox(event_id,workflow_id,aggregate_revision,event_type,topic,schema_version,payload,publish_state)
            VALUES($1,$2,1,'attempt.redispatch',$3,1,'{}',$4)`, NewID(), a, TaskTopic, []string{"PENDING", "PUBLISHED"}[i%2])
		if err != nil {
			t.Fatal(err)
		}
	}
	assert(0, 2, 120, 125)
	// Active claims count even with every task notification deleted (lag-zero).
	_, err := store.pool.Exec(ctx, `UPDATE engine.activity_attempts SET state='CLAIMED', claim_token=$2 WHERE workflow_id=$1`, a, NewID())
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.pool.Exec(ctx, `DELETE FROM engine.outbox WHERE workflow_id=$1`, a)
	if err != nil {
		t.Fatal(err)
	}
	assert(1, 1, 20, 25)
	_, err = store.pool.Exec(ctx, `UPDATE engine.activity_attempts SET heartbeat_deadline=clock_timestamp()-interval '1 second' WHERE workflow_id=$1`, a)
	if err != nil {
		t.Fatal(err)
	}
	assert(0, 1, 20, 25)
	// A dispatch lease expiry does not stop a still-current DISPATCHABLE claim.
	_, err = store.pool.Exec(ctx, `UPDATE engine.activity_attempts SET heartbeat_deadline=clock_timestamp()-interval '1 second' WHERE workflow_id=$1`, b)
	if err != nil {
		t.Fatal(err)
	}
	assert(0, 1, 20, 25)
	_, err = store.pool.Exec(ctx, `UPDATE engine.node_instances SET current_attempt_number=NULL WHERE workflow_id=$1`, b)
	if err != nil {
		t.Fatal(err)
	}
	assert(0, 0, 0, 0)
	_, err = store.pool.Exec(ctx, `UPDATE engine.node_instances SET current_attempt_number=1 WHERE workflow_id=$1`, b)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.pool.Exec(ctx, `UPDATE engine.activity_attempts SET is_current=false WHERE workflow_id=$1`, b)
	if err != nil {
		t.Fatal(err)
	}
	assert(0, 0, 0, 0)
	_, err = store.pool.Exec(ctx, `UPDATE engine.activity_attempts SET is_current=true WHERE workflow_id=$1`, b)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.pool.Exec(ctx, `UPDATE engine.workflow_executions SET state='SUCCEEDED' WHERE workflow_id=$1`, b)
	if err != nil {
		t.Fatal(err)
	}
	assert(0, 0, 0, 0)
	k2DemandFixture(t, store, ns+"-other", 600)
	assert(0, 0, 0, 0)
	if _, err := store.DurableDemand(ctx, ""); err == nil {
		t.Fatal("demand namespace guard bypassed")
	}
	// Exhaust the dedicated pool: acquisition is included in the deadline.
	reader, err := NewDemandFromURL(ctx, integrationDatabaseURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var timeout string
	if err := reader.pool.QueryRow(ctx, `SHOW statement_timeout`).Scan(&timeout); err != nil || timeout != "2s" {
		t.Fatalf("demand statement timeout bypassed: %s %v", timeout, err)
	}
	if reader.pool.Config().MaxConns != 2 {
		t.Fatal("demand connection budget bypassed")
	}
	if _, err := reader.pool.Exec(ctx, `CREATE TABLE engine.k2_forbidden_write(id int)`); err == nil {
		t.Fatal("demand read-only guard bypassed")
	}
	one, err := reader.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer one.Release()
	two, err := reader.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer two.Release()
	deadline, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	started := time.Now()
	if _, err := reader.DurableDemand(deadline, ns); err == nil || time.Since(started) > 3*time.Second {
		t.Fatal("demand query bound bypassed")
	}
}

func TestK2DemandMigrationRollback(t *testing.T) {
	store, ctx := k2Store(t)
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	// Strip transaction wrappers so rollback verification is fully isolated.
	migration := func(suffix string) {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join("..", "..", "migrations", "000019_k2_demand."+suffix+".sql"))
		if err != nil {
			t.Fatal(err)
		}
		q := strings.ReplaceAll(strings.ReplaceAll(string(raw), "BEGIN;", ""), "COMMIT;", "")
		if _, err := tx.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	check := func(want bool) {
		t.Helper()
		var attempts, workflows, version bool
		err := tx.QueryRow(ctx, `SELECT to_regclass('engine.activity_attempts_demand') IS NOT NULL, to_regclass('engine.workflow_executions_demand') IS NOT NULL, EXISTS(SELECT 1 FROM engine.schema_migrations WHERE version=19)`).Scan(&attempts, &workflows, &version)
		if err != nil || attempts != want || workflows != want || version != want {
			t.Fatalf("additive migration rollback violated: attempts=%v workflows=%v version=%v want=%v err=%v", attempts, workflows, version, want, err)
		}
	}
	check(true)
	migration("down")
	check(false)
	var count int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM engine.activity_attempts`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	migration("up")
	check(true)
}
