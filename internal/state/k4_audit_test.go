package state

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/migrations"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestK4AuditConfiguration(t *testing.T) {
	for _, flag := range []string{"", "0", "1", "bad"} {
		t.Setenv("DUR053_K4_CAMPAIGN", flag)
		t.Setenv("POD_UID", "pod")
		t.Setenv("CONTAINER_ID", "container")
		cfg, err := pgxpool.ParseConfig("postgres://unused:unused@localhost/unused")
		if err != nil {
			t.Fatal(err)
		}
		err = configureK4Audit(cfg)
		if flag == "bad" {
			if err == nil {
				t.Fatal("invalid K4 flag accepted")
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if (cfg.ConnConfig.RuntimeParams["durable.k4_campaign"] == "on") != (flag == "1") {
			t.Fatal("K4 audit flag changed defaults")
		}
		if flag == "1" && cfg.ConnConfig.RuntimeParams["durable.k4_session"] != K4Session {
			t.Fatal("K4 session identity missing")
		}
	}
	t.Setenv("DUR053_K4_CAMPAIGN", "1")
	t.Setenv("CONTAINER_ID", "")
	cfg, _ := pgxpool.ParseConfig("postgres://unused:unused@localhost/unused")
	if configureK4Audit(cfg) == nil {
		t.Fatal("K4 missing process identity accepted")
	}
}

func k4Store(t *testing.T) *Store {
	t.Helper()
	databaseURL := os.Getenv("DURABLE_DATABASE_URL")
	if databaseURL == "" {
		if os.Getenv("DURABLE_REQUIRE_DATABASE") == "1" || os.Getenv("DURABLE_RUN_INTEGRATION") == "1" {
			databaseURL = integrationDatabaseURL(t)
		} else {
			t.Skip("K4 DB tests require DURABLE_DATABASE_URL or service CI mode")
		}
	}
	// Isolated migrated DB: SQL mutations must exercise the changed migration,
	// rather than accidentally testing the CI service's preinstalled definition.
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close(context.Background()) })
	name := "k4_" + strings.ReplaceAll(NewID(), "-", "")
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), `DROP DATABASE `+pgx.Identifier{name}.Sanitize()+` WITH (FORCE)`); err != nil {
			t.Error(err)
		}
	})
	cfg := admin.Config().Copy()
	cfg.Database = name
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := migrations.SQL.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		sql, err := migrations.SQL.ReadFile(entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.PgConn().Exec(ctx, string(sql)).ReadAll(); err != nil {
			t.Fatal(entry.Name(), err)
		}
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DUR053_K4_CAMPAIGN", "1")
	t.Setenv("POD_UID", "k4-test-pod")
	t.Setenv("CONTAINER_ID", "k4-test-container")
	s, err := NewFromURL(context.Background(), cfg.ConnString())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

func TestK4HistoryLeaseJoin(t *testing.T) {
	s := k4Store(t)
	ctx := context.Background()
	owner := NewID()
	lease, ok, err := acquireTestLease(ctx, s, owner)
	if err != nil || !ok {
		t.Fatal(err)
	}
	ref := LeaseRef{PartitionID: lease.PartitionID, OwnerID: owner, Epoch: lease.Epoch}
	def := "k4-definition"
	if err := s.CreateDefinition(ctx, DefinitionInput{DefinitionID: def, Version: 1, DefinitionHash: "hash",
		Graph: []byte(`{"nodes":["root"]}`), ActivityVersions: []byte(`{"root":"1"}`), EffectClasses: []byte(`{"root":"PURE_ACTIVITY"}`)}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		var workflow string
		for {
			workflow = NewID()
			p, _ := partition.ID(workflow)
			if int16(p) == ref.PartitionID {
				break
			}
		}
		if _, err := s.CreateWorkflow(ctx, CreateWorkflowInput{WorkflowID: workflow, Namespace: "k4", SubmissionKey: workflow,
			SubmissionPayloadHash: "hash", DefinitionID: def, DefinitionVersion: 1, PartitionID: ref.PartitionID, InitialNodeID: "root", ActorID: "client"}); err != nil {
			t.Fatal(err)
		}
		if err := s.ApplyOwnerTransition(ctx, OwnerTransitionInput{Lease: ref, WorkflowID: workflow, ExpectedRevision: 1,
			NewState: StateWaitingActivity, ActorID: "scheduler-label-is-not-owner-uuid", Reason: "SCHEDULE_ACTIVITY", NodeID: "root"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.ReleaseLease(ctx, ref); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM engine.k4_history_order h
		JOIN engine.transition_history t USING (transition_id)
		JOIN engine.k4_lease_audit a ON a.owner=h.owner AND a.epoch=h.epoch AND a.kind='acquire'
		JOIN engine.k4_lease_audit r ON r.owner=h.owner AND r.epoch=h.epoch AND r.kind='release'
		WHERE t.scheduler_epoch IS NOT NULL AND h.owner=$1 AND a."order" < h."order" AND h."order" < r."order"
		AND h.partition=a.partition AND h.partition=r.partition`, owner).Scan(&count); err != nil || count != 2 {
		t.Fatal("K4 history/lease order or owner join missing", count, err)
	}
	var historyCount int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM engine.k4_history_order`).Scan(&historyCount); err != nil || historyCount != 4 {
		t.Fatal("K4 history audit omitted non-owner history", historyCount, err)
	}
	// Takeover after a released lease retains the existing epoch rule.
	nextOwner := NewID()
	next, acquired, err := s.AcquireLease(ctx, ref.PartitionID, nextOwner, time.Minute)
	if err != nil || !acquired || next.Epoch != ref.Epoch+1 {
		t.Fatal("K4 changed takeover", next, err)
	}
	if err := s.ReleaseLease(ctx, LeaseRef{PartitionID: next.PartitionID, OwnerID: nextOwner, Epoch: next.Epoch}); err != nil {
		t.Fatal(err)
	}
	// Flags off: neither acquire nor history instrumentation is active.
	t.Setenv("DUR053_K4_CAMPAIGN", "0")
	off, err := NewFromURL(ctx, s.pool.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	defer off.Close()
	offOwner := NewID()
	offLease, acquired, err := off.AcquireLease(ctx, ref.PartitionID, offOwner, time.Minute)
	if err != nil || !acquired {
		t.Fatal(err)
	}
	if err := off.ReleaseLease(ctx, LeaseRef{PartitionID: offLease.PartitionID, OwnerID: offOwner, Epoch: offLease.Epoch}); err != nil {
		t.Fatal(err)
	}
	if err := off.pool.QueryRow(ctx, `SELECT count(*) FROM engine.k4_audit WHERE owner_id=$1`, offOwner).Scan(&count); err != nil || count != 0 {
		t.Fatal("K4 audit active when flag off", count, err)
	}
}

func TestK4LeaseAuditAtomic(t *testing.T) {
	s := k4Store(t)
	ctx := context.Background()
	owner := NewID()
	lease, acquired, err := acquireTestLease(ctx, s, owner)
	if err != nil || !acquired {
		t.Fatal(lease, acquired, err)
	}
	ref := LeaseRef{PartitionID: lease.PartitionID, OwnerID: owner, Epoch: lease.Epoch}
	defer func() { _ = s.ReleaseLease(ctx, ref) }()
	var order int64
	var session, transaction, pod, container string
	err = s.pool.QueryRow(ctx, `SELECT event_order, session_id, transaction_id, pod_uid, container_id
        FROM engine.k4_audit WHERE owner_id=$1 AND kind='acquire' ORDER BY event_order DESC LIMIT 1`, owner).
		Scan(&order, &session, &transaction, &pod, &container)
	if err != nil || order <= 0 || session != K4Session || transaction == "" || pod != "k4-test-pod" || container != "k4-test-container" {
		t.Fatal("K4 lease audit row missing", err)
	}

	// Queueing an audit must not commit independently of its caller.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rollbackOwner := NewID()
	if err := s.recordK4Lease(ctx, tx, "acquire", LeaseRef{PartitionID: ref.PartitionID, OwnerID: rollbackOwner, Epoch: ref.Epoch}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM engine.k4_audit WHERE owner_id=$1`, rollbackOwner).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("K4 audit escaped lease transaction")
	}

	// A deferred audit failure must roll back the actual lease UPDATE too.
	if _, err := s.pool.Exec(ctx, `CREATE FUNCTION engine.k4_test_fail() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN RAISE EXCEPTION 'injected K4 audit failure'; END $$;
        CREATE TRIGGER k4_test_fail BEFORE INSERT ON engine.k4_audit FOR EACH ROW
        WHEN (NEW.kind='release' AND NEW.session_id='`+K4Session+`') EXECUTE FUNCTION engine.k4_test_fail()`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = s.pool.Exec(ctx, `DROP TRIGGER IF EXISTS k4_test_fail ON engine.k4_audit; DROP FUNCTION IF EXISTS engine.k4_test_fail()`)
	}()
	if err := s.ReleaseLease(ctx, ref); err == nil {
		t.Fatal("K4 failed audit allowed lease release")
	}
	var currentOwner string
	if err := s.pool.QueryRow(ctx, `SELECT owner_id::text FROM engine.partition_leases WHERE partition_id=$1`, ref.PartitionID).Scan(&currentOwner); err != nil || currentOwner != owner {
		t.Fatal("K4 audit failure did not roll back lease", err)
	}
	if _, err := s.pool.Exec(ctx, `DROP TRIGGER k4_test_fail ON engine.k4_audit; DROP FUNCTION engine.k4_test_fail()`); err != nil {
		t.Fatal(err)
	}

	// Same-owner AcquireLease is audited; RenewLease preserves epoch and needs no new acquire row.
	if _, ok, err := s.AcquireLease(ctx, ref.PartitionID, owner, time.Minute); err != nil || !ok {
		t.Fatal(err)
	}
	if _, err := s.RenewLease(ctx, ref, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseLease(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM engine.k4_lease_audit WHERE owner=$1`, owner).Scan(&count); err != nil || count != 3 {
		t.Fatal("K4 acquire/release audit incomplete", count, err)
	}
	if _, err := s.pool.Exec(ctx, `UPDATE engine.k4_audit SET epoch=epoch WHERE owner_id=$1`, owner); err == nil {
		t.Fatal("K4 audit is mutable")
	}
	export, err := s.ExportK4LeaseAudit(ctx, time.Unix(0, 0))
	if err != nil || export.Schema != "dur053.k4-lease-audit.v1" || len(export.Rows) != 3 {
		t.Fatal("K4 lease export incomplete", export, err)
	}
	for i, row := range export.Rows {
		if row.At <= 0 || row.Owner != owner || row.Transaction == "" || row.Session != K4Session || (i > 0 && row.Order <= export.Rows[i-1].Order) {
			t.Fatal("K4 lease export identity/order missing", row)
		}
	}
	if _, err := s.ExportK4LeaseAudit(ctx, time.Now().Add(time.Hour)); err == nil {
		t.Fatal("K4 export accepted invalid clock origin")
	}
}

func TestK4CommitOrder(t *testing.T) {
	s := k4Store(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	first, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Rollback(ctx) }()
	second, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Rollback(ctx) }()
	a, b := NewID(), NewID()
	if err := s.recordK4Lease(ctx, first, "acquire", LeaseRef{PartitionID: 0, OwnerID: a, Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.recordK4Lease(ctx, second, "release", LeaseRef{PartitionID: 0, OwnerID: b, Epoch: 1}); err != nil {
		t.Fatal("K4 audit order assigned before commit", err)
	}
	// The later-started transaction commits first, defeating allocation-at-write.
	if err := second.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := first.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var aOrder, bOrder int64
	if err := s.pool.QueryRow(ctx, `SELECT event_order FROM engine.k4_audit WHERE owner_id=$1`, a).Scan(&aOrder); err != nil {
		t.Fatal(err)
	}
	if err := s.pool.QueryRow(ctx, `SELECT event_order FROM engine.k4_audit WHERE owner_id=$1`, b).Scan(&bOrder); err != nil {
		t.Fatal(err)
	}
	if aOrder <= bOrder {
		t.Fatal("K4 audit order is not commit ordered")
	}
	var queued int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM engine.k4_audit_queue WHERE session_id=$1`, K4Session).Scan(&queued); err != nil || queued != 0 {
		t.Fatal("K4 queue did not drain", err)
	}
}

func TestK4CommitSerialization(t *testing.T) {
	s := k4Store(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	first, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Rollback(ctx) }()
	if err := s.recordK4Lease(ctx, first, "acquire", LeaseRef{PartitionID: 0, OwnerID: NewID(), Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Exec(ctx, `SET CONSTRAINTS ALL IMMEDIATE`); err != nil {
		t.Fatal(err)
	}
	second, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Rollback(ctx) }()
	if err := s.recordK4Lease(ctx, second, "acquire", LeaseRef{PartitionID: 1, OwnerID: NewID(), Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- second.Commit(ctx) }()
	select {
	case err := <-done:
		t.Fatal("K4 audit did not serialize through commit", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := first.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
