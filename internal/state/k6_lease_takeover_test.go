//go:build dur053_k6

package state

import (
	"context"
	"errors"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/partition"
)

func TestK6LeaseTakeoverPinsIdleBackendAndFencesOriginalAttempt(t *testing.T) {
	store := k4Store(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const workflow = "k6-e2-boundary"
	p, err := partition.ID(workflow)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateDefinition(ctx, DefinitionInput{
		DefinitionID: "e2", Version: 1, DefinitionHash: "e2",
		Graph: []byte(`{"nodes":["root"]}`), ActivityVersions: []byte(`{"root":"1"}`),
		EffectClasses: []byte(`{"root":"PURE_ACTIVITY"}`),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateWorkflow(ctx, CreateWorkflowInput{
		WorkflowID: workflow, Namespace: "e2", SubmissionKey: workflow, SubmissionPayloadHash: "e2",
		DefinitionID: "e2", DefinitionVersion: 1, PartitionID: int16(p),
		InitialNodeID: "root", ActorID: "client",
	}); err != nil {
		t.Fatal(err)
	}
	owner := NewID()
	first, ok, err := store.AcquireLease(ctx, int16(p), owner, 2*time.Second)
	if err != nil || !ok {
		t.Fatalf("predecessor acquisition: %v, %v", ok, err)
	}
	ref := LeaseRef{PartitionID: int16(p), OwnerID: owner, Epoch: first.Epoch}
	waiting := StateWaitingActivity
	if err := store.ApplyOwnerTransition(ctx, OwnerTransitionInput{
		Lease: ref, WorkflowID: workflow, ExpectedRevision: 1,
		NewState: waiting, NodeState: &waiting, NodeID: "root", Reason: "ACTIVITY_SCHEDULED", ActorID: "scheduler",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `
		CREATE TABLE engine.e2_attempt_backend (pid integer);
		CREATE FUNCTION engine.e2_record_attempt_backend() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN INSERT INTO engine.e2_attempt_backend VALUES (pg_backend_pid()); RETURN NEW; END $$;
		CREATE TRIGGER e2_attempt_backend AFTER INSERT ON engine.activity_attempts
			FOR EACH ROW EXECUTE FUNCTION engine.e2_record_attempt_backend();
	`); err != nil {
		t.Fatal(err)
	}
	input := AttemptInput{
		Lease: ref, WorkflowID: workflow, ExpectedRevision: 2, NodeID: "root",
		EffectClass: EffectPure, ActorID: "scheduler",
	}
	var successor Lease
	peer := NewID()
	_, err = store.CreateAttemptForTakeover(ctx, input, func(oldBackend LeaseTakeoverBackend) (context.Context, error) {
		var status, application string
		var inTransaction bool
		if err := store.pool.QueryRow(ctx, `SELECT state, application_name, xact_start IS NOT NULL
			FROM pg_stat_activity WHERE pid = $1`, oldBackend.PID).Scan(&status, &application, &inTransaction); err != nil {
			t.Fatal(err)
		}
		if status != "idle" || inTransaction || application != "dur053-e2-"+K4Session || oldBackend.StartUS <= 0 {
			t.Fatalf("predecessor backend state = %q app=%q transaction=%v backend=%+v",
				status, application, inTransaction, oldBackend)
		}
		if _, won, err := store.AcquireLease(ctx, int16(p), peer, time.Minute); err != nil || won {
			t.Fatalf("peer won before expiry: %v, %v", won, err)
		}
		for {
			dbNow, err := store.DatabaseTime(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !dbNow.Before(first.LeaseExpiresAt) {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		next, won, backend, xid, err := store.AcquireLeaseForTakeover(ctx, int16(p), peer, time.Minute)
		if err != nil || !won || next.Epoch != first.Epoch+1 || backend.PID == oldBackend.PID {
			t.Fatalf("normal takeover = %+v, %v, %+v, %v", next, won, backend, err)
		}
		var auditXID string
		if err := store.pool.QueryRow(ctx, `SELECT transaction_id FROM engine.k4_audit
			WHERE kind='acquire' AND owner_id=$1 AND epoch=$2`, peer, next.Epoch).Scan(&auditXID); err != nil || xid != auditXID {
			t.Fatalf("successor xid: hook=%q committed=%q error=%v", xid, auditXID, err)
		}
		successor = next
		return ctx, nil
	})
	if !errors.Is(err, ErrLeaseNotOwned) {
		t.Fatalf("original pending CreateAttempt = %v, want typed ErrLeaseNotOwned", err)
	}
	wf, err := store.GetWorkflow(ctx, workflow)
	if err != nil || wf.Revision != 2 {
		t.Fatalf("rejected attempt changed workflow revision: %+v, %v", wf, err)
	}
	input.Lease = LeaseRef{PartitionID: int16(p), OwnerID: peer, Epoch: successor.Epoch}
	var pinned int
	if _, err := store.CreateAttemptForTakeover(ctx, input, func(backend LeaseTakeoverBackend) (context.Context, error) {
		pinned = backend.PID
		return ctx, nil
	}); err != nil {
		t.Fatal(err)
	}
	var actual int
	if err := store.pool.QueryRow(ctx, "SELECT pid FROM engine.e2_attempt_backend").Scan(&actual); err != nil || actual != pinned {
		t.Fatalf("CreateAttempt backend: before=%d committed=%d err=%v", pinned, actual, err)
	}
}
