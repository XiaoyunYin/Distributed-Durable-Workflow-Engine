package j7fences

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/engine"
	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/restoreledger"
	"durable-agent-execution-engine/internal/state"
)

func TestPostgresFenceScopeSnapshots(t *testing.T) {
	dsn := os.Getenv("DURABLE_DATABASE_URL")
	if dsn == "" {
		t.Skip("DURABLE_DATABASE_URL is required for the PostgreSQL fence fixture")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	defer cancel()
	store, err := state.NewFromURL(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	owner := state.NewID()
	var l state.Lease
	var id string
	for range 128 {
		candidate := "j7-fence-" + state.NewID()
		p, err := partition.ID(candidate)
		if err != nil {
			t.Fatal(err)
		}
		lease, ok, err := store.AcquireLease(ctx, int16(p), owner, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			id, l = candidate, lease
			break
		}
	}
	if id == "" {
		t.Fatal("no free PostgreSQL fixture partition")
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		ref := state.LeaseRef{PartitionID: l.PartitionID, OwnerID: l.OwnerID, Epoch: l.Epoch}
		if err := store.ReleaseLease(cleanup, ref); err != nil && !errors.Is(err, state.ErrLeaseNotOwned) {
			t.Errorf("release PostgreSQL fixture lease: %v", err)
		}
		for _, sql := range []string{"DELETE FROM engine.workflow_executions WHERE workflow_id=$1",
			"DELETE FROM engine.workflow_definitions WHERE definition_id=$1"} {
			if _, err := store.Pool().Exec(cleanup, sql, id); err != nil {
				t.Errorf("clean PostgreSQL fence fixture: %v", err)
			}
		}
	}()
	const activity = "pure.echo"
	if err := store.CreateDefinition(ctx, state.DefinitionInput{DefinitionID: id, Version: 1, DefinitionHash: id,
		Graph:            json.RawMessage(`{"entry":"pure.echo","nodes":[{"id":"pure.echo","kind":"activity","next":"done"},{"id":"done","kind":"success"}]}`),
		ActivityVersions: json.RawMessage(`{"pure.echo":"v1"}`), EffectClasses: json.RawMessage(`{"pure.echo":"PURE_ACTIVITY"}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateWorkflow(ctx, state.CreateWorkflowInput{WorkflowID: id, Namespace: id, SubmissionKey: id,
		SubmissionPayloadHash: "sub-v1:" + id, DefinitionID: id, DefinitionVersion: 1, PartitionID: l.PartitionID,
		InitialNodeID: activity, InitialInput: json.RawMessage(`{"echo":3}`), ActorID: id}); err != nil {
		t.Fatal(err)
	}
	var db Database
	if err := store.Pool().QueryRow(ctx, `SELECT (SELECT system_identifier FROM pg_control_system()),
		(SELECT timeline_id FROM pg_control_checkpoint())`).Scan(&db.SystemID, &db.Timeline); err != nil {
		t.Fatal(err)
	}
	j := journalFixture()
	probe, err := New(store, j, j.save)
	if err != nil {
		t.Fatal(err)
	}
	scope := Scope{Workflow: id, Namespace: id, Partition: l.PartitionID, Node: activity}
	held, err := probe.CaptureSourceOwner(ctx, scope, db)
	if err != nil || held.Lease.Owner != owner || held.Lease.Epoch != l.Epoch {
		t.Fatalf("capture actual held lease: %+v, %v", held, err)
	}
	runner := engine.New(store, nil)
	runner.OwnerID, runner.ActorID = owner, id
	runner.ExternalActivities = true
	if _, err := runner.Run(ctx, id); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimAttempt(ctx, state.ClaimInput{WorkflowID: id, NodeID: activity, ExpectedAttempt: 1,
		WorkerID: "j7-source-worker", RequestID: id, AttemptLease: workerLease})
	if err != nil {
		t.Fatal(err)
	}
	observed, err := probe.CaptureSourceClaim(ctx, scope, db)
	if err != nil || observed.Attempt.TokenSHA != restoreledger.SHA256([]byte(claim.ClaimToken)) ||
		bytes.Contains(j.files["j7-source-claim.json"], []byte(claim.ClaimToken)) {
		t.Fatalf("capture actual claim without credential bytes: %+v, %v", observed, err)
	}
}
