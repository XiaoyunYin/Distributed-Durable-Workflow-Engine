package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/invariants"
	"durable-agent-execution-engine/internal/state"
	"durable-agent-execution-engine/internal/telemetry"
)

// TestDUR036TelemetryReconstruction is a bounded readiness check, not a
// measurement run. It emits the telemetry snapshots and independent durable
// invariant verdicts used by scripts/m7-readiness.ps1 for one normal execution
// and one crash/resume episode.
func TestDUR036TelemetryReconstruction(t *testing.T) {
	ctx, store := openM1Database(t)
	defer store.Close()
	metrics := telemetry.New("dur036-readiness")
	store.SetTelemetry(metrics)
	time.Sleep(1100 * time.Millisecond)
	metrics.MarkDurableReady()

	driver := ActivityDriverFunc(func(context.Context, Activity) (ActivityResult, error) {
		return ActivityResult{Payload: []byte(`{"ok":true}`)}, nil
	})
	normalDefinition := "dur036-normal-" + state.NewID()
	normalWorkflow, normalLease := createM1Workflow(t, ctx, store, normalDefinition,
		`{"entry":"root","nodes":[{"id":"root","kind":"activity","next":"done"},{"id":"done","kind":"success"}]}`,
		`{"root":"PURE_ACTIVITY"}`)
	defer cleanupReadinessFixture(t, ctx, store, normalWorkflow, normalDefinition, normalLease)
	normalEngine := New(store, driver)
	normalEngine.OwnerID = normalLease.OwnerID
	normalResult, err := normalEngine.Run(ctx, normalWorkflow)
	if err != nil || normalResult.Blocked || normalResult.Workflow.State != state.StateSucceeded {
		t.Fatalf("normal readiness run = %+v, err=%v", normalResult, err)
	}
	normalTrace, err := invariants.Load(ctx, store, []string{normalWorkflow})
	if err != nil {
		t.Fatal(err)
	}
	normalVerdict := invariants.Check(normalTrace)
	if !normalVerdict.Valid {
		t.Fatalf("normal readiness invariant violations: %v", normalVerdict.Violations)
	}
	t.Logf("DUR036 normal workflow=%s state=%s telemetry=%+v invariant_valid=%t", normalWorkflow, normalResult.Workflow.State, metrics.Snapshot(), normalVerdict.Valid)

	faultDefinition := "dur036-fault-" + state.NewID()
	faultWorkflow, faultLease := createM1Workflow(t, ctx, store, faultDefinition,
		`{"entry":"root","nodes":[{"id":"root","kind":"activity","next":"done"},{"id":"done","kind":"success"}]}`,
		`{"root":"PURE_ACTIVITY"}`)
	defer cleanupReadinessFixture(t, ctx, store, faultWorkflow, faultDefinition, faultLease)
	faultEngine := New(store, driver)
	faultEngine.OwnerID = faultLease.OwnerID
	faultEngine.AfterBoundary = func(boundary string) error {
		if boundary == "result_recorded" {
			return errors.New("dur036 injected crash")
		}
		return nil
	}
	if _, err := faultEngine.Run(ctx, faultWorkflow); err == nil {
		t.Fatal("fault readiness run did not stop at result_recorded")
	}
	resumeEngine := New(store, driver)
	resumeEngine.OwnerID = faultLease.OwnerID
	resumed, err := resumeEngine.Run(ctx, faultWorkflow)
	if err != nil || resumed.Blocked || resumed.Workflow.State != state.StateSucceeded {
		t.Fatalf("fault resume = %+v, err=%v", resumed, err)
	}
	faultTrace, err := invariants.Load(ctx, store, []string{faultWorkflow})
	if err != nil {
		t.Fatal(err)
	}
	faultVerdict := invariants.Check(faultTrace)
	if !faultVerdict.Valid {
		t.Fatalf("fault readiness invariant violations: %v", faultVerdict.Violations)
	}
	t.Logf("DUR036 fault workflow=%s boundary=result_recorded resumed_state=%s telemetry=%+v invariant_valid=%t", faultWorkflow, resumed.Workflow.State, metrics.Snapshot(), faultVerdict.Valid)
}

func cleanupReadinessFixture(t *testing.T, ctx context.Context, store *state.Store, workflowID, definitionID string, lease state.Lease) {
	t.Helper()
	_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_executions WHERE workflow_id = $1`, workflowID)
	_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_definitions WHERE definition_id = $1`, definitionID)
	_ = store.ReleaseLease(ctx, state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch})
}
