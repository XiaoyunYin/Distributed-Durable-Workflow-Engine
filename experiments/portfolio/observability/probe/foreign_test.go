package probe_test

import (
	"context"
	"os"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/reconciliation"
	"durable-agent-execution-engine/internal/state"
)

// The serving scheduler runs its own repair loop. This exercises the separate
// reconciler entry point under a real partition lease while that loop is live.
func TestForeignDemandAndReconciler(t *testing.T) {
	dsn := os.Getenv("J3_PROBE_DSN")
	if dsn == "" {
		t.Skip("hosted overlay database required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	store, err := state.NewCampaignFromURL(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	foreign, err := store.DurableDemand(ctx, "j3-foreign")
	if err != nil || foreign.Namespace != "j3-foreign" || foreign.ActiveClaims != 0 ||
		foreign.ClaimableAttempts != 1 || foreign.OldestClaimableAgeSeconds <= 0 {
		t.Fatalf("foreign demand mismatch: %+v, %v", foreign, err)
	}
	t.Logf("real demand query: %+v", foreign)
	part, err := partition.ID("j3-fixture-foreign-1")
	if err != nil {
		t.Fatal(err)
	}
	reconciler := reconciliation.New(store, reconciliation.Config{})
	for ctx.Err() == nil {
		lease, won, err := store.AcquireLease(ctx, int16(part), state.NewID(), 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if !won {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		ref := state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch}
		report, err := reconciler.RunOnce(ctx, ref, "j3-pilot-reconciler")
		released := store.ReleaseLease(ctx, ref)
		if err != nil || released != nil || report.ExpiredAttempts != 0 || report.TimedOut != 0 {
			t.Fatalf("reconciler report %+v, run %v, release %v", report, err, released)
		}
		t.Logf("partition %d reconciler: %+v", part, report)
		return
	}
	t.Fatal("reconciler could not acquire the fixture partition")
}
