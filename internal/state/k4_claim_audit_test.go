package state

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"
)

func TestK4ClaimAuditAtomic(t *testing.T) {
	s := k4Store(t) // R278: creates, migrates, connects to and asserts the isolated DB.
	ctx := context.Background()
	var database string
	if err := s.pool.QueryRow(ctx, `SELECT current_database()`).Scan(&database); err != nil || database != s.pool.Config().ConnConfig.Database {
		t.Fatalf("K4 claim audit must use isolated current_database: %q: %v", database, err)
	}
	workflow, _, lease := createM4Fixture(t, ctx, s, EffectPure)
	if err := s.ApplyOwnerTransition(ctx, OwnerTransitionInput{Lease: leaseRef(lease), WorkflowID: workflow,
		ExpectedRevision: 1, NewState: StateWaitingActivity, ActorID: "scheduler", Reason: "SCHEDULE_ACTIVITY",
		NodeID: "root", NodeState: ptrState(StateWaitingActivity)}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateAttempt(ctx, AttemptInput{Lease: leaseRef(lease), WorkflowID: workflow, NodeID: "root",
		ExpectedRevision: 2, EffectClass: EffectPure, DispatchLease: time.Minute, ActorID: "scheduler"}); err != nil {
		t.Fatal(err)
	}
	input := ClaimInput{WorkflowID: workflow, NodeID: "root", WorkerID: "worker", RequestID: NewID()}
	// Deferred audit failure must roll back the claim update too.
	if _, err := s.pool.Exec(ctx, `CREATE FUNCTION engine.k4_fail_claim() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN RAISE EXCEPTION 'injected claim audit failure'; END $$;
        CREATE TRIGGER k4_fail_claim BEFORE INSERT ON engine.k4_audit FOR EACH ROW
        WHEN (NEW.kind='claim') EXECUTE FUNCTION engine.k4_fail_claim()`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimAttempt(ctx, input); err == nil {
		t.Fatal("K4 claim committed without audit")
	}
	var state string
	if err := s.pool.QueryRow(ctx, `SELECT state FROM engine.activity_attempts WHERE workflow_id=$1`, workflow).Scan(&state); err != nil || state != "DISPATCHABLE" {
		t.Fatal("K4 claim escaped failed audit transaction", state, err)
	}
	if _, err := s.pool.Exec(ctx, `DROP TRIGGER k4_fail_claim ON engine.k4_audit; DROP FUNCTION engine.k4_fail_claim()`); err != nil {
		t.Fatal(err)
	}
	// Delay after capture but before deferred finalization, so statement time
	// cannot masquerade as the finalizer's commit-path clock.
	if _, err := s.pool.Exec(ctx, `CREATE FUNCTION engine.k4_claim_delay() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN PERFORM pg_sleep(0.03); RETURN NULL; END $$;
        CREATE TRIGGER zz_k4_claim_delay AFTER UPDATE ON engine.activity_attempts FOR EACH ROW
        WHEN (NEW.state='CLAIMED') EXECUTE FUNCTION engine.k4_claim_delay()`); err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	result, err := s.ClaimAttempt(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	ended := time.Now()
	var lo, hi time.Time
	var token, session, pod, container, request, worker, transaction string
	var order, attempt int64
	if err := s.pool.QueryRow(ctx, `SELECT at, finalized_at, token_hash, session, pod_uid, container_id,
        worker_request_id, worker_id, "transaction", "order", attempt_number
        FROM engine.k4_claim_audit WHERE workflow_id=$1 AND node_id='root' AND iteration=0`, workflow).
		Scan(&lo, &hi, &token, &session, &pod, &container, &request, &worker, &transaction, &order, &attempt); err != nil {
		t.Fatal("K4 claim audit missing", err)
	}
	sum := sha256.Sum256([]byte(result.ClaimToken))
	if token != hex.EncodeToString(sum[:]) || session != K4Session || pod != "k4-test-pod" || container != "k4-test-container" ||
		request != input.RequestID || worker != input.WorkerID || transaction == "" || order <= 0 || attempt != result.AttemptNumber {
		t.Fatal("K4 claim audit identity missing")
	}
	if lo.Before(began) || hi.Sub(lo) < 20*time.Millisecond || hi.After(ended) {
		t.Fatal("K4 claim commit bracket missing", lo, hi)
	}
	if _, err := s.ClaimAttempt(ctx, input); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM engine.k4_claim_audit WHERE workflow_id=$1`, workflow).Scan(&count); err != nil || count != 1 {
		t.Fatal("K4 claim audit duplicated by retry", count, err)
	}
	if _, err := s.pool.Exec(ctx, `UPDATE engine.k4_audit SET claim_identity=claim_identity WHERE kind='claim'`); err == nil {
		t.Fatal("K4 claim audit mutable")
	}
}
