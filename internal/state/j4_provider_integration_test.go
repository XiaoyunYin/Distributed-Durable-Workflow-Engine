package state

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/partition"
	"github.com/jackc/pgx/v5"
)

func TestPostgresJ4ProviderBudgetAcrossClaims(t *testing.T) {
	url := os.Getenv("J4_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("requires an isolated J4 PostgreSQL database with the base migrations")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := NewFromURL(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var empty bool
	requireJ4DB(t, store.pool.QueryRow(ctx, `SELECT NOT EXISTS(SELECT 1 FROM engine.workflow_executions)
		AND NOT EXISTS(SELECT 1 FROM engine.schema_migrations WHERE version >= 25)
		AND EXISTS(SELECT 1 FROM engine.schema_migrations WHERE version = 23)`).Scan(&empty))
	if !empty {
		t.Fatal("J4 integration requires an empty base-profile database")
	}
	applyJ4SQL(t, ctx, store, "up")
	defer applyJ4SQL(t, context.Background(), store, "down")

	workflow := createJ4ProviderFixture(t, ctx, store)
	now := int64(time.Minute)
	provider := &ProviderControl{store: store, namespace: workflow, bootID: "boot", clock: func() (int64, error) { return now, nil }}
	claim := claimJ4ProviderFixture(t, ctx, store, workflow, 1)
	first, err := provider.Reserve(ctx, claim, "reserve-1")
	requireJ4DB(t, err)
	if first.Permit == nil || first.Permit.Sequence != 1 || first.Permit.DeadlineNS != now+providerHTTPTimeout {
		t.Fatalf("wrong first reservation: %+v", first)
	}
	duplicate, err := provider.Reserve(ctx, claim, "reserve-1")
	requireJ4DB(t, err)
	if duplicate.Permit == nil || *duplicate.Permit != *first.Permit {
		t.Fatal("resending the same request did not return the original permit")
	}
	if _, err := provider.Reserve(ctx, claim, "different"); !errors.Is(err, ErrProviderConflict) {
		t.Fatalf("a second outstanding permit was granted: %v", err)
	}

	// Represent the already witnessed retirement in engine state. B2 has
	// never observed a provider outcome for reservation 1.
	_, err = store.pool.Exec(ctx, `UPDATE engine.activity_attempts SET state='TIMED_OUT', is_current=false
		WHERE workflow_id=$1 AND attempt_number=1`, workflow)
	requireJ4DB(t, err)
	_, err = store.pool.Exec(ctx, `UPDATE engine.node_instances SET current_attempt_number=2 WHERE workflow_id=$1`, workflow)
	requireJ4DB(t, err)
	_, err = store.pool.Exec(ctx, `INSERT INTO engine.activity_attempts
		(workflow_id,node_id,iteration,attempt_number,state,effect_class)
		VALUES ($1,$2,0,2,'DISPATCHABLE','PURE_ACTIVITY')`, workflow, RecordedLLMActivity)
	requireJ4DB(t, err)
	if _, err := provider.Outcome(ctx, claim, ProviderOutcome{CallID: first.Permit.CallID, Kind: "success", ObservedNS: now}); !errors.Is(err, ErrStaleClaim) {
		t.Fatalf("retired claim reported an outcome: %v", err)
	}
	claim = claimJ4ProviderFixture(t, ctx, store, workflow, 2)
	now = first.Permit.DeadlineNS + int64(time.Second)
	recovered, err := provider.Reserve(ctx, claim, "")
	requireJ4DB(t, err)
	if recovered.OutcomeNS != first.Permit.DeadlineNS || recovered.EligibleNS != first.Permit.DeadlineNS+2_302_000_000 {
		t.Fatalf("lost original eligibility: %+v", recovered)
	}
	early, err := provider.Reserve(ctx, claim, "reserve-2")
	requireJ4DB(t, err)
	if early.Permit != nil || early.EligibleNS != recovered.EligibleNS {
		t.Fatalf("reservation was issued too early: %+v", early)
	}
	now = recovered.EligibleNS
	for sequence := 2; sequence <= 4; sequence++ {
		request := NewID()
		permitReply, err := provider.Reserve(ctx, claim, request)
		requireJ4DB(t, err)
		permit := permitReply.Permit
		if permit == nil || permit.Sequence != sequence || permit.Attempt != 2 || permit.CallID == first.Permit.CallID {
			t.Fatalf("bad replacement permit %d: %+v", sequence, permit)
		}
		outcome := ProviderOutcome{CallID: permit.CallID, Kind: "timeout", ObservedNS: permit.DeadlineNS}
		if sequence == 2 {
			outcome.Kind, outcome.ObservedNS, outcome.RetryAfterSeconds = "rate_limited", now+1_000_000, 12
		}
		now = outcome.ObservedNS
		schedule, err := provider.Outcome(ctx, claim, outcome)
		requireJ4DB(t, err)
		if sequence == 2 && schedule.EligibleNS != now+12_000_000_000 {
			t.Fatalf("missing Retry-After floor: %+v", schedule)
		}
		if sequence != 4 && schedule.TerminalState != "" {
			t.Fatal("policy exhausted before cap")
		}
		now = schedule.EligibleNS
	}
	exhausted, err := provider.Reserve(ctx, claim, "post-exhaustion")
	requireJ4DB(t, err)
	if exhausted.Permit != nil || exhausted.TerminalState != AttemptFailedFinal ||
		!jsonEqual(exhausted.Payload, []byte(`{"reason":"PROVIDER_BUDGET_EXHAUSTED"}`)) {
		t.Fatalf("wrong terminal outcome: %+v", exhausted)
	}
	var count int
	requireJ4DB(t, store.pool.QueryRow(ctx, `SELECT count(*) FROM engine.j4_provider_attempts WHERE workflow_id=$1`, workflow).Scan(&count))
	if count != 4 {
		t.Fatalf("durable reservation count %d", count)
	}
	receipt := ResultInput{WorkflowID: workflow, NodeID: RecordedLLMActivity, AttemptNumber: claim.Attempt,
		ClaimToken: claim.ClaimToken, AttemptState: exhausted.TerminalState, Payload: exhausted.Payload}
	requireJ4DB(t, provider.CheckResult(ctx, receipt))
	_, err = store.RecordResultReceipt(ctx, receipt)
	requireJ4DB(t, err)
	if _, err := provider.Reserve(ctx, claim, "after-receipt"); !errors.Is(err, ErrStaleClaim) {
		t.Fatalf("retired claim could reserve after terminal receipt: %v", err)
	}
	_, err = store.pool.Exec(ctx, `DELETE FROM engine.workflow_executions WHERE workflow_id=$1`, workflow)
	requireJ4DB(t, err)
	_, err = store.pool.Exec(ctx, `DELETE FROM engine.workflow_definitions WHERE definition_id=$1`, workflow)
	requireJ4DB(t, err)
}

func applyJ4SQL(t *testing.T, ctx context.Context, store *Store, direction string) {
	t.Helper()
	sql, err := os.ReadFile(filepath.Join("..", "..", "migrations", "j4_b2", "000025_j4_provider_resilience."+direction+".sql"))
	requireJ4DB(t, err)
	_, err = store.pool.Exec(ctx, string(sql), pgx.QueryExecModeSimpleProtocol)
	requireJ4DB(t, err)
}

func createJ4ProviderFixture(t *testing.T, ctx context.Context, store *Store) string {
	t.Helper()
	id := "j4-integration-" + NewID()
	mapped, _ := partition.ID(id)
	requireJ4DB(t, store.CreateDefinition(ctx, DefinitionInput{DefinitionID: id, Version: 1, DefinitionHash: id,
		Graph:            []byte(`{"entry":"j4.recorded-llm","nodes":[{"id":"j4.recorded-llm","kind":"activity"}]}`),
		ActivityVersions: []byte(`{"j4.recorded-llm":"v1"}`), EffectClasses: []byte(`{"j4.recorded-llm":"PURE_ACTIVITY"}`)}))
	_, err := store.CreateWorkflow(ctx, CreateWorkflowInput{WorkflowID: id, Namespace: id, SubmissionKey: id,
		SubmissionPayloadHash: id, DefinitionID: id, DefinitionVersion: 1, PartitionID: int16(mapped), InitialNodeID: RecordedLLMActivity})
	requireJ4DB(t, err)
	for _, query := range []string{
		`UPDATE engine.workflow_executions SET state='WAITING_ACTIVITY' WHERE workflow_id=$1`,
		`UPDATE engine.node_instances SET state='WAITING_ACTIVITY', current_attempt_number=1 WHERE workflow_id=$1`,
		`INSERT INTO engine.activity_attempts (workflow_id,node_id,iteration,attempt_number,state,effect_class)
		 VALUES ($1,'j4.recorded-llm',0,1,'DISPATCHABLE','PURE_ACTIVITY')`,
	} {
		_, err := store.pool.Exec(ctx, query, id)
		requireJ4DB(t, err)
	}
	return id
}

func claimJ4ProviderFixture(t *testing.T, ctx context.Context, store *Store, workflow string, attempt int64) ProviderClaim {
	t.Helper()
	claim, err := store.ClaimAttempt(ctx, ClaimInput{WorkflowID: workflow, NodeID: RecordedLLMActivity,
		ExpectedAttempt: attempt, WorkerID: NewID(), RequestID: NewID(), AttemptLease: 12 * time.Second})
	requireJ4DB(t, err)
	return ProviderClaim{WorkflowID: workflow, NodeID: RecordedLLMActivity, ClaimToken: claim.ClaimToken, BootID: "boot", Attempt: attempt}
}

func requireJ4DB(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
