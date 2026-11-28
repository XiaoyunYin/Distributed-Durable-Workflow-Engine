package state

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"
)

const timeoutTestActor = "j19-scheduler"

func TestPostgresTimeoutRetryPolicy(t *testing.T) {
	t.Setenv(timeoutRetryPolicyEnv, "1")
	ctx, store := openM3Database(t)
	for _, class := range []EffectClass{EffectPure, EffectCooperating} {
		t.Run(string(class), func(t *testing.T) {
			f := newTimeoutFixture(t, ctx, store, class)
			for n := int64(1); n <= 3; n++ {
				f.claim(t, n)
				f.replace(t, n)
			}
			f.claim(t, 4)
			f.exhaust(t, 4, "TIMEOUT_REPLACEMENT_BUDGET_EXHAUSTED")
			policy, err := store.GetRetryPolicy(ctx, f.workflowID, "root", 0)
			if err != nil || policy.MaxRetries != 3 || policy.RetriesUsed != 0 {
				t.Fatalf("reported-failure policy = %+v, err=%v", policy, err)
			}
		})
	}
	t.Run("zero budget", func(t *testing.T) {
		f := newTimeoutFixture(t, ctx, store, EffectPure)
		f.setPolicy(t, 0, nil)
		f.claim(t, 1)
		f.exhaust(t, 1, timeoutBudgetReason)
	})
	t.Run("configured budget", func(t *testing.T) {
		f := newTimeoutFixture(t, ctx, store, EffectPure)
		f.setPolicy(t, 1, nil)
		f.claim(t, 1)
		f.replace(t, 1)
		f.claim(t, 2)
		f.exhaust(t, 2, timeoutBudgetReason)
	})
	t.Run("charges legacy replacements", func(t *testing.T) {
		legacy := New(store.Pool())
		f := newTimeoutFixture(t, ctx, legacy, EffectPure)
		f.setPolicy(t, 1, nil)
		f.claim(t, 1)
		f.replace(t, 1)
		f.store = store
		f.claim(t, 2)
		f.exhaust(t, 2, timeoutBudgetReason)
	})
	t.Run("deadline", func(t *testing.T) {
		f := newTimeoutFixture(t, ctx, store, EffectPure)
		future := f.now(t).Add(time.Hour)
		f.setPolicy(t, 3, &future)
		f.claim(t, 1)
		f.replace(t, 1)
		past := f.now(t).Add(-time.Microsecond)
		f.setPolicy(t, 3, &past)
		f.claim(t, 2)
		f.exhaust(t, 2, "TIMEOUT_REPLACEMENT_DEADLINE_EXCEEDED")
	})
	t.Run("live siblings settle on exhaustion", func(t *testing.T) {
		f := newTimeoutFixture(t, ctx, store, EffectPure)
		f.setPolicy(t, 0, nil)
		f.claim(t, 1)
		// Seed a fan-out state with a live effect and a pending sibling timer.
		for _, query := range []string{
			`INSERT INTO engine.node_instances (workflow_id, node_id, state, current_attempt_number)
			 VALUES ($1, 'effect', 'WAITING_ACTIVITY', 1), ($1, 'timer', 'WAITING_TIMER', NULL)`,
			`INSERT INTO engine.activity_attempts (workflow_id, node_id, iteration, attempt_number, state, effect_class)
			 VALUES ($1, 'effect', 0, 1, 'CLAIMED', 'NON_COOPERATING_EFFECT')`,
			`INSERT INTO engine.timers (timer_id, workflow_id, node_id, iteration, due_at, purpose, owning_revision)
			 VALUES (gen_random_uuid(), $1, 'timer', 0, clock_timestamp() + interval '1 minute', 'WORKFLOW_TIMER', 3)`,
		} {
			if _, err := store.pool.Exec(ctx, query, f.workflowID); err != nil {
				t.Fatal(err)
			}
		}
		f.exhaust(t, 1, timeoutBudgetReason)
		for _, sibling := range []string{"effect", "timer"} {
			node, err := store.GetNode(ctx, f.workflowID, sibling, 0)
			if err != nil || node.State != StateCanceled || node.CurrentAttempt != nil {
				t.Fatalf("sibling %s = %+v, err=%v", sibling, node, err)
			}
		}
		effect, err := store.GetAttempt(ctx, f.workflowID, "effect", 0, 1)
		if err != nil || effect.State != AttemptCanceled || effect.IsCurrent || effect.OutcomeDisposition != "OUTCOME_UNKNOWN" {
			t.Fatalf("sibling effect = %+v, err=%v", effect, err)
		}
		if _, _, pending, err := store.PendingTimer(ctx, f.workflowID, "timer", 0); err != nil || pending {
			t.Fatalf("sibling timer pending=%v, err=%v", pending, err)
		}
	})
	t.Run("success after lost workers", func(t *testing.T) {
		f := newTimeoutFixture(t, ctx, store, EffectPure)
		// Seed spent reported retries independently of the expiry allowance.
		if _, err := store.pool.Exec(ctx, `
			UPDATE engine.retry_policies SET retries_used = max_retries
			WHERE workflow_id = $1 AND node_id = 'root' AND iteration = 0`, f.workflowID); err != nil {
			t.Fatal(err)
		}
		for n := int64(1); n <= 2; n++ {
			lost := f.claim(t, n)
			f.replace(t, n)
			if err := store.RecordResult(ctx, f.result(n, lost)); !errors.Is(err, ErrAttemptNotCurrent) {
				t.Fatalf("late result from lost worker: %v", err)
			}
		}
		claimed := f.claim(t, 3)
		if err := store.RecordResult(ctx, f.result(3, claimed)); err != nil {
			t.Fatal(err)
		}
		consumed, err := store.ConsumeResult(ctx, ConsumeResultInput{
			Lease: f.lease, WorkflowID: f.workflowID, NodeID: "root", AttemptNumber: 3,
			ExpectedRevision: f.revision + 1, NewWorkflowState: StateSucceeded, ActorID: timeoutTestActor,
		})
		if err != nil || consumed.Workflow.State != StateSucceeded || consumed.NodeState != StateSucceeded {
			t.Fatalf("replacement result = %+v, err=%v", consumed, err)
		}
		policy, err := store.GetRetryPolicy(ctx, f.workflowID, "root", 0)
		if err != nil || policy.RetriesUsed != 3 {
			t.Fatalf("reported retry count after recovery = %+v, err=%v", policy, err)
		}
	})
	t.Run("unclaimed redispatch", func(t *testing.T) {
		f := newTimeoutFixture(t, ctx, store, EffectPure)
		past := f.now(t).Add(-time.Second)
		f.setPolicy(t, 0, &past)
		got := f.expire(t, 1)
		if got != (TimeoutResult{AttemptNumber: 1, Redispatched: true}) {
			t.Fatalf("unclaimed expiry = %+v", got)
		}
		f.checkHistory(t, 1, "REDISPATCH_UNCLAIMED", StateWaitingActivity)
		f.checkDispatch(t, 1, "attempt.redispatch")
		attempt, err := store.GetAttempt(ctx, f.workflowID, "root", 0, 1)
		if err != nil || attempt.State != AttemptDispatchable || !attempt.IsCurrent {
			t.Fatalf("redispatched attempt = %+v, err=%v", attempt, err)
		}
	})
	t.Run("non-cooperating reconciliation", func(t *testing.T) {
		f := newTimeoutFixture(t, ctx, store, EffectNonCooperating)
		past := f.now(t).Add(-time.Second)
		f.setPolicy(t, 0, &past)
		claimed := f.claim(t, 1)
		got := f.expire(t, 1)
		if got != (TimeoutResult{AttemptNumber: 1, Reconciliation: true}) {
			t.Fatalf("non-cooperating expiry = %+v", got)
		}
		f.checkHistory(t, 1, "TIMEOUT_RECONCILIATION_REQUIRED", StateReconciliationRequired)
		var eventType string
		if err := store.pool.QueryRow(ctx, `
			SELECT event_type FROM engine.outbox WHERE workflow_id = $1 AND aggregate_revision = $2`,
			f.workflowID, f.revision).Scan(&eventType); err != nil || eventType != "reconciliation.required" {
			t.Fatalf("reconciliation outbox = %q, err=%v", eventType, err)
		}
		var items int
		if err := store.pool.QueryRow(ctx, `
			SELECT count(*) FROM engine.reconciliation_items
			WHERE workflow_id = $1 AND kind = 'EXPIRED_ATTEMPT' AND attempt_number = 1`, f.workflowID).
			Scan(&items); err != nil || items != 1 {
			t.Fatalf("reconciliation items = %d, err=%v", items, err)
		}
		receipt, err := store.RecordResultReceipt(ctx, f.result(1, claimed))
		if err != nil || receipt.Disposition != ResultRecordedAsEvidence {
			t.Fatalf("late non-cooperating result = %+v, err=%v", receipt, err)
		}
	})
}

func TestPostgresTimeoutRetryUnsetLegacyEquivalence(t *testing.T) {
	t.Setenv(timeoutRetryPolicyEnv, "")
	if err := os.Unsetenv(timeoutRetryPolicyEnv); err != nil {
		t.Fatal(err)
	}
	ctx, store := openM3Database(t)
	for _, class := range []EffectClass{EffectPure, EffectCooperating} {
		t.Run(string(class), func(t *testing.T) {
			f := newTimeoutFixture(t, ctx, store, class)
			past := f.now(t).Add(-time.Second)
			f.setPolicy(t, 0, &past)
			before := f.outbox(t)
			for n := int64(1); n <= 4; n++ {
				f.claim(t, n)
				f.replace(t, n)
			}
			after := f.outbox(t)
			if len(after) != len(before)+4 || !slices.Equal(before, after[:len(before)]) {
				t.Fatalf("legacy outbox before = %v, after = %v", before, after)
			}
			wf, err := store.GetWorkflow(ctx, f.workflowID)
			if err != nil || wf.State != StateWaitingActivity || wf.Revision != 7 {
				t.Fatalf("legacy workflow = %+v, err=%v", wf, err)
			}
			p, err := store.GetRetryPolicy(ctx, f.workflowID, "root", 0)
			if err != nil || p.MaxRetries != 0 || p.RetriesUsed != 0 || p.TotalDeadlineAt == nil || !p.TotalDeadlineAt.Equal(past) {
				t.Fatalf("legacy retry policy = %+v, err=%v", p, err)
			}
		})
	}
}

type timeoutFixture struct {
	ctx        context.Context
	store      *Store
	workflowID string
	lease      LeaseRef
	revision   int64
}

func newTimeoutFixture(t *testing.T, ctx context.Context, store *Store, class EffectClass) *timeoutFixture {
	t.Helper()
	id, definition, lease := createM4Fixture(t, ctx, store, class)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cleanupM4Fixture(t, cleanupCtx, store, id, definition, lease, "")
	})
	ref := leaseRef(lease)
	if err := store.ApplyOwnerTransition(ctx, OwnerTransitionInput{
		Lease: ref, WorkflowID: id, ExpectedRevision: 1, NewState: StateWaitingActivity,
		ActorID: timeoutTestActor, Reason: "SCHEDULE_ACTIVITY", NodeID: "root", NodeState: ptrState(StateWaitingActivity),
	}); err != nil {
		t.Fatal(err)
	}
	attempt, err := store.CreateAttempt(ctx, AttemptInput{
		Lease: ref, WorkflowID: id, NodeID: "root", ExpectedRevision: 2, EffectClass: class,
		LogicalEffectKey: "j19-effect", GrantScopeHash: "j19-grant", ActorID: timeoutTestActor,
	})
	if err != nil || attempt.AttemptNumber != 1 {
		t.Fatalf("initial attempt = %+v, err=%v", attempt, err)
	}
	return &timeoutFixture{ctx: ctx, store: store, workflowID: id, lease: ref, revision: 3}
}

func (f *timeoutFixture) now(t *testing.T) time.Time {
	t.Helper()
	now, err := f.store.DatabaseTime(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	return now
}

func (f *timeoutFixture) setPolicy(t *testing.T, budget int, deadline *time.Time) {
	t.Helper()
	if err := f.store.SetRetryPolicy(f.ctx, RetryPolicyInput{
		Lease: f.lease, WorkflowID: f.workflowID, NodeID: "root", ExpectedRevision: f.revision,
		MaxRetries: budget, TotalDeadlineAt: deadline, ActorID: timeoutTestActor,
	}); err != nil {
		t.Fatal(err)
	}
}

func (f *timeoutFixture) claim(t *testing.T, attempt int64) ClaimResult {
	t.Helper()
	claim, err := f.store.ClaimAttempt(f.ctx, ClaimInput{
		WorkflowID: f.workflowID, NodeID: "root", ExpectedAttempt: attempt,
		WorkerID: fmt.Sprintf("j19-worker-%d", attempt), RequestID: NewID(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return claim
}

func (f *timeoutFixture) result(n int64, claim ClaimResult) ResultInput {
	return ResultInput{WorkflowID: f.workflowID, NodeID: "root", AttemptNumber: n,
		ClaimToken: claim.ClaimToken, AttemptState: AttemptSucceeded, Payload: []byte(`{"ok":true}`)}
}

func (f *timeoutFixture) timeoutInput(n int64) TimeoutInput {
	return TimeoutInput{Lease: f.lease, WorkflowID: f.workflowID, NodeID: "root",
		AttemptNumber: n, ExpectedRevision: f.revision, ActorID: timeoutTestActor}
}

func (f *timeoutFixture) expire(t *testing.T, n int64) TimeoutResult {
	t.Helper()
	// Simulate a lost worker at the lease boundary without sleeping or sending a result.
	if _, err := f.store.pool.Exec(f.ctx, `
		UPDATE engine.activity_attempts SET heartbeat_deadline = clock_timestamp()
		WHERE workflow_id = $1 AND node_id = 'root' AND iteration = 0 AND attempt_number = $2`,
		f.workflowID, n); err != nil {
		t.Fatal(err)
	}
	got, err := f.store.TimeoutAttempt(f.ctx, f.timeoutInput(n))
	if err != nil {
		t.Fatal(err)
	}
	f.revision++
	return got
}

func (f *timeoutFixture) replace(t *testing.T, n int64) {
	t.Helper()
	got := f.expire(t, n)
	if got.AttemptNumber != n || got.ReplacementNumber == nil || *got.ReplacementNumber != n+1 ||
		got.Redispatched || got.Reconciliation {
		t.Fatalf("replacement at %d = %+v", n, got)
	}
	old, err := f.store.GetAttempt(f.ctx, f.workflowID, "root", 0, n)
	if err != nil || old.IsCurrent || old.State != AttemptTimedOut {
		t.Fatalf("retired attempt = %+v, err=%v", old, err)
	}
	replacement, err := f.store.GetAttempt(f.ctx, f.workflowID, "root", 0, n+1)
	if err != nil || !replacement.IsCurrent || replacement.State != AttemptDispatchable ||
		replacement.EffectClass != old.EffectClass || replacement.LogicalEffectKey != old.LogicalEffectKey ||
		replacement.GrantScopeHash != old.GrantScopeHash {
		t.Fatalf("replacement attempt = %+v, err=%v", replacement, err)
	}
	f.checkHistory(t, n, "TIMEOUT_REPLACEMENT", StateWaitingActivity)
	f.checkDispatch(t, n+1, "attempt.dispatch")
}

func (f *timeoutFixture) exhaust(t *testing.T, n int64, reason string) {
	t.Helper()
	before := f.outbox(t)
	staleInput := f.timeoutInput(n)
	got := f.expire(t, n)
	if got != (TimeoutResult{AttemptNumber: n}) {
		t.Fatalf("exhaustion at %d = %+v", n, got)
	}
	f.checkHistory(t, n, reason, StateFailed)
	wf, err := f.store.GetWorkflow(f.ctx, f.workflowID)
	if err != nil || wf.State != StateFailed || wf.Revision != f.revision {
		t.Fatalf("workflow after exhaustion = %+v, err=%v", wf, err)
	}
	node, err := f.store.GetNode(f.ctx, f.workflowID, "root", 0)
	if err != nil || node.State != StateFailed || node.CurrentAttempt != nil {
		t.Fatalf("node after exhaustion = %+v, err=%v", node, err)
	}
	last, err := f.store.GetAttempt(f.ctx, f.workflowID, "root", 0, n)
	if err != nil || last.State != AttemptTimedOut || last.IsCurrent {
		t.Fatalf("last attempt = %+v, err=%v", last, err)
	}
	var attempts int
	if err := f.store.pool.QueryRow(f.ctx, `
		SELECT count(*) FROM engine.activity_attempts
		WHERE workflow_id = $1 AND node_id = 'root' AND iteration = 0`, f.workflowID).
		Scan(&attempts); err != nil || int64(attempts) != n {
		t.Fatalf("attempts after exhaustion = %d, err=%v", attempts, err)
	}
	if _, err := f.store.TimeoutAttempt(f.ctx, staleInput); !errors.Is(err, ErrAttemptNotCurrent) {
		t.Fatalf("stale scheduler retry after exhaustion: %v", err)
	}
	if _, err := f.store.ClaimAttempt(f.ctx, ClaimInput{WorkflowID: f.workflowID, NodeID: "root",
		ExpectedAttempt: n, WorkerID: "late-worker", RequestID: NewID()}); !errors.Is(err, ErrAttemptNotCurrent) {
		t.Fatalf("stale dispatch claim after exhaustion: %v", err)
	}
	if after := f.outbox(t); !slices.Equal(before, after) {
		t.Fatalf("outbox changed on exhaustion: before=%v, after=%v", before, after)
	}
}

func (f *timeoutFixture) checkHistory(t *testing.T, n int64, reason string, state WorkflowState) {
	t.Helper()
	history, err := f.store.ListHistory(f.ctx, f.workflowID, f.revision-1, 10)
	if err != nil || len(history) != 1 {
		t.Fatalf("timeout history = %+v, err=%v", history, err)
	}
	h := history[0]
	if h.Revision != f.revision || h.Reason != reason || h.NewState != state ||
		h.OldState == nil || *h.OldState != StateWaitingActivity || h.NodeID == nil || *h.NodeID != "root" ||
		h.Iteration == nil || *h.Iteration != 0 || h.AttemptNumber == nil || *h.AttemptNumber != n ||
		h.ActorKind != "scheduler" || h.ActorID != timeoutTestActor || h.SchedulerEpoch == nil || *h.SchedulerEpoch != f.lease.Epoch {
		t.Fatalf("timeout history = %+v", h)
	}
}

func (f *timeoutFixture) checkDispatch(t *testing.T, n int64, eventType string) {
	t.Helper()
	var gotEvent, topic, payload, publishState string
	var count int
	err := f.store.pool.QueryRow(f.ctx, `
		SELECT count(*) OVER (), event_type, topic, payload::text, publish_state
		FROM engine.outbox WHERE workflow_id = $1 AND aggregate_revision = $2`,
		f.workflowID, f.revision).Scan(&count, &gotEvent, &topic, &payload, &publishState)
	wantPayload := fmt.Sprintf(`{"node_id": "root", "iteration": 0, "workflow_id": %q, "attempt_number": %d}`, f.workflowID, n)
	if err != nil || count != 1 || gotEvent != eventType || topic != "durable-agent.tasks.v1" ||
		payload != wantPayload || publishState != "PENDING" {
		t.Fatalf("dispatch = count %d, type %q, topic %q, state %q, payload %s, err=%v", count, gotEvent, topic, publishState, payload, err)
	}
}

func (f *timeoutFixture) outbox(t *testing.T) []string {
	t.Helper()
	rows, err := f.store.pool.Query(f.ctx, `
		SELECT to_jsonb(o)::text FROM engine.outbox o WHERE workflow_id = $1 ORDER BY aggregate_revision, event_id`, f.workflowID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var row string
		if err := rows.Scan(&row); err != nil {
			t.Fatal(err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}
