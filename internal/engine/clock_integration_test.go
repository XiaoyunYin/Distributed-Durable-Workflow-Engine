package engine

import (
	"context"
	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/state"
	"errors"
	"testing"
	"time"
)

func TestK2SchedulerSkew(t *testing.T) {
	for _, skew := range []time.Duration{-30 * time.Second, 30 * time.Second} {
		t.Run(skew.String(), func(t *testing.T) {
			ctx, store := openM1Database(t)
			defer store.Close()
			fixture := func(graph string) (string, state.Lease) {
				def := "k2-clock-" + state.NewID()
				id, lease := createM1Workflow(t, ctx, store, def, graph, `{"root":"PURE_ACTIVITY"}`)
				t.Cleanup(func() {
					_, _ = store.Pool().Exec(context.Background(), `DELETE FROM engine.workflow_executions WHERE workflow_id=$1`, id)
					_, _ = store.Pool().Exec(context.Background(), `DELETE FROM engine.workflow_definitions WHERE definition_id=$1`, def)
				})
				return id, lease
			}
			clockCalls := 0
			runner := func(owner string) *Engine {
				e := New(store, ActivityDriverFunc(func(context.Context, Activity) (ActivityResult, error) { return ActivityResult{}, nil }))
				e.OwnerID = owner
				e.AttemptLease = time.Second
				e.RetryBackoff = 500 * time.Millisecond
				e.Clock = func() time.Time { clockCalls++; return time.Now().Add(skew) }
				return e
			}
			run := func(e *Engine, id string) RunResult {
				t.Helper()
				r, err := e.Run(ctx, id)
				if err != nil {
					t.Fatalf("skew changed outcome: %v", err)
				}
				return r
			}
			waitSuccess := func(e *Engine, id string) {
				t.Helper()
				end := time.Now().Add(3 * time.Second)
				for time.Now().Before(end) {
					if r := run(e, id); r.Workflow.State == state.StateSucceeded {
						return
					}
					time.Sleep(25 * time.Millisecond)
				}
				t.Fatal("skew changed outcome: no terminal progress")
			}
			t.Run("timer", func(t *testing.T) {
				id, lease := fixture(`{"entry":"root","nodes":[{"id":"root","kind":"timer","delay_ms":500,"next":"done"},{"id":"done","kind":"success"}]}`)
				e := runner(lease.OwnerID)
				if r := run(e, id); !r.Blocked || r.Workflow.State != state.StateWaitingTimer {
					t.Fatal("skew changed outcome: timer did not wait")
				}
				var seconds float64
				if err := store.Pool().QueryRow(ctx, `SELECT extract(epoch from due_at-h.created_at)::float8 FROM engine.timers t JOIN engine.transition_history h ON h.workflow_id=t.workflow_id AND h.revision=t.owning_revision WHERE t.workflow_id=$1`, id).Scan(&seconds); err != nil {
					t.Fatal(err)
				}
				if seconds < 0.45 || seconds > 0.55 {
					t.Fatalf("SQL clock deadline shifted: timer=%f", seconds)
				}
				peer := runner(state.NewID())
				peer.Clock = func() time.Time { clockCalls++; return time.Now().Add(-skew) }
				if r := run(peer, id); r.Workflow.State != state.StateWaitingTimer {
					t.Fatal("skew changed outcome: early timer")
				}
				waitSuccess(peer, id)
				var consumed int
				if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.timers WHERE workflow_id=$1 AND consumed_at IS NOT NULL AND consumed_at>=due_at`, id).Scan(&consumed); err != nil || consumed != 1 {
					t.Fatalf("skew changed outcome: timer consumption %d %v", consumed, err)
				}
				assertM1TraceValid(t, ctx, store, id)
			})
			for _, claimed := range []bool{false, true} {
				t.Run(map[bool]string{false: "retry", true: "claimed-timeout"}[claimed], func(t *testing.T) {
					id, lease := fixture(`{"entry":"root","nodes":[{"id":"root","kind":"activity","next":"done"},{"id":"done","kind":"success"}]}`)
					e := runner(lease.OwnerID)
					stop := errors.New("stop at dispatch")
					e.AfterBoundary = func(boundary string) error {
						if boundary == "attempt_created" {
							return stop
						}
						return nil
					}
					if _, err := e.Run(ctx, id); !errors.Is(err, stop) {
						t.Fatal(err)
					}
					var seconds float64
					if err := store.Pool().QueryRow(ctx, `SELECT extract(epoch from heartbeat_deadline-created_at)::float8 FROM engine.activity_attempts WHERE workflow_id=$1`, id).Scan(&seconds); err != nil {
						t.Fatal(err)
					}
					if seconds < 0.95 || seconds > 1.05 {
						t.Fatalf("SQL clock deadline shifted: dispatch=%f", seconds)
					}
					peer := runner(state.NewID())
					peer.Clock = func() time.Time { clockCalls++; return time.Now().Add(-skew) }
					var oldClaim state.ClaimResult
					if claimed {
						var err error
						oldClaim, err = store.ClaimAttempt(ctx, state.ClaimInput{WorkflowID: id, NodeID: "root", WorkerID: "k2-worker", RequestID: state.NewID(), AttemptLease: 500 * time.Millisecond})
						if err != nil {
							t.Fatal(err)
						}
						// Stop immediately after replacement to inspect the SQL dispatch deadline.
						peer.AfterBoundary = func(boundary string) error {
							if boundary == "attempt_timed_out" {
								return stop
							}
							return nil
						}
						if r := run(peer, id); !r.Blocked {
							t.Fatal("skew changed outcome: premature claim timeout")
						}
						time.Sleep(550 * time.Millisecond)
						if _, err := peer.Run(ctx, id); !errors.Is(err, stop) {
							t.Fatalf("skew changed outcome: timeout %v", err)
						}
						var replacement int64
						if err := store.Pool().QueryRow(ctx, `SELECT attempt_number,extract(epoch from heartbeat_deadline-created_at)::float8 FROM engine.activity_attempts WHERE workflow_id=$1 AND is_current`, id).Scan(&replacement, &seconds); err != nil {
							t.Fatal(err)
						}
						if replacement != 2 || seconds < 0.95 || seconds > 1.05 {
							t.Fatalf("SQL clock deadline shifted: replacement=%d seconds=%f", replacement, seconds)
						}
						if _, err := store.RecordResultReceipt(ctx, state.ResultInput{WorkflowID: id, NodeID: "root", AttemptNumber: 1, ClaimToken: oldClaim.ClaimToken, AttemptState: state.AttemptSucceeded, Payload: []byte("{}")}); !errors.Is(err, state.ErrAttemptNotCurrent) {
							t.Fatalf("skew changed outcome: stale result %v", err)
						}
						peer.AfterBoundary = nil
					} else {
						peer.Driver = ActivityDriverFunc(func(_ context.Context, a Activity) (ActivityResult, error) {
							if a.AttemptNumber == 1 {
								return ActivityResult{AttemptState: state.AttemptFailedRetryable, RetryAfter: 500 * time.Millisecond}, nil
							}
							return ActivityResult{}, nil
						})
						if r := run(peer, id); !r.Blocked || r.Workflow.State != state.StateWaitingTimer {
							t.Fatal("skew changed outcome: retry did not wait")
						}
						if err := store.Pool().QueryRow(ctx, `SELECT extract(epoch from due_at-h.created_at)::float8 FROM engine.timers t JOIN engine.transition_history h ON h.workflow_id=t.workflow_id AND h.revision=t.owning_revision WHERE t.workflow_id=$1 AND purpose='RETRY_BACKOFF'`, id).Scan(&seconds); err != nil {
							t.Fatal(err)
						}
						if seconds < 0.45 || seconds > 0.55 {
							t.Fatalf("SQL clock deadline shifted: retry=%f", seconds)
						}
						if r := run(peer, id); r.Workflow.State != state.StateWaitingTimer {
							t.Fatal("skew changed outcome: early retry")
						}
					}
					waitSuccess(peer, id)
					ref := state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch}
					if err := store.ApplyOwnerTransition(ctx, state.OwnerTransitionInput{Lease: ref, WorkflowID: id, ActorID: "old", Reason: "STALE", NewState: state.StateRunnable}); !errors.Is(err, state.ErrLeaseNotOwned) {
						t.Fatalf("skew changed outcome: stale owner %v", err)
					}
					assertM1TraceValid(t, ctx, store, id)
				})
			}
			if clockCalls == 0 {
				t.Fatal("scheduler clock was not injected")
			}
		})
	}
}

func TestK2ExternalDispatchSkew(t *testing.T) {
	for _, skew := range []time.Duration{-30 * time.Second, 30 * time.Second} {
		t.Run(skew.String(), func(t *testing.T) {
			ctx, store := openM1Database(t)
			defer store.Close()
			id := "k2-external-" + state.NewID()
			def := "k2-external-def-" + state.NewID()
			if err := store.CreateDefinition(ctx, state.DefinitionInput{DefinitionID: def, Version: 1, DefinitionHash: def,
				Graph:         []byte(`{"entry":"pure.echo","nodes":[{"id":"pure.echo","kind":"activity"}]}`),
				EffectClasses: []byte(`{"pure.echo":"PURE_ACTIVITY"}`), ActivityVersions: []byte(`{"pure.echo":"v1"}`)}); err != nil {
				t.Fatal(err)
			}
			partitionID, _ := partition.ID(id)
			if _, err := store.CreateWorkflow(ctx, state.CreateWorkflowInput{WorkflowID: id, Namespace: "k2-clock", SubmissionKey: id, SubmissionPayloadHash: id, DefinitionID: def, DefinitionVersion: 1, PartitionID: int16(partitionID), InitialNodeID: "pure.echo", ActorID: "test"}); err != nil {
				t.Fatal(err)
			}
			defer func() {
				_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_executions WHERE workflow_id=$1`, id)
				_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_definitions WHERE definition_id=$1`, def)
			}()
			e := New(store, nil)
			e.ExternalActivities = true
			e.OwnerID = state.NewID()
			e.AttemptLease = 500 * time.Millisecond
			e.Clock = func() time.Time { return time.Now().Add(skew) }
			run := func() {
				t.Helper()
				if _, err := e.Run(ctx, id); err != nil {
					t.Fatalf("skew changed outcome: external %v", err)
				}
			}
			run()
			e.OwnerID = state.NewID()
			e.Clock = func() time.Time { return time.Now().Add(-skew) }
			run()
			var redispatches int
			count := func() {
				t.Helper()
				if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.transition_history WHERE workflow_id=$1 AND reason='REDISPATCH_UNCLAIMED'`, id).Scan(&redispatches); err != nil {
					t.Fatal(err)
				}
			}
			count()
			if redispatches != 0 {
				t.Fatal("skew changed outcome: premature redispatch")
			}
			time.Sleep(550 * time.Millisecond)
			run()
			count()
			if redispatches != 1 {
				t.Fatalf("skew changed outcome: redispatch=%d", redispatches)
			}
			var seconds float64
			if err := store.Pool().QueryRow(ctx, `SELECT extract(epoch from heartbeat_deadline-clock_timestamp())::float8 FROM engine.activity_attempts WHERE workflow_id=$1 AND is_current`, id).Scan(&seconds); err != nil {
				t.Fatal(err)
			}
			if seconds < 0.3 || seconds > 0.5 {
				t.Fatalf("SQL clock deadline shifted: redispatch=%f", seconds)
			}
			claim, err := store.ClaimAttempt(ctx, state.ClaimInput{WorkflowID: id, NodeID: "pure.echo", WorkerID: "test", RequestID: state.NewID(), AttemptLease: time.Second})
			if err != nil || claim.AttemptNumber != 1 {
				t.Fatalf("skew changed outcome: redispatch changed identity %v", err)
			}
			if _, err := store.RecordResultReceipt(ctx, state.ResultInput{WorkflowID: id, NodeID: "pure.echo", AttemptNumber: claim.AttemptNumber, ClaimToken: claim.ClaimToken, AttemptState: state.AttemptSucceeded, Payload: []byte("{}")}); err != nil {
				t.Fatal(err)
			}
			run()
			wf, err := store.GetWorkflow(ctx, id)
			if err != nil || wf.State != state.StateSucceeded {
				t.Fatalf("skew changed outcome: external terminal %v %v", wf.State, err)
			}
			assertM1TraceValid(t, ctx, store, id)
		})
	}
}
