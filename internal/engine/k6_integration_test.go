package engine

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/k6receipt"
	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/state"
)

type k6EngineFixture struct {
	ctx    context.Context
	store  *state.Store
	runner *Engine
	signer ed25519.PrivateKey
	id     string
	op     K6Operation
}

func setupK6Engine(t *testing.T) k6EngineFixture {
	t.Helper()
	ctx, base := openM1Database(t)
	t.Cleanup(base.Close)
	store := state.NewWithOptions(base.Pool(), state.StoreOptions{K6Approval: true})
	t.Setenv("DUR053_K6_ACTIVITY", "1")
	id := "k6-scheduler-" + state.NewID()
	op := K6Operation{SubmissionKey: "submission-" + id, DefinitionID: "definition-" + id, DefinitionVersion: 1,
		NodeID: state.K6Activity, Resource: "resource-" + id, LogicalKey: "key-" + id,
		ExpectedRevision: "0", Arguments: json.RawMessage(`{"from":"sealed-plan"}`)}
	_, err := store.Pool().Exec(ctx, `INSERT INTO k6_auth.episode (namespace,approver_subject,valid_until,uncertainty_us)
		VALUES ($1,$2,clock_timestamp() + interval '2 minutes',2000)`, id, "system:serviceaccount:"+id+":k6-approver")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, entry := range []struct{ query, value string }{
			{`DELETE FROM engine.workflow_executions WHERE workflow_id=$1`, id},
			{`DELETE FROM engine.workflow_definitions WHERE definition_id=$1`, op.DefinitionID},
			{`DELETE FROM k6_auth.episode WHERE namespace=$1`, id},
		} {
			if _, err := store.Pool().Exec(cleanup, entry.query, entry.value); err != nil {
				t.Error(err)
			}
		}
	})
	createK6Definition(t, ctx, store, op.DefinitionID, state.K6Activity, state.EffectCooperating)
	createK6Workflow(t, ctx, store, id, id, op.SubmissionKey, op.DefinitionID, state.K6Activity)
	raw, hash := sealK6Plan(t, id, op)
	plan, err := ParseK6Plan(raw, hash, id)
	if err != nil {
		t.Fatal(err)
	}
	public, signer, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	runner := New(store, nil)
	runner.OwnerID, runner.ExternalActivities = state.NewID(), true
	runner.K6 = &K6Options{Plan: plan, ReceiptKey: public}
	return k6EngineFixture{ctx, store, runner, signer, id, op}
}

func createK6Definition(t *testing.T, ctx context.Context, store *state.Store, id, node string, class state.EffectClass) {
	t.Helper()
	versions, _ := json.Marshal(map[string]string{node: "v1"})
	classes, _ := json.Marshal(map[string]state.EffectClass{node: class})
	graph, _ := json.Marshal(map[string][]string{"nodes": {node}})
	if err := store.CreateDefinition(ctx, state.DefinitionInput{DefinitionID: id, Version: 1, DefinitionHash: id,
		Graph: graph, ActivityVersions: versions, EffectClasses: classes}); err != nil {
		t.Fatal(err)
	}
}

func createK6Workflow(t *testing.T, ctx context.Context, store *state.Store, id, namespace, key, definition, node string) {
	t.Helper()
	p, err := partition.ID(id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateWorkflow(ctx, state.CreateWorkflowInput{WorkflowID: id, Namespace: namespace,
		SubmissionKey: key, SubmissionPayloadHash: id, DefinitionID: definition, DefinitionVersion: 1,
		PartitionID: int16(p), InitialNodeID: node, InitialInput: json.RawMessage(`{"from":"submission"}`), ActorID: "test-client"}); err != nil {
		t.Fatal(err)
	}
}

func (f k6EngineFixture) pass(t *testing.T, expected state.WorkflowState) {
	t.Helper()
	result, err := f.runner.Run(f.ctx, f.id)
	if err != nil || result.Workflow.State != expected {
		t.Fatalf("scheduler wanted %s, got %s: %v", expected, result.Workflow.State, err)
	}
}

func (f k6EngineFixture) intent(t *testing.T) state.ApprovalIntent {
	t.Helper()
	intents, err := f.store.ListApprovalIntents(f.ctx, f.id)
	if err != nil || len(intents) != 1 {
		t.Fatal("logical operation lost its single approval intent")
	}
	return intents[0]
}

func (f k6EngineFixture) approve(t *testing.T) state.ApprovalIntent {
	t.Helper()
	f.pass(t, state.StateWaitingApproval)
	ints, err := f.store.RuntimeWork(f.ctx, f.id, "", 10)
	if err != nil || len(ints) != 1 || ints[0] != f.id {
		t.Fatal("repair scan missed waiting approval")
	}
	node, err := f.store.GetNode(f.ctx, f.id, state.K6Activity, 0)
	if err != nil || node.CurrentAttempt != nil {
		t.Fatal("scheduler allocated before approval")
	}
	intent := f.intent(t)
	expected, err := state.K6ProposalHash(f.op.Resource, f.op.Arguments, "0")
	if err != nil || intent.ProposalHash != expected {
		t.Fatal("approval proposal was not derived from the sealed operation")
	}
	var code string
	err = f.store.Pool().QueryRow(f.ctx, `SELECT code FROM k6_auth.approval_decision($1,$2,$3,0,$4,$5,'APPROVED')`,
		intent.IntentID, f.id, state.K6Activity, intent.ProposalHash, "system:serviceaccount:"+f.id+":k6-approver").Scan(&code)
	if err != nil || code != "OK" {
		t.Fatalf("approval decision failed: %s %v", code, err)
	}
	f.pass(t, state.StateWaitingActivity)
	intent = f.intent(t)
	if intent.DispatchStatus != "GRANTED" || intent.GrantToken == "" {
		t.Fatal("scheduler did not issue the grant")
	}
	var brokerPayload json.RawMessage
	if err := f.store.Pool().QueryRow(f.ctx, `SELECT payload FROM engine.outbox WHERE workflow_id=$1 AND event_type='attempt.dispatch'`, f.id).Scan(&brokerPayload); err != nil {
		t.Fatal(err)
	}
	task, err := f.store.RuntimeActivity(f.ctx, f.id, brokerPayload)
	if err != nil || task == nil || string(task.Input) != "null" ||
		bytes.Contains(brokerPayload, []byte(intent.GrantToken)) {
		t.Fatal("broker dispatch did not contain only transport identity")
	}
	return intent
}

type k6TestAllocator struct {
	calls int
}

func (a *k6TestAllocator) Allocate(_ context.Context, envelope state.K6Dispatch) (int64, error) {
	a.calls++
	return 100 + envelope.Attempt, nil
}

func (f k6EngineFixture) claim(t *testing.T, number int64, allocator *k6TestAllocator) (state.ClaimInput, state.ClaimResult, state.K6Dispatch) {
	t.Helper()
	input := state.ClaimInput{WorkflowID: f.id, NodeID: state.K6Activity, WorkerID: state.NewID(),
		RequestID: state.NewID(), ExpectedAttempt: number}
	if _, err := f.store.ClaimAttempt(f.ctx, input); !errors.Is(err, state.ErrStaleClaim) {
		t.Fatal("unauthenticated cooperating claim accepted")
	}
	input.K6Authenticated = true
	claim, err := f.store.ClaimAttempt(f.ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	badWorker := input
	badWorker.WorkerID = state.NewID()
	if _, err := f.store.ClaimAttempt(f.ctx, badWorker); !errors.Is(err, state.ErrStaleClaim) {
		t.Fatal("another pod reused an authenticated request ID")
	}
	envelope, err := f.store.K6ClaimEnvelope(f.ctx, input, claim, allocator)
	if err != nil {
		t.Fatal(err)
	}
	return input, claim, envelope
}

func k6ResultInput(envelope state.K6Dispatch, claim state.ClaimResult) state.ResultInput {
	return state.ResultInput{WorkflowID: envelope.WorkflowID, NodeID: envelope.NodeID, AttemptNumber: claim.AttemptNumber,
		ClaimToken: claim.ClaimToken, AttemptState: state.AttemptSucceeded}
}

func (f k6EngineFixture) proof(envelope state.K6Dispatch, operation string) k6receipt.Proof {
	receipt := k6receipt.Receipt{WorkflowID: envelope.WorkflowID, IntentID: envelope.IntentID, ResourceID: envelope.ResourceID,
		CanonicalHash: envelope.CanonicalHash, LogicalKey: envelope.LogicalKey, ScopeHash: envelope.ScopeHash,
		ExpectedRevision: 0, AppliedRevision: 1, ApplicationID: "application"}
	claim := k6receipt.Claim{NodeID: envelope.NodeID, Iteration: envelope.Iteration, Attempt: envelope.Attempt, Fence: envelope.Fence}
	return k6receipt.Proof{Operation: operation, Receipt: receipt, Signature: k6receipt.Sign(f.signer, claim, operation, receipt)}
}

func (f k6EngineFixture) expireClaim(t *testing.T, number int64) {
	t.Helper()
	if _, err := f.store.Pool().Exec(f.ctx, `UPDATE engine.activity_attempts SET heartbeat_deadline=clock_timestamp()-interval '1 second'
		WHERE workflow_id=$1 AND attempt_number=$2`, f.id, number); err != nil {
		t.Fatal(err)
	}
}

func TestK6ReplacementReusesGrant(t *testing.T) {
	f := setupK6Engine(t)
	original := f.approve(t)
	allocator := &k6TestAllocator{}
	input, first, envelope := f.claim(t, 1, allocator)
	wantHash, err := state.CanonicalPayloadHash(f.op.Arguments)
	if err != nil || envelope.WorkflowID != f.id || envelope.NodeID != state.K6Activity || envelope.Iteration != 0 ||
		envelope.Attempt != 1 || envelope.ResourceID != f.op.Resource || envelope.LogicalKey != f.op.LogicalKey ||
		envelope.IntentID != original.IntentID || envelope.GrantToken != original.GrantToken ||
		envelope.ExpectedRevision != "0" || envelope.CanonicalHash != wantHash || !json.Valid(envelope.Arguments) {
		t.Fatal("claim envelope did not bind the persisted operation and its canonical state")
	}
	retry, err := f.store.ClaimAttempt(f.ctx, input)
	if err != nil || retry.ClaimToken != first.ClaimToken {
		t.Fatal("retried claim returned another token")
	}
	replayed, err := f.store.K6ClaimEnvelope(f.ctx, input, retry, allocator)
	if err != nil || replayed.Fence != envelope.Fence || allocator.calls != 1 {
		t.Fatal("retried claim obtained a different fence")
	}
	f.expireClaim(t, 1)
	f.pass(t, state.StateWaitingActivity)
	current := f.intent(t)
	if current.IntentID != original.IntentID || current.GrantToken != original.GrantToken {
		t.Fatal("replacement minted a different grant")
	}
	_, _, replacement := f.claim(t, 2, allocator)
	if replacement.GrantToken != original.GrantToken || replacement.LogicalKey != envelope.LogicalKey ||
		replacement.ScopeHash != envelope.ScopeHash || replacement.Fence == envelope.Fence || allocator.calls != 2 ||
		!replacement.LookupFirst || envelope.LookupFirst {
		t.Fatal("replacement did not bind the original grant to a fresh lookup-first fence")
	}
	stale := f.proof(envelope, "apply")
	if err := f.store.StageK6Result(f.ctx, k6ResultInput(envelope, first), input.WorkerID, &stale, f.runner.K6.ReceiptKey); !errors.Is(err, state.ErrAttemptNotCurrent) {
		t.Fatalf("stale callback was not fenced: %v", err)
	}
	if f.intent(t).DispatchStatus != "GRANTED" {
		t.Fatal("stale callback marked DISPATCHED")
	}
}

func TestK6OutcomeUnknownResolvesByLookup(t *testing.T) {
	f := setupK6Engine(t)
	f.approve(t)
	worker, claim, envelope := f.claim(t, 1, &k6TestAllocator{})
	input := k6ResultInput(envelope, claim)
	unsigned := f.proof(envelope, "lookup")
	unsigned.Signature = ""
	err := f.store.StageK6Result(f.ctx, input, worker.WorkerID, &unsigned, f.runner.K6.ReceiptKey)
	if !errors.Is(err, state.ErrGrantInvalid) {
		t.Fatal("unsigned worker receipt was trusted")
	}
	f.pass(t, state.StateWaitingActivity)
	if f.intent(t).DispatchStatus != "GRANTED" {
		t.Fatal("worker receipt alone marked DISPATCHED")
	}
	unknown := input
	unknown.AttemptState = state.AttemptOutcomeUnknown
	if err := f.store.StageK6Result(f.ctx, unknown, worker.WorkerID, nil, f.runner.K6.ReceiptKey); err != nil {
		t.Fatal(err)
	}
	f.pass(t, state.StateWaitingActivity)
	attempt, err := f.store.GetAttempt(f.ctx, f.id, state.K6Activity, 0, 1)
	if err != nil || attempt.OutcomeDisposition != "OUTCOME_UNKNOWN" || f.intent(t).DispatchStatus != "GRANTED" {
		t.Fatal("uncertainty was hidden or granted a dispatch acknowledgment")
	}
	lookup := f.proof(envelope, "lookup")
	if err := f.store.StageK6Result(f.ctx, input, worker.WorkerID, &lookup, f.runner.K6.ReceiptKey); err != nil {
		t.Fatal(err)
	}
	if f.intent(t).DispatchStatus != "GRANTED" {
		t.Fatal("worker callback acknowledged dispatch outside the scheduler")
	}
	f.pass(t, state.StateSucceeded)
	attempt, err = f.store.GetAttempt(f.ctx, f.id, state.K6Activity, 0, 1)
	if err != nil || attempt.OutcomeDisposition != "KNOWN_SUCCESS" || f.intent(t).DispatchStatus != "DISPATCHED" {
		t.Fatal("authenticated lookup did not resolve current attempt and acknowledge dispatch")
	}
}

func TestK6ExpiredOperationRequiresReconciliation(t *testing.T) {
	f := setupK6Engine(t)
	original := f.approve(t)
	worker, claim, envelope := f.claim(t, 1, &k6TestAllocator{})
	input := k6ResultInput(envelope, claim)
	input.AttemptState = state.AttemptOutcomeUnknown
	if err := f.store.StageK6Result(f.ctx, input, worker.WorkerID, nil, f.runner.K6.ReceiptKey); err != nil {
		t.Fatal(err)
	}
	f.expireClaim(t, 1)
	if _, err := f.store.Pool().Exec(f.ctx, `UPDATE k6_auth.episode SET uncertainty_us=300000000 WHERE namespace=$1`, f.id); err != nil {
		t.Fatal(err)
	}
	f.pass(t, state.StateReconciliationRequired)
	current := f.intent(t)
	if current.IntentID != original.IntentID || current.GrantToken != original.GrantToken || current.DispatchStatus != "GRANTED" {
		t.Fatal("expired operation changed its original grant")
	}
	attempt, err := f.store.GetAttempt(f.ctx, f.id, state.K6Activity, 0, 1)
	if err != nil || attempt.IsCurrent || attempt.OutcomeDisposition != "OUTCOME_UNKNOWN" {
		t.Fatal("expired uncertain attempt was not left visible")
	}
	var count int
	if err := f.store.Pool().QueryRow(f.ctx, `SELECT count(*) FROM engine.activity_attempts WHERE workflow_id=$1`, f.id).Scan(&count); err != nil || count != 1 {
		t.Fatal("expired operation allocated a replacement")
	}
}

func TestK6CompatibilityPureLineagesReachTerminal(t *testing.T) {
	ctx, store := openM1Database(t)
	defer store.Close()
	t.Setenv("DUR053_K6_ACTIVITY", "0")
	legacy := state.RuntimeRegistry()
	for _, lineage := range []struct{ name, capability string }{
		{"old-old", "0"}, {"old-new-opt-out", "0"}, {"new-opt-out-old", "0"}, {"new-new", "1"},
	} {
		t.Run(lineage.name, func(t *testing.T) {
			t.Setenv("DUR053_K6_ACTIVITY", lineage.capability)
			worker := legacy
			if lineage.capability == "1" {
				worker = state.RuntimeRegistry()
			}
			if err := state.CheckRegistryParity(state.RuntimeRegistry(), worker); err != nil {
				t.Fatal(err)
			}
			for _, pure := range []struct{ node, result string }{{"pure.echo", `{"from":"submission"}`}, {"pure.add", `5`}} {
				id := "k6-compat-" + state.NewID()
				createK6Definition(t, ctx, store, id, pure.node, state.EffectPure)
				createK6Workflow(t, ctx, store, id, id, id, id, pure.node)
				defer func() {
					_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_executions WHERE workflow_id=$1`, id)
					_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_definitions WHERE definition_id=$1`, id)
				}()
				if err := store.CheckActivityCompatibility(ctx, id, worker); err != nil {
					t.Fatal(err)
				}
				runner := New(store, nil)
				runner.ExternalActivities = true
				if result, err := runner.Run(ctx, id); err != nil || result.Workflow.State != state.StateWaitingActivity {
					t.Fatalf("PURE dispatch failed: %v", err)
				}
				claim, err := store.ClaimAttempt(ctx, state.ClaimInput{WorkflowID: id, NodeID: pure.node,
					WorkerID: "pure-worker", ExpectedAttempt: 1, RequestID: state.NewID()})
				if err != nil || claim.EffectClass != state.EffectPure {
					t.Fatalf("PURE claim failed: %v", err)
				}
				if _, err := store.RecordResultReceipt(ctx, state.ResultInput{WorkflowID: id, NodeID: pure.node,
					AttemptNumber: 1, ClaimToken: claim.ClaimToken, AttemptState: state.AttemptSucceeded, Payload: json.RawMessage(pure.result)}); err != nil {
					t.Fatal(err)
				}
				if result, err := runner.Run(ctx, id); err != nil || result.Workflow.State != state.StateSucceeded {
					t.Fatalf("PURE workflow did not reach terminal: %v", err)
				}
			}
		})
	}
}

func TestK6OptOutPausesUnsupportedNode(t *testing.T) {
	f := setupK6Engine(t)
	t.Setenv("DUR053_K6_ACTIVITY", "0")
	f.runner.K6 = nil
	f.pass(t, state.StatePausedUnsupportedVersion)
	node, err := f.store.GetNode(f.ctx, f.id, state.K6Activity, 0)
	if err != nil || node.CurrentAttempt != nil {
		t.Fatal("unsupported node reached dispatch")
	}
}
