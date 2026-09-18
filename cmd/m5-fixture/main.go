// Command m5-fixture is the durable target used by the M5 boundary campaign.
// It creates a small PostgreSQL-backed workflow, performs the operation named
// by the case, and then blocks at the controller boundary.  Killing this
// process therefore leaves the same committed prefix a real target crash
// would leave; it is not a string-only toy process.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/faults"
	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/state"
)

func main() {
	caseID := flag.String("case-id", "F01", "campaign case identifier")
	seed := flag.Int("seed", 1, "deterministic campaign seed")
	boundary := flag.String("boundary", "attempt_claimed", "named fault boundary")
	runID := flag.String("run-id", "", "unique campaign run identifier")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	databaseURL, err := databaseURL()
	if err != nil {
		panic(err)
	}
	store, err := state.NewFromURL(ctx, databaseURL)
	if err != nil {
		panic(err)
	}
	defer store.Close()

	if *runID == "" {
		*runID = state.NewID()
	}
	identity := fixtureIdentity(*runID, *caseID, *seed)
	workflowID := "m5-fixture-" + identity
	definitionID := "m5-fixture-def-" + identity
	partitionNumber, err := partition.ID(workflowID)
	if err != nil {
		panic(err)
	}
	definitionGraph := json.RawMessage(`{"entry":"root","nodes":[{"id":"root","kind":"activity","next":"done"},{"id":"done","kind":"success"}]}`)
	effectClass := state.EffectPure
	if *caseID == "F06-cooperating" {
		effectClass = state.EffectCooperating
	} else if *caseID == "F06-noncooperating" || *caseID == "F11-deadline" {
		effectClass = state.EffectNonCooperating
	}
	if err := store.CreateDefinition(ctx, state.DefinitionInput{
		DefinitionID: definitionID, Version: 1, DefinitionHash: "m5-fixture-" + identity,
		Graph: definitionGraph, ActivityVersions: json.RawMessage(`{"root":"1"}`),
		EffectClasses: json.RawMessage(fmt.Sprintf(`{"root":%q}`, effectClass)),
	}); err != nil {
		panic(err)
	}
	created, err := store.CreateWorkflow(ctx, state.CreateWorkflowInput{
		WorkflowID: workflowID, Namespace: "m5-fixture", SubmissionKey: "key-" + identity,
		SubmissionPayloadHash: "sub-v1:m5-fixture-" + identity,
		DefinitionID:          definitionID, DefinitionVersion: 1, PartitionID: int16(partitionNumber),
		InitialNodeID: "root", InitialInput: json.RawMessage(fmt.Sprintf(`{"seed":%d,"run_id":%q}`, *seed, *runID)), ActorID: "m5-campaign",
	})
	if err != nil {
		panic(err)
	}
	if !created.Created {
		panic("fixture workflow was unexpectedly reused")
	}
	ownerID := state.NewID()
	lease, acquired, err := store.AcquireLease(ctx, int16(partitionNumber), ownerID, time.Minute)
	if err != nil || !acquired {
		panic(fmt.Sprintf("acquire fixture lease: acquired=%v err=%v", acquired, err))
	}
	defer func() {
		_ = store.ReleaseLease(context.Background(), state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch})
	}()

	fields := map[string]any{
		"durable_workflow_id": workflowID,
		"durable_node_id":     "root",
		"fixture_case_id":     *caseID,
		"fixture_seed":        *seed,
		"fixture_run_id":      *runID,
	}
	client, err := faults.FromEnvironment()
	if err != nil {
		panic(err)
	}
	if client == nil {
		panic("M5 fixture requires the fault controller environment")
	}
	defer client.Close()

	hit := func() {
		if err := client.Hit(*boundary, fields); err != nil {
			panic(err)
		}
		if err := client.Emit("released", *boundary, fields); err != nil {
			panic(err)
		}
	}

	if *caseID == "F02-rollback" {
		hit()
		return
	}
	if *caseID == "F10-timer-join" {
		if _, err := store.ScheduleTimer(ctx, state.ScheduleTimerInput{
			Lease:      state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch},
			WorkflowID: workflowID, NodeID: "root", Iteration: 0, ExpectedRevision: 1,
			DueAt: time.Now().Add(-time.Second), Purpose: "WORKFLOW_TIMER", ActorID: "m5-campaign",
		}); err != nil {
			panic(err)
		}
		workflow, err := store.GetWorkflow(ctx, workflowID)
		if err != nil {
			panic(err)
		}
		nodeState := state.StateRunnable
		if err := store.ApplyOwnerTransition(ctx, state.OwnerTransitionInput{
			Lease:      state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch},
			WorkflowID: workflowID, ExpectedRevision: workflow.Revision, NewState: state.StateRunnable,
			ActorID: "m5-campaign", Reason: "TIMER_DUE", NodeID: "root", Iteration: 0, NodeState: &nodeState,
		}); err != nil {
			panic(err)
		}
		hit()
		return
	}
	if *caseID == "F06-cooperating" {
		logicalKey := "m5-effect-" + identity
		grant := createFixtureGrant(ctx, store, workflowID, lease, logicalKey)
		arguments := json.RawMessage(`{"action":"restore","value":1}`)
		argumentHash := payloadHash(arguments)
		if _, err := store.ApplyEffect(ctx, state.EffectApplyInput{
			WorkflowID: workflowID, NodeID: "root", Iteration: 0, LogicalEffectKey: logicalKey,
			ArgumentHash: argumentHash, AttemptNumber: 1, GrantScopeHash: grant.GrantScopeHash,
			ExpectedResourceRevision: "0", RequestID: "m5-effect-request-" + identity,
			ResourceID: "m5-resource-" + workflowID, FenceToken: 1, State: arguments,
			IntentID: grant.IntentID, GrantToken: grant.GrantToken,
		}); err != nil {
			panic(err)
		}
		fields["durable_effect_key"] = logicalKey
		hit()
		return
	}
	if *caseID == "F11-cancel-grant" || *caseID == "F11-dispatch-grant" {
		logicalKey := "m5-approval-effect-" + identity
		grant := createFixtureGrant(ctx, store, workflowID, lease, logicalKey)
		fields["durable_effect_key"] = logicalKey
		if *caseID == "F11-cancel-grant" {
			workflow, err := store.GetWorkflow(ctx, workflowID)
			if err != nil {
				panic(err)
			}
			request, err := store.RequestCancellation(ctx, state.CancellationRequestInput{
				WorkflowID: workflowID, ClientKey: "m5-cancel-" + identity, ObservedRevision: workflow.Revision,
			})
			if err != nil {
				panic(err)
			}
			if _, err := store.ApplyCancellationRequest(ctx,
				state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch},
				request.RequestID, "m5-campaign"); err != nil {
				panic(err)
			}
		}
		_ = grant
		hit()
		return
	}
	if *caseID == "F05-offset" {
		events, listErr := store.ListOutbox(ctx, workflowID, 1)
		if listErr != nil || len(events) == 0 {
			panic(fmt.Sprintf("list offset fixture event: %v", listErr))
		}
		if err := store.EnsureConsumerOffset(ctx, "m5-consumer-"+identity, events[0].Topic, 0, 0); err != nil {
			panic(err)
		}
		fields["durable_effect_key"] = events[0].EventID
		hit()
		return
	}
	if err := store.ApplyOwnerTransition(ctx, state.OwnerTransitionInput{
		Lease:      state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch},
		WorkflowID: workflowID, ExpectedRevision: 1, NewState: state.StateWaitingActivity,
		ActorID: "m5-campaign", Reason: "M5_FIXTURE_ACTIVITY", NodeID: "root", Iteration: 0,
		NodeState: func() *state.WorkflowState { value := state.StateWaitingActivity; return &value }(),
	}); err != nil {
		panic(err)
	}
	workflow, err := store.GetWorkflow(ctx, workflowID)
	if err != nil {
		panic(err)
	}
	_, err = store.CreateAttempt(ctx, state.AttemptInput{
		Lease:      state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch},
		WorkflowID: workflowID, NodeID: "root", Iteration: 0, ExpectedRevision: workflow.Revision,
		EffectClass: effectClass, LogicalEffectKey: func() string {
			if effectClass != state.EffectPure {
				return "m5-effect-" + identity
			}
			return ""
		}(), HeartbeatDeadline: time.Now().Add(time.Minute), ActorID: "m5-campaign",
	})
	if err != nil {
		panic(err)
	}
	claim, err := store.ClaimAttempt(ctx, state.ClaimInput{WorkflowID: workflowID, NodeID: "root", Iteration: 0,
		WorkerID: "m5-fixture-worker", RequestID: "m5-request-" + identity, AttemptLease: time.Minute})
	if err != nil {
		panic(err)
	}
	fields["durable_attempt_number"] = claim.AttemptNumber
	if *caseID == "F08-lease-fence" {
		if err := store.ReleaseLease(ctx, state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch}); err != nil {
			panic(err)
		}
		takeover, tookOver, err := store.AcquireLease(ctx, lease.PartitionID, state.NewID(), time.Minute)
		if err != nil || !tookOver {
			panic(fmt.Sprintf("take over fixture lease: acquired=%v err=%v", tookOver, err))
		}
		defer func() {
			_ = store.ReleaseLease(context.Background(), state.LeaseRef{PartitionID: takeover.PartitionID, OwnerID: takeover.OwnerID, Epoch: takeover.Epoch})
		}()
		fields["durable_lease_epoch"] = takeover.Epoch
		hit()
		return
	}
	if *caseID == "F09-stale-result" {
		workflow, err = store.GetWorkflow(ctx, workflowID)
		if err != nil {
			panic(err)
		}
		if _, err := store.Pool().Exec(ctx, `UPDATE engine.activity_attempts SET heartbeat_deadline = clock_timestamp() - interval '1 second' WHERE workflow_id = $1 AND node_id = 'root' AND iteration = 0 AND attempt_number = 1`, workflowID); err != nil {
			panic(err)
		}
		if _, err := store.TimeoutAttempt(ctx, state.TimeoutInput{Lease: state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch},
			WorkflowID: workflowID, NodeID: "root", Iteration: 0, AttemptNumber: 1, ExpectedRevision: workflow.Revision,
			ActorID: "m5-campaign", DispatchLease: time.Minute}); err != nil {
			panic(err)
		}
		hit()
		return
	}
	if *caseID == "F11-deadline" {
		workflow, err := store.GetWorkflow(ctx, workflowID)
		if err != nil {
			panic(err)
		}
		if _, err := store.Pool().Exec(ctx, `UPDATE engine.activity_attempts SET heartbeat_deadline = clock_timestamp() - interval '1 second' WHERE workflow_id = $1 AND node_id = 'root' AND iteration = 0 AND attempt_number = 1`, workflowID); err != nil {
			panic(err)
		}
		if _, err := store.TimeoutAttempt(ctx, state.TimeoutInput{
			Lease:      state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch},
			WorkflowID: workflowID, NodeID: "root", Iteration: 0, AttemptNumber: 1, ExpectedRevision: workflow.Revision,
			ActorID: "m5-campaign", DispatchLease: time.Minute,
		}); err != nil {
			panic(err)
		}
		hit()
		return
	}
	if *caseID == "F06-noncooperating" {
		logicalKey := "m5-effect-" + identity
		argumentHash := payloadHash(json.RawMessage(`{"action":"unknown"}`))
		if _, err := store.RecordUnknownEffect(ctx, workflowID, logicalKey, argumentHash, "", claim.AttemptNumber); err != nil {
			panic(err)
		}
		fields["durable_effect_key"] = logicalKey
		hit()
		return
	}
	if *caseID == "F11-cancel-completion" {
		workflow, err := store.GetWorkflow(ctx, workflowID)
		if err != nil {
			panic(err)
		}
		request, err := store.RequestCancellation(ctx, state.CancellationRequestInput{
			WorkflowID: workflowID, ClientKey: "m5-cancel-" + identity, ObservedRevision: workflow.Revision,
		})
		if err != nil {
			panic(err)
		}
		if _, err := store.ApplyCancellationRequest(ctx,
			state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch},
			request.RequestID, "m5-campaign"); err != nil {
			panic(err)
		}
		hit()
		return
	}

	switch {
	case *caseID == "F03-relay-retry" || *caseID == "F04-after-ack":
		if _, err := store.ClaimOutbox(ctx, state.NewID(), 1, time.Minute); err != nil {
			panic(err)
		}
		hit()
	case strings.Contains(*caseID, "result") || *caseID == "F07-crash-resume" || *caseID == "F10-timer-join":
		if _, err := store.RecordResultReceipt(ctx, state.ResultInput{WorkflowID: workflowID, NodeID: "root", Iteration: 0,
			AttemptNumber: claim.AttemptNumber, ClaimToken: claim.ClaimToken, AttemptState: state.AttemptSucceeded,
			Payload: json.RawMessage(`{"ok":true}`), EventType: "activity.result"}); err != nil {
			panic(err)
		}
		if *boundary == "result_recorded" {
			hit()
		}
		if *caseID == "F07-crash-resume" {
			workflow, err = store.GetWorkflow(ctx, workflowID)
			if err != nil {
				panic(err)
			}
			if _, err := store.ConsumeResult(ctx, state.ConsumeResultInput{Lease: state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch},
				WorkflowID: workflowID, NodeID: "root", Iteration: 0, AttemptNumber: claim.AttemptNumber,
				ExpectedRevision: workflow.Revision, NewWorkflowState: state.StateSucceeded, ActorID: "m5-campaign"}); err != nil {
				panic(err)
			}
		}
		if *boundary != "result_recorded" {
			hit()
		}
	default:
		hit()
	}
}

func fixtureIdentity(runID, caseID string, seed int) string {
	clean := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' {
			return r
		}
		return '-'
	}, caseID)
	return fmt.Sprintf("%s-%s-%d", clean, runID, seed)
}

func payloadHash(payload json.RawMessage) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func createFixtureGrant(ctx context.Context, store *state.Store, workflowID string, lease state.Lease, logicalKey string) state.ApprovalGrant {
	ref := state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch}
	arguments := json.RawMessage(`{"action":"restore","value":1}`)
	intent, err := store.CreateApprovalIntent(ctx, state.ApprovalIntentInput{
		Lease: ref, WorkflowID: workflowID, NodeID: "root", Iteration: 0, ExpectedRevision: 1,
		Target: "m5-resource-" + workflowID, CanonicalArguments: arguments,
		ExpectedResourceRevision: "0", ValidUntil: time.Now().Add(time.Hour), ActorID: "m5-campaign",
	})
	if err != nil {
		panic(err)
	}
	if _, err := store.RecordApprovalDecision(ctx, state.ApprovalDecisionInput{
		IntentID: intent.IntentID, ApproverID: "m5-approver", ProposalHash: intent.ProposalHash,
		Decision: "APPROVED", DecisionReason: "M5 fixture",
	}); err != nil {
		panic(err)
	}
	workflow, err := store.GetWorkflow(ctx, workflowID)
	if err != nil {
		panic(err)
	}
	grant, err := store.ApplyApproval(ctx, state.ApplyApprovalInput{
		Lease: ref, WorkflowID: workflowID, NodeID: "root", Iteration: 0,
		IntentID: intent.IntentID, LogicalEffectKey: logicalKey, ExpectedRevision: workflow.Revision,
		ActorID: "m5-campaign",
	})
	if err != nil {
		panic(err)
	}
	return grant
}

func databaseURL() (string, error) {
	if value := os.Getenv("DURABLE_DATABASE_URL"); value != "" {
		return value, nil
	}
	if value := os.Getenv("DATABASE_URL"); value != "" {
		return value, nil
	}
	working, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for directory := working; ; directory = filepath.Dir(directory) {
		data, readErr := os.ReadFile(filepath.Join(directory, ".env"))
		if readErr == nil {
			values := map[string]string{}
			for _, line := range strings.Split(string(data), "\n") {
				parts := strings.SplitN(strings.TrimSpace(line), "=", 2)
				if len(parts) == 2 {
					values[parts[0]] = strings.Trim(parts[1], "\r\"")
				}
			}
			port := values["POSTGRES_PORT"]
			if port == "" {
				port = "5432"
			}
			return (&url.URL{Scheme: "postgresql", User: url.UserPassword(values["POSTGRES_USER"], values["POSTGRES_PASSWORD"]),
				Host: "127.0.0.1:" + port, Path: "/" + values["POSTGRES_DB"], RawQuery: "sslmode=disable"}).String(), nil
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			break
		}
	}
	return "", fmt.Errorf("set DURABLE_DATABASE_URL or provide .env")
}
