package incident

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/api"
	"durable-agent-execution-engine/internal/effects"
	"durable-agent-execution-engine/internal/engine"
	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/state"
)

func TestDUR033AProductionPath(t *testing.T) {
	if os.Getenv("DURABLE_RUN_INTEGRATION") != "1" && os.Getenv("DURABLE_REQUIRE_DATABASE") != "1" {
		t.Skip("DUR-033A production integration requires DURABLE_RUN_INTEGRATION=1 or service CI mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	store, err := state.NewFromURL(ctx, dur033aDatabaseURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	runID := state.NewID()
	definitionID := DefinitionID + "-" + runID
	namespace := "dur033a-" + runID
	source := NewSourceCorpus(store)
	fixture, err := source.SeedFixture(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = store.Pool().Exec(cleanupCtx, `DELETE FROM effects.effect_call_attempts WHERE workflow_id LIKE $1`, "%"+runID+"%")
		_, _ = store.Pool().Exec(cleanupCtx, `DELETE FROM effects.effect_records WHERE workflow_id LIKE $1`, "%"+runID+"%")
		_, _ = store.Pool().Exec(cleanupCtx, `DELETE FROM effects.sandbox_effect_state WHERE resource_id LIKE $1`, "%"+runID+"%")
		_, _ = store.Pool().Exec(cleanupCtx, `DELETE FROM effects.effect_resource_fences WHERE resource_id LIKE $1`, "%"+runID+"%")
		_, _ = store.Pool().Exec(cleanupCtx, `DELETE FROM engine.workflow_executions WHERE namespace = $1`, namespace)
		_, _ = store.Pool().Exec(cleanupCtx, `DELETE FROM engine.workflow_definitions WHERE definition_id = $1`, definitionID)
		_ = source.DeleteFixture(cleanupCtx, fixture)
	}()
	if err := InstallDefinition(ctx, store, definitionID); err != nil {
		t.Fatal(err)
	}

	ownerID, heldLease, workflowID := acquireFixturePartition(t, ctx, store, runID)
	defer func() {
		_ = store.ReleaseLease(context.Background(), state.LeaseRef{PartitionID: heldLease.PartitionID, OwnerID: heldLease.OwnerID, Epoch: heldLease.Epoch})
	}()
	handler := api.NewServer(store).Handler()
	httpServer := httptest.NewServer(handler)
	defer httpServer.Close()

	var submitted struct {
		Workflow struct {
			WorkflowID  string              `json:"workflow_id"`
			Revision    int64               `json:"revision"`
			State       state.WorkflowState `json:"state"`
			PartitionID int16               `json:"partition_id"`
		} `json:"workflow"`
		Created bool `json:"created"`
	}
	status, body := postJSON(t, httpServer.URL+"/v1/workflows", map[string]any{
		"workflow_id": workflowID, "namespace": namespace, "submission_key": "incident-" + runID,
		"payload":       map[string]any{"case_id": fixture.CaseID},
		"definition_id": definitionID, "definition_version": DefinitionVersion,
		"initial_node_id": "investigate", "initial_input": map[string]any{"query": "connection pool saturation"},
		"actor_id": "dur033a-client",
	}, &submitted)
	if status != http.StatusCreated || !submitted.Created || submitted.Workflow.WorkflowID != workflowID {
		t.Fatalf("submission status=%d body=%s response=%+v", status, body, submitted)
	}

	arguments := json.RawMessage(`{"action":"restore","replicas":1}`)
	target := "dur033a-resource-" + runID
	investigation := InvestigationDriver{Corpus: source, Query: "connection pool saturation",
		Diagnosis: "connection_pool_saturation", TargetResourceID: target,
		CanonicalArguments: arguments, ExpectedResourceRevision: "0"}
	firstEngine := engine.New(store, investigation)
	firstEngine.OwnerID, firstEngine.WorkerID, firstEngine.ActorID = ownerID, "dur033a-worker", "dur033a-scheduler"
	firstEngine.MaxSteps = 2
	first, err := firstEngine.Run(ctx, workflowID)
	if err != nil {
		t.Fatalf("investigation engine run: %v", err)
	}
	if first.Workflow.State != state.StateRunnable {
		t.Fatalf("investigation did not leave workflow runnable: %+v", first.Workflow)
	}
	hits, err := source.Search(ctx, "connection pool saturation")
	if err != nil || len(hits) == 0 || hits[0].ChunkID != fixture.ChunkID {
		t.Fatalf("production source query hits=%+v err=%v", hits, err)
	}

	approvalOwner := state.NewID()
	lease, acquired, err := store.AcquireLease(ctx, submitted.Workflow.PartitionID, approvalOwner, time.Minute)
	if err != nil || !acquired {
		t.Fatalf("acquire approval lease: lease=%+v acquired=%v err=%v", lease, acquired, err)
	}
	remediationKey := workflowID + ":remediation:0"
	var intent state.ApprovalIntent
	status, body = postJSON(t, httpServer.URL+"/v1/workflows/"+workflowID+"/nodes/remediation/iterations/0/approval", map[string]any{
		"partition_id": lease.PartitionID, "owner_id": lease.OwnerID, "epoch": lease.Epoch,
		"expected_revision": first.Workflow.Revision, "target": target,
		"canonical_arguments": json.RawMessage(arguments), "expected_resource_revision": "0",
		"valid_until": time.Now().Add(time.Hour), "actor_id": "dur033a-scheduler",
	}, &intent)
	if status != http.StatusCreated || intent.IntentID == "" {
		t.Fatalf("approval intent status=%d body=%s intent=%+v", status, body, intent)
	}

	effectService := effects.New(store)
	argumentHash, err := ArgumentHash(arguments)
	if err != nil {
		t.Fatal(err)
	}
	// R049: an effect cannot run before the proposal has an approved grant.
	if _, err := effectService.Apply(ctx, state.EffectApplyInput{
		WorkflowID: workflowID, NodeID: "remediation", Iteration: 0, LogicalEffectKey: remediationKey,
		ArgumentHash: argumentHash, AttemptNumber: 1, ExpectedResourceRevision: "0", RequestID: "pre-approval-" + runID,
		ResourceID: target, FenceToken: 1, State: arguments, IntentID: intent.IntentID, GrantToken: "not-granted",
	}); !errors.Is(err, state.ErrGrantInvalid) {
		t.Fatalf("approval-before-dispatch error=%v, want ErrGrantInvalid", err)
	}

	var grant state.ApprovalGrant
	status, body = postJSON(t, httpServer.URL+"/v1/approvals/"+intent.IntentID+"/decision", map[string]any{
		"approver_id": "dur033a-approver", "proposal_hash": intent.ProposalHash,
		"decision": "APPROVED", "decision_reason": "bounded integration test",
	}, &intent)
	if status != http.StatusOK {
		t.Fatalf("approval decision status=%d body=%s", status, body)
	}
	// R049: applying with a stale workflow revision is rejected before a grant
	// is issued, so the scheduler cannot authorize a stale proposal.
	status, body = postJSON(t, httpServer.URL+"/v1/approvals/"+intent.IntentID+"/apply", map[string]any{
		"partition_id": lease.PartitionID, "owner_id": lease.OwnerID, "epoch": lease.Epoch,
		"workflow_id": workflowID, "node_id": "remediation", "iteration": 0,
		"expected_revision": first.Workflow.Revision, "intent_id": intent.IntentID,
		"logical_effect_key": remediationKey, "actor_id": "dur033a-scheduler",
	}, &grant)
	if status != http.StatusConflict {
		t.Fatalf("stale approval apply status=%d body=%s", status, body)
	}
	workflowAfterIntent, err := store.GetWorkflow(ctx, workflowID)
	if err != nil {
		t.Fatal(err)
	}
	status, body = postJSON(t, httpServer.URL+"/v1/approvals/"+intent.IntentID+"/apply", map[string]any{
		"partition_id": lease.PartitionID, "owner_id": lease.OwnerID, "epoch": lease.Epoch,
		"workflow_id": workflowID, "node_id": "remediation", "iteration": 0,
		"expected_revision": workflowAfterIntent.Revision, "intent_id": intent.IntentID,
		"logical_effect_key": remediationKey, "actor_id": "dur033a-scheduler",
	}, &grant)
	if status != http.StatusOK || grant.GrantToken == "" {
		t.Fatalf("approval apply status=%d body=%s grant=%+v", status, body, grant)
	}

	attack := func(label string, input state.EffectApplyInput, want error) {
		t.Helper()
		_, got := effectService.Apply(ctx, input)
		if !errors.Is(got, want) {
			t.Fatalf("R049 %s error=%v, want %v", label, got, want)
		}
	}
	base := state.EffectApplyInput{WorkflowID: workflowID, NodeID: "remediation", Iteration: 0,
		LogicalEffectKey: remediationKey, ArgumentHash: argumentHash, AttemptNumber: 1,
		GrantScopeHash: grant.GrantScopeHash, ExpectedResourceRevision: "0", ResourceID: target,
		FenceToken: 1, State: arguments, IntentID: intent.IntentID, GrantToken: grant.GrantToken}
	// R049: resource, canonical arguments, and expected resource revision are
	// bound to the approved intent, not asserted by the effect caller.
	wrongResource := base
	wrongResource.RequestID = "wrong-resource-" + runID
	wrongResource.ResourceID = "dur033a-other-resource-" + runID
	attack("resource", wrongResource, state.ErrEffectResource)
	wrongArguments := base
	wrongArguments.RequestID = "wrong-arguments-" + runID
	wrongArguments.State = json.RawMessage(`{"action":"delete","replicas":0}`)
	wrongArguments.ArgumentHash, _ = ArgumentHash(wrongArguments.State)
	attack("arguments", wrongArguments, state.ErrEffectArguments)
	wrongRevision := base
	wrongRevision.RequestID = "wrong-revision-" + runID
	wrongRevision.ExpectedResourceRevision = "99"
	attack("resource revision", wrongRevision, state.ErrEffectVersion)

	secondEngine := engine.New(store, EffectDriver{Service: effectService, IntentID: intent.IntentID,
		GrantToken: grant.GrantToken, GrantScopeHash: grant.GrantScopeHash,
		TargetResourceID: target, ExpectedRevision: "0"})
	secondEngine.OwnerID, secondEngine.WorkerID, secondEngine.ActorID = approvalOwner, "dur033a-worker", "dur033a-scheduler"
	result, err := secondEngine.Run(ctx, workflowID)
	if err != nil || result.Workflow.State != state.StateSucceeded {
		t.Fatalf("approved remediation result=%+v err=%v", result, err)
	}
	if _, err := effectService.Lookup(ctx, workflowID, remediationKey); err != nil {
		t.Fatalf("effect receipt missing: %v", err)
	}
	// R049: the single-use grant is scoped to the approved logical effect key.
	reused := base
	reused.LogicalEffectKey = workflowID + ":different-effect:0"
	reused.RequestID = "grant-reuse-" + runID
	attack("grant reuse", reused, state.ErrGrantInvalid)

	entries, err := store.ListHistory(ctx, workflowID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, reason := range []string{"WORKFLOW_CREATED", "RESULT_CONSUMED", "APPROVAL_REQUESTED", "APPROVAL_GRANTED"} {
		found := false
		for _, entry := range entries {
			if entry.Reason == reason {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("history missing scheduler/API evidence %q: %+v", reason, entries)
		}
	}
}

func postJSON(t *testing.T, endpoint string, request any, response any) (int, string) {
	t.Helper()
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, response); err != nil {
			t.Fatalf("decode HTTP response: %v; body=%s", err, raw)
		}
	}
	return resp.StatusCode, string(raw)
}

func acquireFixturePartition(t *testing.T, ctx context.Context, store *state.Store, runID string) (string, state.Lease, string) {
	t.Helper()
	ownerID := state.NewID()
	var lease state.Lease
	acquired := false
	for partitionID := int16(0); partitionID < 16; partitionID++ {
		candidate, ok, err := store.AcquireLease(ctx, partitionID, ownerID, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			lease, acquired = candidate, true
			break
		}
	}
	if !acquired {
		t.Fatal("no free partition for DUR-033A integration")
	}
	for attempt := 0; attempt < 1000; attempt++ {
		workflowID := "dur033a-" + runID + "-" + state.NewID()
		mapped, err := partition.ID(workflowID)
		if err == nil && int16(mapped) == lease.PartitionID {
			return ownerID, lease, workflowID
		}
	}
	t.Fatalf("could not generate workflow for partition %d", lease.PartitionID)
	return "", state.Lease{}, ""
}

func dur033aDatabaseURL(t *testing.T) string {
	t.Helper()
	if value := os.Getenv("DURABLE_DATABASE_URL"); value != "" {
		return value
	}
	working, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
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
				Host: "127.0.0.1:" + port, Path: "/" + values["POSTGRES_DB"], RawQuery: "sslmode=disable"}).String()
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			break
		}
	}
	t.Fatal("set DURABLE_DATABASE_URL or provide .env for DUR-033A integration")
	return ""
}
