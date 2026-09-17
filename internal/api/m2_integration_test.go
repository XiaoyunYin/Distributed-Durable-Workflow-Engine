package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/state"
)

func TestM2WorkerAPIConcurrencyRetriesAndStaleResult(t *testing.T) {
	databaseURL := apiIntegrationDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := state.NewFromURL(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	definitionID := "dur009-api-" + state.NewID()
	namespace := "dur009-api-" + state.NewID()
	if err := store.CreateDefinition(ctx, state.DefinitionInput{
		DefinitionID: definitionID, Version: 1, DefinitionHash: "hash-" + definitionID,
		Graph: []byte(`{"nodes":["root"]}`), ActivityVersions: []byte(`{"root":"v1"}`),
		EffectClasses: []byte(`{"root":"PURE_ACTIVITY"}`),
	}); err != nil {
		t.Fatal(err)
	}
	var workflows []state.Workflow
	var leases []state.Lease
	cleanup := func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		for _, workflow := range workflows {
			_, _ = store.Pool().Exec(cleanupCtx, `DELETE FROM engine.workflow_executions WHERE workflow_id = $1`, workflow.WorkflowID)
		}
		for index := range leases {
			lease := leases[index]
			_ = store.ReleaseLease(cleanupCtx, state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch})
		}
		_, _ = store.Pool().Exec(cleanupCtx, `DELETE FROM engine.workflow_definitions WHERE definition_id = $1`, definitionID)
	}
	defer cleanup()

	newFixture := func(label string) (state.Workflow, state.Lease) {
		t.Helper()
		ownerID := state.NewID()
		var lease state.Lease
		var acquired bool
		for partitionID := int16(0); partitionID < 16; partitionID++ {
			lease, acquired, err = store.AcquireLease(ctx, partitionID, ownerID, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if acquired {
				break
			}
		}
		if !acquired {
			t.Fatal("could not acquire fixture lease")
		}
		var workflowID string
		for {
			candidate := "dur009-" + label + "-" + state.NewID()
			mapped, mapErr := partition.ID(candidate)
			if mapErr == nil && int16(mapped) == lease.PartitionID {
				workflowID = candidate
				break
			}
		}
		created, createErr := store.CreateWorkflow(ctx, state.CreateWorkflowInput{
			WorkflowID: workflowID, Namespace: namespace, SubmissionKey: "key-" + workflowID,
			SubmissionPayloadHash: "sub-v1:" + workflowID, DefinitionID: definitionID,
			DefinitionVersion: 1, PartitionID: lease.PartitionID, InitialNodeID: "root",
			InitialInput: []byte(`{}`), ActorID: "client",
		})
		if createErr != nil {
			t.Fatal(createErr)
		}
		workflow := created.Workflow
		nodeState := state.StateWaitingActivity
		if err := store.ApplyOwnerTransition(ctx, state.OwnerTransitionInput{
			Lease:      state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch},
			WorkflowID: workflowID, ExpectedRevision: 1, NewState: state.StateWaitingActivity,
			ActorID: "scheduler", Reason: "SCHEDULE_ACTIVITY", NodeID: "root", NodeState: &nodeState,
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.CreateAttempt(ctx, state.AttemptInput{
			Lease:      state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch},
			WorkflowID: workflowID, NodeID: "root", Iteration: 0, ExpectedRevision: 2,
			EffectClass: state.EffectPure, HeartbeatDeadline: time.Now().Add(time.Minute), ActorID: "scheduler",
		}); err != nil {
			t.Fatal(err)
		}
		workflows = append(workflows, workflow)
		leases = append(leases, lease)
		return workflow, lease
	}

	workflow, _ := newFixture("claims")
	handler := httptest.NewServer(NewServer(store).Handler())
	defer handler.Close()
	claimURL := fmt.Sprintf("%s/v1/workflows/%s/nodes/root/iterations/0/claim", handler.URL, workflow.WorkflowID)
	client := &http.Client{Timeout: 5 * time.Second}
	start := make(chan struct{})
	type claimOutcome struct {
		status int
		body   []byte
		err    error
		id     string
	}
	outcomes := make(chan claimOutcome, 10)
	var group sync.WaitGroup
	for index := 0; index < 10; index++ {
		requestID := fmt.Sprintf("request-%d", index)
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			response, requestErr := client.Post(claimURL, "application/json", bytes.NewReader([]byte(fmt.Sprintf(
				`{"worker_id":"worker-%s","request_id":"%s"}`, requestID, requestID))))
			if requestErr != nil {
				outcomes <- claimOutcome{err: requestErr, id: requestID}
				return
			}
			body, readErr := io.ReadAll(response.Body)
			_ = response.Body.Close()
			outcomes <- claimOutcome{status: response.StatusCode, body: body, err: readErr, id: requestID}
		}()
	}
	close(start)
	group.Wait()
	close(outcomes)
	var winner claimOutcome
	winners := 0
	for outcome := range outcomes {
		if outcome.err != nil {
			t.Fatal(outcome.err)
		}
		if outcome.status == http.StatusOK {
			winner = outcome
			winners++
		} else if outcome.status != http.StatusConflict {
			t.Fatalf("claim status = %d body = %s", outcome.status, outcome.body)
		}
	}
	if winners != 1 {
		t.Fatalf("concurrent API claim winners = %d, want one", winners)
	}
	var claim claimAttemptResponse
	if err := json.Unmarshal(winner.body, &claim); err != nil {
		t.Fatal(err)
	}
	retryResponse, retryBody := postJSON(t, client, claimURL, fmt.Sprintf(
		`{"worker_id":"worker-retry","request_id":"%s"}`, winner.id))
	if retryResponse.StatusCode != http.StatusOK {
		t.Fatalf("claim retry = %d %s", retryResponse.StatusCode, retryBody)
	}
	var retryClaim claimAttemptResponse
	if err := json.Unmarshal(retryBody, &retryClaim); err != nil {
		t.Fatal(err)
	}
	if retryClaim.AttemptNumber != claim.AttemptNumber || retryClaim.ClaimToken != claim.ClaimToken {
		t.Fatalf("claim retry changed identity: first=%+v retry=%+v", claim, retryClaim)
	}
	resultURL := fmt.Sprintf("%s/v1/workflows/%s/nodes/root/iterations/0/result", handler.URL, workflow.WorkflowID)
	resultBody := fmt.Sprintf(`{"attempt_number":%d,"claim_token":"%s","attempt_state":"SUCCEEDED","payload":{"ok":true}}`, claim.AttemptNumber, claim.ClaimToken)
	resultResponse, _ := postJSON(t, client, resultURL, resultBody)
	if resultResponse.StatusCode != http.StatusOK {
		t.Fatalf("result status = %d", resultResponse.StatusCode)
	}
	retryResult, _ := postJSON(t, client, resultURL, resultBody)
	if retryResult.StatusCode != http.StatusOK {
		t.Fatalf("result retry status = %d", retryResult.StatusCode)
	}
	conflictResult, conflictBody := postJSON(t, client, resultURL, fmt.Sprintf(`{"attempt_number":%d,"claim_token":"%s","attempt_state":"SUCCEEDED","payload":{"ok":false}}`, claim.AttemptNumber, claim.ClaimToken))
	if conflictResult.StatusCode != http.StatusConflict || !bytes.Contains(conflictBody, []byte("RESULT_CONFLICT")) {
		t.Fatalf("conflicting result = %d %s", conflictResult.StatusCode, conflictBody)
	}

	staleWorkflow, staleLease := newFixture("stale")
	staleURL := fmt.Sprintf("%s/v1/workflows/%s/nodes/root/iterations/0/claim", handler.URL, staleWorkflow.WorkflowID)
	staleResponse, staleBody := postJSON(t, client, staleURL, `{"worker_id":"old-worker","request_id":"old-request","attempt_lease_ms":1000}`)
	if staleResponse.StatusCode != http.StatusOK {
		t.Fatalf("stale fixture claim = %d %s", staleResponse.StatusCode, staleBody)
	}
	var oldClaim claimAttemptResponse
	if err := json.Unmarshal(staleBody, &oldClaim); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Pool().Exec(ctx, `UPDATE engine.activity_attempts SET heartbeat_deadline = clock_timestamp() - interval '1 second' WHERE workflow_id = $1`, staleWorkflow.WorkflowID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Pool().Exec(ctx, `UPDATE engine.partition_leases SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE partition_id = $1`, staleLease.PartitionID); err != nil {
		t.Fatal(err)
	}
	newOwner := state.NewID()
	taken, takenOK, err := store.AcquireLease(ctx, staleLease.PartitionID, newOwner, time.Minute)
	if err != nil || !takenOK {
		t.Fatalf("stale fixture takeover = %+v acquired=%v err=%v", taken, takenOK, err)
	}
	_, err = store.TimeoutAttempt(ctx, state.TimeoutInput{
		Lease:      state.LeaseRef{PartitionID: taken.PartitionID, OwnerID: taken.OwnerID, Epoch: taken.Epoch},
		WorkflowID: staleWorkflow.WorkflowID, NodeID: "root", Iteration: 0, AttemptNumber: oldClaim.AttemptNumber,
		ExpectedRevision: 3, ActorID: "new-scheduler", DispatchLease: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	staleResultURL := fmt.Sprintf("%s/v1/workflows/%s/nodes/root/iterations/0/result", handler.URL, staleWorkflow.WorkflowID)
	lateResponse, lateBody := postJSON(t, client, staleResultURL, fmt.Sprintf(
		`{"attempt_number":%d,"claim_token":"%s","attempt_state":"SUCCEEDED","payload":{"late":true}}`, oldClaim.AttemptNumber, oldClaim.ClaimToken))
	if lateResponse.StatusCode != http.StatusConflict || !bytes.Contains(lateBody, []byte("STALE_ATTEMPT")) {
		t.Fatalf("stale API result = %d %s", lateResponse.StatusCode, lateBody)
	}
	_ = store.ReleaseLease(ctx, state.LeaseRef{PartitionID: taken.PartitionID, OwnerID: taken.OwnerID, Epoch: taken.Epoch})
}

func postJSON(t *testing.T, client *http.Client, url, body string) (*http.Response, []byte) {
	t.Helper()
	response, err := client.Post(url, "application/json", bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response, data
}
