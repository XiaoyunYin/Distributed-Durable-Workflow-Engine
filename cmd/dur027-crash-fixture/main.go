// Command dur027-crash-fixture is the durable target for DUR-027 fault
// episodes. It owns a PostgreSQL scheduler lease, creates a claimed activity,
// and then waits at an acknowledged boundary. The parent either kills this
// process tree or asks the paused process to resume after takeover.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/state"
)

type boundaryRecord struct {
	Boundary            string    `json:"boundary"`
	WorkflowID          string    `json:"workflow_id"`
	DefinitionID        string    `json:"definition_id"`
	Namespace           string    `json:"namespace"`
	Partition           int16     `json:"partition"`
	OwnerID             string    `json:"owner_id"`
	Epoch               int64     `json:"epoch"`
	AttemptNumber       int64     `json:"attempt_number"`
	ClaimToken          string    `json:"claim_token"`
	LeaseExpiresAt      time.Time `json:"lease_expires_at"`
	AttemptDeadlineAt   time.Time `json:"attempt_deadline_at"`
	RenewalIntervalMS   int       `json:"renewal_interval_ms"`
	Mode                string    `json:"mode"`
	RenewalsBeforeFault int       `json:"renewals_before_fault"`
}

func main() {
	databaseURL := flag.String("database-url", "", "PostgreSQL connection URL")
	runID := flag.String("run-id", "", "unique episode identifier")
	mode := flag.String("mode", "owner_crash", "owner_crash or owner_pause")
	ttlMS := flag.Int("ttl-ms", 100, "scheduler lease TTL in milliseconds")
	resumeFile := flag.String("resume-file", "", "file created by the parent to resume a paused owner")
	flag.Parse()
	if strings.TrimSpace(*databaseURL) == "" || strings.TrimSpace(*runID) == "" || (*mode != "owner_crash" && *mode != "owner_pause") || *ttlMS <= 0 {
		panic("database URL, run ID, valid mode, and positive TTL are required")
	}
	ttl := time.Duration(*ttlMS) * time.Millisecond
	ctx := context.Background()
	store, err := state.NewFromURL(ctx, *databaseURL)
	if err != nil {
		panic(err)
	}
	defer store.Close()

	namespace := "dur027-crash-" + *runID
	definitionID := namespace + "-definition"
	if err := store.CreateDefinition(ctx, state.DefinitionInput{
		DefinitionID: definitionID, Version: 1, DefinitionHash: "dur027-" + *runID,
		Graph:            []byte(`{"entry":"root","nodes":[{"id":"root","kind":"activity"}]}`),
		ActivityVersions: []byte(`{"root":"1"}`), EffectClasses: []byte(`{"root":"PURE_ACTIVITY"}`),
	}); err != nil {
		panic(err)
	}

	ownerID := state.NewID()
	var lease state.Lease
	var workflowID string
	var acquired bool
	for suffix := 0; suffix < 256; suffix++ {
		candidate := fmt.Sprintf("dur027-crash-%s-workflow-%d", *runID, suffix)
		mapped, mapErr := partition.ID(candidate)
		if mapErr != nil {
			panic(mapErr)
		}
		var acquireErr error
		lease, acquired, acquireErr = store.AcquireLease(ctx, int16(mapped), ownerID, ttl)
		if acquireErr != nil {
			panic(acquireErr)
		}
		if acquired {
			workflowID = candidate
			break
		}
	}
	if !acquired {
		panic("dur027 crash fixture could not acquire a partition lease")
	}
	ref := state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerID, Epoch: lease.Epoch}
	defer func() { _ = store.ReleaseLease(context.Background(), ref) }()
	created, err := store.CreateWorkflow(ctx, state.CreateWorkflowInput{
		WorkflowID: workflowID, Namespace: namespace, SubmissionKey: "submission-" + *runID,
		SubmissionPayloadHash: "sub-v1:" + *runID, DefinitionID: definitionID, DefinitionVersion: 1,
		PartitionID: lease.PartitionID, InitialNodeID: "root", InitialInput: []byte(`{"dur027":true}`), ActorID: "dur027-fixture",
	})
	if err != nil || !created.Created {
		panic(fmt.Sprintf("create workflow: created=%v err=%v", created.Created, err))
	}
	nodeState := state.StateWaitingActivity
	if err := store.ApplyOwnerTransition(ctx, state.OwnerTransitionInput{
		Lease: ref, WorkflowID: workflowID, ExpectedRevision: 1, NewState: state.StateWaitingActivity,
		ActorID: "dur027-fixture", Reason: "DUR027_FIXTURE_ACTIVITY", NodeID: "root", Iteration: 0,
		NodeState: &nodeState,
	}); err != nil {
		panic(err)
	}
	workflow, err := store.GetWorkflow(ctx, workflowID)
	if err != nil {
		panic(err)
	}
	attempt, err := store.CreateAttempt(ctx, state.AttemptInput{
		Lease: ref, WorkflowID: workflowID, NodeID: "root", Iteration: 0, ExpectedRevision: workflow.Revision,
		EffectClass: state.EffectPure, HeartbeatDeadline: time.Now().Add(2 * ttl), ActorID: "dur027-fixture",
	})
	if err != nil {
		panic(err)
	}
	claim, err := store.ClaimAttempt(ctx, state.ClaimInput{WorkflowID: workflowID, NodeID: "root", Iteration: 0,
		WorkerID: "dur027-fixture-worker", RequestID: "request-" + *runID, AttemptLease: 2 * ttl})
	if err != nil {
		panic(err)
	}
	lease, err = store.RenewLease(ctx, ref, ttl)
	if err != nil {
		panic(err)
	}
	deadline := claim.HeartbeatDeadline
	if deadline.IsZero() {
		if attempt.HeartbeatDeadline != nil {
			deadline = *attempt.HeartbeatDeadline
		}
	}
	record := boundaryRecord{Boundary: "owner_ready", WorkflowID: workflowID, DefinitionID: definitionID,
		Namespace: namespace, Partition: lease.PartitionID, OwnerID: ownerID, Epoch: lease.Epoch,
		AttemptNumber: claim.AttemptNumber, ClaimToken: claim.ClaimToken, LeaseExpiresAt: lease.LeaseExpiresAt,
		AttemptDeadlineAt: deadline, RenewalIntervalMS: *ttlMS / 3, Mode: *mode, RenewalsBeforeFault: 1}
	writeBoundary(record)

	if *mode == "owner_crash" {
		go renewUntilKilled(ctx, store, ref, ttl)
		select {}
	}
	if strings.TrimSpace(*resumeFile) == "" {
		panic("owner_pause requires resume-file")
	}
	for {
		if data, readErr := os.ReadFile(*resumeFile); readErr == nil && strings.TrimSpace(string(data)) == "pause" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	for {
		if time.Now().After(lease.LeaseExpiresAt) {
			writeBoundary(boundaryRecord{Boundary: "owner_paused_expired", WorkflowID: workflowID, DefinitionID: definitionID,
				Namespace: namespace, Partition: lease.PartitionID, OwnerID: ownerID, Epoch: lease.Epoch,
				AttemptNumber: claim.AttemptNumber, LeaseExpiresAt: lease.LeaseExpiresAt, AttemptDeadlineAt: deadline,
				RenewalIntervalMS: *ttlMS / 3, Mode: *mode, RenewalsBeforeFault: 1})
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	for {
		if _, err := os.Stat(*resumeFile); err == nil {
			_, renewErr := store.RenewLease(ctx, ref, ttl)
			if !errors.Is(renewErr, state.ErrLeaseNotOwned) {
				panic(fmt.Sprintf("paused owner renewal = %v, want ErrLeaseNotOwned", renewErr))
			}
			writeBoundary(boundaryRecord{Boundary: "owner_resumed_stale", WorkflowID: workflowID, DefinitionID: definitionID,
				Namespace: namespace, Partition: lease.PartitionID, OwnerID: ownerID, Epoch: lease.Epoch,
				AttemptNumber: claim.AttemptNumber, LeaseExpiresAt: lease.LeaseExpiresAt, AttemptDeadlineAt: deadline,
				RenewalIntervalMS: *ttlMS / 3, Mode: *mode})
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func renewUntilKilled(ctx context.Context, store *state.Store, ref state.LeaseRef, ttl time.Duration) {
	interval := ttl / 3
	if interval < 5*time.Millisecond {
		interval = 5 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		if _, err := store.RenewLease(ctx, ref, ttl); err != nil {
			return
		}
	}
}

func writeBoundary(record boundaryRecord) {
	encoded, err := json.Marshal(record)
	if err != nil {
		panic(err)
	}
	fmt.Printf("DUR027_%s %s\n", strings.ToUpper(record.Boundary), encoded)
}
