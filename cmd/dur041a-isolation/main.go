// dur041a-isolation is the durable probe used by the local scheduler
// isolation campaign. It records a lease identity before a Docker pause,
// observes peer takeover, and attempts a mutation with the retained identity
// after the original runtime is reconnected.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/state"
)

type fixture struct {
	SchemaVersion    string    `json:"schema_version"`
	WorkflowID       string    `json:"workflow_id"`
	DefinitionID     string    `json:"definition_id"`
	Namespace        string    `json:"namespace"`
	PartitionID      int16     `json:"partition_id"`
	HoldWorkflowIDs  []string  `json:"hold_workflow_ids,omitempty"`
	OldOwnerID       string    `json:"old_owner_id,omitempty"`
	OldEpoch         int64     `json:"old_epoch,omitempty"`
	NewOwnerID       string    `json:"new_owner_id,omitempty"`
	NewEpoch         int64     `json:"new_epoch,omitempty"`
	ObservedAtUTC    time.Time `json:"observed_at_utc"`
	TakeoverAtUTC    time.Time `json:"takeover_at_utc,omitempty"`
	ReconnectedAtUTC time.Time `json:"reconnected_at_utc,omitempty"`
	BeforeRevision   int64     `json:"before_revision,omitempty"`
	AfterRevision    int64     `json:"after_revision,omitempty"`
	StaleError       string    `json:"stale_error,omitempty"`
	StaleRejected    bool      `json:"stale_rejected"`
}

func main() {
	mode := flag.String("mode", "", "prepare, capture, wait-takeover, stale-attempt, or cleanup")
	path := flag.String("fixture", "", "fixture JSON path")
	timeout := flag.Duration("timeout", 45*time.Second, "takeover wait timeout")
	flag.Parse()
	if *mode == "" || *path == "" {
		fatal("-mode and -fixture are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout+15*time.Second)
	defer cancel()
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		fatal("DATABASE_URL is required")
	}
	store, err := state.NewFromURL(ctx, databaseURL)
	if err != nil {
		fatal(err.Error())
	}
	defer store.Close()

	var result fixture
	switch *mode {
	case "prepare":
		result = prepare(ctx, store)
	case "capture":
		result = readFixture(*path)
		capture(ctx, store, &result)
	case "wait-takeover":
		result = readFixture(*path)
		waitTakeover(ctx, store, &result, *timeout)
	case "stale-attempt":
		result = readFixture(*path)
		staleAttempt(ctx, store, &result)
	case "cleanup":
		result = readFixture(*path)
		cleanup(ctx, store, result)
		return
	default:
		fatal("unknown mode: " + *mode)
	}
	writeFixture(*path, result)
	encoded, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(encoded))
}

func prepare(ctx context.Context, store *state.Store) fixture {
	definitionID := "dur041a-isolation-def-" + state.NewID()
	workflowID := "dur041a-isolation-wf-" + state.NewID()
	partitionID, err := partition.ID(workflowID)
	if err != nil {
		fatal(err.Error())
	}
	namespace := "local-runtime"
	if err := store.CreateDefinition(ctx, state.DefinitionInput{
		DefinitionID:     definitionID,
		Version:          1,
		DefinitionHash:   definitionID,
		Graph:            []byte(`{"entry":"pure.echo","nodes":[{"id":"pure.echo","kind":"activity"}]}`),
		ActivityVersions: []byte(`{"pure.echo":"v1"}`),
		EffectClasses:    []byte(`{"pure.echo":"PURE_ACTIVITY"}`),
	}); err != nil {
		fatal(err.Error())
	}
	mainInput := state.CreateWorkflowInput{
		WorkflowID:            workflowID,
		Namespace:             namespace,
		SubmissionKey:         workflowID,
		SubmissionPayloadHash: "sub-v1:" + workflowID,
		DefinitionID:          definitionID,
		DefinitionVersion:     1,
		PartitionID:           int16(partitionID),
		InitialNodeID:         "pure.echo",
		InitialInput:          []byte(`{"value":"isolation-probe"}`),
		ActorID:               "dur041a-probe",
	}
	if _, err := store.CreateWorkflow(ctx, mainInput); err != nil {
		fatal(err.Error())
	}
	// Keep a same-partition cohort pending while the runtime is observed. The
	// deployed scheduler acquires and releases a lease per workflow, so a
	// single tiny fixture can be missed between polling attempts. These
	// companions make the observation window durable without changing the
	// fault: the original runtime process still owns the captured lease.
	holdWorkflowIDs := make([]string, 0, 48)
	for len(holdWorkflowIDs) < cap(holdWorkflowIDs) {
		holdID := "dur041a-isolation-hold-" + state.NewID()
		holdPartition, partitionErr := partition.ID(holdID)
		if partitionErr != nil || int16(holdPartition) != int16(partitionID) {
			continue
		}
		if _, err := store.CreateWorkflow(ctx, state.CreateWorkflowInput{
			WorkflowID:            holdID,
			Namespace:             namespace,
			SubmissionKey:         holdID,
			SubmissionPayloadHash: "sub-v1:" + holdID,
			DefinitionID:          definitionID,
			DefinitionVersion:     1,
			PartitionID:           int16(partitionID),
			InitialNodeID:         "pure.echo",
			InitialInput:          []byte(`{"value":"isolation-hold"}`),
			ActorID:               "dur041a-hold",
		}); err != nil {
			fatal(err.Error())
		}
		holdWorkflowIDs = append(holdWorkflowIDs, holdID)
	}
	return fixture{SchemaVersion: "dur041a-isolation.v1", WorkflowID: workflowID,
		DefinitionID: definitionID, Namespace: namespace, PartitionID: int16(partitionID),
		HoldWorkflowIDs: holdWorkflowIDs, ObservedAtUTC: time.Now().UTC()}
}

func capture(ctx context.Context, store *state.Store, item *fixture) {
	if err := store.Pool().QueryRow(ctx, `SELECT owner_id::text, epoch FROM engine.partition_leases
		WHERE partition_id = $1 AND owner_id IS NOT NULL`, item.PartitionID).Scan(&item.OldOwnerID, &item.OldEpoch); err != nil {
		fatal(fmt.Sprintf("capture original scheduler lease: %v", err))
	}
	item.ObservedAtUTC = time.Now().UTC()
}

func waitTakeover(ctx context.Context, store *state.Store, item *fixture, timeout time.Duration) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		var owner *string
		var epoch int64
		if err := store.Pool().QueryRow(ctx, `SELECT owner_id::text, epoch FROM engine.partition_leases
			WHERE partition_id = $1`, item.PartitionID).Scan(&owner, &epoch); err != nil {
			fatal(fmt.Sprintf("observe takeover: %v", err))
		}
		if owner != nil && epoch > item.OldEpoch && *owner != item.OldOwnerID {
			item.NewOwnerID, item.NewEpoch = *owner, epoch
			item.TakeoverAtUTC = time.Now().UTC()
			return
		}
		select {
		case <-deadline.C:
			fatal(fmt.Sprintf("peer did not take over partition %d from owner %s epoch %d", item.PartitionID, item.OldOwnerID, item.OldEpoch))
		case <-ctx.Done():
			fatal(ctx.Err().Error())
		case <-ticker.C:
		}
	}
}

func staleAttempt(ctx context.Context, store *state.Store, item *fixture) {
	workflow, err := store.GetWorkflow(ctx, item.WorkflowID)
	if err != nil {
		fatal(err.Error())
	}
	item.BeforeRevision = workflow.Revision
	err = store.ApplyOwnerTransition(ctx, state.OwnerTransitionInput{
		Lease:      state.LeaseRef{PartitionID: item.PartitionID, OwnerID: item.OldOwnerID, Epoch: item.OldEpoch},
		WorkflowID: workflow.WorkflowID, ExpectedRevision: workflow.Revision,
		NewState: state.StateCanceled, ActorID: "dur041a-stale-probe", Reason: "STALE_EPOCH_PROBE",
	})
	item.StaleError = fmt.Sprint(err)
	item.StaleRejected = errors.Is(err, state.ErrLeaseNotOwned)
	workflowAfter, readErr := store.GetWorkflow(ctx, item.WorkflowID)
	if readErr != nil {
		fatal(readErr.Error())
	}
	item.AfterRevision = workflowAfter.Revision
	item.ReconnectedAtUTC = time.Now().UTC()
	if !item.StaleRejected || item.BeforeRevision != item.AfterRevision {
		fatal(fmt.Sprintf("stale transition result was unsafe: rejected=%v before=%d after=%d error=%v", item.StaleRejected, item.BeforeRevision, item.AfterRevision, err))
	}
}

func cleanup(ctx context.Context, store *state.Store, item fixture) {
	workflowIDs := append([]string{item.WorkflowID}, item.HoldWorkflowIDs...)
	if _, err := store.Pool().Exec(ctx, `DELETE FROM engine.workflow_executions WHERE workflow_id = ANY($1::text[])`, workflowIDs); err != nil {
		fatal(fmt.Sprintf("cleanup workflow: %v", err))
	}
	if _, err := store.Pool().Exec(ctx, `DELETE FROM engine.workflow_definitions WHERE definition_id = $1`, item.DefinitionID); err != nil {
		fatal(fmt.Sprintf("cleanup definition: %v", err))
	}
}

func readFixture(path string) fixture {
	data, err := os.ReadFile(path)
	if err != nil {
		fatal(err.Error())
	}
	var item fixture
	if err := json.Unmarshal(data, &item); err != nil {
		fatal(err.Error())
	}
	return item
}

func writeFixture(path string, item fixture) {
	data, err := json.MarshalIndent(item, "", "  ")
	if err != nil {
		fatal(err.Error())
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		fatal(err.Error())
	}
}

func fatal(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(1)
}
