package invariants

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/state"
)

func TestM4LoadIncludesEffectAndApprovalEvidence(t *testing.T) {
	if os.Getenv("DURABLE_RUN_INTEGRATION") != "1" && os.Getenv("DURABLE_REQUIRE_DATABASE") != "1" {
		t.Skip("M4 invariant integration tests require PostgreSQL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := state.NewFromURL(ctx, invariantDatabaseURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	definitionID := "dur023a-m4-" + state.NewID()
	if err := store.CreateDefinition(ctx, state.DefinitionInput{
		DefinitionID: definitionID, Version: 1, DefinitionHash: "hash-" + definitionID,
		Graph:            []byte(`{"entry":"root","nodes":[{"id":"root","kind":"activity"}]}`),
		ActivityVersions: []byte(`{"root":"1"}`), EffectClasses: []byte(`{"root":"PURE_ACTIVITY"}`),
	}); err != nil {
		t.Fatal(err)
	}
	ownerID := state.NewID()
	var lease state.Lease
	var acquired bool
	for partitionID := int16(8); partitionID < 16; partitionID++ {
		lease, acquired, err = store.AcquireLease(ctx, partitionID, ownerID, time.Minute)
		if err != nil || acquired {
			break
		}
	}
	if err != nil || !acquired {
		t.Fatalf("acquire invariant fixture lease = %+v acquired=%v err=%v", lease, acquired, err)
	}
	var workflowID string
	for i := 0; i < 200; i++ {
		candidate := "dur-invariant-m4-" + state.NewID()
		mapped, mapErr := partition.ID(candidate)
		if mapErr == nil && int16(mapped) == lease.PartitionID {
			workflowID = candidate
			break
		}
	}
	if workflowID == "" {
		t.Fatal("could not map invariant fixture workflow to lease")
	}
	if _, err := store.CreateWorkflow(ctx, state.CreateWorkflowInput{
		WorkflowID: workflowID, Namespace: "dur-invariant-m4", SubmissionKey: workflowID,
		SubmissionPayloadHash: "sub-v1:" + workflowID, DefinitionID: definitionID, DefinitionVersion: 1,
		PartitionID: lease.PartitionID, InitialNodeID: "root", InitialInput: []byte(`{}`), ActorID: "fixture",
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_executions WHERE workflow_id = $1`, workflowID)
		_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_definitions WHERE definition_id = $1`, definitionID)
		_ = store.ReleaseLease(ctx, state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch})
	}()
	if _, err := store.Pool().Exec(ctx, `
		INSERT INTO engine.effect_records (workflow_id, logical_effect_key, argument_hash, attempt_number, outcome, receipt)
		VALUES ($1, 'effect-1', 'args-1', 1, 'APPLIED', '{"receipt":true}')`, workflowID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Pool().Exec(ctx, `
		INSERT INTO engine.effect_call_attempts (call_id, workflow_id, logical_effect_key, argument_hash, attempt_number, request_id, outcome)
		VALUES ($1, $2, 'effect-1', 'args-1', 1, 'request-1', 'APPLIED')`, state.NewID(), workflowID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Pool().Exec(ctx, `
		INSERT INTO engine.approval_action_intents
			(intent_id, workflow_id, node_id, iteration, proposal_hash, target, canonical_arguments, valid_until)
		VALUES ($1, $2, 'root', 0, 'proposal-1', 'sandbox.write', '{"resource":"r"}', clock_timestamp() + interval '1 hour')`, state.NewID(), workflowID); err != nil {
		t.Fatal(err)
	}
	trace, err := Load(ctx, store, []string{workflowID})
	if err != nil {
		t.Fatal(err)
	}
	if len(trace.Effects) != 1 || len(trace.EffectCalls) != 1 || len(trace.Approvals) != 1 {
		t.Fatalf("loaded M4 evidence counts = effects %d calls %d approvals %d", len(trace.Effects), len(trace.EffectCalls), len(trace.Approvals))
	}
	if verdict := Check(trace); !verdict.Valid {
		t.Fatalf("loaded M4 trace rejected: %v", verdict.Violations)
	}
}

func invariantDatabaseURL(t *testing.T) string {
	t.Helper()
	if value := os.Getenv("DURABLE_DATABASE_URL"); value != "" {
		return value
	}
	directory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		data, readErr := os.ReadFile(filepath.Join(directory, ".env"))
		if readErr == nil {
			values := map[string]string{}
			for _, line := range strings.Split(string(data), "\n") {
				parts := strings.SplitN(strings.TrimSpace(line), "=", 2)
				if len(parts) == 2 {
					values[parts[0]] = strings.Trim(parts[1], "\r\"")
				}
			}
			if values["POSTGRES_USER"] != "" && values["POSTGRES_DB"] != "" {
				port := values["POSTGRES_PORT"]
				if port == "" {
					port = "5432"
				}
				return fmt.Sprintf("postgresql://%s:%s@127.0.0.1:%s/%s?sslmode=disable",
					values["POSTGRES_USER"], values["POSTGRES_PASSWORD"], port, values["POSTGRES_DB"])
			}
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			break
		}
		directory = parent
	}
	t.Fatal("set DURABLE_DATABASE_URL or provide .env")
	return ""
}
