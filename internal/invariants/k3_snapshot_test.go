package invariants

import (
	"context"
	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/state"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

func TestK3CheckerSnapshot(t *testing.T) {
	if os.Getenv("DURABLE_RUN_INTEGRATION") != "1" && os.Getenv("DURABLE_REQUIRE_DATABASE") != "1" {
		t.Skip("requires PostgreSQL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	writer, err := state.NewFromURL(ctx, invariantDatabaseURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	reader, err := state.NewCheckerFromURL(ctx, invariantDatabaseURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var timeout float64
	if err := reader.Pool().QueryRow(ctx, `SELECT EXTRACT(EPOCH FROM current_setting('statement_timeout')::interval)`).Scan(&timeout); err != nil || timeout <= 2 || timeout > 30 {
		t.Fatalf("checker export timeout bypassed: %v %v", timeout, err)
	}
	if reader.Pool().Config().MaxConns != 2 {
		t.Fatal("checker connection budget bypassed")
	}
	var ro, iso string
	if err := reader.Pool().QueryRow(ctx, `SHOW default_transaction_read_only`).Scan(&ro); err != nil || ro != "on" {
		t.Fatal("checker read-only bypassed", err, ro)
	}
	if err := reader.Pool().QueryRow(ctx, `SHOW default_transaction_isolation`).Scan(&iso); err != nil || iso != "repeatable read" {
		t.Fatal("checker isolation bypassed", err, iso)
	}
	if _, err := reader.Pool().Exec(ctx, `SELECT pg_sleep(2.1)`); err != nil {
		t.Fatal("checker inherited demand limit", err)
	}
	ns := "k3-snapshot-" + state.NewID()
	definition := ns
	if err := writer.CreateDefinition(ctx, state.DefinitionInput{DefinitionID: definition, Version: 1, DefinitionHash: definition, Graph: json.RawMessage(`{"entry":"pure.echo","nodes":[{"id":"pure.echo","kind":"activity"}]}`), ActivityVersions: json.RawMessage(`{"pure.echo":"v1"}`), EffectClasses: json.RawMessage(`{"pure.echo":"PURE_ACTIVITY"}`)}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = writer.Pool().Exec(ctx, `DELETE FROM engine.workflow_executions WHERE namespace=$1`, ns)
		_, _ = writer.Pool().Exec(ctx, `DELETE FROM engine.workflow_definitions WHERE definition_id=$1`, definition)
	}()
	create := func() string {
		id := state.NewID()
		part, _ := partition.ID(id)
		_, err := writer.CreateWorkflow(ctx, state.CreateWorkflowInput{WorkflowID: id, Namespace: ns, SubmissionKey: id, SubmissionPayloadHash: "sub-v1:" + id, DefinitionID: definition, DefinitionVersion: 1, PartitionID: int16(part), InitialNodeID: "pure.echo", InitialInput: json.RawMessage(`{}`), ActorID: "fixture"})
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	first := create()
	late := ""
	trace, err := loadSelected(ctx, reader, nil, ns, func() { late = create() })
	if err != nil {
		t.Fatal(err)
	}
	if len(trace.Workflows) != 1 || trace.Workflows[0].WorkflowID != first {
		t.Fatal("namespace snapshot projections raced", trace.Workflows)
	}
	for _, raw := range trace.Snapshot["engine.workflow_executions"].Rows {
		var row struct {
			WorkflowID string `json:"workflow_id"`
		}
		if err := json.Unmarshal(raw, &row); err != nil {
			t.Fatal(err)
		}
		if row.WorkflowID == late {
			t.Fatal("namespace snapshot raw rows raced")
		}
	}
	next, err := LoadNamespace(ctx, reader, ns)
	if err != nil || len(next.Workflows) != 2 {
		t.Fatal("subsequent snapshot omitted late submission", err)
	}
}
func TestK3CheckerRequiresBudget(t *testing.T) {
	for _, ctx := range []context.Context{context.Background(), func() context.Context { c, cancel := context.WithCancel(context.Background()); cancel(); return c }()} {
		if _, err := state.NewCheckerFromURL(ctx, "invalid"); err == nil || !strings.Contains(err.Error(), "positive export deadline") {
			t.Fatal("checker budget guard bypassed", err)
		}
	}
}
