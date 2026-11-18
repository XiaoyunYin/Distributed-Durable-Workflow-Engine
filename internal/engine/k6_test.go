package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	"durable-agent-execution-engine/internal/state"
)

func sealK6Plan(t *testing.T, namespace string, operations ...K6Operation) ([]byte, string) {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"namespace": namespace, "operations": operations})
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(raw)
	return raw, hex.EncodeToString(hash[:])
}

func TestK6SealedPlan(t *testing.T) {
	op := K6Operation{SubmissionKey: "submit-1", DefinitionID: "def", DefinitionVersion: 1, NodeID: state.K6Activity,
		Resource: "resource-1", LogicalKey: "key-1", ExpectedRevision: "0", Arguments: []byte(`{"amount":1}`)}
	raw, digest := sealK6Plan(t, "episode", op)
	if _, err := ParseK6Plan(raw, digest+"00", "episode"); err == nil {
		t.Fatal("different plan digest accepted")
	}
	plan, err := ParseK6Plan(raw, digest, "episode")
	if err != nil {
		t.Fatal(err)
	}
	workflow := state.Workflow{Namespace: "episode", SubmissionKey: "submit-1", DefinitionID: "def", DefinitionVersion: 1}
	node := state.NodeInstance{NodeID: state.K6Activity}
	got, err := plan.operation(workflow, node)
	if err != nil || got.LogicalKey != op.LogicalKey || string(got.Arguments) != string(op.Arguments) {
		t.Fatalf("wrong plan binding: %v", err)
	}
	workflow.SubmissionKey = "other"
	if _, err := plan.operation(workflow, node); err == nil {
		t.Fatal("unregistered submission key accepted")
	}
	collision := op
	collision.SubmissionKey, collision.Resource = "submit-2", "resource-2"
	raw, digest = sealK6Plan(t, "episode", op, collision)
	if _, err := ParseK6Plan(raw, digest, "episode"); err == nil {
		t.Fatal("sealed plan accepted reuse of a logical key")
	}
	op.Arguments = []byte(`{"amount":1e2}`)
	raw, digest = sealK6Plan(t, "episode", op)
	if _, err := ParseK6Plan(raw, digest, "episode"); err == nil {
		t.Fatal("sealed plan accepted unstable JSON")
	}
}
