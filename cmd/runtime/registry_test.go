package main

import (
	"bytes"
	"context"
	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/state"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestK2CandidateRegistryCommand(t *testing.T) {
	var output bytes.Buffer
	if err := registryCommand([]string{"activity-registry"}, &output); err != nil {
		t.Fatal(err)
	}
	var manifest []state.ActivityVersion
	if err := json.Unmarshal(output.Bytes(), &manifest); err != nil {
		t.Fatal(err)
	}
	if err := state.CheckRegistryParity(state.RuntimeRegistry(), manifest); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DATABASE_URL", "")
	t.Setenv("RUNTIME_NAMESPACE", "")
	if err := registryCommand([]string{"compatibility-check"}, &output); err == nil {
		t.Fatal("pre-rollout required configuration bypassed")
	}
}

func TestK2PreRolloutCommand(t *testing.T) {
	url := os.Getenv("DURABLE_DATABASE_URL")
	if os.Getenv("DURABLE_RUN_INTEGRATION") != "1" || url == "" {
		t.Skip("native PostgreSQL integration not enabled")
	}
	t.Setenv("DATABASE_URL", url)
	t.Setenv("DUR048_ALLOW_FIXTURE_ACTIVITY", "0")
	dir := t.TempDir()
	path := filepath.Join(dir, "worker.json")
	write := func(raw string) {
		if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
	}
	store, err := state.NewFromURL(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ns := "k2-command-" + state.NewID()
	for _, raw := range []string{`null`, `[]`, `[{"name":"pure.echo","version":"v1"}]`, `[{"name":"pure.echo","version":"v2"},{"name":"pure.add","version":"v1"}]`, `[{"name":"pure.echo","version":"v1","unknown":true}]`, `[] []`} {
		write(raw)
		if err := registryCommand([]string{"compatibility-check", "--namespace", ns, "--worker-registry", path}, &bytes.Buffer{}); err == nil {
			t.Fatalf("pre-rollout parity guard bypassed: %s", raw)
		}
	}
	raw, _ := json.Marshal(state.RuntimeRegistry())
	write(string(raw))
	if err := registryCommand([]string{"compatibility-check", "--namespace", ns, "--worker-registry", path}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	// A persisted workflow referencing the version dropped by the candidate.
	id := ns
	err = store.CreateDefinition(context.Background(), state.DefinitionInput{DefinitionID: id, Version: 1, DefinitionHash: id, Graph: []byte(`{"entry":"pure.echo","nodes":[{"id":"pure.echo","kind":"activity"}]}`), ActivityVersions: []byte(`{"pure.echo":"v0"}`), EffectClasses: []byte(`{"pure.echo":"PURE_ACTIVITY"}`)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		store.Pool().Exec(context.Background(), `DELETE FROM engine.workflow_executions WHERE definition_id=$1`, id)
		store.Pool().Exec(context.Background(), `DELETE FROM engine.workflow_definitions WHERE definition_id=$1`, id)
	}()
	mapped, _ := partition.ID(id)
	_, err = store.CreateWorkflow(context.Background(), state.CreateWorkflowInput{WorkflowID: id, PartitionID: int16(mapped), Namespace: ns, SubmissionKey: id, SubmissionPayloadHash: id, DefinitionID: id, DefinitionVersion: 1, InitialNodeID: "pure.echo"})
	if err != nil {
		t.Fatal(err)
	}
	if err := registryCommand([]string{"compatibility-check", "--namespace", ns, "--worker-registry", path}, &bytes.Buffer{}); err == nil {
		t.Fatal("pre-rollout live version guard bypassed")
	}
	t.Setenv("DATABASE_URL", "postgresql://invalid:invalid@127.0.0.1:1/absent?sslmode=disable")
	if err := registryCommand([]string{"compatibility-check", "--namespace", ns, "--worker-registry", path}, &bytes.Buffer{}); err == nil {
		t.Fatal("pre-rollout database failure accepted")
	}
}
