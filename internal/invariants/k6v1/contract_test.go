package k6v1

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestK6Closure(t *testing.T) {
	command := exec.Command("go", "list", "-deps", ".")
	out, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps: %v\n%s", err, out)
	}
	for _, dep := range strings.Fields(string(out)) {
		for _, denied := range []string{
			"net", "github.com/jackc/pgx", "durable-agent-execution-engine/internal/state",
			"durable-agent-execution-engine/internal/engine", "durable-agent-execution-engine/internal/effects",
			"durable-agent-execution-engine/internal/k6receipt", "durable-agent-execution-engine/internal/partition",
			"durable-agent-execution-engine/internal/transport", "durable-agent-execution-engine/internal/faults",
		} {
			if dep == denied || strings.HasPrefix(dep, denied+"/") {
				t.Errorf("forbidden dependency in K6 closure: %s", dep)
			}
		}
	}
}

func TestK6Canonical(t *testing.T) {
	type object struct {
		Z string `json:"z"`
		A int64  `json:"a"`
		B []int  `json:"b"`
		N *int   `json:"n"`
	}
	got, err := Canonical(object{Z: "<é>&", A: 9007199254740993, B: []int{}})
	if err != nil || string(got) != "{\"a\":9007199254740993,\"b\":[],\"n\":null,\"z\":\"<é>&\"}\n" {
		t.Fatalf("canonical bytes: %s (%v)", got, err)
	}
}

func TestK6RegistryPin(t *testing.T) {
	data, err := os.ReadFile("../../../experiments/portfolio/k8s-recovery/k6-registry-v1.json")
	if err != nil || Hash(data) != RegistrySHA256 {
		t.Fatalf("registry byte pin drift: %v", err)
	}
}

func TestK6CensusKinds(t *testing.T) {
	specs := EngineRelations(false)
	for _, entry := range []CensusEntry{
		{Name: "engine.workflow_executions", Kind: "p"},
		{Name: "engine.workflow_executions", Kind: "r", Partition: true},
		{Name: "engine.workflow_executions", Kind: "v"},
		{Name: "engine.workflow_executions", Kind: "m"},
		{Name: "engine.workflow_executions", Kind: "S"},
		{Name: "k6_f3.deadline_writes", Kind: "r"},
		{Name: "k6_f3.deadline_writes_id_seq", Kind: "S"},
		{Name: "source_corpus.future", Kind: "r"},
		{Name: "public.pg_stat_statements", Kind: "v"},
	} {
		if got, _ := Classify(entry, specs, false); got != Unclassified {
			t.Errorf("unexpected census classification for %+v: %s", entry, got)
		}
	}
}
