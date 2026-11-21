package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"durable-agent-execution-engine/internal/invariants/k6live"
	"durable-agent-execution-engine/internal/invariants/k6v1"
)

func k6Manifest(database string, instrumented bool) k6v1.Manifest {
	d := strings.Repeat("a", 64)
	m := k6v1.Manifest{
		Schema: k6v1.EvidenceVersion, Campaign: "test", Job: "fixture", RunID: "1",
		Episode: "hook-0-0", Ordinal: 0, Seed: 530001, Cell: "hook-0", Workload: "PURE",
		Namespace: "episode", ResetID: "reset-a", BarrierID: "barrier-a", Database: database,
		EngineInstance: "engine-instance", PlanSHA256: d,
		RegistrySHA256: k6v1.RegistrySHA256, SourceTreeSHA256: d, ImageSetSHA256: d, FreezeSHA256: d, TopologySHA256: d,
		Instrumentation: []string{}, RequiredSources: k6v1.RequiredSources(instrumented, false),
	}
	if instrumented {
		m.Instrumentation = []string{k6v1.DeadlineWrites}
	}
	return m
}

func putFile(t *testing.T, path string, raw []byte) {
	t.Helper()
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func getFile(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func canonical(t *testing.T, obj any) []byte {
	t.Helper()
	raw, err := k6v1.Canonical(obj)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestK6ModeExclusivity(t *testing.T) {
	path := t.TempDir()
	for _, args := range [][]string{
		{k6Offline, path, "-namespace", "episode"},
		{k6Offline, path, "-workflow-ids", "subset"},
		{k6Offline, path, k6Live, "private.json"},
		{k6Offline, path, k6Offline, path},
		{k6Live, "private.json", "-database-url", "other"},
		{"-snapshot", path, k6Offline, path},
		{"--k6-offline-v2", path},
		{"-k6--offline-v1", path},
		{k6Offline},
		{k6Offline + "="},
	} {
		var out, diagnostic bytes.Buffer
		if !hasK6Selector(args) || k6Mode(args, &out, &diagnostic) != k6v1.Usage || out.Len() != 0 {
			t.Errorf("mixed or invalid K6 selectors accepted: %v", args)
		}
	}
	var out, diagnostic bytes.Buffer
	if got := k6Mode([]string{"--k6-offline-v1=" + path}, &out, &diagnostic); got != k6v1.Fail || !strings.Contains(out.String(), "required_proof_missing:index.json") {
		t.Fatalf("valid selector failed to dispatch: %d %s %s", got, &out, &diagnostic)
	}
}

func TestK6ModeStrictDecoding(t *testing.T) {
	config := k6live.Config{
		Schema: k6live.ConfigVersion, DatabaseURL: "redacted", EpisodeDir: "unused",
		Manifest: k6Manifest("db", false), PlanFile: "plan.json", ArrivalsFile: "arrivals.json",
	}
	if !config.Valid() {
		t.Fatal("strict-decoding fixture must be valid before modifying the JSON")
	}
	raw := string(canonical(t, config))
	for name, input := range map[string]string{
		"duplicate":        strings.Replace(raw, `"schema":"dur053.k6-live-config.v1"`, `"schema":"dur053.k6-live-config.v1","schema":"dur053.k6-live-config.v1"`, 1),
		"nested duplicate": strings.Replace(raw, `"cell":"hook-0"`, `"cell":"hook-0","cell":"hook-0"`, 1),
		"unknown":          strings.Replace(raw, `"manifest":{`, `"manifest":{"credential_secret_sentinel":"secret",`, 1),
		"case":             strings.Replace(raw, `"episode_dir":`, `"EPISODE_DIR":`, 1),
		"version":          strings.Replace(raw, ".k6-live-config.v1", ".k6-live-config.v2", 1),
		"trailing":         raw + `{}`,
		"invalid UTF-8":    strings.Replace(raw, "unused", "\xff", 1),
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "private.json")
			putFile(t, path, []byte(input))
			var out, diagnostic bytes.Buffer
			if got := k6Mode([]string{k6Live, path}, &out, &diagnostic); got != k6v1.Usage {
				t.Errorf("invalid K6 JSON accepted: %d", got)
			}
			if out.Len() != 0 || diagnostic.String() != "K6_LIVE_CONFIG\n" {
				t.Error("invalid input escaped the private-config diagnostic boundary")
			}
		})
	}
}
