// Command dur050-fixture installs the immutable workflow definitions used by
// the DUR-050 pilot and emits the load-generator configuration for them.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"time"

	"durable-agent-execution-engine/internal/engine"
	"durable-agent-execution-engine/internal/state"
)

const (
	definitionVersion = 1
	activityVersion   = "v1"
)

var identifierPattern = regexp.MustCompile(`^[A-Za-z0-9-]{1,48}$`)

type graphDocument struct {
	Entry string      `json:"entry"`
	Nodes []graphNode `json:"nodes"`
}

type graphNode struct {
	ID       string   `json:"id"`
	Kind     string   `json:"kind"`
	Next     string   `json:"next,omitempty"`
	Branches []string `json:"branches,omitempty"`
	Join     string   `json:"join,omitempty"`
}

type familyDefinition struct {
	Name              string                `json:"name"`
	DefinitionID      string                `json:"definition_id"`
	DefinitionVersion int                   `json:"definition_version"`
	InitialNodeID     string                `json:"initial_node_id"`
	Payload           json.RawMessage       `json:"payload"`
	InitialInput      json.RawMessage       `json:"initial_input"`
	Definition        state.DefinitionInput `json:"-"`
}

type loadgenConfig struct {
	APIURL    string             `json:"api_url"`
	Namespace string             `json:"namespace"`
	RunID     string             `json:"run_id"`
	Seed      int64              `json:"seed"`
	Families  []familyDefinition `json:"families"`
}

func main() {
	databaseURL := flag.String("database-url", os.Getenv("DATABASE_URL"), "campaign PostgreSQL URL; defaults to DATABASE_URL")
	apiURL := flag.String("api-url", "", "private runtime API base URL")
	namespace := flag.String("namespace", "", "dur050-* namespace")
	runID := flag.String("run-id", "pilot-calibration", "load-generator run identifier")
	seed := flag.Int64("seed", 50050, "deterministic family-shuffle seed")
	flag.Parse()
	if err := run(*databaseURL, *apiURL, *namespace, *runID, *seed); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(databaseURL, apiURL, namespace, runID string, seed int64) error {
	if os.Getenv("DUR049_RECORD_LEASE_ACQUISITIONS") != "0" {
		return errors.New("DUR049_RECORD_LEASE_ACQUISITIONS must be explicitly set to 0")
	}
	parsedAPI, err := url.ParseRequestURI(apiURL)
	if err != nil || parsedAPI.Host == "" || (parsedAPI.Scheme != "http" && parsedAPI.Scheme != "https") {
		return errors.New("-api-url must be an absolute http(s) URL")
	}
	if !identifierPattern.MatchString(namespace) || len(namespace) < len("dur050-") || namespace[:len("dur050-")] != "dur050-" {
		return errors.New("-namespace must start with dur050- and contain only letters, digits, or hyphens")
	}
	if !identifierPattern.MatchString(runID) {
		return errors.New("-run-id must contain only letters, digits, or hyphens")
	}
	if databaseURL == "" {
		return errors.New("-database-url or DATABASE_URL is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := state.NewFromURL(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("open campaign database: %w", err)
	}
	defer store.Close()
	if err := verifyDatabase(ctx, store); err != nil {
		return err
	}

	families := buildFamilies()
	if err := validateGraphs(); err != nil {
		return err
	}
	for _, family := range families {
		if err := store.CreateDefinition(ctx, family.Definition); err != nil {
			return fmt.Errorf("install %s workflow definition: %w", family.Name, err)
		}
	}
	config := loadgenConfig{APIURL: apiURL, Namespace: namespace, RunID: runID, Seed: seed, Families: families}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(config); err != nil {
		return fmt.Errorf("encode load-generator configuration: %w", err)
	}
	return nil
}

func verifyDatabase(ctx context.Context, store *state.Store) error {
	var version int
	if err := store.Pool().QueryRow(ctx, `SELECT COALESCE(max(version), 0) FROM engine.schema_migrations`).Scan(&version); err != nil {
		return fmt.Errorf("read migration version: %w", err)
	}
	if version < 18 {
		return fmt.Errorf("DUR-050 requires migrations through 000018; database is at %d", version)
	}
	var preload string
	var extension bool
	if err := store.Pool().QueryRow(ctx, `SELECT current_setting('shared_preload_libraries'), EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')`).Scan(&preload, &extension); err != nil {
		return fmt.Errorf("verify pg_stat_statements: %w", err)
	}
	if !extension || !contains(preload, "pg_stat_statements") {
		return errors.New("DUR-050 requires pg_stat_statements preloaded and installed")
	}
	return nil
}

func buildFamilies() []familyDefinition {
	seq := graphDocument{Entry: "dur050.sha256.seq8.0"}
	seqVersions := map[string]string{}
	seqEffects := map[string]string{}
	for index := 0; index < 8; index++ {
		id := fmt.Sprintf("dur050.sha256.seq8.%d", index)
		next := "succeeded"
		if index < 7 {
			next = fmt.Sprintf("dur050.sha256.seq8.%d", index+1)
		}
		seq.Nodes = append(seq.Nodes, graphNode{ID: id, Kind: "activity", Next: next})
		seqVersions[id], seqEffects[id] = activityVersion, string(state.EffectPure)
	}
	seq.Nodes = append(seq.Nodes, graphNode{ID: "succeeded", Kind: "success"})

	fanout := graphDocument{Entry: "root"}
	branchIDs := make([]string, 8)
	fanoutVersions := map[string]string{}
	fanoutEffects := map[string]string{}
	for index := range branchIDs {
		id := fmt.Sprintf("dur050.sha256.fanout8.%d", index)
		branchIDs[index] = id
		fanout.Nodes = append(fanout.Nodes, graphNode{ID: id, Kind: "activity", Next: "join"})
		fanoutVersions[id], fanoutEffects[id] = activityVersion, string(state.EffectPure)
	}
	fanout.Nodes = append([]graphNode{{ID: "root", Kind: "fanout", Branches: branchIDs, Join: "join"}}, fanout.Nodes...)
	fanout.Nodes = append(fanout.Nodes, graphNode{ID: "join", Kind: "join", Next: "succeeded"}, graphNode{ID: "succeeded", Kind: "success"})

	seqDefinition := makeDefinition("dur050-seq-8-v1", seq, seqVersions, seqEffects)
	fanoutDefinition := makeDefinition("dur050-fanout-8-v1", fanout, fanoutVersions, fanoutEffects)
	emptyInput := json.RawMessage(`{"input":"fixed-256-byte-dur050-seed-v1"}`)
	return []familyDefinition{
		{Name: "seq-8", DefinitionID: seqDefinition.DefinitionID, DefinitionVersion: definitionVersion, InitialNodeID: seq.Entry,
			Payload: json.RawMessage(`{"profile":"seq-8"}`), InitialInput: emptyInput, Definition: seqDefinition},
		{Name: "fanout-8", DefinitionID: fanoutDefinition.DefinitionID, DefinitionVersion: definitionVersion, InitialNodeID: fanout.Entry,
			Payload: json.RawMessage(`{"profile":"fanout-8"}`), InitialInput: emptyInput, Definition: fanoutDefinition},
	}
}

func makeDefinition(id string, graph graphDocument, versions, effects map[string]string) state.DefinitionInput {
	graphJSON, _ := json.Marshal(graph)
	versionsJSON, _ := json.Marshal(versions)
	effectsJSON, _ := json.Marshal(effects)
	hashInput, _ := json.Marshal(struct {
		Graph           json.RawMessage   `json:"graph"`
		ActivityVersion map[string]string `json:"activity_versions"`
		EffectClasses   map[string]string `json:"effect_classes"`
	}{graphJSON, versions, effects})
	hash := sha256.Sum256(hashInput)
	return state.DefinitionInput{DefinitionID: id, Version: definitionVersion, DefinitionHash: hex.EncodeToString(hash[:]),
		Graph: graphJSON, ActivityVersions: versionsJSON, EffectClasses: effectsJSON}
}

func contains(value, token string) bool {
	for start := 0; start+len(token) <= len(value); start++ {
		if value[start:start+len(token)] == token {
			return true
		}
	}
	return false
}

func validateGraphs() error {
	for _, family := range buildFamilies() {
		if _, err := engine.ParseGraph(family.Definition.Graph); err != nil {
			return fmt.Errorf("parse %s graph: %w", family.Name, err)
		}
	}
	return nil
}
