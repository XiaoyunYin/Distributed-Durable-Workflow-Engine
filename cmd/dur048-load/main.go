// Command dur048-load drives the deployed HTTP -> PostgreSQL -> Kafka -> worker
// path. It intentionally does not call the engine directly: the API submission
// and the durable terminal rows are the two sides of the reconciliation.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/state"
)

type workflowView struct {
	WorkflowID string              `json:"workflow_id"`
	State      state.WorkflowState `json:"state"`
	Revision   int64               `json:"revision"`
}

type workflowResponse struct {
	Workflow workflowView `json:"workflow"`
	Created  bool         `json:"created"`
}

type runRow struct {
	WorkflowID  string              `json:"workflow_id"`
	Traceparent string              `json:"traceparent"`
	AcceptedAt  time.Time           `json:"accepted_at"`
	TerminalAt  *time.Time          `json:"terminal_at,omitempty"`
	State       state.WorkflowState `json:"state"`
	Revision    int64               `json:"revision"`
	LatencyMS   int64               `json:"latency_ms"`
	Error       string              `json:"error,omitempty"`
}

type artifact struct {
	Schema         string         `json:"schema"`
	Status         string         `json:"status"`
	Mode           string         `json:"mode"`
	GeneratedAtUTC time.Time      `json:"generated_at_utc"`
	SourceCommit   string         `json:"source_commit,omitempty"`
	Protocol       map[string]any `json:"protocol"`
	Rows           []runRow       `json:"rows"`
	Reconciliation map[string]any `json:"reconciliation"`
	Validation     map[string]any `json:"validation"`
}

func main() {
	mode := flag.String("mode", "normal", "normal or recovery")
	baseURL := flag.String("url", envOr("DUR048_RUNTIME_URL", "http://127.0.0.1:8080"), "runtime API URL")
	databaseURL := flag.String("database-url", os.Getenv("DATABASE_URL"), "PostgreSQL URL")
	output := flag.String("output", "experiments/m8/dur042-pilot", "artifact directory")
	namespace := flag.String("namespace", "", "workflow namespace")
	rate := flag.Float64("rate", 1, "open-loop submissions per second")
	count := flag.Int("count", 4, "accepted workflows")
	wait := flag.Duration("wait", 90*time.Second, "terminal reconciliation deadline")
	workerSlots := flag.Int("worker-slots", 4, "fixed worker capacity recorded in the protocol")
	activity := flag.String("activity", "pure.echo", "registered activity name")
	flag.Parse()
	if *mode != "normal" && *mode != "recovery" {
		fatal("mode must be normal or recovery")
	}
	if *databaseURL == "" {
		fatal("DATABASE_URL is required")
	}
	if *count <= 0 || *rate <= 0 || *workerSlots <= 0 {
		fatal("count, rate, and worker-slots must be positive")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *wait+30*time.Second)
	defer cancel()
	store, err := state.NewFromURL(ctx, *databaseURL)
	if err != nil {
		fatal("open database: %v", err)
	}
	defer store.Close()

	seed := state.NewID()
	if *namespace == "" {
		*namespace = "dur048-" + *mode + "-" + seed
	}
	definitionID := "dur048-def-" + seed
	graph := json.RawMessage(fmt.Sprintf(`{"entry":%q,"nodes":[{"id":%q,"kind":"activity","next":"done"},{"id":"done","kind":"success"}]}`, *activity, *activity))
	if err := store.CreateDefinition(ctx, state.DefinitionInput{
		DefinitionID: definitionID, Version: 1, DefinitionHash: definitionID,
		Graph: graph, ActivityVersions: json.RawMessage(fmt.Sprintf(`{%q:"v1"}`, *activity)),
		EffectClasses: json.RawMessage(fmt.Sprintf(`{%q:"PURE_ACTIVITY"}`, *activity)),
	}); err != nil {
		fatal("create definition: %v", err)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	rows := make([]runRow, 0, *count)
	interval := time.Duration(float64(time.Second) / *rate)
	for i := 0; i < *count; i++ {
		id := "dur048-" + *mode + "-" + state.NewID()
		parent := makeTraceparent(id)
		accepted := time.Now().UTC()
		body := map[string]any{
			"workflow_id": id, "namespace": *namespace, "submission_key": id,
			"payload":       map[string]any{"mode": *mode, "sequence": i},
			"definition_id": definitionID, "definition_version": 1,
			"initial_node_id": *activity, "initial_input": map[string]any{"mode": *mode, "sequence": i},
			"actor_id": "dur048-load",
		}
		var response workflowResponse
		if err := postJSON(client, *baseURL+"/v1/workflows", parent, body, &response); err != nil {
			rows = append(rows, runRow{WorkflowID: id, Traceparent: parent, AcceptedAt: accepted, Error: err.Error()})
		} else if !response.Created && response.Workflow.WorkflowID != id {
			rows = append(rows, runRow{WorkflowID: id, Traceparent: parent, AcceptedAt: accepted, Error: "submission was not created for requested workflow"})
		} else {
			rows = append(rows, runRow{WorkflowID: id, Traceparent: parent, AcceptedAt: accepted})
		}
		if i+1 < *count {
			time.Sleep(interval)
		}
	}

	deadline := time.Now().Add(*wait)
	for time.Now().Before(deadline) {
		allTerminal := true
		for index := range rows {
			if rows[index].TerminalAt != nil {
				continue
			}
			var response workflowResponse
			if err := getJSON(client, *baseURL+"/v1/workflows/"+rows[index].WorkflowID, rows[index].Traceparent, &response); err != nil {
				rows[index].Error = err.Error()
				allTerminal = false
				continue
			}
			rows[index].Error = ""
			rows[index].State, rows[index].Revision = response.Workflow.State, response.Workflow.Revision
			if terminal(response.Workflow.State) {
				now := time.Now().UTC()
				rows[index].TerminalAt = &now
				rows[index].LatencyMS = now.Sub(rows[index].AcceptedAt).Milliseconds()
			} else {
				allTerminal = false
			}
		}
		if allTerminal {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	terminalCount, failedRows := 0, 0
	for _, row := range rows {
		if row.TerminalAt != nil {
			terminalCount++
		}
		if row.Error != "" || row.TerminalAt == nil || !terminal(row.State) {
			failedRows++
		}
	}
	validation := map[string]any{
		"accepted": len(rows), "terminal": terminalCount, "incomplete": len(rows) - terminalCount,
		"failed_rows": failedRows, "worker_slots_fixed": *workerSlots,
		"requires_terminal_for_every_accepted": true,
	}
	status := "PASS"
	if terminalCount != len(rows) || failedRows != 0 {
		status = "FAIL"
	}
	result := artifact{Schema: "dur048-http-path.v1", Status: status, Mode: *mode,
		GeneratedAtUTC: time.Now().UTC(), SourceCommit: envOr("DURABLE_SOURCE_COMMIT", ""),
		Protocol: map[string]any{"offered_rate_per_second": *rate, "accepted_workflows": *count,
			"worker_slots": *workerSlots, "activity": *activity, "runtime_url": *baseURL,
			"trace_backend": "OTLP collector file exporter", "clock_model": "client UTC and database terminal state; no cross-host duration inference"},
		Rows: rows, Reconciliation: map[string]any{"accepted": len(rows), "terminal": terminalCount,
			"pending": len(rows) - terminalCount, "failed": failedRows}, Validation: validation}
	if err := os.MkdirAll(*output, 0o755); err != nil {
		fatal("create output: %v", err)
	}
	data, _ := json.MarshalIndent(result, "", "  ")
	if err := os.WriteFile(filepath.Join(*output, *mode+".json"), append(data, '\n'), 0o644); err != nil {
		fatal("write artifact: %v", err)
	}
	if err := cleanupRun(ctx, store, rows, definitionID); err != nil {
		fatal("cleanup DUR-048 run: %v", err)
	}
	if status != "PASS" {
		os.Exit(1)
	}
}

func cleanupRun(ctx context.Context, store *state.Store, rows []runRow, definitionID string) error {
	workflowIDs := make([]string, 0, len(rows))
	for _, row := range rows {
		workflowIDs = append(workflowIDs, row.WorkflowID)
	}
	if len(workflowIDs) == 0 {
		return nil
	}
	tx, err := store.Pool().Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `
		DELETE FROM engine.event_inbox
		WHERE event_id IN (SELECT event_id FROM engine.outbox WHERE workflow_id = ANY($1::text[]))`, workflowIDs); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM engine.attempt_result_evidence
		WHERE workflow_id = ANY($1::text[])`, workflowIDs); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM engine.workflow_executions
		WHERE workflow_id = ANY($1::text[])`, workflowIDs); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM engine.workflow_definitions
		WHERE definition_id = $1`, definitionID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func postJSON(client *http.Client, url, parent string, body any, target any) error {
	return requestJSON(client, http.MethodPost, url, parent, body, target)
}
func getJSON(client *http.Client, url, parent string, target any) error {
	return requestJSON(client, http.MethodGet, url, parent, nil, target)
}
func requestJSON(client *http.Client, method, url, parent string, body any, target any) error {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = strings.NewReader(string(encoded))
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("traceparent", parent)
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		payload, _ := io.ReadAll(response.Body)
		return fmt.Errorf("HTTP %s: %s", response.Status, strings.TrimSpace(string(payload)))
	}
	return json.NewDecoder(response.Body).Decode(target)
}

func makeTraceparent(seed string) string {
	digest := sha256.Sum256([]byte(seed))
	return "00-" + hex.EncodeToString(digest[:16]) + "-" + hex.EncodeToString(digest[16:24]) + "-01"
}
func terminal(value state.WorkflowState) bool {
	return value == state.StateSucceeded || value == state.StateFailed || value == state.StateRejected || value == state.StateCanceled || value == state.StateAbandoned
}
func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
func fatal(format string, args ...any) { fmt.Fprintf(os.Stderr, format+"\n", args...); os.Exit(2) }
