package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/state"
)

func TestWorkflowAPIResponseLossHistoryAndRetention(t *testing.T) {
	databaseURL := apiIntegrationDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := state.NewFromURL(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	definitionID := "dur006-" + state.NewID()
	namespace := "dur006-" + state.NewID()
	if err := store.CreateDefinition(ctx, state.DefinitionInput{
		DefinitionID: definitionID, Version: 1, DefinitionHash: "hash-1",
		Graph:            json.RawMessage(`{"nodes":["root"]}`),
		ActivityVersions: json.RawMessage(`{}`),
		EffectClasses:    json.RawMessage(`{"root":"PURE_ACTIVITY"}`),
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = store.Pool().Exec(cleanupCtx, `DELETE FROM engine.workflow_executions WHERE namespace = $1`, namespace)
		_, _ = store.Pool().Exec(cleanupCtx, `DELETE FROM engine.workflow_definitions WHERE definition_id = $1`, definitionID)
	}()

	workflowID := "dur006-" + state.NewID()
	submissionKey := "submission-" + state.NewID()
	requestBodyWith := func(id, key, definition string, version int, node string, initialInput json.RawMessage, payload string) []byte {
		request, err := json.Marshal(map[string]any{
			"workflow_id": id, "namespace": namespace, "submission_key": key,
			"payload": json.RawMessage(payload), "definition_id": definition,
			"definition_version": version, "initial_node_id": node,
			"initial_input": initialInput,
		})
		if err != nil {
			t.Fatal(err)
		}
		return request
	}
	requestBody := func(payload string) []byte {
		return requestBodyWith(workflowID, submissionKey, definitionID, 1, "root", json.RawMessage(`{}`), payload)
	}
	handler := NewServer(store).Handler()
	httpServer := httptest.NewServer(handler)
	defer httpServer.Close()

	// A real HTTP transport receives the response headers, closes the response
	// body, and reports a client-side connection loss. The server has already
	// committed the transaction, so the retry must resolve the same workflow.
	dropClient := &http.Client{Transport: responseDropTransport{base: http.DefaultTransport}}
	firstRequest, err := http.NewRequest(http.MethodPost, httpServer.URL+"/v1/workflows", bytes.NewReader(requestBody(`{"b":2,"a":1}`)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dropClient.Do(firstRequest); err == nil {
		t.Fatal("response-loss transport unexpectedly returned success")
	}
	second := httptest.NewRecorder()
	handler.ServeHTTP(second, httptest.NewRequest(http.MethodPost, "/v1/workflows", bytes.NewReader(requestBody(`{"a":1,"b":2}`))))
	if second.Code != http.StatusOK {
		t.Fatalf("retry submission status = %d: %s", second.Code, second.Body.String())
	}
	var retry workflowResponse
	if err := json.NewDecoder(second.Body).Decode(&retry); err != nil {
		t.Fatal(err)
	}
	if retry.Created || retry.Workflow.WorkflowID != workflowID {
		t.Fatalf("retry response = %+v", retry)
	}

	statusResponse := httptest.NewRecorder()
	handler.ServeHTTP(statusResponse, httptest.NewRequest(http.MethodGet, "/v1/workflows/"+workflowID+"?expected_revision=1", nil))
	if statusResponse.Code != http.StatusOK {
		t.Fatalf("status query = %d: %s", statusResponse.Code, statusResponse.Body.String())
	}
	var status workflowResponse
	if err := json.NewDecoder(statusResponse.Body).Decode(&status); err != nil {
		t.Fatal(err)
	}
	if status.Workflow.Revision != 1 || status.Workflow.State != state.StateRunnable {
		t.Fatalf("status response = %+v", status)
	}

	historyResponseRecorder := httptest.NewRecorder()
	handler.ServeHTTP(historyResponseRecorder, httptest.NewRequest(http.MethodGet, "/v1/workflows/"+workflowID+"/history?limit=1", nil))
	if historyResponseRecorder.Code != http.StatusOK {
		t.Fatalf("history query = %d: %s", historyResponseRecorder.Code, historyResponseRecorder.Body.String())
	}
	var history historyResponse
	if err := json.NewDecoder(historyResponseRecorder.Body).Decode(&history); err != nil {
		t.Fatal(err)
	}
	if len(history.Entries) != 1 || history.Entries[0].Reason != "WORKFLOW_CREATED" {
		t.Fatalf("history response = %+v", history)
	}
	retained := httptest.NewRecorder()
	handler.ServeHTTP(retained, httptest.NewRequest(http.MethodGet, "/v1/workflows/"+workflowID+"/history?after_revision=1", nil))
	if retained.Code != http.StatusOK {
		t.Fatalf("retained history query = %d: %s", retained.Code, retained.Body.String())
	}
	var retainedHistory historyResponse
	if err := json.NewDecoder(retained.Body).Decode(&retainedHistory); err != nil {
		t.Fatal(err)
	}
	if len(retainedHistory.Entries) != 0 {
		t.Fatalf("history after revision 1 = %+v", retainedHistory)
	}

	stale := httptest.NewRecorder()
	handler.ServeHTTP(stale, httptest.NewRequest(http.MethodGet, "/v1/workflows/"+workflowID+"?expected_revision=0", nil))
	if stale.Code != http.StatusConflict || !strings.Contains(stale.Body.String(), "STALE_REVISION") {
		t.Fatalf("stale query = %d: %s", stale.Code, stale.Body.String())
	}

	conflict := httptest.NewRecorder()
	handler.ServeHTTP(conflict, httptest.NewRequest(http.MethodPost, "/v1/workflows", bytes.NewReader(requestBody(`{"a":1,"b":3}`))))
	if conflict.Code != http.StatusConflict || !strings.Contains(conflict.Body.String(), "PAYLOAD_CONFLICT") {
		t.Fatalf("payload conflict = %d: %s", conflict.Code, conflict.Body.String())
	}
	meaningConflict := httptest.NewRecorder()
	handler.ServeHTTP(meaningConflict, httptest.NewRequest(http.MethodPost, "/v1/workflows", bytes.NewReader(requestBodyWith(workflowID, submissionKey, definitionID, 1, "root", json.RawMessage(`{"amount":9999}`), `{"a":1,"b":2}`))))
	if meaningConflict.Code != http.StatusConflict || !strings.Contains(meaningConflict.Body.String(), "PAYLOAD_CONFLICT") {
		t.Fatalf("execution-meaning conflict = %d: %s", meaningConflict.Code, meaningConflict.Body.String())
	}

	unknownDefinition := httptest.NewRecorder()
	handler.ServeHTTP(unknownDefinition, httptest.NewRequest(http.MethodPost, "/v1/workflows", bytes.NewReader(requestBodyWith("dur006-"+state.NewID(), "unknown-definition-"+state.NewID(), "missing-definition", 1, "root", json.RawMessage(`{}`), `{}`))))
	if unknownDefinition.Code != http.StatusNotFound || !strings.Contains(unknownDefinition.Body.String(), "DEFINITION_NOT_FOUND") {
		t.Fatalf("unknown definition = %d: %s", unknownDefinition.Code, unknownDefinition.Body.String())
	}

	unknownNode := httptest.NewRecorder()
	handler.ServeHTTP(unknownNode, httptest.NewRequest(http.MethodPost, "/v1/workflows", bytes.NewReader(requestBodyWith("dur006-"+state.NewID(), "unknown-node-"+state.NewID(), definitionID, 1, "missing", json.RawMessage(`{}`), `{}`))))
	if unknownNode.Code != http.StatusUnprocessableEntity || !strings.Contains(unknownNode.Body.String(), "UNKNOWN_INITIAL_NODE") {
		t.Fatalf("unknown node = %d: %s", unknownNode.Code, unknownNode.Body.String())
	}

	workflowIDConflict := httptest.NewRecorder()
	handler.ServeHTTP(workflowIDConflict, httptest.NewRequest(http.MethodPost, "/v1/workflows", bytes.NewReader(requestBodyWith(workflowID, "different-submission-"+state.NewID(), definitionID, 1, "root", json.RawMessage(`{}`), `{}`))))
	if workflowIDConflict.Code != http.StatusConflict || !strings.Contains(workflowIDConflict.Body.String(), "WORKFLOW_ID_CONFLICT") {
		t.Fatalf("workflow ID conflict = %d: %s", workflowIDConflict.Code, workflowIDConflict.Body.String())
	}

	duplicateKeys := httptest.NewRecorder()
	handler.ServeHTTP(duplicateKeys, httptest.NewRequest(http.MethodPost, "/v1/workflows", strings.NewReader(`{"submission_key":"duplicate-keys","payload":{"a":1,"a":2},"definition_id":"`+definitionID+`","definition_version":1,"initial_node_id":"root"}`)))
	if duplicateKeys.Code != http.StatusBadRequest || !strings.Contains(duplicateKeys.Body.String(), "INVALID_REQUEST") {
		t.Fatalf("duplicate keys = %d: %s", duplicateKeys.Code, duplicateKeys.Body.String())
	}
	var workflowCount, outboxCount, historyCount int
	if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.workflow_executions WHERE namespace = $1`, namespace).Scan(&workflowCount); err != nil {
		t.Fatal(err)
	}
	if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.outbox WHERE workflow_id = $1`, workflowID).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.transition_history WHERE workflow_id = $1`, workflowID).Scan(&historyCount); err != nil {
		t.Fatal(err)
	}
	if workflowCount != 1 || outboxCount != 1 || historyCount != 1 {
		t.Fatalf("idempotency counts = workflows %d outbox %d history %d", workflowCount, outboxCount, historyCount)
	}

	for round := 0; round < 5; round++ {
		concurrentWorkflowID := "dur006-" + state.NewID()
		concurrentKey := "concurrent-submission-" + state.NewID()
		statuses := make(chan int, 12)
		var waitGroup sync.WaitGroup
		for requestNumber := 0; requestNumber < 12; requestNumber++ {
			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()
				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/workflows", bytes.NewReader(requestBodyWith(concurrentWorkflowID, concurrentKey, definitionID, 1, "root", json.RawMessage(`{}`), `{}`))))
				statuses <- recorder.Code
			}()
		}
		waitGroup.Wait()
		close(statuses)
		createdCount, retryCount := 0, 0
		for statusCode := range statuses {
			switch statusCode {
			case http.StatusCreated:
				createdCount++
			case http.StatusOK:
				retryCount++
			default:
				t.Fatalf("concurrent identical request status = %d", statusCode)
			}
		}
		if createdCount != 1 || retryCount != 11 {
			t.Fatalf("concurrent identical requests = %d created, %d retries", createdCount, retryCount)
		}
	}

	missing := httptest.NewRecorder()
	handler.ServeHTTP(missing, httptest.NewRequest(http.MethodGet, "/v1/workflows/does-not-exist", nil))
	if missing.Code != http.StatusNotFound || !strings.Contains(missing.Body.String(), "NOT_FOUND") {
		t.Fatalf("missing workflow = %d: %s", missing.Code, missing.Body.String())
	}
	unknownRoute := httptest.NewRecorder()
	handler.ServeHTTP(unknownRoute, httptest.NewRequest(http.MethodGet, "/v1/not-a-route", nil))
	if unknownRoute.Code != http.StatusNotFound || !strings.Contains(unknownRoute.Body.String(), `"code":"NOT_FOUND"`) {
		t.Fatalf("unknown route = %d: %s", unknownRoute.Code, unknownRoute.Body.String())
	}
}

type responseDropTransport struct {
	base http.RoundTripper
}

func (t responseDropTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := t.base.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	_ = response.Body.Close()
	return nil, fmt.Errorf("simulated response connection loss")
}

func apiIntegrationDatabaseURL(t *testing.T) string {
	t.Helper()
	if os.Getenv("DURABLE_RUN_INTEGRATION") != "1" && os.Getenv("DURABLE_REQUIRE_DATABASE") != "1" {
		t.Skip("API integration tests require DURABLE_RUN_INTEGRATION=1 or service CI mode")
	}
	if value := os.Getenv("DURABLE_DATABASE_URL"); value != "" {
		return value
	}
	workingDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for directory := workingDirectory; ; directory = filepath.Dir(directory) {
		candidate := filepath.Join(directory, ".env")
		if data, readErr := os.ReadFile(candidate); readErr == nil {
			values := map[string]string{}
			for _, line := range strings.Split(string(data), "\n") {
				parts := strings.SplitN(strings.TrimSpace(line), "=", 2)
				if len(parts) == 2 {
					values[parts[0]] = strings.Trim(parts[1], "\r\"")
				}
			}
			user := values["POSTGRES_USER"]
			database := values["POSTGRES_DB"]
			password := values["POSTGRES_PASSWORD"]
			port := values["POSTGRES_PORT"]
			if port == "" {
				port = "5432"
			}
			if user != "" && database != "" && password != "" {
				return fmt.Sprintf("postgresql://%s:%s@127.0.0.1:%s/%s?sslmode=disable", user, password, port, database)
			}
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			break
		}
	}
	t.Fatal("set DURABLE_DATABASE_URL or provide .env for API integration tests")
	return ""
}
