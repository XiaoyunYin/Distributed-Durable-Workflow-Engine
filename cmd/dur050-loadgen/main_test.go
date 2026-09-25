package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestOpenLoopScheduleDoesNotWaitForPriorWorkflowResponse(t *testing.T) {
	var active atomic.Int32
	var maximum atomic.Int32
	var arrivals atomic.Int32
	joined := make(chan struct{})
	var joinedOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request submissionRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		current := active.Add(1)
		for previous := maximum.Load(); current > previous && !maximum.CompareAndSwap(previous, current); previous = maximum.Load() {
		}
		defer active.Add(-1)
		if arrivals.Add(1) == 2 {
			joinedOnce.Do(func() { close(joined) })
		}
		select {
		case <-joined:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"created": true, "workflow": map[string]string{"workflow_id": request.WorkflowID}})
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	client := newHTTPClient()
	defer client.CloseIdleConnections()
	records, summary, err := runCampaign(ctx, client, server.URL+"/v1/workflows", testCampaignConfig(), 2, 2, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if maximum.Load() != 2 {
		t.Fatalf("simultaneous HTTP requests=%d, want 2 (a completed-response loop would deadlock here)", maximum.Load())
	}
	if summary.Scheduled != 2 || summary.Accepted != 2 || summary.GeneratorCapacityMiss != 0 || len(records) != 2 {
		t.Fatalf("open-loop summary=%+v records=%+v", summary, records)
	}
	for _, record := range records {
		if record.Outcome != "accepted" || record.Attempts != 1 {
			t.Fatalf("unexpected submission record: %+v", record)
		}
	}
}

func TestUncertainResponseRetriesSameIdempotencyIdentity(t *testing.T) {
	var mu sync.Mutex
	seen := map[string][]string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var request submissionRequest
		if err := json.Unmarshal(body, &request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		seen[request.WorkflowID] = append(seen[request.WorkflowID], string(body))
		callNumber := len(seen[request.WorkflowID])
		mu.Unlock()
		if request.WorkflowID == "dur050-test-run-test-000001" && callNumber == 1 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `{"error":{"code":"DATABASE_UNAVAILABLE","message":"temporary"}}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"created": true, "workflow": map[string]string{"workflow_id": request.WorkflowID}})
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	client := newHTTPClient()
	defer client.CloseIdleConnections()
	records, summary, err := runCampaign(ctx, client, server.URL+"/v1/workflows", testCampaignConfig(), 2, 2, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Accepted != 2 || summary.Ambiguous != 0 {
		t.Fatalf("reconciled retry summary=%+v", summary)
	}
	mu.Lock()
	defer mu.Unlock()
	for workflowID, bodies := range seen {
		if workflowID == "dur050-test-run-test-000001" {
			if len(bodies) != 2 || bodies[0] != bodies[1] {
				t.Fatalf("uncertain outcome retry changed request identity/body: %q", bodies)
			}
		}
	}
	var retried bool
	for _, record := range records {
		if record.WorkflowID == "dur050-test-run-test-000001" {
			retried = record.Attempts == 2 && record.Outcome == "accepted"
		}
	}
	if !retried {
		t.Fatalf("same-key retry was not reflected in records: %+v", records)
	}
}

func TestConfigRequiresBothBalancedWorkloadFamilies(t *testing.T) {
	config := testCampaignConfig()
	if err := validateConfig(config); err != nil {
		t.Fatal(err)
	}
	config.Families = config.Families[:1]
	if err := validateConfig(config); err == nil {
		t.Fatal("one-family configuration unexpectedly passed")
	}
}

func TestGeneratorCPUOver80PercentForMoreThanOnePercentFails(t *testing.T) {
	samples := make([]cpuIntervalSample, 100)
	for index := range samples {
		cpuPercent := 20.0
		if index < 2 {
			cpuPercent = 81
		}
		samples[index] = cpuIntervalSample{StartElapsedSeconds: float64(index), EndElapsedSeconds: float64(index + 1), CPUPercent: cpuPercent}
	}
	summary := runSummary{Status: "PENDING_CPU_VALIDATION"}
	applyCPUValidation(&summary, samples, 100*time.Second, 2, nil)
	if summary.Status != "FAIL" || summary.GeneratorCPU.Status != "FAIL" {
		t.Fatalf("over-limit CPU series passed: %+v", summary)
	}
	if summary.GeneratorCPU.OverThresholdRatio != 0.02 {
		t.Fatalf("over-threshold ratio=%f, want 0.02", summary.GeneratorCPU.OverThresholdRatio)
	}
	if !strings.Contains(summary.InvalidReason, "exceeded 80% for more than 1%") {
		t.Fatalf("failure does not name the protocol CPU rule: %q", summary.InvalidReason)
	}
	encoded, err := json.Marshal(summary)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"cpu_percent_normalized_per_core"`) || !strings.Contains(string(encoded), `"samples"`) {
		t.Fatalf("CPU interval series is missing from the JSON summary: %s", encoded)
	}
}

func TestGeneratorCPUAtExactlyOnePercentDoesNotFail(t *testing.T) {
	samples := make([]cpuIntervalSample, 100)
	for index := range samples {
		cpuPercent := 80.0
		if index == 0 {
			cpuPercent = 80.01
		}
		samples[index] = cpuIntervalSample{StartElapsedSeconds: float64(index), EndElapsedSeconds: float64(index + 1), CPUPercent: cpuPercent}
	}
	summary := runSummary{Status: "PENDING_CPU_VALIDATION"}
	applyCPUValidation(&summary, samples, 100*time.Second, 2, nil)
	if summary.Status != "PASS" || summary.GeneratorCPU.OverThresholdRatio != 0.01 {
		t.Fatalf("CPU exactly at allowed overage should pass: %+v", summary)
	}
}

func TestGeneratorCPUValidationFailsWithoutSamples(t *testing.T) {
	summary := runSummary{Status: "PENDING_CPU_VALIDATION"}
	applyCPUValidation(&summary, nil, time.Minute, 2, nil)
	if summary.Status != "FAIL" || !strings.Contains(summary.InvalidReason, "no valid measurement window or samples") {
		t.Fatalf("missing CPU evidence did not fail closed: %+v", summary)
	}
}

func testCampaignConfig() campaignConfig {
	return campaignConfig{APIURL: "http://127.0.0.1:8080", Namespace: "dur050-test", RunID: "run-test", Seed: 50050,
		Families: []familyConfig{
			{Name: "seq-8", DefinitionID: "seq-v1", DefinitionVersion: 1, InitialNodeID: "activity-0", Payload: json.RawMessage(`{"profile":"seq-8"}`), InitialInput: json.RawMessage(`{}`)},
			{Name: "fanout-8", DefinitionID: "fanout-v1", DefinitionVersion: 1, InitialNodeID: "root", Payload: json.RawMessage(`{"profile":"fanout-8"}`), InitialInput: json.RawMessage(`{}`)},
		},
	}
}

func ExamplemakeSubmission() {
	config := testCampaignConfig()
	request := makeSubmission(config, config.Families[0], "dur050-test-run-test-000001", "run-test/000001")
	encoded, _ := json.Marshal(request)
	fmt.Println(string(encoded))
	// Output:
	// {"workflow_id":"dur050-test-run-test-000001","namespace":"dur050-test","submission_key":"run-test/000001","payload":{"profile":"seq-8"},"definition_id":"seq-v1","definition_version":1,"initial_node_id":"activity-0","initial_input":{},"actor_id":"dur050-loadgen"}
}
