package main

import (
	"bytes"
	"strings"
	"testing"
)

const firstPhaseRow = `{"workflow_id":"a","scheduled_ns":1000000000,"terminal_ns":1150000000,"phase_ns":[5000000,10000000,110000000,25000000]}`
const secondPhaseRow = `{"workflow_id":"b","scheduled_ns":2000000000,"terminal_ns":2700000000,"phase_ns":[25000000,50000000,125000000,500000000]}`

func TestCheckedWorkflowHistogramMatchesWitnessedPhases(t *testing.T) {
	var output bytes.Buffer
	if err := renderCheckedPhases(strings.NewReader("["+firstPhaseRow+","+secondPhaseRow+"]"), &output); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`# TYPE durable_j9a_workflow_stage_seconds histogram`,
		`durable_j9a_workflow_stage_seconds_bucket{role="j9a-offline",stage="scheduled_to_attempt_created",le="0.005"} 1`,
		`durable_j9a_workflow_stage_seconds_sum{role="j9a-offline",stage="scheduled_to_attempt_created"} 0.03`,
		`durable_j9a_workflow_stage_seconds_sum{role="j9a-offline",stage="attempt_created_to_worker_start"} 0.06`,
		`durable_j9a_workflow_stage_seconds_sum{role="j9a-offline",stage="worker_start_to_result_recorded"} 0.235`,
		`durable_j9a_workflow_stage_seconds_sum{role="j9a-offline",stage="result_recorded_to_succeeded"} 0.525`,
		`durable_j9a_workflow_stage_seconds_bucket{role="j9a-offline",stage="scheduled_to_succeeded",le="0.5"} 1`,
		`durable_j9a_workflow_stage_seconds_bucket{role="j9a-offline",stage="scheduled_to_succeeded",le="+Inf"} 2`,
		`durable_j9a_workflow_stage_seconds_sum{role="j9a-offline",stage="scheduled_to_succeeded"} 0.85`,
		`durable_j9a_workflow_stage_seconds_count{role="j9a-offline",stage="scheduled_to_succeeded"} 2`,
	} {
		if !strings.Contains(output.String(), want+"\n") {
			t.Errorf("missing sample %q", want)
		}
	}
	if strings.Contains(output.String(), "workflow_id") || strings.Contains(output.String(), "durable_j9a_relay") {
		t.Fatal("unexpected series in workflow histogram")
	}
}

func TestCheckedWorkflowHistogramRejectsInvalidRows(t *testing.T) {
	for _, tc := range []struct{ name, input string }{
		{"nonarray", "{}"},
		{"duplicate", "[" + firstPhaseRow + "," + firstPhaseRow + "]"},
		{"out_of_order", "[" + secondPhaseRow + "," + firstPhaseRow + "]"},
		{"malformed", "[" + firstPhaseRow},
		{"negative", "[" + strings.Replace(firstPhaseRow, "5000000,", "-5000000,", 1) + "]"},
		{"null", "[" + strings.Replace(strings.Replace(firstPhaseRow, "5000000,", "null,", 1), "1150000000", "1145000000", 1) + "]"},
		{"wrong_span", "[" + strings.Replace(firstPhaseRow, "1150000000", "1250000000", 1) + "]"},
		{"trailing_data", "[" + firstPhaseRow + "]{}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			if err := renderCheckedPhases(strings.NewReader(tc.input), &output); err == nil || output.Len() != 0 {
				t.Fatalf("render error = %v, output bytes = %d", err, output.Len())
			}
		})
	}
}
