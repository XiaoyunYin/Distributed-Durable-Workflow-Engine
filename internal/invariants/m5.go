package invariants

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

const faultTraceSchemaVersion = "fault-trace.v1"

// FaultEvidence is the controller-owned summary of one deterministic fault
// run.  It intentionally records observed facts separately from the requested
// action; a request, timeout, or process exit is not itself proof that a fault
// reached its named boundary.
type FaultEvidence struct {
	RunID    string
	Seed     int
	Boundary string
	// Durable identity is carried in failpoint fields with the explicit
	// durable_* names. Generic fixture IDs are intentionally not treated as a
	// database join key.
	WorkflowID        string
	NodeID            string
	AttemptNumber     int64
	EffectKey         string
	BoundaryReached   bool
	RequestedAction   string
	ObservedAction    string
	ProcessExitKnown  bool
	ProcessReturnCode *int
	CleanupCompleted  bool
	CleanupTimedOut   bool
	Fields            map[string]any
}

// CheckWithFaults joins controller evidence to the independent durable-state
// verdict.  The checker does not execute controller code or production
// transition validators; it only checks the persisted/parsed evidence shape.
func CheckWithFaults(trace Trace, faults []FaultEvidence) Verdict {
	verdict := Check(trace)
	violations := append([]string(nil), verdict.Violations...)
	checkFaultEvidence(faults, &violations)
	checkFaultJoins(trace, faults, &violations)
	return Verdict{Valid: len(violations) == 0, Violations: violations}
}

func checkFaultEvidence(faults []FaultEvidence, violations *[]string) {
	seenRuns := make(map[string]struct{}, len(faults))
	for _, fault := range faults {
		if fault.RunID == "" || fault.Seed < 0 || fault.Boundary == "" {
			*violations = append(*violations, "fault evidence identity is incomplete")
		}
		if _, exists := seenRuns[fault.RunID]; exists {
			*violations = append(*violations, "fault evidence run ID is duplicated: "+fault.RunID)
		}
		seenRuns[fault.RunID] = struct{}{}
		if !fault.CleanupCompleted {
			*violations = append(*violations, "fault run lacks bounded cleanup: "+fault.RunID)
		}
		if fault.CleanupTimedOut {
			*violations = append(*violations, "fault run has unbounded cleanup: "+fault.RunID)
			if fault.ProcessExitKnown {
				*violations = append(*violations, "process exit was recorded after cleanup timeout: "+fault.RunID)
			}
		}
		if !fault.ProcessExitKnown {
			*violations = append(*violations, "fault run lacks an observed process outcome: "+fault.RunID)
		}
		if fault.RequestedAction == "" {
			*violations = append(*violations, "fault run lacks requested action: "+fault.RunID)
		}
		if fault.RequestedAction == "release" && !fault.BoundaryReached {
			*violations = append(*violations, "release was requested before the boundary was reached: "+fault.RunID)
		}
		if fault.BoundaryReached && fault.ObservedAction == "boundary_not_reached" {
			*violations = append(*violations, "fault evidence contradicts its boundary acknowledgement: "+fault.RunID)
		}
		if fault.RequestedAction != "" && fault.ObservedAction == "" {
			*violations = append(*violations, "fault run lacks observed action: "+fault.RunID)
		}
	}
}

func checkFaultJoins(trace Trace, faults []FaultEvidence, violations *[]string) {
	for _, fault := range faults {
		if fault.WorkflowID == "" {
			continue
		}
		if !traceHasWorkflow(trace, fault.WorkflowID) {
			*violations = append(*violations, "fault evidence references missing durable workflow: "+fault.WorkflowID)
			continue
		}
		key := fmt.Sprintf("%s/%s/%d", fault.WorkflowID, fault.NodeID, fault.AttemptNumber)
		switch fault.Boundary {
		case "attempt_claimed", "claim_committed":
			if !traceHasAttempt(trace, fault.WorkflowID, fault.NodeID, fault.AttemptNumber) {
				*violations = append(*violations, "fault boundary has no durable attempt: "+key)
			}
		case "attempt_replaced":
			if !traceHasReplacedAttempt(trace, fault.WorkflowID, fault.NodeID, fault.AttemptNumber) {
				*violations = append(*violations, "fault boundary has no durable replacement attempt: "+key)
			}
		case "result_recorded":
			if !traceHasRecordedResult(trace, fault.WorkflowID, fault.NodeID) {
				*violations = append(*violations, "fault boundary has no durable recorded result: "+key)
			}
		case "result_consumed":
			if !traceHasResult(trace, fault.WorkflowID, fault.NodeID) {
				*violations = append(*violations, "fault boundary has no accepted durable result: "+key)
			}
		case "after_broker_ack", "outbox_published":
			if !traceHasOutbox(trace, fault.WorkflowID) {
				*violations = append(*violations, "fault boundary has no durable outbox event: "+fault.WorkflowID)
			}
		case "reconciliation_required":
			if !traceHasReconciliation(trace, fault.WorkflowID) {
				*violations = append(*violations, "fault boundary has no reconciliation evidence: "+fault.WorkflowID)
			}
		}
	}
}

func traceHasWorkflow(trace Trace, workflowID string) bool {
	for _, item := range trace.History {
		if item.WorkflowID == workflowID {
			return true
		}
	}
	for _, item := range trace.Attempts {
		if item.WorkflowID == workflowID {
			return true
		}
	}
	for _, item := range trace.Submissions {
		if item.WorkflowID == workflowID {
			return true
		}
	}
	return false
}

func traceHasAttempt(trace Trace, workflowID, nodeID string, attemptNumber int64) bool {
	for _, attempt := range trace.Attempts {
		if attempt.WorkflowID == workflowID && attempt.NodeID == nodeID &&
			(attemptNumber <= 0 || attempt.AttemptNumber == attemptNumber) {
			return true
		}
	}
	return false
}

func traceHasResult(trace Trace, workflowID, nodeID string) bool {
	for _, result := range trace.Results {
		if result.WorkflowID == workflowID && result.NodeID == nodeID && result.Accepted {
			return true
		}
	}
	return false
}

func traceHasRecordedResult(trace Trace, workflowID, nodeID string) bool {
	for _, attempt := range trace.Attempts {
		if attempt.WorkflowID == workflowID && attempt.NodeID == nodeID && attempt.ResultRecorded {
			return true
		}
	}
	return false
}

func traceHasReplacedAttempt(trace Trace, workflowID, nodeID string, attemptNumber int64) bool {
	old := false
	replacement := false
	for _, attempt := range trace.Attempts {
		if attempt.WorkflowID != workflowID || attempt.NodeID != nodeID {
			continue
		}
		if attempt.AttemptNumber == attemptNumber && attempt.State == "TIMED_OUT" && !attempt.IsCurrent {
			old = true
		}
		if attempt.AttemptNumber > attemptNumber && attempt.IsCurrent {
			replacement = true
		}
	}
	return old && replacement
}

func traceHasOutbox(trace Trace, workflowID string) bool {
	for _, event := range trace.Outbox {
		if event.WorkflowID == workflowID {
			return true
		}
	}
	return false
}

func traceHasReconciliation(trace Trace, workflowID string) bool {
	for _, item := range trace.Reconciliation {
		if item.WorkflowID == workflowID && (item.Status == "OPEN" || item.Status == "RESOLVED") {
			return true
		}
	}
	return false
}

type faultTraceRecord struct {
	SchemaVersion  string          `json:"schema_version"`
	RunID          string          `json:"run_id"`
	Sequence       int             `json:"sequence"`
	Seed           int             `json:"seed"`
	Event          string          `json:"event"`
	Boundary       string          `json:"boundary"`
	Command        json.RawMessage `json:"command"`
	Action         string          `json:"action"`
	ReturnCode     *int            `json:"return_code"`
	CleanupBounded *bool           `json:"bounded"`
	Fields         map[string]any  `json:"fields"`
}

// ParseFaultTrace reads the fault-trace.v1 JSONL format without importing the
// controller package.  This keeps the checker independent from the mechanism
// that produced the evidence.
func ParseFaultTrace(r io.Reader) ([]FaultEvidence, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 1024), 4<<20)
	byRun := map[string]*FaultEvidence{}
	lastSequence := map[string]int{}
	order := []string{}
	for scanner.Scan() {
		var record faultTraceRecord
		decoder := json.NewDecoder(bytes.NewReader(scanner.Bytes()))
		decoder.UseNumber()
		if err := decoder.Decode(&record); err != nil {
			return nil, fmt.Errorf("decode fault trace record: %w", err)
		}
		if record.SchemaVersion != faultTraceSchemaVersion {
			return nil, fmt.Errorf("unsupported fault trace schema %q", record.SchemaVersion)
		}
		if record.RunID == "" || record.Sequence <= 0 {
			return nil, fmt.Errorf("fault trace record has no run_id")
		}
		if expected := lastSequence[record.RunID] + 1; record.Sequence != expected {
			return nil, fmt.Errorf("fault trace sequence for %s is %d, want %d", record.RunID, record.Sequence, expected)
		}
		lastSequence[record.RunID] = record.Sequence
		fault, exists := byRun[record.RunID]
		if !exists {
			fault = &FaultEvidence{RunID: record.RunID, Seed: record.Seed, Boundary: record.Boundary,
				Fields: map[string]any{}}
			byRun[record.RunID] = fault
			order = append(order, record.RunID)
		}
		if fault.Seed != record.Seed {
			return nil, fmt.Errorf("fault trace seed changed within run %s", record.RunID)
		}
		if fault.Boundary == "" {
			fault.Boundary = record.Boundary
		}
		for key, value := range record.Fields {
			fault.Fields[key] = value
		}
		applyDurableIdentity(fault, record.Fields)
		switch record.Event {
		case "boundary_reached":
			fault.BoundaryReached = true
			if fault.Boundary == "" {
				fault.Boundary = record.Boundary
			}
		case "command":
			if command := decodeCommand(record.Command); command != "" {
				fault.RequestedAction = command
			}
		case "fault_observed":
			fault.ObservedAction = record.Action
		case "released":
			fault.ObservedAction = "released"
		case "boundary_timeout", "process_exited_before_boundary":
			if fault.ObservedAction == "" {
				fault.ObservedAction = "boundary_not_reached"
			}
		case "process_exited":
			fault.ProcessExitKnown = true
			fault.ProcessReturnCode = record.ReturnCode
		case "cleanup_timeout", "cleanup_unbounded":
			fault.CleanupTimedOut = true
		case "cleanup_completed":
			fault.CleanupCompleted = record.CleanupBounded != nil && *record.CleanupBounded
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	result := make([]FaultEvidence, 0, len(order))
	for _, runID := range order {
		result = append(result, *byRun[runID])
	}
	return result, nil
}

func decodeCommand(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var command string
	if json.Unmarshal(raw, &command) == nil {
		return command
	}
	// process_started records argv as an array. It is intentionally accepted
	// as metadata, not interpreted as a controller command.
	var argv []string
	if json.Unmarshal(raw, &argv) == nil && len(argv) > 0 {
		return "process_started"
	}
	return ""
}

func applyDurableIdentity(fault *FaultEvidence, fields map[string]any) {
	if value, ok := fields["durable_workflow_id"].(string); ok {
		fault.WorkflowID = value
	}
	if value, ok := fields["durable_node_id"].(string); ok {
		fault.NodeID = value
	}
	if value, ok := fields["durable_effect_key"].(string); ok {
		fault.EffectKey = value
	}
	if value, ok := fields["durable_attempt_number"].(json.Number); ok {
		if parsed, err := value.Int64(); err == nil {
			fault.AttemptNumber = parsed
		}
	}
}

// LoadFaultTrace opens a controller trace for a campaign/checker join.
func LoadFaultTrace(path string) ([]FaultEvidence, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return ParseFaultTrace(file)
}
