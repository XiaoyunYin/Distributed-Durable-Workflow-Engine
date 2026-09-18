package invariants

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// FaultEvidence is the controller-owned summary of one deterministic fault
// run.  It intentionally records observed facts separately from the requested
// action; a request, timeout, or process exit is not itself proof that a fault
// reached its named boundary.
type FaultEvidence struct {
	RunID             string
	Seed              int
	Boundary          string
	BoundaryReached   bool
	RequestedAction   string
	ObservedAction    string
	ProcessExitKnown  bool
	ProcessReturnCode *int
	CleanupCompleted  bool
}

// CheckWithFaults joins controller evidence to the independent durable-state
// verdict.  The checker does not execute controller code or production
// transition validators; it only checks the persisted/parsed evidence shape.
func CheckWithFaults(trace Trace, faults []FaultEvidence) Verdict {
	verdict := Check(trace)
	violations := append([]string(nil), verdict.Violations...)
	checkFaultEvidence(faults, &violations)
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

type faultTraceRecord struct {
	RunID          string `json:"run_id"`
	Seed           int    `json:"seed"`
	Event          string `json:"event"`
	Boundary       string `json:"boundary"`
	Command        string `json:"command"`
	Action         string `json:"action"`
	ReturnCode     *int   `json:"return_code"`
	CleanupBounded bool   `json:"bounded"`
}

// ParseFaultTrace reads the fault-trace.v1 JSONL format without importing the
// controller package.  This keeps the checker independent from the mechanism
// that produced the evidence.
func ParseFaultTrace(r io.Reader) ([]FaultEvidence, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 1024), 4<<20)
	byRun := map[string]*FaultEvidence{}
	order := []string{}
	for scanner.Scan() {
		var record faultTraceRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return nil, fmt.Errorf("decode fault trace record: %w", err)
		}
		if record.RunID == "" {
			return nil, fmt.Errorf("fault trace record has no run_id")
		}
		fault, exists := byRun[record.RunID]
		if !exists {
			fault = &FaultEvidence{RunID: record.RunID, Seed: record.Seed, Boundary: record.Boundary}
			byRun[record.RunID] = fault
			order = append(order, record.RunID)
		}
		if fault.Boundary == "" {
			fault.Boundary = record.Boundary
		}
		switch record.Event {
		case "boundary_reached":
			fault.BoundaryReached = true
			if fault.Boundary == "" {
				fault.Boundary = record.Boundary
			}
		case "command":
			if record.Command != "" {
				fault.RequestedAction = record.Command
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
		case "cleanup_completed":
			fault.CleanupCompleted = record.CleanupBounded
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

// LoadFaultTrace opens a controller trace for a campaign/checker join.
func LoadFaultTrace(path string) ([]FaultEvidence, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return ParseFaultTrace(file)
}
