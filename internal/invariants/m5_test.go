package invariants

import (
	"strings"
	"testing"
)

func validFaultEvidence() FaultEvidence {
	code := 0
	return FaultEvidence{RunID: "run-1", Seed: 23, Boundary: "after-effect",
		BoundaryReached: true, RequestedAction: "release", ObservedAction: "released",
		ProcessExitKnown: true, ProcessReturnCode: &code, CleanupCompleted: true}
}

func TestCheckWithFaultEvidenceAcceptsObservedRun(t *testing.T) {
	if verdict := CheckWithFaults(validTrace(), []FaultEvidence{validFaultEvidence()}); !verdict.Valid {
		t.Fatalf("valid fault evidence rejected: %v", verdict.Violations)
	}
}

func TestCheckWithFaultEvidenceRejectsMutations(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*FaultEvidence)
		want   string
	}{
		{"missing cleanup", func(e *FaultEvidence) { e.CleanupCompleted = false }, "bounded cleanup"},
		{"release before boundary", func(e *FaultEvidence) { e.BoundaryReached = false }, "before the boundary"},
		{"missing observed action", func(e *FaultEvidence) { e.ObservedAction = "" }, "observed action"},
		{"missing process outcome", func(e *FaultEvidence) { e.ProcessExitKnown = false }, "process outcome"},
		{"contradictory boundary", func(e *FaultEvidence) { e.ObservedAction = "boundary_not_reached" }, "contradicts"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			evidence := validFaultEvidence()
			testCase.mutate(&evidence)
			verdict := CheckWithFaults(validTrace(), []FaultEvidence{evidence})
			if verdict.Valid || !strings.Contains(strings.Join(verdict.Violations, "\n"), testCase.want) {
				t.Fatalf("expected %q, got valid=%v violations=%v", testCase.want, verdict.Valid, verdict.Violations)
			}
		})
	}
}

func TestParseFaultTraceBuildsObservedSummary(t *testing.T) {
	trace := strings.NewReader(`{"schema_version":"fault-trace.v1","run_id":"run-1","sequence":1,"event":"boundary_reached","seed":23,"boundary":"after-effect"}
{"schema_version":"fault-trace.v1","run_id":"run-1","sequence":2,"event":"command","seed":23,"boundary":"after-effect","command":"release"}
{"schema_version":"fault-trace.v1","run_id":"run-1","sequence":3,"event":"fault_observed","seed":23,"boundary":"after-effect","action":"released"}
{"schema_version":"fault-trace.v1","run_id":"run-1","sequence":4,"event":"process_exited","seed":23,"boundary":"after-effect","return_code":0}
{"schema_version":"fault-trace.v1","run_id":"run-1","sequence":5,"event":"cleanup_completed","seed":23,"boundary":"after-effect","bounded":true}
`)
	evidence, err := ParseFaultTrace(trace)
	if err != nil || len(evidence) != 1 {
		t.Fatalf("parse fault trace = %v, %v", evidence, err)
	}
	if verdict := CheckWithFaults(validTrace(), evidence); !verdict.Valid {
		t.Fatalf("parsed fault evidence rejected: %v", verdict.Violations)
	}
}
