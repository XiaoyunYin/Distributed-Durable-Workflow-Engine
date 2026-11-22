package k6live

import (
	"testing"

	"durable-agent-execution-engine/internal/invariants/k6v1"
)

func TestK6RacedCutCannotBeSelected(t *testing.T) {
	files := []k6v1.File{{Path: "engine/snapshot.json"}}
	attempt := k6v1.CutAttempt{Generation: 1, Reasons: []string{"CUT_RACED:external"}}
	if canSelect(attempt, "stage", files, true) {
		t.Fatal("raced cut selected")
	}
	attempt = k6v1.CutAttempt{Generation: 2}
	if canSelect(attempt, "stage", files, false) {
		t.Fatal("discontinuous cut selected")
	}
	if !canSelect(attempt, "stage", files, true) {
		t.Fatal("clean retake discarded")
	}
}
