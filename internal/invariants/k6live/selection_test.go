package k6live

import (
	"testing"

	"durable-agent-execution-engine/internal/invariants/k6v1"
)

func TestK6RacedCutCannotBeSelected(t *testing.T) {
	files := []k6v1.File{{Path: k6v1.SnapshotPath}}
	attempt := k6v1.CutAttempt{Generation: 1, Databases: []k6v1.DatabaseCut{{DB: "engine"}}, Reasons: []string{"CUT_RACED:external"}}
	if canSelect(attempt, "stage", files, false, true) {
		t.Fatal("raced cut selected")
	}
	attempt.Generation, attempt.Reasons = 2, nil
	if canSelect(attempt, "stage", files, false, false) {
		t.Fatal("discontinuous cut selected")
	}
	if !canSelect(attempt, "stage", files, false, true) {
		t.Fatal("clean retake discarded")
	}
}

func TestK6CutDeadlineSalvageSelection(t *testing.T) {
	files := []k6v1.File{{Path: k6v1.SnapshotPath}, {Path: k6v1.SinkSnapshotPath}}
	attempt := k6v1.CutAttempt{
		Generation: 1, Databases: []k6v1.DatabaseCut{{DB: "engine"}, {DB: "sink"}}, Reasons: []string{"CUT_DEADLINE"},
	}
	if !canSelect(attempt, "stage", files, false, true) {
		t.Fatal("completed deadline-only cut discarded")
	}
	if canSelect(attempt, "stage", files, true, true) || canSelect(attempt, "stage", files, false, false) ||
		canSelect(attempt, "", files, false, true) || canSelect(attempt, "stage", nil, false, true) {
		t.Fatal("late cut selected without terminal, continuous, staged evidence")
	}
	for _, reason := range []string{"CUT_RACED:external", "CUT_INTERRUPTED", "CUT_BINDING:engine", "COLUMN_UNCLASSIFIED:engine.workflow_executions"} {
		attempt.Reasons = []string{"CUT_DEADLINE", reason}
		if canSelect(attempt, "stage", files, false, true) {
			t.Fatalf("late cut with %s selected", reason)
		}
	}
	attempt.Reasons, attempt.Databases = []string{"CUT_DEADLINE"}, nil
	if canSelect(attempt, "stage", files, false, true) {
		t.Fatal("late cut without database cuts selected")
	}
}
