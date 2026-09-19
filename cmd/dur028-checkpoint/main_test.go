package main

import (
	"context"
	"encoding/json"
	"testing"

	"durable-agent-execution-engine/internal/engine"
	"durable-agent-execution-engine/internal/state"

	"github.com/jackc/pgx/v5"
)

type memoryCheckpointSink struct {
	checkpoint *state.Checkpoint
}

func (s *memoryCheckpointSink) Save(_ context.Context, sequence int64, schemaVersion int, payload json.RawMessage) error {
	s.checkpoint = &state.Checkpoint{Sequence: sequence, SchemaVersion: schemaVersion, Payload: append(json.RawMessage(nil), payload...)}
	return nil
}

func (s *memoryCheckpointSink) Load(context.Context) (state.Checkpoint, error) {
	if s.checkpoint == nil {
		return state.Checkpoint{}, pgx.ErrNoRows
	}
	return *s.checkpoint, nil
}

var _ engine.CheckpointSink = (*memoryCheckpointSink)(nil)
var _ engine.CheckpointReader = (*memoryCheckpointSink)(nil)

func TestExpectedDigestIsStable(t *testing.T) {
	if got, want := expectedDigest(280028), expectedDigest(280028); got != want || len(got) != 64 {
		t.Fatalf("digest = %q, want stable sha256", got)
	}
	if expectedDigest(280028) == expectedDigest(280029) {
		t.Fatal("different seeds produced the same digest")
	}
}

func TestChunkDriverCheckpointPrefixAndRecovery(t *testing.T) {
	driver := &chunkDriver{interval: 5, seed: 280028, crash: true, barrier: crashChunk}
	sink := &memoryCheckpointSink{}
	panicked := false
	func() {
		defer func() { panicked = recover() != nil }()
		_, _ = driver.RunWithCheckpoint(context.Background(), engine.Activity{}, sink)
	}()
	if !panicked {
		t.Fatal("expected the frozen crash barrier to panic")
	}
	if driver.firstRun != crashChunk || driver.crashProgress != 99 {
		t.Fatalf("first run = %d, crash progress = %d", driver.firstRun, driver.crashProgress)
	}
	if sink.checkpoint == nil {
		t.Fatal("crash did not leave a durable checkpoint prefix")
	}
	var saved chunkPayload
	if err := json.Unmarshal(sink.checkpoint.Payload, &saved); err != nil {
		t.Fatal(err)
	}
	if saved.Completed != 95 || sink.checkpoint.Sequence != 18 {
		t.Fatalf("checkpoint = %+v sequence=%d, want chunk 95 sequence 18", saved, sink.checkpoint.Sequence)
	}
	driver.crash = false
	result, err := driver.RunWithCheckpoint(context.Background(), engine.Activity{}, sink)
	if err != nil {
		t.Fatal(err)
	}
	if driver.recoveryRun != 105 || driver.computed != 205 || string(result.Payload) == "" {
		t.Fatalf("recovery counters: first=%d recovery=%d computed=%d payload=%s", driver.firstRun, driver.recoveryRun, driver.computed, result.Payload)
	}
	if driver.finalDigest != expectedDigest(driver.seed) {
		t.Fatalf("final digest = %s, want %s", driver.finalDigest, expectedDigest(driver.seed))
	}
}

func TestValidateArtifactRejectsIncompleteRows(t *testing.T) {
	rows := []runReport{{CaseID: "one", Status: "PASS", FinalHash: "a", ExpectedHash: "a", TerminalState: string(state.StateSucceeded)}}
	got := validateArtifact(rows, 1, nil, conclusions{}, 0)
	if got.Status != "FAIL" {
		t.Fatal("incomplete artifact was accepted")
	}
}
