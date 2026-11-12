package dur053

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"

	"durable-agent-execution-engine/internal/invariants"
)

// Preserve the pre-B.6.3 algorithm as an independent byte-output oracle.
func perRowReference(c Config) (invariants.Arrivals, error) {
	if err := c.Validate(); err != nil {
		return invariants.Arrivals{}, err
	}
	result := invariants.Arrivals{Schema: "dur053.arrivals.v1", Offered: c.Count, Status: "IN_PROGRESS", Namespace: c.Namespace, RunID: c.RunID, Arm: c.Arm, Seed: c.Seed, Rate: c.Rate, Concurrency: c.Concurrency, CPUUnits: c.CPUUnits, Rows: make([]invariants.Arrival, c.Count)}
	for i := range result.Rows {
		activity := Activity(i, c.Arm)
		key := fmt.Sprintf("dur053-%s-%d-%06d", c.RunID, c.Seed, i)
		var input any = map[string]any{"run_id": c.RunID, "sequence": i}
		var expected any = input
		if activity == "pure.add" {
			input = []int{2, 3}
			expected = 5
		}
		if activity == "dur053.cpu" {
			digest := make([]byte, 32)
			for j := 0; j < c.CPUUnits; j++ {
				sum := sha256.Sum256(digest)
				digest = sum[:]
			}
			expected = map[string]any{"digest": hex.EncodeToString(digest), "work_units": c.CPUUnits}
		}
		body, _ := json.Marshal(map[string]any{"workflow_id": key, "namespace": c.Namespace, "submission_key": key, "payload": input, "definition_id": DefinitionID(c.RunID, activity), "definition_version": 1, "initial_node_id": activity, "initial_input": input, "actor_id": "dur053-loadgen"})
		want, _ := json.Marshal(expected)
		result.Rows[i] = invariants.Arrival{Sequence: i, Key: key, WorkflowID: key, Activity: activity, ScheduledNS: int64(Offset(i, c.Rate)), Outcome: "NOT_SENT", Response: json.RawMessage("null"), Request: body, Expected: want}
	}
	return result, nil
}

func TestB63PlanBytes(t *testing.T) {
	for _, arm := range []string{"cpu", "short", "medium", "long"} {
		for _, units := range []int{1, 17} {
			c := testConfig()
			c.Arm, c.Count, c.CPUUnits = arm, 12, units
			got, err := Plan(c)
			if err != nil {
				t.Fatal(err)
			}
			want, err := perRowReference(c)
			if err != nil {
				t.Fatal(err)
			}
			actual, _ := json.MarshalIndent(got, "", "  ")
			reference, _ := json.MarshalIndent(want, "", "  ")
			if !bytes.Equal(actual, reference) {
				t.Fatalf("B63 Plan bytes changed: %s/%d", arm, units)
			}
		}
	}
}

func TestB63DigestOncePerPlan(t *testing.T) {
	for _, count := range []int{1, 7} {
		for _, arm := range []string{"cpu", "short"} {
			c := testConfig()
			c.Count, c.Arm, c.CPUUnits = count, arm, 17
			calls := 0
			got, err := plan(c, func(units int) string {
				calls++
				if units != 17 {
					t.Fatal("B63 digest units changed")
				}
				return cpuExpectedDigest(units)
			})
			if err != nil {
				t.Fatal(err)
			}
			expectedCalls := 0
			if arm == "cpu" {
				expectedCalls = 1
			}
			if calls != expectedCalls {
				t.Fatalf("B63 per-row digest: got %d calls, want %d", calls, expectedCalls)
			}
			public, _ := Plan(c)
			a, _ := json.Marshal(got)
			b, _ := json.Marshal(public)
			if !bytes.Equal(a, b) {
				t.Fatal("B63 Plan bypassed counted implementation")
			}
		}
	}
}
