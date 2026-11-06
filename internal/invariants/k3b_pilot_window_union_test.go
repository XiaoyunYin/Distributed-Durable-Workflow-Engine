package invariants

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

func TestK3bPilotWindows(t *testing.T) {
	root := "../../experiments/portfolio/k8s-recovery/k3-smoke-v1/42031189823/"
	var trace Trace
	var arrivals Arrivals
	var witness WitnessExport
	for file, target := range map[string]any{"kind-snapshot.json": &trace, "kind-arrivals.json": &arrivals, "kind-witness-export.json": &witness} {
		raw, err := os.ReadFile(root + file)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, target); err != nil {
			t.Fatal(err)
		}
	}
	makeWindows := func() Arrivals {
		result := arrivals
		result.Rows = append([]Arrival(nil), arrivals.Rows...)
		for i := range result.Rows {
			seq := i % 5
			ns := int64(seq) * int64(time.Second)
			finish := ns + 1
			result.Rows[i].Sequence = seq
			result.Rows[i].ScheduledNS = ns
			result.Rows[i].SendNS = &ns
			result.Rows[i].FinishedNS = &finish
			result.Rows[i].LatenessNS = 0
		}
		for i := 0; i < 2; i++ {
			window := result
			window.Windows = nil
			window.RunID = []string{"warmup", "measured"}[i]
			window.Rows = append([]Arrival(nil), result.Rows[i*5:(i+1)*5]...)
			window.Offered = 5
			result.Windows = append(result.Windows, window)
		}
		return result
	}
	valid := makeWindows()
	if r := CheckCampaign(trace, valid, witness); !r.Valid {
		t.Fatal(r.Violations)
	}
	tests := []struct {
		name, message string
		change        func(*Arrivals)
	}{
		{"metadata", "invalid campaign window metadata", func(a *Arrivals) { a.Windows[1].Namespace = "other" }},
		{"nested", "invalid campaign window metadata", func(a *Arrivals) { a.Windows[1].Windows = []Arrivals{arrivals} }},
		{"identity", "invalid campaign window metadata", func(a *Arrivals) { a.Windows[1].RunID = "warmup" }},
		{"bytes", "window bytes differ", func(a *Arrivals) { a.Windows[1].Rows[0].Activity = "fabricated" }},
		{"missing", "windows do not cover", func(a *Arrivals) { a.Windows = a.Windows[:1] }},
		{"extra", "windows exceed", func(a *Arrivals) { a.Rows = a.Rows[:9]; a.Offered = 9 }},
		{"sequence", "arrival identity", func(a *Arrivals) { a.Rows[5].Sequence = 5; a.Windows[1].Rows[0].Sequence = 5 }},
		{"rate", "schedule/send/response", func(a *Arrivals) { a.Windows[1].Rate = 2 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			a := makeWindows()
			test.change(&a)
			r := CheckCampaign(trace, a, witness)
			if !strings.Contains(strings.Join(r.Violations, "\n"), test.message) {
				t.Fatalf("pilot window guard bypassed: %s: %v", test.name, r.Violations)
			}
		})
	}
}
