package invariants

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

func lifecycleFixture() (LifecycleTimeline, LifecycleLeaseAudit) {
	end := 4.0
	death := 5.0
	i := LifecycleIdentity{Namespace: "ns", Workflow: "w", Node: "n", Attempt: 1, TokenHash: strings.Repeat("a", 64)}
	timeline := LifecycleTimeline{Schema: "dur053.k4-timeline.v1", Episode: "e", Namespace: "ns",
		Sessions:    []LifecycleSession{{Session: "s", PodUID: "uid", ContainerID: "container", Role: "worker", Start: 0, LastAlive: 4, Death: &death, DeathEvidence: "independent-watch", Signals: []LifecycleSignal{{At: 3, Signal: 15, Evidence: "signal-delivery"}}}},
		Claims:      []LifecycleClaim{{Identity: i, Session: "s", Order: 1, At: 1}},
		Invocations: []LifecycleInvocation{{ID: "v", Identity: i, Session: "s", Start: 2, Finish: &end, LastAlive: 3, Outcome: "success"}},
		History:     []LifecycleHistory{{Identity: i, Order: 4, At: 4, Kind: "receipt", Receipt: "r", Revision: 1}},
		Receipts:    []LifecycleReceipt{{ID: "r", Identity: i, Session: "s", Order: 4, At: 4, Category: "committed", Outcome: "success"}},
		Workflows:   []LifecycleWorkflow{{Workflow: "w", State: "SUCCEEDED", Terminal: &end}},
	}
	lifecycleSeal(&timeline)
	return timeline, LifecycleLeaseAudit{Schema: "dur053.k4-lease-audit.v1", Episode: "e", Rows: []LifecycleLease{}}
}
func lifecycleSeal(t *LifecycleTimeline) {
	t.Witness = nil
	for _, s := range t.Sessions {
		data := ""
		ack := []string{}
		for _, v := range t.Invocations {
			if v.Session != s.Session {
				continue
			}
			ack = append(ack, v.ID)
			for _, kind := range []string{"start", "finish"} {
				at := v.Start
				if kind == "finish" {
					if v.Finish == nil {
						continue
					}
					at = *v.Finish
				}
				b, _ := json.Marshal(map[string]any{"event": kind, "id": v.ID, "identity": v.Identity, "at": at})
				data += string(b) + "\n"
			}
		}
		hash := sha256.Sum256([]byte(data))
		t.Witness = append(t.Witness, LifecycleWitness{Session: s.Session, PodUID: s.PodUID, ContainerID: s.ContainerID, Data: data, SHA256: hex.EncodeToString(hash[:]), Acknowledged: ack, Complete: true})
	}
}
func TestLifecycleFixtures(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*LifecycleTimeline, *LifecycleLeaseAudit)
		valid   bool
		message string
	}{
		{"clean drain", func(*LifecycleTimeline, *LifecycleLeaseAudit) {}, true, "clean drain rejected"},
		{"confirmed kill mid-activity", func(x *LifecycleTimeline, _ *LifecycleLeaseAudit) {
			x.Invocations[0].Finish = nil
			x.Receipts = nil
			x.History = nil
			x.Workflows = nil
			lifecycleSeal(x)
		}, true, "confirmed kill rejected"},
		{"unknown interval", func(x *LifecycleTimeline, _ *LifecycleLeaseAudit) {
			x.Invocations[0].Finish = nil
			x.Sessions[0].Death = nil
			lifecycleSeal(x)
		}, false, "unknown interval accepted"},
		{"late stale rejection", func(x *LifecycleTimeline, _ *LifecycleLeaseAudit) {
			i := x.Claims[0].Identity
			x.History = append(x.History, LifecycleHistory{Identity: i, Order: 5, At: 5, Kind: "supersede"})
			x.Receipts = append(x.Receipts, LifecycleReceipt{ID: "late", Identity: i, Session: "s", Order: 6, At: 6, Category: "rejected", Rejection: "stale_attempt"})
		}, true, "typed stale rejection counted as committed"},
		{"stale committed", func(x *LifecycleTimeline, _ *LifecycleLeaseAudit) {
			x.History = append(x.History, LifecycleHistory{Identity: x.Claims[0].Identity, Order: 3, At: 3, Kind: "supersede"})
		}, false, "stale committed receipt accepted"},
		{"cross-workflow supersession", func(x *LifecycleTimeline, a *LifecycleLeaseAudit) {
			x.History[0].Owner = "old"
			x.History[0].Epoch = 1
			x.History[0].Partition = 7
			a.Rows = []LifecycleLease{{Order: 1, At: 1, Kind: "acquire", Partition: 7, Owner: "old", Epoch: 1, Session: "s", Transaction: "a"}, {Order: 3, At: 3, Kind: "acquire", Partition: 7, Owner: "new-workflow-owner", Epoch: 2, Session: "s", Transaction: "b"}}
		}, false, "cross-workflow superseded transition accepted"},
		{"missing dead session", func(x *LifecycleTimeline, _ *LifecycleLeaseAudit) {
			death := 2.0
			x.Sessions = append(x.Sessions, LifecycleSession{Session: "dead", PodUID: "dead-uid", ContainerID: "dead-container", Start: 0, LastAlive: 1, Death: &death, DeathEvidence: "independent-watch"})
		}, false, "missing dead session accepted"},
		{"missing acknowledged start", func(x *LifecycleTimeline, _ *LifecycleLeaseAudit) {
			x.Witness[0].Acknowledged = append(x.Witness[0].Acknowledged, "missing")
		}, false, "missing acknowledged start accepted"},
		{"forged inventory", func(x *LifecycleTimeline, _ *LifecycleLeaseAudit) { x.Witness[0].SHA256 = strings.Repeat("0", 64) }, false, "forged inventory accepted"},
		{"claim without execution", func(x *LifecycleTimeline, _ *LifecycleLeaseAudit) {
			i := x.Claims[0].Identity
			i.Attempt = 2
			x.Claims = append(x.Claims, LifecycleClaim{Identity: i, Session: "s", Order: 6, At: 5})
		}, true, "claim without execution rejected"},
		{"evidence only", func(x *LifecycleTimeline, _ *LifecycleLeaseAudit) {
			x.Receipts = append(x.Receipts, LifecycleReceipt{ID: "evidence", Identity: x.Claims[0].Identity, Session: "s", Order: 6, At: 6, Category: "evidence_only"})
		}, true, "evidence-only counted as committed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			x, a := lifecycleFixture()
			tc.mutate(&x, &a)
			r := CheckLifecycle(x, a)
			if r.Valid != tc.valid {
				t.Fatalf("%s: %+v", tc.message, r)
			}
			if tc.name == "claim without execution" && r.ClaimedUnexecuted != 1 {
				t.Fatal("claim without start counted as execution")
			}
			if tc.name == "confirmed kill mid-activity" && (r.Intervals[0].Lower != 1 || r.Intervals[0].Upper == nil || *r.Intervals[0].Upper != 3) {
				t.Fatal("kill interval bounds incorrect")
			}
		})
	}
}
func TestLifecycleStrictDecode(t *testing.T) {
	x, _ := lifecycleFixture()
	b, _ := json.Marshal(x)
	// Go fixture uses nil optional arrays; wire contracts require explicit arrays.
	var obj map[string]any
	_ = json.Unmarshal(b, &obj)
	for _, change := range []func(map[string]any){func(m map[string]any) { delete(m, "claims") }, func(m map[string]any) { m["unknown"] = 1 }} {
		change(obj)
		raw, _ := json.Marshal(obj)
		var target LifecycleTimeline
		if DecodeLifecycle(raw, &target) == nil {
			t.Fatal("unknown or missing lifecycle field accepted")
		}
	}
}

func TestLifecycleLeaseOwnership(t *testing.T) {
	x, a := lifecycleFixture()
	x.History[0].Owner, x.History[0].Epoch = "owner", 1
	if r := CheckLifecycle(x, a); r.Complete || r.Valid {
		t.Fatalf("absent acquisition accepted: %+v", r)
	}
	x.History[0].Epoch = 2
	a.Rows = []LifecycleLease{
		{Order: 1, Kind: "acquire", Owner: "owner", Epoch: 2, Session: "s", Transaction: "earlier"},
		{Order: 2, Kind: "acquire", Owner: "owner", Epoch: 1, Session: "s", Transaction: "newest"},
	}
	if r := CheckLifecycle(x, a); r.Complete || r.Valid {
		t.Fatalf("epoch above newest acquisition accepted: %+v", r)
	}
	for _, tc := range []struct {
		name       string
		reacquire  bool
		takeover   bool
		epoch      int64
		owner      string
		complete   bool
		superseded int
	}{
		{"own release", false, false, 1, "owner", true, 1},
		{"release then reacquire", true, false, 1, "owner", true, 0},
		{"release with takeover", false, true, 1, "owner", true, 1},
		{"future epoch", false, false, 2, "owner", false, 0},
		{"unknown owner", false, false, 1, "unknown", false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x, a := lifecycleFixture()
			x.History[0].Owner, x.History[0].Epoch = tc.owner, tc.epoch
			a.Rows = []LifecycleLease{
				{Order: 1, Kind: "acquire", Owner: "owner", Epoch: 1, Session: "s", Transaction: "acquire"},
				{Order: 2, Kind: "release", Owner: "owner", Epoch: 1, Session: "s", Transaction: "release"},
			}
			if tc.reacquire {
				a.Rows = append(a.Rows, LifecycleLease{Order: 3, Kind: "acquire", Owner: "owner", Epoch: 1, Session: "s", Transaction: "reacquire"})
			}
			if tc.takeover {
				a.Rows = append(a.Rows, LifecycleLease{Order: 3, Kind: "acquire", Owner: "new-owner", Epoch: 2, Session: "s", Transaction: "takeover"})
			}
			r := CheckLifecycle(x, a)
			if r.Complete != tc.complete || r.SupersededTransitions != tc.superseded || r.Valid != (tc.complete && tc.superseded == 0) {
				t.Fatalf("lease ownership check bypassed: %+v", r)
			}
		})
	}
}
