package k6v1

import (
	"slices"
	"testing"
	"time"
)

func TestK6ConfirmationSourceBinding(t *testing.T) {
	at := UTC(time.Unix(1, 0))
	snapshot := ControlSnapshot{BeforeUTC: at, AfterUTC: at, CRDList: "1", PodList: "2",
		Pods: []ControlPod{}, ChaosCRDs: []string{}}
	m := Manifest{Episode: "medium-16-0", Namespace: "namespace", ResetID: "reset", BarrierID: "barrier"}
	observation := ControlObservation{Schema: ControlVersion, Binding: Binding(m), Before: snapshot, After: snapshot}
	if !observation.valid(m) {
		t.Fatal("bound control source rejected")
	}
	observation.Binding.ResetID = "another-reset"
	if observation.valid(m) {
		t.Fatal("foreign control source accepted")
	}
	observation.Binding = Binding(m)
	observation.After.CRDList = ""
	if observation.valid(m) {
		t.Fatal("incomplete control snapshot accepted")
	}
}

func TestK6ConfirmationControl(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status string
		reason string
	}{
		{"Clean", "confirmed", "cell_predicate_satisfied"},
		{"Idle", "pending", "no_positive_population"},
		{"UnprovenProgress", "pending", "no_positive_population"},
		{"FaultCell", "pending", "cell_not_exercised"},
		{"StaleChallengeMissing", "missing", "required_proof_missing:DeployedStaleChallenge"},
		{"SignedSkewMissing", "missing", "required_proof_missing:SignedProcessSkew"},
		{"MissingSource", "missing", "required_proof_missing:" + ControlPath},
		{"RequiredSourceDrop", "missing", "required_proof_missing:" + ControlPath},
		{"MissingQuiet", "missing", "required_proof_missing:control_window"},
		{"LateOpening", "missing", "required_proof_missing:control_window"},
		{"StartOutsideWindow", "missing", "required_proof_missing:control_window"},
		{"WrongUID", "missing", "required_proof_missing:control_runtime"},
		{"MissingRole", "missing", "required_proof_missing:control_runtime"},
		{"UnlistedWorker", "missing", "required_proof_missing:control_runtime"},
		{"ChaosAPI", "missing", "required_proof_missing:control_fault_inventory"},
		{"ArmedHook", "missing", "required_proof_missing:control_fault_inventory"},
		{"OpaqueEnv", "missing", "required_proof_missing:control_fault_inventory"},
		{"InjectedControl", "failed", "CONTROL_PLAN_MISMATCH"},
		{"RemovalInControl", "failed", "CONTROL_PLAN_MISMATCH"},
		{"RuntimeChanged", "failed", "CONTROL_RUNTIME_CHANGED"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			at := func(n int) string { return UTC(time.Unix(int64(n), 0)) }
			m := Manifest{Cell: "medium-16", Workload: "PURE"}
			plan := Plan{FaultMode: "control", SealedUTC: at(1)}
			arrivals := Arrivals{SealedUTC: at(5), Rows: []Arrival{
				{Attempts: []Send{{SentUTC: at(3), ObservedUTC: at(4)}}},
			}}
			session := WorkerSession{PodUID: "worker", ContainerID: "worker", ContainerStartedUTC: at(1)}
			runtime := session.Runtime()
			roster := Stream[WatchRow]{Rows: []Record[WatchRow]{
				{Position: Position{UTC: at(1)}, Data: WatchRow{Kind: "runtime", Runtime: &runtime}},
				{Position: Position{UTC: at(1)}, Data: WatchRow{Kind: "session", Session: &session}},
			}}
			pods := []ControlPod{}
			for _, role := range []string{"api", "relay", "scheduler", "worker", "worker-control"} {
				id := RuntimeIdentity{PodUID: role, ContainerID: role, StartedUTC: at(1)}
				pods = append(pods, ControlPod{Role: role, Runtime: id, EnvNames: []string{}})
			}
			before := ControlSnapshot{BeforeUTC: at(2), AfterUTC: at(2), CRDList: "1", PodList: "2",
				Pods: pods, ChaosCRDs: []string{}}
			after := before
			after.BeforeUTC, after.AfterUTC, after.PodList = at(6), at(7), "3"
			after.Pods = slices.Clone(pods)
			c := ControlObservation{Schema: ControlVersion, Binding: Binding(m), Before: before, After: after}
			ledger := Stream[WorkerRow]{Rows: []Record[WorkerRow]{
				{Position: Position{UTC: at(4)}, Data: WorkerRow{Kind: "start"}},
			}}
			e := External{Control: &c, QuietStartUTC: at(8), Roster: roster, Sessions: map[string]Stream[WorkerRow]{"worker": ledger}}
			r := newResult(m, "")
			r.Namespace = &NamespaceCounts{TerminalExecutions: 1}
			for i := range r.Labels {
				if r.Labels[i].Registered {
					r.Labels[i].Outcome = "covered/pass"
				}
			}
			var issues []Finding
			switch tc.name {
			case "Idle":
				r.Namespace.TerminalExecutions = 0
			case "UnprovenProgress":
				r.Labels[10].Outcome = "NOT COVERED"
			case "FaultCell":
				m.Cell = "hook-0"
			case "StaleChallengeMissing":
				m.Cell, plan.FaultMode = "hook-5", "injected"
			case "SignedSkewMissing":
				m.Cell, plan.FaultMode = "medium-11", "injected"
			case "MissingSource":
				e.Control = nil
			case "RequiredSourceDrop":
				e.Control = nil
				issues = []Finding{*missing(ControlPath)}
				r.failIntegrity(&issues[0])
			case "MissingQuiet":
				e.QuietStartUTC = ""
			case "LateOpening":
				c.Before.AfterUTC = at(4)
			case "StartOutsideWindow":
				ledger.Rows[0].Position.UTC = at(1)
			case "WrongUID":
				session.PodUID = "wrong"
			case "MissingRole":
				c.Before.Pods, c.After.Pods = pods[1:], pods[1:]
			case "UnlistedWorker":
				other := runtime
				other.PodUID = "other-worker"
				e.Roster.Rows = append(e.Roster.Rows, Record[WatchRow]{
					Position: Position{UTC: at(1)}, Data: WatchRow{Kind: "runtime", Runtime: &other},
				})
			case "ChaosAPI":
				c.After.ChaosCRDs = []string{"podchaos.chaos-mesh.org"}
			case "ArmedHook":
				c.After.Pods[0].EnvNames = []string{"DUR053_K6_HOOK_URL"}
			case "OpaqueEnv":
				c.After.Pods[0].EnvFrom = true
			case "InjectedControl":
				plan.FaultMode = "injected"
			case "RemovalInControl":
				removed := at(5)
				arrivals.RemovalUTC = &removed
			case "RuntimeChanged":
				c.After.Pods[0].Runtime.ContainerID = "other"
			}
			r.checkConfirmation(m, plan, arrivals, issues, e)
			r.finish()
			want := Confirmation{tc.status, tc.reason}
			if r.Confirmation != want {
				t.Fatalf("control verdict: got %+v, want %+v", r.Confirmation, want)
			}
			if (tc.status == "missing" || tc.status == "failed") && (len(r.Findings) != 1 || r.ExitCode() != Fail) {
				t.Fatalf("expected single typed failure: %+v", r.Findings)
			}
			if tc.status == "confirmed" && r.ExitCode() != Pass {
				t.Fatalf("confirmed control did not pass: %+v", r)
			}
		})
	}
}
