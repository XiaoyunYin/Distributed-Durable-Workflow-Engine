package k6v1

import (
	"slices"
	"testing"
	"time"
)

func TestControlRequiresKafkaGenerationUnset(t *testing.T) {
	const name = "RUNTIME_KAFKA_GENERATION"
	for _, pod := range []ControlPod{
		{EnvNames: []string{name}},
		{EnvNames: []string{name}, LegacyEnv: map[string]string{name: ""}},
		{EnvNames: []string{name}, LegacyEnv: map[string]string{name: "j7-restore"}},
		{LegacyEnv: map[string]string{name: ""}},
		{EnvFrom: true},
	} {
		if !(ControlSnapshot{Pods: []ControlPod{pod}}).faultConfigured() {
			t.Fatalf("K6 accepted a configured or opaque Kafka generation: %+v", pod)
		}
	}
	if (ControlSnapshot{Pods: []ControlPod{{}}}).faultConfigured() {
		t.Fatal("unset generation must preserve K6 control admission")
	}
}

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
	armedEnv := map[string]string{
		"ArmedHook":          "DUR053_K6_HOOK_URL",
		"ArmedClock":         "DUR053_K6_CLOCK",
		"ArmedPartition":     "DUR053_K6_PARTITION_CELL",
		"ArmedLeaseTakeover": "DUR053_K6_LEASE_TAKEOVER_ROLE",
		"ArmedRelayStall":    "DUR053_K6_RELAY_STALL",
		"ArmedAcquireHold":   "RUNTIME_SCHEDULER_HOLD_AFTER_ACQUIRE_MS",
		"ArmedReleaseMarker": "DUR053_F1A_RELEASE_MARKER",
		"ArmedActivityDelay": "DUR048_ACTIVITY_DELAY_MS",
		"ArmedShutdownMode":  "RUNTIME_SHUTDOWN_MODE",
		"ArmedK4Hold":        "DUR053_K4_HOLD_FILE",
		"UnknownEnv":         "FUTURE_FAULT",
		"UnknownK6Env":       "DUR053_K6_FUTURE_ACTUATOR",
	}
	for _, tc := range []struct {
		name   string
		status string
		reason string
	}{
		{"Clean", "confirmed", "cell_predicate_satisfied"},
		{"LegacyTimeoutRetry", "confirmed", "cell_predicate_satisfied"},
		{"EnabledTimeoutRetry", "missing", "required_proof_missing:control_fault_inventory"},
		{"UnprovenTimeoutRetry", "missing", "required_proof_missing:control_fault_inventory"},
		{"EmptyTimeoutRetry", "missing", "required_proof_missing:control_fault_inventory"},
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
		{"ArmedClock", "missing", "required_proof_missing:control_fault_inventory"},
		{"ArmedPartition", "missing", "required_proof_missing:control_fault_inventory"},
		{"ArmedLeaseTakeover", "missing", "required_proof_missing:control_fault_inventory"},
		{"ArmedRelayStall", "missing", "required_proof_missing:control_fault_inventory"},
		{"ArmedAcquireHold", "missing", "required_proof_missing:control_fault_inventory"},
		{"ArmedReleaseMarker", "missing", "required_proof_missing:control_fault_inventory"},
		{"ArmedActivityDelay", "missing", "required_proof_missing:control_fault_inventory"},
		{"ArmedShutdownMode", "missing", "required_proof_missing:control_fault_inventory"},
		{"ArmedK4Hold", "missing", "required_proof_missing:control_fault_inventory"},
		{"UnknownEnv", "missing", "required_proof_missing:control_fault_inventory"},
		{"UnknownK6Env", "missing", "required_proof_missing:control_fault_inventory"},
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
				env := []string{"DUR053_K6_CAPTURE", "DUR053_K6_ACTIVITY", "POD_UID"}
				if role == "scheduler" && armedEnv[tc.name] != "" {
					env = append(env, armedEnv[tc.name])
				}
				pods = append(pods, ControlPod{Role: role, Runtime: id, EnvNames: env})
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
			case "LegacyTimeoutRetry", "EnabledTimeoutRetry", "UnprovenTimeoutRetry", "EmptyTimeoutRetry":
				const setting = "RUNTIME_TIMEOUT_RETRY_POLICY"
				c.After.Pods[2].EnvNames = append(slices.Clone(c.After.Pods[2].EnvNames), setting)
				if tc.name != "UnprovenTimeoutRetry" {
					value := map[string]string{"LegacyTimeoutRetry": "0", "EnabledTimeoutRetry": "1"}[tc.name]
					c.After.Pods[2].LegacyEnv = map[string]string{setting: value}
				}
				if tc.name == "LegacyTimeoutRetry" {
					c.Before.Pods[2] = c.After.Pods[2]
				}
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
