package k6v1

import (
	"fmt"
	"testing"
	"time"
)

func medium3Partition(s StaleObservation) *StalePartition {
	at := func(seconds int64) int64 { return s.Call.AtNS + (seconds-24)*int64(time.Second) }
	stamp := func(seconds int64) string { return UTC(time.Unix(0, at(seconds))) }
	const address = "192.0.2.1"
	timeout, success := 124, 0
	counters := func(packets, bytes int) string {
		return fmt.Sprintf("[0:0] -A CHAOS-OUTPUT -j drop-chain\n[%d:%d] -A drop-chain -m set --match-set dst-set dst,dst -j DROP\n", packets, bytes)
	}
	return &StalePartition{
		Pod: "scheduler", PodUID: s.Before.PodUID, ContainerID: s.Before.ContainerID, HostPID: 44, Address: address,
		Lock:     &StaleLockProbe{at(4) + 1, at(5), 1, "00000", "00000"},
		IssuedNS: at(6), ConfirmedNS: at(12), AbsentNS: at(20) + 1, KernelGoneNS: at(20) + 2,
		Controller: []byte(`{"apiVersion":"chaos-mesh.org/v1alpha1","kind":"NetworkChaos",
			"metadata":{"namespace":"ns","name":"cut","uid":"cr"},
			"spec":{"action":"partition","direction":"to","mode":"one","selector":{"pods":{"ns":["scheduler"]}},
				"externalTargets":["192.0.2.1"]},
			"status":{"conditions":[{"type":"AllInjected","status":"True"}],
				"experiment":{"containerRecords":[{"id":"ns/scheduler","selectorKey":".","phase":"Injected"}]}}}`),
		Daemon: []byte(`{"metadata":{"namespace":"ns","name":"scheduler"},
			"spec":{"iptables":[{"name":"drop-chain","source":"ns/cut","direction":"output","ipsets":["dst-set"]}]}}`),
		RulesBefore: counters(0, 0), RulesDuring: counters(1, 60), RulesHeld: counters(2, 120),
		RulesAfter: "*filter\nCOMMIT\n",
		Sets:       "add dst-set net-set\nadd dst-set ports-set\nadd net-set 192.0.2.1/32\n",
		Failed:     &StaleRoute{at(8), at(9), address, 5432, 44, &timeout},
		Restored:   &StaleRoute{at(20) + 3, at(20) + 4, address, 5432, 44, &success},
		Execution: &StaleExecution{
			StaleBackend: StaleBackend{5, 100, "dur053-e2-" + s.Before.Session, "active", true},
			DBTime:       UTC(time.Unix(0, at(23)+3)), QueryStart: stamp(23),
			Query:    "SELECT owner_id::text, epoch, lease_expires_at, clock_timestamp() FROM engine.partition_leases WHERE partition_id = $1 FOR UPDATE",
			WaitType: "Lock", BlockedBy: []int64{17}, ObserverPID: 17, StartNS: at(23) + 1, EndNS: at(23) + 4,
			RolledBack: "00000",
		},
	}
}

func TestK6Medium3Challenge(t *testing.T) {
	for _, tc := range []struct {
		name, code string
		change     func(*StaleObservation)
	}{
		{name: "Clean"},
		{"N5Harness", "STALE_CHALLENGE_ORIGIN", func(s *StaleObservation) { s.Call.PID++ }},
		{"WrongPartitionTarget", "STALE_CHALLENGE_ORIGIN", func(s *StaleObservation) { s.Partition.PodUID = "other" }},
		{"WrongOrder", "STALE_CHALLENGE_ORDER", func(s *StaleObservation) { s.Call.AtNS = 1 }},
		{"WrongInjectionOrder", "STALE_CHALLENGE_ORDER", func(s *StaleObservation) { s.Partition.ConfirmedNS = s.Takeover.EndNS }},
		{"LateRestoration", "STALE_CHALLENGE_ORDER", func(s *StaleObservation) {
			s.Partition.Restored.EndNS = s.Call.AtNS - (3 * time.Second).Nanoseconds()
		}},
		{"EarlyExecution", "STALE_CHALLENGE_ORDER", func(s *StaleObservation) { s.Partition.Execution.StartNS = s.Partition.Restored.EndNS }},
		{"MissingLockProbe", "required_proof_missing:stale_challenge_lock_probe", func(s *StaleObservation) { s.Partition.Lock = nil }},
		{"WrongProbeEpoch", "required_proof_missing:stale_challenge_lock_probe", func(s *StaleObservation) { s.Partition.Lock.Epoch++ }},
		{"LockNotAcquired", "required_proof_missing:stale_challenge_lock_probe", func(s *StaleObservation) { s.Partition.Lock.SQLState = "55P03" }},
		{"LockNotRolledBack", "required_proof_missing:stale_challenge_lock_probe", func(s *StaleObservation) { s.Partition.Lock.Rollback = "25P02" }},
		{"MissingController", "required_proof_missing:stale_challenge_partition", func(s *StaleObservation) { s.Partition.Controller = nil }},
		{"MissingDaemon", "required_proof_missing:stale_challenge_partition", func(s *StaleObservation) { s.Partition.Daemon = nil }},
		{"WrongDestination", "required_proof_missing:stale_challenge_partition", func(s *StaleObservation) {
			s.Partition.Sets = "add dst-set net-set\nadd dst-set ports-set\nadd net-set 192.0.2.2/32\n"
		}},
		{"NoRisingCounters", "required_proof_missing:stale_challenge_partition", func(s *StaleObservation) {
			s.Partition.RulesDuring = s.Partition.RulesBefore
		}},
		{"MissingFailedRoute", "required_proof_missing:stale_challenge_partition", func(s *StaleObservation) { s.Partition.Failed = nil }},
		{"FailedRouteConnects", "required_proof_missing:stale_challenge_partition", func(s *StaleObservation) { *s.Partition.Failed.Exit = 0 }},
		{"RuleStillPresent", "required_proof_missing:stale_challenge_route_restoration", func(s *StaleObservation) {
			s.Partition.RulesAfter = s.Partition.RulesDuring
		}},
		{"MissingRestoration", "required_proof_missing:stale_challenge_route_restoration", func(s *StaleObservation) { s.Partition.Restored = nil }},
		{"RoutingStillBlocked", "required_proof_missing:stale_challenge_route_restoration", func(s *StaleObservation) { *s.Partition.Restored.Exit = 124 }},
		{"MissingExecution", "required_proof_missing:stale_challenge_backend_execution", func(s *StaleObservation) { s.Partition.Execution = nil }},
		{"IdleBackend", "required_proof_missing:stale_challenge_backend_execution", func(s *StaleObservation) { s.Partition.Execution.State = "idle" }},
		{"WrongBackend", "required_proof_missing:stale_challenge_backend_execution", func(s *StaleObservation) { s.Partition.Execution.StartUS++ }},
		{"WrongQuery", "required_proof_missing:stale_challenge_backend_execution", func(s *StaleObservation) {
			s.Partition.Execution.Query = "SELECT 1"
		}},
		{"UnrelatedBlocker", "required_proof_missing:stale_challenge_backend_execution", func(s *StaleObservation) {
			s.Partition.Execution.BlockedBy = []int64{18}
		}},
		{"ObserverDidNotRollback", "required_proof_missing:stale_challenge_backend_execution", func(s *StaleObservation) {
			s.Partition.Execution.RolledBack = ""
		}},
		{"MissingACK", "required_proof_missing:stale_challenge_ack", func(s *StaleObservation) { s.Journal = "" }},
		{"MissingProgress", "required_proof_missing:stale_challenge_successor_progress", func(s *StaleObservation) { s.Progress = "" }},
		{"Committed", "STALE_CHALLENGE_COMMITTED", func(s *StaleObservation) { s.Rejected.Attempts++ }},
		{"UncorroboratedCommit", "required_proof_missing:stale_challenge_rejection", func(s *StaleObservation) { s.Call.Outcome = "COMMITTED" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, plan, arrivals, s, e := staleFixtureFor(t, "medium-3")
			s.Partition = medium3Partition(s)
			if tc.change != nil {
				tc.change(&s)
			}
			e.checkStaleChallenge(m, plan, arrivals, nil, &s)
			result := e.result
			if tc.code == "" {
				if result.Confirmation.Status != "confirmed" || result.Labels[2].Outcome != "covered/pass" || len(result.Findings) != 0 {
					t.Fatalf("medium-3 verdict: %+v %+v %+v", result.Confirmation, result.Labels[2], result.Findings)
				}
			} else if result.Confirmation.Reason != tc.code || len(result.Findings) != 1 || result.Findings[0].Code != tc.code {
				t.Fatalf("medium-3 verdict: got %+v %+v, want %s", result.Confirmation, result.Findings, tc.code)
			}
		})
	}
}
