package k6v1

import (
	"testing"
	"time"
)

func shutdownFixture(abort observedWorker) External {
	s := abort.session
	runtime := s.Runtime()
	w := abort.row.Data
	signal := Record[WatchRow]{Position: Position{1, UTC(fixed)}, Data: WatchRow{
		Kind: "termination_requested", Runtime: &runtime, DeletionUTC: UTC(fixed.Add(30 * time.Second)),
	}}
	finished := UTC(fixed.Add(time.Second))
	exit := Record[WatchRow]{Position: Position{2, UTC(fixed.Add(2 * time.Second))}, Data: WatchRow{
		Kind: "exit", Runtime: &runtime, Exit: &RuntimeExit{finished, 143, 15},
	}}
	binding := Record[WatchRow]{Position: Position{3, exit.Position.UTC}, Data: WatchRow{
		Kind: "shutdown", Session: &s, Shutdown: &ShutdownBinding{
			Invocation: w.Invocation, Attempt: *w.Attempt, StartSequence: 2, SignalSequence: 1, AbortSequence: 3, ExitSequence: 2,
		},
	}}
	start := Record[WorkerRow]{Position: Position{2, UTC(fixed.Add(-time.Second))}, Data: w}
	start.Data.Kind = "start"
	return External{Roster: Stream[WatchRow]{Rows: []Record[WatchRow]{signal, exit, binding}},
		Sessions: map[string]Stream[WorkerRow]{s.ID: {Rows: []Record[WorkerRow]{{}, start, abort.row}}}}
}

func TestK6ShutdownBinding(t *testing.T) {
	for _, name := range []string{"Matched", "ChangedInvocation", "ChangedClaim", "ChangedTokenHash", "ChangedRuntime", "ChangedSignalRuntime",
		"LateSignal", "SignalAtAbort", "ExitBeforeAbort", "ObservedExitBeforeAbort", "LaterStart", "LaterRuntimeSessionStart", "UnboundExit",
		"WrongStart", "WrongAbort", "SignalAfterExitInRoster"} {
		t.Run(name, func(t *testing.T) {
			abort := observedWorker{session: WorkerSession{ID: "session", Claimant: "worker"}, startSequence: 2, row: Record[WorkerRow]{
				Position: Position{3, UTC(fixed.Add(time.Second))}, Data: WorkerRow{Kind: "abort", Invocation: "invocation",
					Attempt: &AttemptIdentity{Workflow: "wf", Node: "node", Attempt: 2, ClaimHash: Hash([]byte("claim")), RequestID: "req"}},
			}}
			s := shutdownFixture(abort)
			signal, exit, binding := &s.Roster.Rows[0], &s.Roster.Rows[1], s.Roster.Rows[2].Data.Shutdown
			ledger := s.Sessions[abort.session.ID]
			switch name {
			case "ChangedInvocation":
				binding.Invocation = "other"
				ledger.Rows[1].Data.Invocation = binding.Invocation
			case "ChangedClaim", "ChangedTokenHash":
				if name == "ChangedClaim" {
					binding.Attempt.RequestID = "other"
				} else {
					binding.Attempt.ClaimHash = Hash([]byte("other"))
				}
				ledger.Rows[1].Data.Attempt = &binding.Attempt
			case "ChangedRuntime", "ChangedSignalRuntime":
				runtime := abort.session.Runtime()
				runtime.ContainerID = "other"
				if name == "ChangedRuntime" {
					exit.Data.Runtime = &runtime
				} else {
					signal.Data.Runtime = &runtime
				}
			case "LateSignal":
				signal.Position.UTC = UTC(fixed.Add(time.Second + time.Millisecond))
			case "SignalAtAbort":
				signal.Position.UTC = abort.row.Position.UTC
			case "ExitBeforeAbort":
				exit.Data.Exit.FinishedUTC = UTC(fixed)
			case "ObservedExitBeforeAbort":
				exit.Position.UTC = UTC(fixed.Add(time.Second - time.Millisecond))
				exit.Data.Exit.FinishedUTC = UTC(fixed)
				// Same second in the exit status and the abort. The
				// observer can order them more precisely than finishedAt.
				ledger.Rows[2].Position.UTC = UTC(fixed.Add(time.Second - time.Microsecond))
			case "LaterStart", "LaterRuntimeSessionStart":
				later := Record[WorkerRow]{Position: Position{4, abort.row.Position.UTC}, Data: WorkerRow{Kind: "start"}}
				if name == "LaterStart" {
					ledger.Rows = append(ledger.Rows, later)
					s.Sessions[abort.session.ID] = ledger
				} else {
					other := abort.session
					other.ID = "other"
					s.Sessions[other.ID] = Stream[WorkerRow]{Rows: []Record[WorkerRow]{{Data: WorkerRow{Session: &other}}, later}}
				}
			case "UnboundExit":
				s.Roster.Rows = s.Roster.Rows[:2]
			case "WrongStart":
				abort.startSequence = 1
			case "WrongAbort":
				binding.AbortSequence = 2
			case "SignalAfterExitInRoster":
				s.Roster.Rows[0], s.Roster.Rows[1] = *exit, *signal
				binding.SignalSequence, binding.ExitSequence = 2, 1
			}
			if s.ShutdownWitnessed(abort.session, abort.row.Position.Sequence, abort.startSequence) != (name == "Matched") {
				t.Fatalf("incorrect shutdown binding for %s", name)
			}
		})
	}
}
