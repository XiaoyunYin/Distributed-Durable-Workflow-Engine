package k6v1

import (
	"testing"
	"time"
)

func shutdownFixture(abort observedWorker) External {
	s := abort.session
	runtime := s.Runtime()
	w := abort.row.Data
	exit := Record[WatchRow]{Position: Position{1, UTC(fixed)}, Data: WatchRow{
		Kind: "exit", Runtime: &runtime, Exit: &RuntimeExit{UTC(fixed), 143, 15},
	}}
	binding := Record[WatchRow]{Position: Position{2, abort.row.Position.UTC}, Data: WatchRow{
		Kind: "shutdown", Session: &s, Shutdown: &ShutdownBinding{w.Invocation, *w.Attempt, 2, 1},
	}}
	start := Record[WorkerRow]{Position: Position{2, UTC(fixed.Add(-time.Second))}, Data: w}
	start.Data.Kind = "start"
	return External{Roster: Stream[WatchRow]{Rows: []Record[WatchRow]{exit, binding}},
		Sessions: map[string]Stream[WorkerRow]{s.ID: {Rows: []Record[WorkerRow]{{}, start, abort.row}}}}
}

func TestK6ShutdownBinding(t *testing.T) {
	for _, name := range []string{"Matched", "ChangedInvocation", "ChangedClaim", "ChangedRuntime", "ExitAfterAbort", "UnboundExit", "WrongStart"} {
		t.Run(name, func(t *testing.T) {
			abort := observedWorker{session: WorkerSession{ID: "session", Claimant: "worker"}, startSequence: 2, row: Record[WorkerRow]{
				Position: Position{3, UTC(fixed.Add(time.Second))}, Data: WorkerRow{Kind: "abort", Invocation: "invocation",
					Attempt: &AttemptIdentity{Workflow: "wf", Node: "node", Attempt: 2, ClaimHash: Hash([]byte("claim")), RequestID: "req"}},
			}}
			s := shutdownFixture(abort)
			exit, binding := &s.Roster.Rows[0], s.Roster.Rows[1].Data.Shutdown
			switch name {
			case "ChangedInvocation":
				binding.Invocation = "other"
				ledger := s.Sessions[abort.session.ID]
				ledger.Rows[1].Data.Invocation = binding.Invocation
			case "ChangedClaim":
				binding.Attempt.RequestID = "other"
				ledger := s.Sessions[abort.session.ID]
				ledger.Rows[1].Data.Attempt = &binding.Attempt
			case "ChangedRuntime":
				exit.Data.Runtime.ContainerID = "other"
			case "ExitAfterAbort":
				exit.Position.UTC = UTC(fixed.Add(2 * time.Second))
			case "UnboundExit":
				s.Roster.Rows = s.Roster.Rows[:1]
			case "WrongStart":
				abort.startSequence = 1
			}
			if matchedShutdown(s, abort) != (name == "Matched") {
				t.Fatalf("incorrect shutdown binding for %s", name)
			}
		})
	}
}
