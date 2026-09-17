package invariants

import (
	"strings"
	"testing"

	"durable-agent-execution-engine/internal/state"
)

func validTransportTrace() Trace {
	return Trace{
		History:     []HistoryRecord{{WorkflowID: "wf", Revision: 1, NewState: "RUNNABLE", Reason: "WORKFLOW_CREATED"}},
		Submissions: []SubmissionRecord{{Namespace: "default", Key: "k", Hash: "sub-v1:x", WorkflowID: "wf"}},
		Outbox: []OutboxRecord{{WorkflowID: "wf", EventID: "11111111-1111-4111-8111-111111111111",
			AggregateRevision: 1, EventType: "workflow.created", Topic: state.EventTopic,
			PublishState: string(state.OutboxPublished), PayloadValid: true}},
	}
}

func TestCheckAcceptsTransportEvidence(t *testing.T) {
	trace := validTransportTrace()
	trace.Inbox = []InboxRecord{{ConsumerID: "consumer", EventID: trace.Outbox[0].EventID,
		Topic: "durable-agent.events.v1", Partition: 0, Offset: 0, Disposition: string(state.InboxAccepted)}}
	trace.Wakeups = []WakeupRecord{{WorkflowID: "wf", EventID: trace.Outbox[0].EventID,
		PartitionID: 0, State: string(state.WakeupConsumed)}}
	if verdict := Check(trace); !verdict.Valid {
		t.Fatalf("valid transport trace rejected: %v", verdict.Violations)
	}
}

func TestCheckRejectsTransportObligations(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Trace)
		message string
	}{
		{name: "missing required outbox", mutate: func(trace *Trace) {
			trace.Outbox = nil
			trace.Wakeups = []WakeupRecord{{WorkflowID: "wf", EventID: "missing", PartitionID: 0, State: string(state.WakeupPending)}}
		}, message: "lacks an outbox obligation"},
		{name: "duplicate event ID", mutate: func(trace *Trace) {
			trace.Outbox = append(trace.Outbox, trace.Outbox[0])
		}, message: "event ID is duplicated"},
		{name: "unknown inbox event", mutate: func(trace *Trace) {
			trace.Inbox = []InboxRecord{{ConsumerID: "consumer", EventID: "22222222-2222-4222-8222-222222222222",
				Topic: "durable-agent.events.v1", Partition: 0, Offset: 0, Disposition: string(state.InboxAccepted)}}
		}, message: "references unknown event"},
		{name: "offset maps to two events", mutate: func(trace *Trace) {
			trace.Outbox = append(trace.Outbox, OutboxRecord{WorkflowID: "wf", EventID: "33333333-3333-4333-8333-333333333333",
				AggregateRevision: 2, EventType: "activity.result", Topic: state.EventTopic,
				PublishState: string(state.OutboxPublished), PayloadValid: true})
			trace.Inbox = []InboxRecord{
				{ConsumerID: "consumer", EventID: trace.Outbox[0].EventID, Topic: "durable-agent.events.v1", Partition: 0, Offset: 0, Disposition: string(state.InboxAccepted)},
				{ConsumerID: "consumer", EventID: trace.Outbox[1].EventID, Topic: "durable-agent.events.v1", Partition: 0, Offset: 0, Disposition: string(state.InboxAccepted)},
			}
		}, message: "offset maps to multiple event IDs"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			trace := validTransportTrace()
			testCase.mutate(&trace)
			verdict := Check(trace)
			if verdict.Valid || !strings.Contains(strings.Join(verdict.Violations, "\n"), testCase.message) {
				t.Fatalf("expected %q, got valid=%v violations=%v", testCase.message, verdict.Valid, verdict.Violations)
			}
		})
	}
}
