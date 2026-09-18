// Package invariants contains an independent, evidence-based checker for M1.
// It intentionally does not call state transition validators: its verdict is
// derived from persisted history and submission records using its own rules.
package invariants

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"durable-agent-execution-engine/internal/state"
)

type HistoryRecord struct {
	WorkflowID     string
	Revision       int64
	ActorKind      string
	SchedulerEpoch *int64
	OldState       string
	NewState       string
	Reason         string
}

type AcceptedResult struct {
	WorkflowID string
	NodeID     string
	Iteration  int
	Accepted   bool
}

type SubmissionRecord struct {
	Namespace  string
	Key        string
	Hash       string
	WorkflowID string
}

type AttemptRecord struct {
	WorkflowID      string
	NodeID          string
	Iteration       int
	AttemptNumber   int64
	State           string
	ClaimToken      string
	WorkerID        string
	WorkerRequestID string
	Outcome         string
	IsCurrent       bool
	ResultRecorded  bool
}

type OutboxRecord struct {
	WorkflowID        string
	EventID           string
	AggregateRevision int64
	EventType         string
	Topic             string
	PublishState      string
	PayloadValid      bool
}

type InboxRecord struct {
	ConsumerID  string
	EventID     string
	Topic       string
	Partition   int
	Offset      int64
	Disposition string
}

type WakeupRecord struct {
	WorkflowID  string
	EventID     string
	PartitionID int16
	State       string
}

type ReconciliationRecord struct {
	WorkflowID string
	Kind       string
	Reference  string
	Status     string
}

type CheckpointRecord struct {
	WorkflowID    string
	NodeID        string
	Iteration     int
	Sequence      int64
	SchemaVersion int
	SourceAttempt int64
	PayloadValid  bool
	PayloadHash   string
}

type EffectRecordSnapshot struct {
	WorkflowID       string
	LogicalEffectKey string
	ArgumentHash     string
	AttemptNumber    int64
	GrantScopeHash   string
	Outcome          string
	ReceiptPresent   bool
}

type EffectCallRecord struct {
	WorkflowID       string
	LogicalEffectKey string
	ArgumentHash     string
	AttemptNumber    int64
	RequestID        string
	Outcome          string
}

type ApprovalRecord struct {
	IntentID       string
	WorkflowID     string
	NodeID         string
	Iteration      int
	ProposalHash   string
	Target         string
	ArgumentsValid bool
	Decision       string
	DispatchStatus string
	GrantScopeHash string
	GrantToken     string
}

type Trace struct {
	History        []HistoryRecord
	Results        []AcceptedResult
	Submissions    []SubmissionRecord
	Attempts       []AttemptRecord
	Outbox         []OutboxRecord
	Inbox          []InboxRecord
	Wakeups        []WakeupRecord
	Reconciliation []ReconciliationRecord
	Checkpoints    []CheckpointRecord
	Effects        []EffectRecordSnapshot
	EffectCalls    []EffectCallRecord
	Approvals      []ApprovalRecord
}

type Verdict struct {
	Valid      bool
	Violations []string
}

// Load reads the evidence used by the M1 checker. It intentionally maps rows
// into checker-owned structs before validation, so Check cannot accidentally
// reuse a production transition decision.
func Load(ctx context.Context, store *state.Store, workflowIDs []string) (Trace, error) {
	var trace Trace
	for _, workflowID := range workflowIDs {
		workflow, err := store.GetWorkflow(ctx, workflowID)
		if err != nil {
			return Trace{}, err
		}
		history, err := store.ListHistory(ctx, workflowID, 0, 1000)
		if err != nil {
			return Trace{}, err
		}
		for _, record := range history {
			oldState := ""
			if record.OldState != nil {
				oldState = string(*record.OldState)
			}
			trace.History = append(trace.History, HistoryRecord{WorkflowID: workflowID,
				Revision: record.Revision, ActorKind: record.ActorKind, SchedulerEpoch: record.SchedulerEpoch,
				OldState: oldState, NewState: string(record.NewState), Reason: record.Reason})
		}
		nodes, err := store.ListNodes(ctx, workflowID)
		if err != nil {
			return Trace{}, err
		}
		for _, node := range nodes {
			if len(node.AcceptedResult) != 0 {
				trace.Results = append(trace.Results, AcceptedResult{WorkflowID: workflowID,
					NodeID: node.NodeID, Iteration: node.Iteration, Accepted: true})
			}
		}
		attempts, err := store.ListAttempts(ctx, workflowID)
		if err != nil {
			return Trace{}, err
		}
		for _, attempt := range attempts {
			trace.Attempts = append(trace.Attempts, AttemptRecord{
				WorkflowID: attempt.WorkflowID, NodeID: attempt.NodeID, Iteration: attempt.Iteration,
				AttemptNumber: attempt.AttemptNumber, State: string(attempt.State), ClaimToken: attempt.ClaimToken,
				WorkerID: attempt.WorkerID, WorkerRequestID: attempt.WorkerRequestID,
				Outcome: attempt.OutcomeDisposition, IsCurrent: attempt.IsCurrent,
				ResultRecorded: len(attempt.Result) != 0,
			})
		}
		outbox, err := store.ListOutbox(ctx, workflowID, 10000)
		if err != nil {
			return Trace{}, err
		}
		for _, event := range outbox {
			trace.Outbox = append(trace.Outbox, OutboxRecord{
				WorkflowID: event.WorkflowID, EventID: event.EventID,
				AggregateRevision: event.AggregateRevision, EventType: event.EventType,
				Topic:        event.Topic,
				PublishState: string(event.PublishState), PayloadValid: json.Valid(event.Payload),
			})
		}
		inbox, err := store.ListInboxForWorkflow(ctx, workflowID, 10000)
		if err != nil {
			return Trace{}, err
		}
		for _, message := range inbox {
			trace.Inbox = append(trace.Inbox, InboxRecord{ConsumerID: message.ConsumerID,
				EventID: message.EventID, Topic: message.Topic, Partition: message.Partition,
				Offset: message.Offset, Disposition: string(message.Disposition)})
		}
		wakeups, err := store.ListWakeups(ctx, workflowID, 10000)
		if err != nil {
			return Trace{}, err
		}
		for _, wakeup := range wakeups {
			trace.Wakeups = append(trace.Wakeups, WakeupRecord{WorkflowID: wakeup.WorkflowID,
				EventID: wakeup.EventID, PartitionID: wakeup.PartitionID, State: string(wakeup.State)})
		}
		reconciliationItems, err := store.ListReconciliationItemsForWorkflow(ctx, workflowID, 10000)
		if err != nil {
			return Trace{}, err
		}
		for _, item := range reconciliationItems {
			trace.Reconciliation = append(trace.Reconciliation, ReconciliationRecord{
				WorkflowID: item.WorkflowID, Kind: string(item.Kind), Reference: item.Reference, Status: item.Status})
		}
		checkpoints, err := store.ListCheckpoints(ctx, workflowID)
		if err != nil {
			return Trace{}, err
		}
		for _, checkpoint := range checkpoints {
			trace.Checkpoints = append(trace.Checkpoints, CheckpointRecord{
				WorkflowID: checkpoint.WorkflowID, NodeID: checkpoint.NodeID,
				Iteration: checkpoint.Iteration, Sequence: checkpoint.Sequence,
				SchemaVersion: checkpoint.SchemaVersion, SourceAttempt: checkpoint.SourceAttempt,
				PayloadValid: json.Valid(checkpoint.Payload), PayloadHash: checkpoint.PayloadHash,
			})
		}
		effects, err := store.ListEffectRecords(ctx, workflowID)
		if err != nil {
			return Trace{}, err
		}
		for _, effect := range effects {
			trace.Effects = append(trace.Effects, EffectRecordSnapshot{
				WorkflowID: effect.WorkflowID, LogicalEffectKey: effect.LogicalEffectKey,
				ArgumentHash: effect.ArgumentHash, AttemptNumber: effect.AttemptNumber,
				GrantScopeHash: effect.GrantScopeHash, Outcome: effect.Outcome,
				ReceiptPresent: len(effect.Receipt) != 0,
			})
		}
		calls, err := store.ListEffectCallAttempts(ctx, workflowID)
		if err != nil {
			return Trace{}, err
		}
		for _, call := range calls {
			trace.EffectCalls = append(trace.EffectCalls, EffectCallRecord{
				WorkflowID: call.WorkflowID, LogicalEffectKey: call.LogicalEffectKey,
				ArgumentHash: call.ArgumentHash, AttemptNumber: call.AttemptNumber,
				RequestID: call.RequestID, Outcome: call.Outcome,
			})
		}
		approvals, err := store.ListApprovalIntents(ctx, workflowID)
		if err != nil {
			return Trace{}, err
		}
		for _, approval := range approvals {
			trace.Approvals = append(trace.Approvals, ApprovalRecord{
				IntentID: approval.IntentID, WorkflowID: approval.WorkflowID,
				NodeID: approval.NodeID, Iteration: approval.Iteration,
				ProposalHash: approval.ProposalHash, Target: approval.Target,
				ArgumentsValid: json.Valid(approval.CanonicalArguments),
				Decision:       approval.Decision, DispatchStatus: approval.DispatchStatus,
				GrantScopeHash: approval.GrantScopeHash, GrantToken: approval.GrantToken,
			})
		}
		trace.Submissions = append(trace.Submissions, SubmissionRecord{Namespace: workflow.Namespace,
			Key: workflow.SubmissionKey, Hash: workflow.PayloadHash, WorkflowID: workflowID})
	}
	globalItems, err := store.ListGlobalReconciliationItems(ctx, 10000)
	if err != nil {
		return Trace{}, err
	}
	for _, item := range globalItems {
		trace.Reconciliation = append(trace.Reconciliation, ReconciliationRecord{
			WorkflowID: item.WorkflowID, Kind: string(item.Kind), Reference: item.Reference, Status: item.Status})
	}
	return trace, nil
}

func Check(trace Trace) Verdict {
	var violations []string
	checkHistory(trace.History, &violations)
	checkResults(trace.Results, &violations)
	checkSubmissions(trace.Submissions, &violations)
	checkOwnershipAndAttempts(trace.History, trace.Attempts, &violations)
	checkTransport(trace, &violations)
	checkM4(trace, &violations)
	return Verdict{Valid: len(violations) == 0, Violations: violations}
}

func checkM4(trace Trace, violations *[]string) {
	checkpointsByNode := make(map[string][]CheckpointRecord)
	for _, checkpoint := range trace.Checkpoints {
		if checkpoint.WorkflowID == "" || checkpoint.NodeID == "" || checkpoint.Iteration < 0 ||
			checkpoint.Sequence < 0 || checkpoint.SchemaVersion <= 0 || checkpoint.SourceAttempt <= 0 ||
			!checkpoint.PayloadValid || checkpoint.PayloadHash == "" {
			*violations = append(*violations, "checkpoint evidence is incomplete")
		}
		key := fmt.Sprintf("%s\x00%s\x00%d", checkpoint.WorkflowID, checkpoint.NodeID, checkpoint.Iteration)
		checkpointsByNode[key] = append(checkpointsByNode[key], checkpoint)
	}
	for key, checkpoints := range checkpointsByNode {
		sort.Slice(checkpoints, func(i, j int) bool { return checkpoints[i].Sequence < checkpoints[j].Sequence })
		for index, checkpoint := range checkpoints {
			if checkpoint.Sequence != int64(index) {
				*violations = append(*violations, "checkpoint sequence is not contiguous: "+key)
				break
			}
			if index > 0 && checkpoint.SchemaVersion != checkpoints[index-1].SchemaVersion {
				*violations = append(*violations, "checkpoint schema changed within an activity: "+key)
			}
		}
	}

	effects := make(map[string]EffectRecordSnapshot)
	for _, effect := range trace.Effects {
		if effect.WorkflowID == "" || effect.LogicalEffectKey == "" || effect.ArgumentHash == "" || effect.AttemptNumber <= 0 {
			*violations = append(*violations, "effect record identity is incomplete")
		}
		key := effect.WorkflowID + "\x00" + effect.LogicalEffectKey
		if _, exists := effects[key]; exists {
			*violations = append(*violations, "effect key has duplicate records: "+key)
		}
		effects[key] = effect
		switch effect.Outcome {
		case "APPLIED":
			if !effect.ReceiptPresent {
				*violations = append(*violations, "applied effect lacks a receipt: "+key)
			}
		case "OUTCOME_UNKNOWN":
			// An unknown effect must remain visible until an operator resolves it.
		default:
			*violations = append(*violations, "effect has unknown outcome: "+key)
		}
	}
	callRequests := make(map[string]struct{})
	for _, call := range trace.EffectCalls {
		if call.WorkflowID == "" || call.LogicalEffectKey == "" || call.ArgumentHash == "" || call.AttemptNumber <= 0 || call.RequestID == "" {
			*violations = append(*violations, "effect call identity is incomplete")
		}
		key := call.WorkflowID + "\x00" + call.RequestID
		if _, exists := callRequests[key]; exists {
			*violations = append(*violations, "effect request ID is duplicated: "+key)
		}
		callRequests[key] = struct{}{}
		if call.Outcome != "APPLIED" && call.Outcome != "DUPLICATE" && call.Outcome != "CONFLICT" && call.Outcome != "REJECTED" && call.Outcome != "UNKNOWN" {
			*violations = append(*violations, "effect call has unknown outcome: "+key)
		}
	}
	for _, approval := range trace.Approvals {
		if approval.IntentID == "" || approval.WorkflowID == "" || approval.NodeID == "" || approval.Iteration < 0 || approval.ProposalHash == "" || approval.Target == "" || !approval.ArgumentsValid {
			*violations = append(*violations, "approval intent is incomplete")
		}
		if approval.Decision != "PENDING" && approval.Decision != "APPROVED" && approval.Decision != "REJECTED" {
			*violations = append(*violations, "approval decision is unknown: "+approval.IntentID)
		}
		if approval.DispatchStatus != "NOT_GRANTED" && approval.DispatchStatus != "GRANTED" && approval.DispatchStatus != "DISPATCHED" && approval.DispatchStatus != "CANCELED" {
			*violations = append(*violations, "approval dispatch status is unknown: "+approval.IntentID)
		}
		if approval.DispatchStatus == "GRANTED" || approval.DispatchStatus == "DISPATCHED" {
			if approval.Decision != "APPROVED" || approval.GrantScopeHash == "" || approval.GrantToken == "" {
				*violations = append(*violations, "approval grant lacks matching approved decision: "+approval.IntentID)
			}
		}
	}
}

// checkTransport derives transport verdicts from the independent snapshots,
// not from relay or inbox transition helpers. Pending rows are obligations,
// not failures; missing, malformed, or contradictory evidence is a failure.
func checkTransport(trace Trace, violations *[]string) {
	// Traces created by the M0/M1 unit fixtures intentionally predate the M3
	// transport snapshot. A database-loaded M3 trace always has at least one
	// outbox row, so only an explicitly transport-bearing trace opts into these
	// additional obligations.
	if len(trace.Outbox) == 0 && len(trace.Inbox) == 0 && len(trace.Wakeups) == 0 && len(trace.Reconciliation) == 0 {
		return
	}
	workflowIDs := make(map[string]struct{})
	for _, submission := range trace.Submissions {
		workflowIDs[submission.WorkflowID] = struct{}{}
	}
	events := make(map[string]OutboxRecord)
	for _, event := range trace.Outbox {
		if event.EventID == "" || event.WorkflowID == "" || event.AggregateRevision <= 0 ||
			event.EventType == "" || event.Topic == "" || !event.PayloadValid {
			*violations = append(*violations, "outbox event identity or payload is incomplete")
		}
		expectedTopic, knownEventType := state.EventTopicFor(event.EventType)
		if !knownEventType && event.PublishState != string(state.OutboxQuarantined) {
			*violations = append(*violations, "outbox event has unknown event type: "+event.EventID)
		} else if knownEventType && event.Topic != expectedTopic {
			*violations = append(*violations, "outbox event is on the wrong topic: "+event.EventID)
		}
		if _, exists := events[event.EventID]; exists {
			*violations = append(*violations, "outbox event ID is duplicated: "+event.EventID)
		}
		events[event.EventID] = event
		if _, exists := workflowIDs[event.WorkflowID]; !exists {
			*violations = append(*violations, "outbox event references unknown workflow: "+event.EventID)
		}
		if event.PublishState != string(state.OutboxPending) && event.PublishState != string(state.OutboxClaimed) &&
			event.PublishState != string(state.OutboxPublished) && event.PublishState != string(state.OutboxQuarantined) {
			*violations = append(*violations, "outbox event has unknown publication state: "+event.EventID)
		}
	}
	for _, history := range trace.History {
		if !requiresOutbox(history.Reason) {
			continue
		}
		found := false
		for _, event := range trace.Outbox {
			if event.WorkflowID == history.WorkflowID && event.AggregateRevision == history.Revision {
				found = true
				break
			}
		}
		if !found {
			*violations = append(*violations, fmt.Sprintf("history revision lacks an outbox obligation: %s/%d", history.WorkflowID, history.Revision))
		}
	}
	seenOffsets := make(map[string]string)
	for _, message := range trace.Inbox {
		if message.ConsumerID == "" || message.EventID == "" || message.Topic == "" || message.Partition < 0 || message.Offset < 0 {
			*violations = append(*violations, "inbox message identity is incomplete")
		}
		event, exists := events[message.EventID]
		if !exists {
			*violations = append(*violations, "inbox message references unknown event: "+message.EventID)
		} else if event.Topic != message.Topic {
			*violations = append(*violations, "inbox message topic conflicts with outbox event: "+message.EventID)
		}
		key := fmt.Sprintf("%s\x00%s\x00%d\x00%d", message.ConsumerID, message.Topic, message.Partition, message.Offset)
		if prior, exists := seenOffsets[key]; exists && prior != message.EventID {
			*violations = append(*violations, "inbox offset maps to multiple event IDs: "+key)
		}
		seenOffsets[key] = message.EventID
		if message.Disposition != string(state.InboxAccepted) && message.Disposition != string(state.InboxAlreadyHandled) &&
			message.Disposition != string(state.InboxStale) && message.Disposition != string(state.InboxQuarantined) {
			*violations = append(*violations, "inbox has unknown disposition: "+message.EventID)
		}
	}
	seenWakeups := make(map[string]struct{})
	poisonReferences := make(map[string]struct{})
	for _, wakeup := range trace.Wakeups {
		if wakeup.WorkflowID == "" || wakeup.EventID == "" || wakeup.PartitionID < 0 || wakeup.PartitionID >= 16 {
			*violations = append(*violations, "scheduler wake-up identity is incomplete")
		}
		if _, exists := events[wakeup.EventID]; !exists {
			*violations = append(*violations, "scheduler wake-up references unknown event: "+wakeup.EventID)
		}
		if _, exists := seenWakeups[wakeup.EventID]; exists {
			*violations = append(*violations, "event has duplicate scheduler wake-ups: "+wakeup.EventID)
		}
		seenWakeups[wakeup.EventID] = struct{}{}
		if wakeup.State != string(state.WakeupPending) && wakeup.State != string(state.WakeupClaimed) &&
			wakeup.State != string(state.WakeupConsumed) {
			*violations = append(*violations, "scheduler wake-up has unknown state: "+wakeup.EventID)
		}
	}
	for _, item := range trace.Reconciliation {
		globalPoison := item.WorkflowID == "" && item.Kind == string(state.ReconcilePoisonRecord) &&
			strings.HasPrefix(item.Reference, "transport/")
		if (item.WorkflowID == "" && !globalPoison) || item.Kind == "" || item.Reference == "" {
			*violations = append(*violations, "reconciliation item identity is incomplete")
		}
		if item.WorkflowID != "" {
			if _, exists := workflowIDs[item.WorkflowID]; !exists {
				*violations = append(*violations, "reconciliation item references unknown workflow: "+item.Reference)
			}
		} else if !globalPoison {
			*violations = append(*violations, "reconciliation item references unknown workflow: "+item.Reference)
		}
		switch item.Kind {
		case string(state.ReconcileExpiredAttempt), string(state.ReconcilePendingOutbox),
			string(state.ReconcileLostWakeup), string(state.ReconcileDueTimer), string(state.ReconcilePoisonRecord):
		default:
			*violations = append(*violations, "reconciliation item has unknown kind: "+item.Reference)
		}
		switch item.Status {
		case "OPEN", "RESOLVED", "ABANDONED":
		default:
			*violations = append(*violations, "reconciliation item has unknown status: "+item.Reference)
		}
		if item.Kind == string(state.ReconcilePendingOutbox) && strings.HasPrefix(item.Reference, "outbox/") {
			eventID := strings.TrimPrefix(item.Reference, "outbox/")
			if event, exists := events[eventID]; !exists {
				*violations = append(*violations, "pending-outbox item references unknown event: "+item.Reference)
			} else if item.Status == "OPEN" && event.PublishState == string(state.OutboxPublished) {
				*violations = append(*violations, "published outbox event has an open reconciliation item: "+eventID)
			}
		}
		if item.Kind == string(state.ReconcilePoisonRecord) {
			poisonReferences[item.Reference] = struct{}{}
		}
	}
	for eventID, event := range events {
		if event.PublishState == string(state.OutboxQuarantined) {
			if _, exists := poisonReferences["outbox/"+eventID]; !exists {
				*violations = append(*violations, "quarantined outbox event lacks a poison reconciliation item: "+eventID)
			}
		}
	}
}

func requiresOutbox(reason string) bool {
	switch reason {
	case "WORKFLOW_CREATED", "ATTEMPT_RESULT_RECORDED", "RESULT_CONSUMED",
		"GRAPH_ADVANCED", "TIMER_SCHEDULED", "CANCELED_ALL_ACTIVE_NODES",
		"REDISPATCH_UNCLAIMED", "TIMEOUT_REPLACEMENT", "TIMEOUT_RECONCILIATION_REQUIRED":
		return true
	default:
		return false
	}
}

func checkOwnershipAndAttempts(history []HistoryRecord, attempts []AttemptRecord, violations *[]string) {
	byWorkflow := make(map[string][]HistoryRecord)
	for _, record := range history {
		byWorkflow[record.WorkflowID] = append(byWorkflow[record.WorkflowID], record)
	}
	for workflowID, records := range byWorkflow {
		sort.Slice(records, func(i, j int) bool { return records[i].Revision < records[j].Revision })
		var previousEpoch int64
		for _, record := range records {
			if record.ActorKind == "scheduler" {
				if record.SchedulerEpoch == nil || *record.SchedulerEpoch <= 0 {
					*violations = append(*violations, "scheduler history is missing an ownership epoch: "+workflowID)
				} else if previousEpoch != 0 && *record.SchedulerEpoch < previousEpoch {
					*violations = append(*violations, "scheduler epoch moved backwards: "+workflowID)
				} else {
					previousEpoch = *record.SchedulerEpoch
				}
			} else if record.SchedulerEpoch != nil {
				*violations = append(*violations, "non-scheduler history carries a scheduler epoch: "+workflowID)
			}
		}
	}
	byNode := make(map[string][]AttemptRecord)
	for _, attempt := range attempts {
		if attempt.WorkflowID == "" || attempt.NodeID == "" || attempt.Iteration < 0 || attempt.AttemptNumber <= 0 {
			*violations = append(*violations, "attempt identity is incomplete")
			continue
		}
		key := fmt.Sprintf("%s\x00%s\x00%d", attempt.WorkflowID, attempt.NodeID, attempt.Iteration)
		byNode[key] = append(byNode[key], attempt)
		if attempt.IsCurrent && isTerminalAttempt(attempt.State) {
			*violations = append(*violations, "terminal attempt remains current: "+key)
		}
		if attempt.State == "CLAIMED" && (attempt.ClaimToken == "" || attempt.WorkerID == "" || attempt.WorkerRequestID == "") {
			*violations = append(*violations, "claimed attempt is missing worker ownership: "+key)
		}
		if attempt.ResultRecorded && (attempt.State == "TIMED_OUT" || attempt.State == "REPLACED" || attempt.State == "CANCELED" || attempt.State == "OUTCOME_UNKNOWN") {
			*violations = append(*violations, "stale worker result recorded on settled attempt: "+key)
		}
	}
	for key, records := range byNode {
		sort.Slice(records, func(i, j int) bool { return records[i].AttemptNumber < records[j].AttemptNumber })
		current := 0
		for index, record := range records {
			want := int64(index + 1)
			if record.AttemptNumber != want {
				*violations = append(*violations, fmt.Sprintf("attempt generation is not contiguous for %s", key))
				break
			}
			if record.IsCurrent {
				current++
			}
		}
		if current > 1 {
			*violations = append(*violations, "more than one current attempt: "+key)
		}
	}
}

func isTerminalAttempt(state string) bool {
	switch state {
	case "SUCCEEDED", "FAILED_RETRYABLE", "FAILED_FINAL", "TIMED_OUT", "REPLACED", "OUTCOME_UNKNOWN", "CANCELED":
		return true
	default:
		return false
	}
}

func checkHistory(history []HistoryRecord, violations *[]string) {
	byWorkflow := make(map[string][]HistoryRecord)
	for _, record := range history {
		if record.WorkflowID == "" {
			*violations = append(*violations, "history record has no workflow ID")
			continue
		}
		byWorkflow[record.WorkflowID] = append(byWorkflow[record.WorkflowID], record)
	}
	for workflowID, records := range byWorkflow {
		sort.Slice(records, func(i, j int) bool { return records[i].Revision < records[j].Revision })
		if len(records) == 0 || records[0].Revision != 1 || records[0].OldState != "" || records[0].NewState != "RUNNABLE" {
			*violations = append(*violations, "workflow history does not start with revision 1 creation: "+workflowID)
		}
		var previousRevision int64
		previousState := ""
		terminalSeen := false
		for index, record := range records {
			if record.Revision <= 0 || (index > 0 && record.Revision != previousRevision+1) {
				*violations = append(*violations, fmt.Sprintf("history revision %d is not contiguous for %s", record.Revision, workflowID))
			}
			if index > 0 && record.OldState != previousState {
				*violations = append(*violations, fmt.Sprintf("history old state %q does not follow %q for %s", record.OldState, previousState, workflowID))
			}
			if index > 0 && !allowedTransition(record.OldState, record.NewState) {
				*violations = append(*violations, fmt.Sprintf("illegal workflow transition %s -> %s for %s", record.OldState, record.NewState, workflowID))
			}
			if terminalSeen {
				*violations = append(*violations, "workflow has history after its terminal decision: "+workflowID)
			}
			if isTerminal(record.NewState) {
				terminalSeen = true
			}
			previousRevision = record.Revision
			previousState = record.NewState
		}
	}
}

func allowedTransition(from, to string) bool {
	if isTerminal(from) {
		return false
	}
	allowed := map[string]map[string]bool{
		"RUNNABLE": {
			"RUNNABLE": true, "WAITING_ACTIVITY": true, "WAITING_APPROVAL": true,
			"WAITING_TIMER": true, "PAUSED_UNSUPPORTED_VERSION": true,
			"SUCCEEDED": true, "FAILED": true, "CANCELED": true,
		},
		"WAITING_ACTIVITY": {
			"WAITING_ACTIVITY": true, "RUNNABLE": true, "WAITING_TIMER": true,
			"RECONCILIATION_REQUIRED": true, "FAILED": true, "CANCELED": true,
		},
		"WAITING_TIMER":              {"WAITING_TIMER": true, "RUNNABLE": true, "CANCELED": true},
		"WAITING_APPROVAL":           {"WAITING_APPROVAL": true, "RUNNABLE": true, "REJECTED": true, "CANCELED": true},
		"PAUSED_UNSUPPORTED_VERSION": {"PAUSED_UNSUPPORTED_VERSION": true, "RUNNABLE": true, "CANCELED": true, "ABANDONED": true},
		"RECONCILIATION_REQUIRED":    {"RECONCILIATION_REQUIRED": true, "RUNNABLE": true, "CANCELED": true, "ABANDONED": true},
	}
	return allowed[from][to]
}

func checkResults(results []AcceptedResult, violations *[]string) {
	seen := make(map[string]struct{})
	for _, result := range results {
		if result.WorkflowID == "" || result.NodeID == "" || result.Iteration < 0 {
			*violations = append(*violations, "accepted result identity is incomplete")
		}
		if !result.Accepted {
			continue
		}
		key := fmt.Sprintf("%s\x00%s\x00%d", result.WorkflowID, result.NodeID, result.Iteration)
		if _, exists := seen[key]; exists {
			*violations = append(*violations, "node has more than one accepted result: "+key)
		}
		seen[key] = struct{}{}
	}
}

func checkSubmissions(submissions []SubmissionRecord, violations *[]string) {
	byKey := make(map[string]SubmissionRecord)
	byWorkflow := make(map[string]SubmissionRecord)
	for _, submission := range submissions {
		if submission.Namespace == "" || submission.Key == "" || submission.Hash == "" || submission.WorkflowID == "" {
			*violations = append(*violations, "submission identity is incomplete")
			continue
		}
		key := submission.Namespace + "\x00" + submission.Key
		if prior, exists := byKey[key]; exists && (prior.Hash != submission.Hash || prior.WorkflowID != submission.WorkflowID) {
			*violations = append(*violations, "submission key maps to conflicting identity: "+key)
		}
		byKey[key] = submission
		if prior, exists := byWorkflow[submission.WorkflowID]; exists && (prior.Namespace != submission.Namespace || prior.Key != submission.Key) {
			*violations = append(*violations, "workflow ID maps to multiple submissions: "+submission.WorkflowID)
		}
		byWorkflow[submission.WorkflowID] = submission
	}
}

func isTerminal(state string) bool {
	switch state {
	case "SUCCEEDED", "FAILED", "REJECTED", "CANCELED", "ABANDONED":
		return true
	default:
		return false
	}
}
