package main

import (
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"durable-agent-execution-engine/internal/effects/k6sink"
	"durable-agent-execution-engine/internal/k6ingress"
	"durable-agent-execution-engine/internal/k6receipt"
	"durable-agent-execution-engine/internal/restoreledger"
)

type orphanObservation struct {
	Schema      string   `json:"schema"`
	BootID      string   `json:"boot_id"`
	ConfigSHA   string   `json:"config_sha256"`
	Authority   string   `json:"authority"`
	RestoreSHA  string   `json:"restore_sha256,omitempty"`
	SourceSHA   string   `json:"source_observation_sha256,omitempty"`
	SinkSHA     string   `json:"sink_snapshot_sha256,omitempty"`
	DB          orphanDB `json:"engine"`
	CompletedNS int64    `json:"completed_ns,omitempty"`
}

type orphanVerdict struct {
	Schema             string    `json:"schema"`
	BootID             string    `json:"boot_id"`
	Workflow           string    `json:"workflow_id"`
	RestoreSHA         string    `json:"restore_sha256"`
	SourceSHA          string    `json:"source_observation_sha256"`
	RebindSHA          string    `json:"grant_reader_rebind_sha256"`
	IngressSHA         string    `json:"effect_ingress_sha256,omitempty"`
	EngineSHA          string    `json:"final_engine_snapshot_sha256,omitempty"`
	SinkSHA            string    `json:"final_sink_snapshot_sha256,omitempty"`
	OriginalResultSHA  string    `json:"source_result_sha256"`
	RestoredResultSHA  string    `json:"restored_result_sha256,omitempty"`
	ApplicationID      string    `json:"application_id"`
	LookupCallID       int64     `json:"lookup_call_id,omitempty"`
	ReplacementAttempt int64     `json:"replacement_attempt,omitempty"`
	ReplacementFence   int64     `json:"replacement_fence,omitempty"`
	ReplacementClaim   string    `json:"replacement_claim_sha256,omitempty"`
	ReplacementApplies *int      `json:"replacement_apply_calls,omitempty"`
	Mutations          int       `json:"journal_mutations,omitempty"`
	Revision           int64     `json:"resource_revision,omitempty"`
	StateHash          string    `json:"post_apply_state_sha256,omitempty"`
	ResultRevision     int64     `json:"result_recorded_revision,omitempty"`
	ConsumedRevision   int64     `json:"result_consumed_revision,omitempty"`
	ConsumedAt         time.Time `json:"result_consumed_at,omitempty"`
	CompletedNS        int64     `json:"completed_ns,omitempty"`
	Unresolved         []string  `json:"unresolved_workflows"`
}

func sameAllocation(a k6sink.Allocation, hold effectAtCut, attempt, fence int64) bool {
	return a.IntentID == hold.Intent && a.GrantHash == hold.GrantSHA && a.ResourceID == hold.Resource &&
		a.Attempt == attempt && a.Fence == fence
}

func sameCall(c k6sink.Call, hold effectAtCut, attempt int64, operation, outcome string) bool {
	return c.CallID > 0 && c.Operation == operation && c.Outcome == outcome && c.Attempt == attempt &&
		c.WorkflowID == hold.Attempt.WorkflowID && c.LogicalKey == hold.LogicalKey && c.IntentID == hold.Intent &&
		c.ResourceID == hold.Resource && c.RequestID != "" && c.MessageID != "" &&
		restoreledger.IsSHA256(c.BodyHash) && restoreledger.IsSHA256(c.ConnectionHash) && !c.ObservedAt.IsZero()
}

func verifySourceSink(s k6sink.Snapshot, hold effectAtCut) error {
	if len(s.Resources) != 1 || len(s.Allocations) != 1 || len(s.Calls) != 1 ||
		len(s.Receipts) != 0 || len(s.Journal) != 0 || len(s.RequestBindings) != 1 {
		return errors.New("E1 source hold must show one allocation and zero applications")
	}
	r := s.Resources[0]
	if r.ResourceID != hold.Resource || r.InitialRevision != 0 || r.Revision != 0 ||
		r.LatestFence != hold.Fence || !restoreledger.IsSHA256(r.InitialStateHash) ||
		r.InitialStateHash != r.FinalStateHash || !sameAllocation(s.Allocations[0], hold, 1, hold.Fence) ||
		!sameCall(s.Calls[0], hold, 1, "allocate", "ALLOCATED") ||
		!matchingBinding(s, s.Calls[0]) {
		return errors.New("E1 source sink has the wrong held identity or initial state")
	}
	return s.CheckRevisionChain()
}

func matchingBinding(s k6sink.Snapshot, call k6sink.Call) bool {
	return slices.Contains(s.RequestBindings, k6sink.RequestBinding{RequestID: call.RequestID, BodyHash: call.BodyHash})
}

func matchingReceiptCall(call k6sink.Call, receipt k6receipt.Receipt) bool {
	raw, _ := json.Marshal(receipt)
	return call.ApplicationID != nil && *call.ApplicationID == receipt.ApplicationID &&
		call.ResponseHash != nil && *call.ResponseHash == restoreledger.SHA256(raw)
}

func verifyRetainedSink(source, prewrite, final k6sink.Snapshot, hold effectAtCut, receipt k6receipt.Receipt,
	replacement orphanAttempt) (k6sink.Call, k6sink.Call, error) {
	bad := errors.New("E1 sink must retain exactly the original application and one replacement lookup")
	if err := verifySourceSink(source, hold); err != nil {
		return k6sink.Call{}, k6sink.Call{}, err
	}
	if len(prewrite.Calls) != 2 || len(final.Calls) != 4 || len(prewrite.Allocations) != 1 ||
		len(final.Allocations) != 2 || len(prewrite.Receipts) != 1 || len(final.Receipts) != 1 ||
		len(prewrite.Journal) != 1 || len(final.Journal) != 1 || len(prewrite.Resources) != 1 ||
		len(final.Resources) != 1 || len(prewrite.RequestBindings) != 2 || len(final.RequestBindings) != 4 {
		return k6sink.Call{}, k6sink.Call{}, bad
	}
	original, lookup := final.Calls[1], final.Calls[3]
	for i, call := range final.Calls {
		if !matchingBinding(final, call) || (i > 0 && (call.CallID <= final.Calls[i-1].CallID ||
			call.ObservedAt.Before(final.Calls[i-1].ObservedAt))) {
			return original, lookup, bad
		}
	}
	old, end := prewrite.Resources[0], final.Resources[0]
	if !sameSinkCall(source.Calls[0], final.Calls[0]) ||
		!sameSinkCall(prewrite.Calls[0], final.Calls[0]) || !sameSinkCall(prewrite.Calls[1], original) ||
		!matchingBinding(prewrite, original) || !matchingBinding(prewrite, prewrite.Calls[0]) ||
		!sameCall(original, hold, 1, "apply", "APPLIED") || !matchingReceiptCall(original, receipt) ||
		!sameCall(final.Calls[2], hold, 2, "allocate", "ALLOCATED") ||
		!sameCall(lookup, hold, 2, "lookup", "FOUND") || !matchingReceiptCall(lookup, receipt) ||
		!sameAllocation(prewrite.Allocations[0], hold, 1, hold.Fence) ||
		!sameAllocation(final.Allocations[0], hold, 1, hold.Fence) ||
		!sameAllocation(final.Allocations[1], hold, replacement.Attempt, replacement.Fence) ||
		old.LatestFence != hold.Fence || end.LatestFence != replacement.Fence ||
		old.ResourceID != hold.Resource || end.ResourceID != old.ResourceID || old.Revision != 1 ||
		end.Revision != old.Revision || end.InitialRevision != 0 || old.InitialRevision != 0 ||
		old.InitialStateHash != source.Resources[0].InitialStateHash || end.InitialStateHash != old.InitialStateHash ||
		!restoreledger.IsSHA256(old.FinalStateHash) || end.FinalStateHash != old.FinalStateHash ||
		prewrite.Receipts[0] != receipt || final.Receipts[0] != receipt ||
		final.Journal[0] != prewrite.Journal[0] || final.Journal[0].IntentID != hold.Intent ||
		final.Journal[0].LogicalKey != hold.LogicalKey {
		return original, lookup, bad
	}
	return original, lookup, errors.Join(prewrite.CheckRevisionChain(), final.CheckRevisionChain())
}

func sameSinkCall(a, b k6sink.Call) bool {
	applicationA, applicationB := a.ApplicationID, b.ApplicationID
	responseA, responseB := a.ResponseHash, b.ResponseHash
	a.ApplicationID, b.ApplicationID, a.ResponseHash, b.ResponseHash = nil, nil, nil, nil
	observedAt := a.ObservedAt.Equal(b.ObservedAt)
	a.ObservedAt, b.ObservedAt = time.Time{}, time.Time{}
	return observedAt && a == b && sameOptionalString(applicationA, applicationB) && sameOptionalString(responseA, responseB)
}

func sameOptionalString(a, b *string) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && *a == *b)
}

func checkIngress(rows []k6ingress.Record, hold effectAtCut, original, lookup k6sink.Call,
	cut, kill, rebound, final time.Time) error {
	// The ordinary relay emits five contiguous records per successful request
	// for this single-E1 fixture. Additional inbound traffic or an unsealed
	// suffix can conceal a repeat call and gives no credit.
	if len(rows) != 11 || rows[10].Event != "seal" || rows[10].HighWater != 10 ||
		rows[10].Time.Before(rows[9].Time) || rows[10].Time.After(final) {
		return errors.New("E1 requires a sealed ingress journal with only the original apply and replacement lookup")
	}
	for i, c := range []k6sink.Call{original, lookup} {
		first := i * 5
		start, end := cut, kill
		if i == 1 {
			start, end = rebound, final
		}
		events := []string{"inbound", "sink_send", "sink_response", "worker_delivery", "close"}
		inbound := rows[first]
		for j, event := range events {
			r := rows[first+j]
			if r.Event != event || r.Session != rows[10].Session || r.Sequence != uint64(first+j+1) ||
				r.Ordinal != uint64(i+1) || r.Copy != 0 || r.Method != "POST" || r.Operation != c.Operation ||
				r.Attempt != c.Attempt || r.IntentID != hold.Intent || r.GrantHash != hold.GrantSHA ||
				r.KeyHash != restoreledger.SHA256([]byte(hold.LogicalKey)) ||
				r.ResourceHash != restoreledger.SHA256([]byte(hold.Resource)) ||
				r.RequestIDHash != restoreledger.SHA256([]byte(c.RequestID)) ||
				r.InvocationHash != restoreledger.SHA256([]byte(c.MessageID)) ||
				r.ConnectionHash != c.ConnectionHash || r.WorkerBinding != inbound.WorkerBinding ||
				r.Time.Before(start) || r.Time.After(end) ||
				(j > 0 && r.Time.Before(rows[first+j-1].Time)) {
				return fmt.Errorf("E1 ingress does not join the %s call to the recovery window", c.Operation)
			}
			switch event {
			case "inbound", "sink_send":
				if r.BodyHash != c.BodyHash || r.WireHash != inbound.WireHash || r.WireLength != inbound.WireLength {
					return errors.New("E1 ingress request and independent sink body disagree")
				}
			case "sink_response", "worker_delivery":
				if c.ResponseHash == nil || c.ApplicationID == nil || r.Status != 200 ||
					r.BodyHash != *c.ResponseHash || r.ApplicationID != *c.ApplicationID ||
					r.WireHash != rows[first+2].WireHash || r.WireLength != rows[first+2].WireLength {
					return errors.New("E1 ingress did not witness delivery of the matching sink receipt")
				}
			}
		}
	}
	if rows[0].ConnectionHash == rows[5].ConnectionHash {
		return errors.New("E1 ingress original apply and lookup are not separate requests")
	}
	return nil
}

func verifyRecoveredEffect(db orphanDB, hold effectAtCut, original k6receipt.Proof, originalResult string,
	key ed25519.PublicKey, rebind orphanDB, t0, gate time.Time, v *orphanVerdict) (orphanAttempt, error) {
	failed := errors.New("E1 did not consume exactly one signed result on a live replacement claim")
	if db.State != "SUCCEEDED" || db.NodeState != "SUCCEEDED" || db.Current != nil ||
		db.Unresolved != 0 || len(db.Attempts) != 2 || !restoreledger.IsSHA256(originalResult) ||
		db.ResultSHA != originalResult {
		return orphanAttempt{}, failed
	}
	old, next := db.Attempts[0], db.Attempts[1]
	if !matchOrphanAttempt(old, hold) || !matchOrphanAttempt(next, hold) ||
		old.Attempt != 1 || old.Current || old.State != "TIMED_OUT" || old.Fence != hold.Fence ||
		old.ClaimSHA != hold.Attempt.ClaimSHA || !old.Deadline.Equal(rebind.Attempts[0].Deadline) ||
		present(old.Result) || present(old.Proof) ||
		next.Attempt != 2 || next.Current || next.State != "SUCCEEDED" || next.Outcome != "KNOWN_SUCCESS" ||
		!restoreledger.IsSHA256(next.ClaimSHA) || next.ClaimSHA == old.ClaimSHA ||
		next.Worker == "" || next.Request == "" || next.Fence <= old.Fence {
		return next, failed
	}
	claim := k6receipt.Claim{NodeID: next.NodeID, Iteration: next.Iteration, Attempt: next.Attempt, Fence: next.Fence}
	var proof k6receipt.Proof
	if json.Unmarshal(next.Proof, &proof) != nil || proof.Operation != "lookup" ||
		proof.Receipt != original.Receipt || !k6receipt.Verify(key, claim, proof) ||
		!isEffectResult(next.Result, proof.Receipt) || !isEffectResult(db.Accepted, proof.Receipt) {
		return next, failed
	}
	var timeout, recorded, consumed dispatchHistory
	for _, row := range db.History {
		switch row.Reason {
		case "TIMEOUT_REPLACEMENT":
			if timeout.Revision != 0 || row.Attempt != 1 || row.ActorKind != "scheduler" || row.Epoch <= 0 ||
				row.CreatedAt.Before(old.Deadline) {
				return next, failed
			}
			timeout = row
		case "ATTEMPT_RESULT_RECORDED":
			if recorded.Revision != 0 || row.Attempt != 2 || row.ActorKind != "worker" {
				return next, failed
			}
			recorded = row
		case "RESULT_CONSUMED":
			if consumed.Revision != 0 || row.Attempt != 2 || row.ActorKind != "scheduler" || row.Epoch <= 0 ||
				row.NewState != "SUCCEEDED" {
				return next, failed
			}
			consumed = row
		default:
			continue
		}
		if row.WorkflowID != db.Workflow || row.NodeID != hold.Attempt.NodeID || row.Iteration != hold.Attempt.Iteration ||
			row.CreatedAt.Before(t0) || row.CreatedAt.After(db.DBTime) {
			return next, failed
		}
	}
	if timeout.Revision <= rebind.Revision || recorded.Revision <= timeout.Revision || consumed.Revision <= recorded.Revision ||
		db.Revision != consumed.Revision || recorded.CreatedAt.Before(timeout.CreatedAt) ||
		consumed.CreatedAt.Before(recorded.CreatedAt) || consumed.CreatedAt.After(gate) ||
		!recorded.CreatedAt.Before(next.Deadline) ||
		!consumed.CreatedAt.Before(db.Grant.Expires.Add(-time.Duration(db.Grant.UncertaintyUS)*time.Microsecond)) {
		return next, failed
	}
	v.ResultRevision, v.ConsumedRevision, v.ConsumedAt = recorded.Revision, consumed.Revision, consumed.CreatedAt
	return next, nil
}

func isEffectResult(raw json.RawMessage, receipt k6receipt.Receipt) bool {
	var got k6receipt.Receipt
	var fields map[string]json.RawMessage
	return json.Unmarshal(raw, &got) == nil && json.Unmarshal(raw, &fields) == nil &&
		len(fields) == 9 && got == receipt
}
