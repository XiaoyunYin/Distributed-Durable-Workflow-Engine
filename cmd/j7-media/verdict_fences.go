package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"durable-agent-execution-engine/internal/j7fences"
	"durable-agent-execution-engine/internal/restoreledger"
)

type retainedFenceSample struct {
	Scope       j7fences.Scope             `json:"scope"`
	Database    j7fences.Database          `json:"database"`
	DBTime      time.Time                  `json:"database_time"`
	StartedNS   int64                      `json:"started_ns"`
	CompletedNS int64                      `json:"completed_ns"`
	Rows        map[string]json.RawMessage `json:"rows"`
}

func (d drillInputs) fenceSample(name, sha, id string) (retainedFenceSample, error) {
	var s retainedFenceSample
	if !restoreledger.IsSHA256(sha) || d.b.original(name+".json") != sha {
		return s, fmt.Errorf("fence sample %s differs from its bound digest", name)
	}
	if err := d.b.decode(name+".json", &s); err != nil {
		return s, err
	}
	h := d.ledger.Header
	authority, timeline := h.Target, d.restore.State.Timeline
	if name == "j7-source-owner" || name == "j7-source-claim" {
		authority, timeline = h.Source, d.backup.Source.Timeline
	}
	if s.Scope.Workflow != id || s.Scope.Namespace != d.work.Fixture.Namespace ||
		s.Database.SystemID != d.restore.State.SystemID || s.Database.Timeline != timeline ||
		s.StartedNS <= 0 || s.CompletedNS < s.StartedNS || s.DBTime.IsZero() {
		return s, errors.New("fence sample database, time or workflow identity differs")
	}
	err := d.witness(name, sha, authority, timeline, s.StartedNS, s.CompletedNS, restoreledger.StateProbe)
	return s, err
}

func (d drillInputs) witness(phase, sha, authority string, timeline uint32, start, end int64, kind string) error {
	count := 0
	for _, w := range d.ledger.Witnesses {
		if w.Phase != phase {
			continue
		}
		if w.EvidenceSHA != sha || !restoreledger.IsSHA256(sha) || w.Authority != authority || w.Timeline != timeline ||
			w.StartedNS != start || w.CompletedNS != end || w.Kind != kind || w.RecordedNS < end {
			return fmt.Errorf("ledger witness does not match %s", phase)
		}
		count++
	}
	if count != 1 {
		return fmt.Errorf("expected one ledger witness for %s", phase)
	}
	return nil
}

func unchangedFence(before, after retainedFenceSample, x restoreledger.Exchange) bool {
	if before.Scope != after.Scope || before.Database != after.Database ||
		before.CompletedNS > x.Outcome.SentNS || x.OutcomeSync > after.StartedNS ||
		before.CompletedNS-before.StartedNS > int64(clockBound) || after.CompletedNS-after.StartedNS > int64(clockBound) {
		return false
	}
	a, _ := json.Marshal(before.Rows)
	b, _ := json.Marshal(after.Rows)
	return len(before.Rows) != 0 && bytes.Equal(a, b)
}

func (d drillInputs) checkFences(x map[string]restoreledger.Exchange) error {
	var owner j7fences.OwnerEvidence
	var worker j7fences.WorkerEvidence
	if err := errors.Join(d.b.decode("owner.json", &owner), d.b.decode("worker.json", &worker)); err != nil {
		return err
	}
	held, claim := d.cohorts.L1, d.cohorts.F1
	s := owner.Source
	f := worker.Source
	if s.Scope.Workflow != held.WorkflowID || int(s.Scope.Partition) != held.Partition || s.Lease.Owner != held.Owner ||
		s.Lease.Epoch != held.Epoch || !s.Lease.Expires.After(s.DBTime) ||
		owner.Successor.PartitionID != s.Scope.Partition || owner.Successor.OwnerID == "" ||
		owner.Successor.OwnerID == held.Owner || owner.Successor.Epoch <= held.Epoch ||
		f.Scope.Workflow != claim.WorkflowID || f.Attempt.Node != claim.NodeID || f.Attempt.Number != 1 ||
		f.Attempt.TokenSHA != claim.ClaimSHA || !f.Attempt.Deadline.After(f.DBTime) ||
		worker.Replacement.NodeID != claim.NodeID || worker.Replacement.Iteration != claim.Iteration ||
		worker.Replacement.Attempt != 2 || !restoreledger.IsSHA256(worker.Replacement.TokenSHA256) ||
		worker.Replacement.TokenSHA256 == claim.ClaimSHA {
		return errors.New("stale probes do not join the archived lease and claim with distinct successors")
	}
	n1 := d.work.NonMarkers[5]
	var n1sync int64
	for _, op := range d.ledger.Exchanges {
		if op.Operation.RequestID == n1.RequestID {
			n1sync = op.OutcomeSync
		}
	}
	if worker.N1Request != n1.RequestID || worker.N1SyncedNS != n1sync || n1sync == 0 {
		return errors.New("worker fence did not follow N1's durable acknowledgment")
	}
	expected := []struct{ id, workflow, kind, disposition, code string }{
		{"j7-owner-acquire", held.WorkflowID, restoreledger.AcquireLease, restoreledger.Acknowledged, ""},
		{"j7-owner-stale", held.WorkflowID, restoreledger.OwnerWrite, restoreledger.Rejected, "LEASE_NOT_OWNED"},
		{"j7-owner-fresh", held.WorkflowID, restoreledger.OwnerWrite, restoreledger.Acknowledged, ""},
		{"j7-owner-release", held.WorkflowID, restoreledger.ReleaseLease, restoreledger.Acknowledged, ""},
		{"j7-worker-claim", claim.WorkflowID, restoreledger.Claim, restoreledger.Acknowledged, ""},
		{"j7-worker-stale-result", claim.WorkflowID, restoreledger.Result, restoreledger.Rejected, worker.DenialCode},
		{"j7-worker-fresh-result", claim.WorkflowID, restoreledger.Result, restoreledger.Acknowledged, ""},
	}
	if worker.DenialCode != "STALE_ATTEMPT" && worker.DenialCode != "STALE_CLAIM" {
		return errors.New("worker denial is not a typed claim fence")
	}
	for _, want := range expected {
		op := x[want.id]
		if op.Outcome == nil || op.Operation.Authority != d.ledger.Header.Target ||
			op.Operation.WorkflowID != want.workflow || op.Operation.Kind != want.kind ||
			op.Disposition() != want.disposition || op.Outcome.Code != want.code ||
			op.Outcome.SentNS < d.loss.T0NS || op.OutcomeSync > d.loss.T0NS+int64(480*time.Second) {
			return fmt.Errorf("missing or invalid fenced exchange %s", want.id)
		}
	}
	oldLease := restoreledger.LeaseRef{PartitionID: s.Scope.Partition, OwnerID: s.Lease.Owner, Epoch: s.Lease.Epoch}
	oldClaim := restoreledger.ClaimRef{NodeID: f.Attempt.Node, Iteration: f.Attempt.Iteration,
		Attempt: f.Attempt.Number, TokenSHA256: f.Attempt.TokenSHA}
	if !leaseEquals(x["j7-owner-stale"].Operation.Lease, oldLease) ||
		!leaseEquals(x["j7-owner-fresh"].Operation.Lease, owner.Successor) ||
		!leaseEquals(x["j7-owner-release"].Operation.Lease, owner.Successor) ||
		!leaseEquals(x["j7-owner-acquire"].Outcome.Lease, owner.Successor) ||
		x["j7-worker-stale-result"].Operation.Claim == nil || *x["j7-worker-stale-result"].Operation.Claim != oldClaim ||
		x["j7-worker-fresh-result"].Operation.Claim == nil ||
		*x["j7-worker-fresh-result"].Operation.Claim != worker.Replacement ||
		x["j7-worker-claim"].Outcome.Claim == nil || *x["j7-worker-claim"].Outcome.Claim != worker.Replacement ||
		x["j7-worker-stale-result"].Outcome.HTTPStatus != 409 ||
		x["j7-worker-fresh-result"].Outcome.HTTPStatus != 200 ||
		x["j7-worker-claim"].Outcome.SentNS < n1sync {
		return errors.New("fenced exchanges did not use the recorded authority references")
	}
	all := []struct{ stem, sha, id string }{
		{"j7-source-owner", s.Evidence, held.WorkflowID},
		{"j7-owner-expiry", owner.ExpirySHA, held.WorkflowID},
		{"j7-owner-before", owner.BeforeSHA, held.WorkflowID},
		{"j7-owner-after-denial", owner.AfterSHA, held.WorkflowID},
		{"j7-owner-committed", owner.FreshSHA, held.WorkflowID},
		{"j7-owner-released", owner.ReleasedSHA, held.WorkflowID},
		{"j7-owner-terminal", owner.TerminalSHA, held.WorkflowID},
		{"j7-source-claim", f.Evidence, claim.WorkflowID},
		{"j7-worker-replacement", worker.ReplacementSHA, claim.WorkflowID},
		{"j7-worker-before", worker.BeforeSHA, claim.WorkflowID},
		{"j7-worker-after-denial", worker.AfterSHA, claim.WorkflowID},
		{"j7-worker-terminal", worker.TerminalSHA, claim.WorkflowID},
	}
	samples := map[string]retainedFenceSample{}
	for _, a := range all {
		sample, err := d.fenceSample(a.stem, a.sha, a.id)
		if err != nil {
			return err
		}
		samples[a.stem] = sample
	}
	if samples["j7-source-owner"].CompletedNS > d.cut.StartedNS ||
		samples["j7-source-claim"].CompletedNS > d.cut.StartedNS ||
		samples["j7-owner-expiry"].DBTime.Before(s.Lease.Expires) ||
		samples["j7-worker-replacement"].DBTime.Before(f.Attempt.Deadline) ||
		!unchangedFence(samples["j7-owner-before"], samples["j7-owner-after-denial"], x["j7-owner-stale"]) ||
		!unchangedFence(samples["j7-worker-before"], samples["j7-worker-after-denial"], x["j7-worker-stale-result"]) ||
		samples["j7-owner-after-denial"].CompletedNS > x["j7-owner-fresh"].Outcome.SentNS ||
		samples["j7-worker-after-denial"].CompletedNS > x["j7-worker-fresh-result"].Outcome.SentNS ||
		samples["j7-owner-terminal"].StartedNS < x["j7-owner-release"].OutcomeSync ||
		samples["j7-worker-terminal"].StartedNS < x["j7-worker-fresh-result"].OutcomeSync {
		return errors.New("stale challenge changed durable state or missed its archived expiry or ordering")
	}
	for _, want := range expected {
		delete(x, want.id)
	}
	if len(x) != 0 {
		return errors.New("sealed ledger contains operations outside the frozen workload and probes")
	}
	return nil
}

func leaseEquals(got *restoreledger.LeaseRef, want restoreledger.LeaseRef) bool {
	return got != nil && *got == want
}
