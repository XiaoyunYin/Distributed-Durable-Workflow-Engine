package main

import (
	"bytes"
	"encoding/json"

	"durable-agent-execution-engine/internal/j7fences"
	"durable-agent-execution-engine/internal/restoreledger"
)

func (f *drillFixture) fence(name, authority string, scope j7fences.Scope, timeline uint32, at int64) string {
	r := retainedFenceSample{Scope: scope, Database: j7fences.Database{SystemID: 123, Timeline: timeline},
		DBTime: f.clock.wall(at), StartedNS: at, CompletedNS: at + 10,
		Rows: map[string]json.RawMessage{"outbox": json.RawMessage(`[]`)}}
	sha := f.put(name+".json", r)
	f.witness(authority, name, sha, timeline, r.StartedNS, r.CompletedNS)
	return sha
}

func (f *drillFixture) sourceFenceFixtures(cohorts cohortsAtCut) (j7fences.OwnerEvidence, j7fences.WorkerEvidence) {
	h := &f.header
	src := j7fences.Database{SystemID: 123, Timeline: 1}
	target := j7fences.Target{Database: j7fences.Database{SystemID: 123, Timeline: 2},
		CutNS: f.ns(59), T0NS: f.ns(76)}
	l1 := j7fences.Scope{Workflow: "l1", Namespace: "j7", Partition: 5}
	f1 := j7fences.Scope{Workflow: "f1", Namespace: "j7", Node: "pure.echo"}
	ls := j7fences.Lease{Owner: cohorts.L1.Owner, Epoch: cohorts.L1.Epoch, Expires: cohorts.L1.Expires}
	o := j7fences.OwnerEvidence{Source: j7fences.SourceOwner{Scope: l1, DBTime: f.clock.wall(f.ns(55)),
		Authority: h.Source, BootID: h.BootID, Database: src, Lease: ls,
		StartedNS: f.ns(55), EndedNS: f.ns(55) + 10, Evidence: f.fence("j7-source-owner", h.Source, l1, 1, f.ns(55))},
		Target: target, Successor: restoreledger.LeaseRef{PartitionID: l1.Partition, OwnerID: "l1-new-owner", Epoch: 2}}
	w := j7fences.WorkerEvidence{
		Source: j7fences.SourceClaim{Scope: f1, DBTime: f.clock.wall(f.ns(56)), Authority: h.Source, BootID: h.BootID,
			Database: src, Attempt: j7fences.Attempt{Node: "pure.echo", Number: 1,
				TokenSHA: cohorts.F1.ClaimSHA, Deadline: cohorts.F1.Deadline},
			StartedNS: f.ns(56), EndedNS: f.ns(56) + 10, Evidence: f.fence("j7-source-claim", h.Source, f1, 1, f.ns(56))},
		Target: target, DenialCode: "STALE_CLAIM",
		Replacement: restoreledger.ClaimRef{NodeID: "pure.echo", Attempt: 2, TokenSHA256: testDigest},
		N1Request:   "n1-submit",
	}
	return o, w
}

func (f *drillFixture) targetFences(owner j7fences.OwnerEvidence, worker j7fences.WorkerEvidence) {
	l1, f1 := owner.Source.Scope, worker.Source.Scope
	h := &f.header
	oldLease := restoreledger.LeaseRef{PartitionID: l1.Partition, OwnerID: owner.Source.Lease.Owner, Epoch: owner.Source.Lease.Epoch}
	oldClaim := restoreledger.ClaimRef{NodeID: f1.Node, Attempt: 1, TokenSHA256: worker.Source.Attempt.TokenSHA}
	aa, ok := restoreledger.Acknowledged, restoreledger.Rejected
	sendLease := func(at int64, label, kind, disposition, code string, sent, result *restoreledger.LeaseRef) {
		x := restoreledger.Outcome{Disposition: disposition, Code: code, Lease: result}
		if disposition == aa {
			x.DurableID = "l1"
		}
		f.ledger.exchange(at, restoreledger.Operation{RequestID: "j7-owner-" + label, Kind: kind,
			Phase: "j7-owner-fence", Authority: h.Target, WorkflowID: "l1", Lease: sent, RequestSHA256: testDigest}, x)
	}
	sendClaim := func(at int64, label, kind, disposition, code string, status int, sent, result *restoreledger.ClaimRef) {
		x := restoreledger.Outcome{Disposition: disposition, Code: code, Claim: result, HTTPStatus: status}
		if disposition == aa {
			x.DurableID = "f1"
		}
		f.ledger.exchange(at, restoreledger.Operation{RequestID: "j7-worker-" + label, Kind: kind,
			Phase: "j7-worker-fence", Authority: h.Target, WorkflowID: "f1", Claim: sent, RequestSHA256: testDigest}, x)
	}
	at := f.ns(97)
	owner.ExpirySHA = f.fence("j7-owner-expiry", h.Target, l1, 2, at)
	sendLease(at+100, "acquire", restoreledger.AcquireLease, aa, "", nil, &owner.Successor)
	owner.BeforeSHA = f.fence("j7-owner-before", h.Target, l1, 2, at+200)
	sendLease(at+300, "stale", restoreledger.OwnerWrite, ok, "LEASE_NOT_OWNED", &oldLease, nil)
	owner.AfterSHA = f.fence("j7-owner-after-denial", h.Target, l1, 2, at+400)
	sendLease(at+500, "fresh", restoreledger.OwnerWrite, aa, "", &owner.Successor, nil)
	owner.FreshSHA = f.fence("j7-owner-committed", h.Target, l1, 2, at+600)
	sendLease(at+700, "release", restoreledger.ReleaseLease, aa, "", &owner.Successor, nil)
	owner.ReleasedSHA = f.fence("j7-owner-released", h.Target, l1, 2, at+800)
	at = f.ns(102)
	worker.ReplacementSHA = f.fence("j7-worker-replacement", h.Target, f1, 2, at)
	sendClaim(at+100, "claim", restoreledger.Claim, aa, "", 200, nil, &worker.Replacement)
	worker.BeforeSHA = f.fence("j7-worker-before", h.Target, f1, 2, at+200)
	sendClaim(at+300, "stale-result", restoreledger.Result, ok, worker.DenialCode, 409, &oldClaim, nil)
	worker.AfterSHA = f.fence("j7-worker-after-denial", h.Target, f1, 2, at+400)
	sendClaim(at+500, "fresh-result", restoreledger.Result, aa, "", 200, &worker.Replacement, nil)
	owner.TerminalSHA = f.fence("j7-owner-terminal", h.Target, l1, 2, f.ns(108))
	worker.TerminalSHA = f.fence("j7-worker-terminal", h.Target, f1, 2, f.ns(109))
	f.put("owner.json", owner)
	f.put("worker.json", worker)
}

func (f *drillFixture) orphanFixtures(o orphanFixture) (orphanObservation, orphanVerdict) {
	h := &f.header
	src := orphanObservation{Schema: "j7.e1-source-hold.v1", BootID: h.BootID, Authority: h.Source, DB: o.prior,
		SinkSHA: f.put("source-sink.json", o.source), CompletedNS: o.prior.EndedNS + 2}
	sourceSHA := f.put("source-engine.json", src)
	f.witness(h.Source, orphanSourcePhase, sourceSHA, 1, src.DB.StartedNS, src.CompletedNS)
	dst := orphanObservation{Schema: "j7.e1-reader-rebind.v1", BootID: h.BootID, Authority: h.Target,
		DB: o.rebound, CompletedNS: o.rebound.EndedNS + 2, SourceSHA: sourceSHA}
	zero := 0
	var stream bytes.Buffer
	encode := json.NewEncoder(&stream)
	for _, row := range o.rows {
		if err := encode.Encode(row); err != nil {
			f.t.Fatal(err)
		}
	}
	f.files["effect-ingress.jsonl"] = stream.Bytes()
	sinkSHA := f.put("final-sink.json", o.final)
	f.put("sink-quiet-start.json", o.final)
	return dst, orphanVerdict{Schema: "j7.e1-reconciliation.v1", BootID: h.BootID, Workflow: "e1", SourceSHA: sourceSHA,
		EngineSHA: f.put("final-engine.json", o.end), SinkSHA: sinkSHA, IngressSHA: restoreledger.SHA256(stream.Bytes()),
		OriginalResultSHA: o.end.ResultSHA, RestoredResultSHA: o.end.ResultSHA, ApplicationID: o.apply.Receipt.ApplicationID,
		LookupCallID: o.final.Calls[3].CallID, ReplacementAttempt: 2, ReplacementFence: 2,
		ReplacementClaim: o.end.Attempts[1].ClaimSHA, ReplacementApplies: &zero,
		Mutations: 1, Revision: 1, StateHash: o.final.Resources[0].FinalStateHash, ResultRevision: 7, ConsumedRevision: 8,
		ConsumedAt: o.end.History[2].CreatedAt, CompletedNS: f.ns(146), Unresolved: []string{}}
}
