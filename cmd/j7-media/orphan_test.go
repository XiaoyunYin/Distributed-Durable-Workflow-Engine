package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"os"
	"slices"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/effects/k6sink"
	"durable-agent-execution-engine/internal/k6ingress"
	"durable-agent-execution-engine/internal/k6receipt"
	"durable-agent-execution-engine/internal/restoreledger"
	"durable-agent-execution-engine/internal/state"
	"github.com/jackc/pgx/v5"
)

type orphanFixture struct {
	hold                  effectAtCut
	clock                 orphanClock
	source, pre, final    k6sink.Snapshot
	prior, rebound, end   orphanDB
	signing               ed25519.PrivateKey
	apply                 k6receipt.Proof
	rows                  []k6ingress.Record
	cut, kill, open, done time.Time
}

func makeOrphanFixture() orphanFixture {
	mark := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fromStart := func(d time.Duration) time.Time { return mark.Add(d) }
	digest := func(s string) string { return restoreledger.SHA256([]byte(s)) }
	h := effectAtCut{Attempt: attemptAtCut{WorkflowID: "e1", NodeID: state.K6Activity, Attempt: 1,
		ClaimSHA: digest("first token"), Deadline: fromStart(100 * time.Second)},
		Intent: "00ca81b8-46ca-4997-a395-0f9c10a53ce2", Resource: "resource", LogicalKey: "logical",
		CanonicalHash: digest("canonical"), ScopeHash: digest("scope"), GrantSHA: digest("grant"), Fence: 1}
	r := k6receipt.Receipt{WorkflowID: h.Attempt.WorkflowID, LogicalKey: h.LogicalKey, ResourceID: h.Resource,
		CanonicalHash: h.CanonicalHash, ScopeHash: h.ScopeHash, IntentID: h.Intent, AppliedRevision: 1,
		ApplicationID: "0ee3e34f-dc87-46c2-824f-95ed55431394"}
	raw, _ := json.Marshal(r)
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	proof := func(operation string, attempt int64) k6receipt.Proof {
		c := k6receipt.Claim{NodeID: h.Attempt.NodeID, Attempt: attempt, Fence: attempt}
		return k6receipt.Proof{Receipt: r, Operation: operation, Signature: k6receipt.Sign(key, c, operation, r)}
	}
	f := orphanFixture{hold: h, clock: orphanClock{StartNS: int64(time.Second), StartUTC: mark}, signing: key, apply: proof("apply", 1),
		cut: fromStart(60 * time.Second), kill: fromStart(76 * time.Second), open: fromStart(95 * time.Second),
		done: fromStart(145 * time.Second)}
	grant := orphanGrant{Intent: h.Intent, Subject: "system:serviceaccount:fixture:k6-approver", Decision: "APPROVED",
		Status: "GRANTED", Resource: h.Resource, Key: h.LogicalKey, Canonical: h.CanonicalHash,
		Scope: h.ScopeHash, TokenSHA: h.GrantSHA, ExpectedRevision: "0", DecidedAt: fromStart(3 * time.Second),
		Expires: fromStart(grantWindow), WindowEnd: fromStart(grantWindow), IntentEnd: fromStart(grantWindow), UncertaintyUS: 10_000}
	original := orphanAttempt{WorkflowID: "e1", NodeID: h.Attempt.NodeID, Attempt: 1, ClaimSHA: h.Attempt.ClaimSHA,
		Worker: "source", Request: "source-claim", Deadline: h.Attempt.Deadline, State: "CLAIMED",
		Class: "COOPERATING_EFFECT", Current: true, Outcome: "NONE", Fence: 1, LogicalKey: h.LogicalKey, ScopeHash: h.ScopeHash}
	fixtureDB := func(timeline uint32, at time.Duration, state, addr string) orphanDB {
		ns := f.clock.StartNS + int64(at)
		return orphanDB{SystemID: 123, Timeline: timeline, Address: addr, Database: "engine", FlushLSN: "0/2200000", DBTime: fromStart(at),
			StartedNS: ns, EndedNS: ns + 50_000, ReadOnly: true, Workflow: "e1", Namespace: "fixture",
			State: state, NodeState: state, Grant: grant,
			Sessions: []readerSession{{PID: 19, Client: "10.1.1.7", StartedAt: fromStart(at - time.Second), TLS: true}}}
	}
	first := int64(1)
	f.prior = fixtureDB(1, 58*time.Second, "WAITING_ACTIVITY", "10.1.1.15")
	f.prior.Current, f.prior.Attempts, f.prior.Revision = &first, []orphanAttempt{original}, 5
	f.rebound = fixtureDB(2, 94*time.Second, "WAITING_ACTIVITY", "10.1.1.16")
	f.rebound.Current, f.rebound.Attempts, f.rebound.Revision = &first, []orphanAttempt{original}, 5
	f.end = fixtureDB(2, 145*time.Second, "SUCCEEDED", "10.1.1.16")
	f.end.Grant.Status = "DISPATCHED"
	replacement := original
	replacement.Attempt, replacement.Fence, replacement.Current = 2, 2, false
	replacement.State, replacement.Outcome = "SUCCEEDED", "KNOWN_SUCCESS"
	replacement.ClaimSHA, replacement.Worker, replacement.Request = digest("replacement token"), "target", "replacement-claim"
	replacement.Deadline, replacement.Result = fromStart(162*time.Second), raw
	replacement.Proof, _ = json.Marshal(proof("lookup", 2))
	original.Current, original.State = false, "TIMED_OUT"
	f.end.Accepted, f.end.ResultSHA, f.end.Attempts = raw, digest("postgres jsonb accepted result"), []orphanAttempt{original, replacement}
	f.end.Revision = 9
	f.end.History = []dispatchHistory{
		{WorkflowID: "e1", Revision: 6, ActorKind: "scheduler", Epoch: 2, NodeID: h.Attempt.NodeID, Attempt: 1,
			Reason: "TIMEOUT_REPLACEMENT", CreatedAt: fromStart(101 * time.Second)},
		{WorkflowID: "e1", Revision: 7, ActorKind: "worker", NodeID: h.Attempt.NodeID, Attempt: 2,
			Reason: "ATTEMPT_RESULT_RECORDED", CreatedAt: fromStart(105 * time.Second)},
		{WorkflowID: "e1", Revision: 8, ActorKind: "scheduler", Epoch: 2, NodeID: h.Attempt.NodeID, Attempt: 2, NewState: "RUNNABLE",
			Reason: "RESULT_CONSUMED", CreatedAt: fromStart(106 * time.Second)},
		{WorkflowID: "e1", Revision: 9, ActorKind: "scheduler", Epoch: 2, NewState: "SUCCEEDED",
			Reason: "GRAPH_ADVANCED", CreatedAt: fromStart(107 * time.Second)},
	}
	call := func(id, attempt int64, op, outcome string, at time.Duration) k6sink.Call {
		return k6sink.Call{CallID: id, Operation: op, WorkflowID: "e1", LogicalKey: h.LogicalKey, IntentID: h.Intent,
			ResourceID: h.Resource, MessageID: outcome, Attempt: attempt, RequestID: op + outcome,
			BodyHash: digest(op), ConnectionHash: digest(outcome), Outcome: outcome, ObservedAt: fromStart(at)}
	}
	firstAllocation := call(1, 1, "allocate", "ALLOCATED", 40*time.Second)
	applied := call(2, 1, "apply", "APPLIED", 64*time.Second)
	secondAllocation := call(3, 2, "allocate", "ALLOCATED", 102*time.Second)
	secondAllocation.RequestID = "replacement-allocation"
	lookup := call(4, 2, "lookup", "FOUND", 104*time.Second)
	result := digest(string(raw))
	applied.ResponseHash, lookup.ResponseHash = &result, &result
	applied.ApplicationID, lookup.ApplicationID = &r.ApplicationID, &r.ApplicationID
	f.source = k6sink.Snapshot{
		Resources:   []k6sink.ResourceState{{ResourceID: h.Resource, LatestFence: 1, InitialStateHash: digest("before"), FinalStateHash: digest("before")}},
		Allocations: []k6sink.Allocation{{IntentID: h.Intent, GrantHash: h.GrantSHA, Attempt: 1, ResourceID: h.Resource, Fence: 1}},
		Calls:       []k6sink.Call{firstAllocation},
		RequestBindings: []k6sink.RequestBinding{
			{RequestID: firstAllocation.RequestID, BodyHash: firstAllocation.BodyHash},
		},
	}
	f.pre = f.source
	f.pre.Resources = slices.Clone(f.source.Resources)
	f.pre.Resources[0].Revision, f.pre.Resources[0].FinalStateHash = 1, digest("after")
	f.pre.Calls = append(slices.Clone(f.source.Calls), applied)
	f.pre.RequestBindings = append(slices.Clone(f.source.RequestBindings), k6sink.RequestBinding{RequestID: applied.RequestID, BodyHash: applied.BodyHash})
	f.pre.Journal = []k6sink.Mutation{{ResourceID: h.Resource, Revision: 1, OldStateHash: digest("before"), StateHash: digest("after"), IntentID: h.Intent, LogicalKey: h.LogicalKey}}
	f.pre.Receipts = []k6sink.Receipt{r}
	f.final = f.pre
	f.final.Resources = slices.Clone(f.pre.Resources)
	f.final.Resources[0].LatestFence = 2
	f.final.Calls = append(slices.Clone(f.pre.Calls), secondAllocation, lookup)
	f.final.Allocations = append(slices.Clone(f.pre.Allocations), k6sink.Allocation{IntentID: h.Intent, GrantHash: h.GrantSHA, Attempt: 2, ResourceID: h.Resource, Fence: 2})
	f.final.RequestBindings = append(slices.Clone(f.pre.RequestBindings),
		k6sink.RequestBinding{RequestID: secondAllocation.RequestID, BodyHash: secondAllocation.BodyHash},
		k6sink.RequestBinding{RequestID: lookup.RequestID, BodyHash: lookup.BodyHash})
	const session = "0123456789abcdef0123456789abcdef"
	const relayPeer, sinkPeer = "spiffe://fixture/relay", "spiffe://fixture/sink"
	for i, c := range []k6sink.Call{applied, lookup} {
		inbound := k6ingress.Record{Session: session, Ordinal: uint64(i + 1), Method: "POST", Operation: c.Operation,
			Attempt: c.Attempt, IntentID: h.Intent, IntentHash: digest(h.Intent), GrantHash: h.GrantSHA,
			KeyHash: digest(h.LogicalKey), ResourceHash: digest(h.Resource), RequestIDHash: digest(c.RequestID),
			InvocationHash: digest(c.MessageID), ConnectionHash: c.ConnectionHash, WorkerBinding: digest(c.Operation + "worker"),
			RelayPeer: relayPeer, SinkPeer: sinkPeer}
		for j, event := range []string{"inbound", "sink_send", "sink_response", "worker_delivery", "close"} {
			r := inbound
			r.Event, r.Sequence, r.Time = event, uint64(len(f.rows)+1), c.ObservedAt.Add(time.Duration(j-2)*time.Millisecond)
			if j < 2 {
				r.BodyHash, r.WireHash, r.Length, r.WireLength = c.BodyHash, digest("wire"), 20, 100
			} else if j < 4 {
				r.BodyHash, r.WireHash, r.Length, r.WireLength = *c.ResponseHash, digest("wire-reply"), 20, 100
				r.ApplicationID, r.Status = *c.ApplicationID, 200
			}
			f.rows = append(f.rows, r)
		}
	}
	f.rows = append(f.rows, k6ingress.Record{Session: session, Event: "seal", Sequence: 11, HighWater: 10,
		Time: f.done.Add(-time.Second), RelayPeer: relayPeer, SinkPeer: sinkPeer})
	return f
}

func TestOrphanRequiresSealedIngressAndOneRetainedApplication(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*orphanFixture)
	}{
		{"lookup-found", nil},
		{"extra-inbound", func(f *orphanFixture) { f.rows = append(f.rows, f.rows[5]) }},
		{"missing-seal", func(f *orphanFixture) { f.rows = f.rows[:10] }},
		{"duplicate-apply", func(f *orphanFixture) { f.final.Calls[3].Operation = "apply" }},
		{"extra-mutation", func(f *orphanFixture) { f.final.Journal = append(f.final.Journal, f.final.Journal[0]) }},
		{"changed-state", func(f *orphanFixture) { f.final.Resources[0].FinalStateHash = f.final.Resources[0].InitialStateHash }},
		{"wrong-application", func(f *orphanFixture) { f.rows[8].ApplicationID = "14722928-3d12-4531-9674-231670893e7d" }},
		{"wrong-lookup-connection", func(f *orphanFixture) { f.rows[7].ConnectionHash = f.rows[2].ConnectionHash }},
		{"apply-before-cut", func(f *orphanFixture) { f.rows[0].Time = f.cut.Add(-time.Millisecond) }},
		{"lookup-before-rebind", func(f *orphanFixture) { f.open = f.done }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := makeOrphanFixture()
			if tc.mutate != nil {
				tc.mutate(&f)
			}
			original, lookup, err := verifyRetainedSink(f.source, f.pre, f.final, f.hold, f.apply.Receipt, f.end.Attempts[1])
			if err == nil {
				var wire bytes.Buffer
				enc := json.NewEncoder(&wire)
				for _, r := range f.rows {
					if err := enc.Encode(r); err != nil {
						t.Fatal(err)
					}
				}
				var decoded []k6ingress.Record
				decoded, err = k6ingress.ReadJournal(&wire, "spiffe://fixture/relay", "spiffe://fixture/sink")
				if err == nil {
					err = checkIngress(decoded, f.hold, original, lookup, f.cut, f.kill, f.open, f.done)
				}
			}
			if (err == nil) != (tc.mutate == nil) {
				t.Fatalf("orphan ingress and journal = %v", err)
			}
		})
	}
}

func TestOrphanRequiresSignedReplacementAndOrderedConsumption(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*orphanFixture)
	}{
		{"signed-lookup", nil},
		{"old-claim", func(f *orphanFixture) { f.end.Attempts[1].ClaimSHA = f.hold.Attempt.ClaimSHA }},
		{"old-signature", func(f *orphanFixture) { f.end.Attempts[1].Proof, _ = json.Marshal(f.apply) }},
		{"wrong-fence", func(f *orphanFixture) { f.end.Attempts[1].Fence++ }},
		{"source-result-mismatch", func(f *orphanFixture) { f.end.ResultSHA = f.hold.GrantSHA }},
		{"second-consumption", func(f *orphanFixture) { f.end.History = append(f.end.History, f.end.History[2]) }},
		{"missing-terminal-graph", func(f *orphanFixture) { f.end.History = f.end.History[:3] }},
		{"early-timeout", func(f *orphanFixture) { f.end.History[0].CreatedAt = f.cut }},
		{"late-consumption", func(f *orphanFixture) { f.end.History[2].CreatedAt = f.done }},
		{"unresolved", func(f *orphanFixture) { f.end.Unresolved++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := makeOrphanFixture()
			sourceResultSHA := f.end.ResultSHA
			if tc.mutate != nil {
				tc.mutate(&f)
			}
			var out orphanVerdict
			key := f.signing.Public().(ed25519.PublicKey)
			_, err := verifyRecoveredEffect(f.end, f.hold, f.apply, sourceResultSHA, key, f.rebound,
				f.kill, f.done.Add(-time.Second), &out)
			if (err == nil) != (tc.mutate == nil) {
				t.Fatalf("replacement verifier = %v", err)
			}
			if tc.mutate == nil && (out.ResultRevision != 7 || out.ConsumedRevision != 8) {
				t.Fatalf("wrong result history = %+v", out)
			}
		})
	}
}

func TestOrphanReaderRequiresTargetTLSAndUnchangedApproval(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*orphanFixture)
	}{
		{"same-grant-new-reader", nil},
		{"source-timeline", func(f *orphanFixture) { f.rebound.Timeline = f.prior.Timeline }},
		{"source-address", func(f *orphanFixture) { f.rebound.Address = f.prior.Address }},
		{"unprotected-reader", func(f *orphanFixture) { f.rebound.Sessions[0].TLS = false }},
		{"old-reader", func(f *orphanFixture) { f.rebound.Sessions[0] = f.prior.Sessions[0] }},
		{"new-grant", func(f *orphanFixture) { f.rebound.Grant.TokenSHA = f.end.ResultSHA }},
		{"expired-grant", func(f *orphanFixture) { f.rebound.Grant.Expires = f.cut }},
		{"unbounded-clock", func(f *orphanFixture) { f.rebound.EndedNS += int64(time.Second) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := makeOrphanFixture()
			if tc.mutate != nil {
				tc.mutate(&f)
			}
			input := orphanInputs{cfg: orphanConfig{Clock: f.clock, Namespace: "fixture", SinkClient: "10.1.1.7"}, hold: f.hold}
			recovery := reboundInputs{restored: restoreReceipt{
				State: targetState{SystemID: 123, Timeline: 2}, CompletedNS: f.clock.StartNS + int64(90*time.Second)},
				source: orphanObservation{DB: f.prior}}
			if err := input.checkRebind(f.rebound, recovery); (err == nil) != (tc.mutate == nil) {
				t.Fatalf("reader rebind = %v", err)
			}
		})
	}
}

func TestPostgresOrphanObserverQuery(t *testing.T) {
	dsn := os.Getenv("DURABLE_DATABASE_URL")
	if dsn == "" {
		t.Skip("DURABLE_DATABASE_URL is required for the migrated E1 observer query")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	store, err := state.NewCheckerFromURL(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var raw []byte
	err = store.Pool().QueryRow(ctx, orphanSQL, "absent-"+state.NewID(), "absent-"+state.NewID(), state.K6Activity, 0).Scan(&raw)
	if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("observer SQL on the migrated database: %v", err)
	}
}
