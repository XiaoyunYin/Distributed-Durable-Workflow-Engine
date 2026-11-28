package j7fences

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/api"
	"durable-agent-execution-engine/internal/restoreledger"
	"durable-agent-execution-engine/internal/state"
)

type testJournal struct {
	header    restoreledger.Header
	now       int64
	sends     []restoreledger.Operation
	outcomes  []restoreledger.Outcome
	witnesses []restoreledger.Witness
	files     map[string][]byte
}

func journalFixture() *testJournal {
	return &testJournal{header: restoreledger.Header{Source: "source-r1", Target: "target-r1", BootID: "linux-boot"},
		now: int64(10 * time.Second), files: make(map[string][]byte)}
}

func (j *testJournal) Header() restoreledger.Header { return j.header }
func (j *testJournal) NowNS() (int64, error) {
	j.now += int64(time.Millisecond)
	return j.now, nil
}
func (j *testJournal) RecordSend(o restoreledger.Operation) (int64, error) {
	j.sends = append(j.sends, o)
	return j.NowNS()
}
func (j *testJournal) RecordOutcome(o restoreledger.Outcome) (int64, error) {
	j.outcomes = append(j.outcomes, o)
	return j.NowNS()
}
func (j *testJournal) RecordWitness(w restoreledger.Witness) error {
	j.witnesses = append(j.witnesses, w)
	return nil
}
func (j *testJournal) save(name string, body []byte) error {
	if j.files[name] != nil {
		return errors.New("duplicate artifact")
	}
	j.files[name] = bytes.Clone(body)
	return nil
}

var (
	dbSource      = Database{SystemID: 42, Timeline: 1}
	dbTarget      = Database{SystemID: 42, Timeline: 2}
	targetFixture = Target{Database: dbTarget, CutNS: int64(2 * time.Second), T0NS: int64(3 * time.Second)}
	utcFixture    = time.Date(2026, 11, 28, 12, 0, 30, 0, time.UTC)
	sourceDigest  = strings.Repeat("a", 64)
)

func newSample(j *testJournal, scope Scope, values map[string]any) sample {
	a, _ := j.NowNS()
	b, _ := j.NowNS()
	row := sample{Scope: scope, Database: dbTarget, DBTime: utcFixture, StartedNS: a, CompletedNS: b,
		Rows: map[string]json.RawMessage{}}
	for key, value := range values {
		row.Rows[key] = payload(value)
	}
	return row
}

type fakeOwner struct {
	t        *testing.T
	j        *testJournal
	lease    Lease
	workflow Workflow
	history  []history
	changes  []state.OwnerTransitionInput
	dirty    bool
	reads    int
}

func (f *fakeOwner) AcquireLease(_ context.Context, partition int16, owner string, ttl time.Duration) (state.Lease, bool, error) {
	if ttl != probeLease || f.reads < 2 || f.j.sends[len(f.j.sends)-1].Kind != restoreledger.AcquireLease {
		f.t.Fatal("acquisition preceded its ledger row or changed the lease period")
	}
	f.lease = Lease{owner, f.lease.Epoch + 1, utcFixture.Add(ttl)}
	return state.Lease{PartitionID: partition, OwnerID: owner, Epoch: f.lease.Epoch, LeaseExpiresAt: f.lease.Expires}, true, nil
}
func (f *fakeOwner) ReleaseLease(_ context.Context, r state.LeaseRef) error {
	if r.OwnerID != f.lease.Owner || r.Epoch != f.lease.Epoch || f.j.sends[len(f.j.sends)-1].Kind != restoreledger.ReleaseLease {
		f.t.Fatal("unbound release")
	}
	f.lease.Owner, f.lease.Expires = "", time.Time{}
	return nil
}
func (f *fakeOwner) ApplyOwnerTransition(_ context.Context, r state.OwnerTransitionInput) error {
	f.changes = append(f.changes, r)
	if r.Lease.OwnerID != f.lease.Owner {
		if f.dirty {
			f.workflow.Revision++
		}
		return state.ErrLeaseNotOwned
	}
	f.workflow.Revision++
	f.history = append(f.history, history{Revision: f.workflow.Revision, Actor: r.ActorID,
		ActorKind: "scheduler", Epoch: r.Lease.Epoch, Reason: r.Reason, NewState: r.NewState, At: utcFixture})
	return nil
}

func TestOwnerProbePairsFencedWritesAndReleases(t *testing.T) {
	for _, tc := range []struct {
		name  string
		dirty bool
	}{
		{"clean", false}, {"denied_but_changed", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			j := journalFixture()
			scope := Scope{Namespace: "j7", Workflow: "l1", Partition: 3}
			f := &fakeOwner{t: t, j: j, dirty: tc.dirty,
				lease:    Lease{"old-owner", 4, utcFixture.Add(-time.Second)},
				workflow: Workflow{"l1", "j7", 3, state.StateRunnable, 1},
				history:  []history{{Revision: 1, Reason: "CREATED", NewState: state.StateRunnable}}}
			src := SourceOwner{Authority: j.header.Source, BootID: j.header.BootID, Database: dbSource, Scope: scope,
				Lease: f.lease, DBTime: f.lease.Expires.Add(-time.Second), StartedNS: 100, EndedNS: 101, Evidence: sourceDigest}
			p := &Probe{journal: j, save: j.save, repo: f, read: func(_ context.Context, got Scope, withLease bool) (sample, error) {
				f.reads++
				rows := map[string]any{"workflow": f.workflow, "outbox": []int{}, "attempts": []int{}, "history": f.history}
				if withLease {
					rows["lease"] = f.lease
				}
				s := newSample(j, got, rows)
				if f.reads == 1 {
					s.DBTime = src.Lease.Expires.Add(-time.Millisecond)
				}
				return s, nil
			}}
			out, err := p.RunOwner(t.Context(), src, targetFixture)
			if tc.dirty {
				if err == nil || !strings.Contains(err.Error(), "changed across") || len(f.changes) != 1 || out.AfterSHA == "" || f.lease.Owner != "" {
					t.Fatalf("mutating denial or lease release lost: %+v, %v", out, err)
				}
				return
			}
			if err != nil || len(f.changes) != 2 || out.FreshRevision != 2 || out.ReleasedSHA == "" || f.lease.Owner != "" {
				t.Fatalf("owner probe: %+v, %v", out, err)
			}
			a, b := f.changes[0], f.changes[1]
			if a.Lease.OwnerID != src.Lease.Owner || b.Lease.OwnerID == a.Lease.OwnerID || b.Lease.Epoch <= a.Lease.Epoch ||
				j.outcomes[1].Code != "LEASE_NOT_OWNED" || j.outcomes[1].Disposition != restoreledger.Rejected {
				t.Fatalf("weak owner denial: %+v", j.outcomes)
			}
			a.Lease = b.Lease
			if !reflect.DeepEqual(a, b) {
				t.Fatal("positive owner request changed beyond its lease")
			}
			f.workflow.State, f.workflow.Revision = state.StateSucceeded, 3
			f.history = append(f.history, history{Revision: 3, Actor: "deployed", ActorKind: "scheduler",
				Epoch: out.Successor.Epoch + 1, NewState: state.StateSucceeded, Reason: "RESULT_CONSUMED"})
			final, err := p.FinishOwner(t.Context(), out)
			if err != nil || final.TerminalSHA == "" {
				t.Fatalf("finish owner: %+v, %v", final, err)
			}
		})
	}
}

const (
	svc      = "system:serviceaccount:j7:worker"
	subject  = svc + ":pod:worker-0:uid:worker-uid"
	oldToken = "31ba6f40-033f-4a3a-9a9a-d3416328cf72"
	newToken = "f86b2816-90c3-4ded-8fd7-66ef75fdc97b"
)

type workerBackend struct {
	mu      sync.Mutex
	t       *testing.T
	claimed bool
	results []state.ResultInput
}

func (b *workerBackend) CreateWorkflow(context.Context, state.CreateWorkflowInput) (state.CreateWorkflowResult, error) {
	panic("unexpected submission")
}
func (b *workerBackend) GetWorkflow(context.Context, string) (state.Workflow, error) {
	panic("unexpected workflow route")
}
func (b *workerBackend) ListHistory(context.Context, string, int64, int) ([]state.TransitionRecord, error) {
	panic("unexpected history route")
}
func (b *workerBackend) HeartbeatAttempt(context.Context, state.HeartbeatInput) (time.Time, error) {
	panic("unexpected heartbeat")
}
func (b *workerBackend) ClaimAttempt(_ context.Context, in state.ClaimInput) (state.ClaimResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if in.WorkerID != subject || in.RequestID != claimID || in.ExpectedAttempt != 2 ||
		in.AttemptLease != workerLease || in.WorkflowID != "f1" || in.NodeID != "pure.echo" {
		b.t.Errorf("wrong actual claim route or authority: %+v", in)
		return state.ClaimResult{}, state.ErrInvalidAttemptState
	}
	b.claimed = true
	return state.ClaimResult{AttemptNumber: 2, ClaimToken: newToken, EffectClass: state.EffectPure,
		HeartbeatDeadline: utcFixture.Add(workerLease)}, nil
}
func (b *workerBackend) RecordResultReceipt(_ context.Context, in state.ResultInput) (state.ResultReceipt, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.results = append(b.results, in)
	if in.AttemptNumber == 1 && in.ClaimToken == oldToken {
		return state.ResultReceipt{}, state.ErrAttemptNotCurrent
	}
	if in.AttemptNumber != 2 || in.ClaimToken != newToken || in.AttemptState != state.AttemptSucceeded {
		return state.ResultReceipt{}, state.ErrInvalidAttemptState
	}
	return state.ResultReceipt{Disposition: state.ResultAccepted, WorkflowID: in.WorkflowID, NodeID: in.NodeID,
		Iteration: in.Iteration, AttemptNumber: in.AttemptNumber, AttemptState: in.AttemptState, Payload: in.Payload}, nil
}

func authenticatedFixture(t *testing.T, backend *workerBackend) WorkerControl {
	t.Helper()
	const issuer = "https://test-issuer.example"
	const aud = "j7-worker"
	claims := payload(map[string]any{"iss": issuer, "sub": svc, "aud": []string{aud}, "exp": time.Now().Add(time.Hour).Unix()})
	bearer := "header." + base64.RawURLEncoding.EncodeToString(claims) + ".signature"
	remote := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/apis/authentication.k8s.io/v1/tokenreviews" || r.Header.Get("Authorization") != "Bearer reviewer" {
			t.Error("wrong token review route or credential")
			w.WriteHeader(401)
			return
		}
		var request struct{ Spec struct{ Token string } }
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.Spec.Token != bearer {
			t.Error("wrong reviewed token")
			w.WriteHeader(401)
			return
		}
		_, _ = w.Write([]byte(`{"status":{"authenticated":true,"audiences":["j7-worker"],"user":{"username":"system:serviceaccount:j7:worker","extra":{
			"authentication.kubernetes.io/pod-name":["worker-0"],"authentication.kubernetes.io/pod-uid":["worker-uid"]}}}}`))
	}))
	t.Cleanup(remote.Close)
	dir := t.TempDir()
	ca := filepath.Join(dir, "reviewer.pem")
	credential := filepath.Join(dir, "reviewer")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: remote.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(credential, []byte("reviewer"), 0o600); err != nil {
		t.Fatal(err)
	}
	reviewer, err := api.NewTokenReviewer(api.IdentityConfig{APIURL: remote.URL, CAFile: ca, ReviewerTokenFile: credential,
		Issuer: issuer, Audience: aud, Namespace: "j7", ServiceAccount: "worker"})
	if err != nil {
		t.Fatal(err)
	}
	app := httptest.NewTLSServer(api.NewServer(backend).WithRoutes(api.RoutesWorkerControl).WithAuthentication(reviewer).Handler())
	t.Cleanup(app.Close)
	return WorkerControl{Origin: app.URL, Client: app.Client(), Identity: subject,
		Bearer: func(context.Context) (string, error) { return bearer, nil }}
}

func TestWorkerProbeUsesAuthenticatedAPIAndExactArchivedToken(t *testing.T) {
	j := journalFixture()
	scope := Scope{Namespace: "j7", Workflow: "f1", Partition: 2, Node: "pure.echo"}
	input := json.RawMessage(`{"echo":7}`)
	old := Attempt{Node: scope.Node, Number: 1, State: state.AttemptClaimed, Class: state.EffectPure, Current: true,
		TokenSHA: restoreledger.SHA256([]byte(oldToken)), Deadline: utcFixture.Add(-time.Second), Worker: "original-pod", Request: "old-request", Outcome: "NONE"}
	n1 := restoreledger.Exchange{Operation: restoreledger.Operation{RequestID: "n1-create", WorkflowID: "n1", Namespace: "j7",
		Kind: restoreledger.Submit, Authority: j.header.Target}, SendSynced: targetFixture.T0NS + 1,
		Outcome: &restoreledger.Outcome{RequestID: "n1-create", Disposition: restoreledger.Acknowledged, HTTPStatus: 201,
			DurableID: "n1", SentNS: targetFixture.T0NS + 2, CompletedNS: targetFixture.T0NS + 3},
		OutcomeSync: targetFixture.T0NS + 4}
	backend := &workerBackend{t: t}
	control := authenticatedFixture(t, backend)
	src := SourceClaim{Authority: j.header.Source, BootID: j.header.BootID, Database: dbSource, Scope: scope, Attempt: old,
		DBTime: old.Deadline.Add(-time.Second), StartedNS: 100, EndedNS: 101, Evidence: sourceDigest}
	p := &Probe{journal: j, save: j.save, read: func(_ context.Context, got Scope, _ bool) (sample, error) {
		backend.mu.Lock()
		defer backend.mu.Unlock()
		retired := old
		retired.State, retired.Current = state.AttemptTimedOut, false
		next := Attempt{Node: scope.Node, Number: 2, State: state.AttemptDispatchable, Class: state.EffectPure,
			Current: true, Deadline: utcFixture.Add(30 * time.Second), Outcome: "NONE"}
		n := node{ID: scope.Node, Current: 2, Input: input}
		w := Workflow{"f1", "j7", 2, state.StateWaitingActivity, 5}
		h := []history{{Revision: 5, ActorKind: "scheduler", Epoch: 4, Reason: "TIMEOUT_REPLACEMENT",
			At: utcFixture, Node: scope.Node, Attempt: 1}}
		if backend.claimed {
			next.State, next.TokenSHA, next.Worker, next.Request, next.Deadline =
				state.AttemptClaimed, restoreledger.SHA256([]byte(newToken)), subject, claimID, utcFixture.Add(workerLease)
		}
		if len(backend.results) == 2 {
			next.State, next.Current, next.Outcome, next.Result = state.AttemptSucceeded, false, "KNOWN_SUCCESS", input
			n.Current, n.Result = 0, input
			w.State, w.Revision = state.StateSucceeded, 7
			h = append(h, history{Revision: 6, Actor: subject, ActorKind: "worker", Reason: "ATTEMPT_RESULT_RECORDED",
				Node: scope.Node, Attempt: 2}, history{Revision: 7, ActorKind: "scheduler", Epoch: 5, Reason: "RESULT_CONSUMED",
				Node: scope.Node, Attempt: 2, NewState: state.StateSucceeded})
		}
		return newSample(j, got, map[string]any{"workflow": w, "nodes": []node{n}, "attempts": []Attempt{retired, next},
			"history": h, "outbox": []outbox{{ID: "new-event", Revision: 5, Kind: "attempt.dispatch",
				Payload: []byte(`{"workflow_id":"f1","node_id":"pure.echo","iteration":0,"attempt_number":2}`)}},
			"publications": json.RawMessage(`[{"event_id":"new-event","broker_acknowledged":true}]`)}), nil
	}}
	in := WorkerInput{Source: src, Target: targetFixture, ArchivedToken: oldToken, N1Workflow: "n1", N1Ack: n1, Result: input, Control: control}
	invalid := in
	invalid.ArchivedToken = newToken
	if _, err := p.RunWorker(t.Context(), invalid); err == nil || len(j.sends) != 0 {
		t.Fatal("different token reached the worker API")
	}
	invalid = in
	invalid.N1Ack.OutcomeSync = 0
	if _, err := p.RunWorker(t.Context(), invalid); err == nil || len(j.sends) != 0 {
		t.Fatal("F1 was released without a durable N1 acknowledgment")
	}
	got, err := p.RunWorker(t.Context(), in)
	if err != nil || got.DenialCode != "STALE_ATTEMPT" || got.TerminalSHA == "" || len(j.outcomes) != 3 ||
		j.outcomes[1].Disposition != restoreledger.Rejected || j.outcomes[2].Disposition != restoreledger.Acknowledged {
		t.Fatalf("worker fence: %+v, %v, %+v", got, err, j.outcomes)
	}
	if len(backend.results) != 2 || backend.results[0].ClaimToken != oldToken || backend.results[1].ClaimToken != newToken ||
		!resultEqual(backend.results[0].Payload, backend.results[1].Payload) {
		t.Fatal("stale and fresh worker controls changed the valid result or exact claim references")
	}
	for _, artifact := range j.files {
		if bytes.Contains(artifact, []byte(oldToken)) || bytes.Contains(artifact, []byte(newToken)) {
			t.Fatal("raw claim token leaked into the probe evidence")
		}
	}
}

func TestWorkerDenialRejectsMissingRouteWrongAuthAndOutage(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		code   string
	}{
		{"wrong_auth", 401, "UNAUTHENTICATED"}, {"missing_workflow", 404, "NOT_FOUND"},
		{"wrong_conflict", 409, "STALE_REVISION"}, {"outage", 503, "STALE_ATTEMPT"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": tc.code}})
			}))
			defer server.Close()
			p := &Probe{journal: journalFixture()}
			s := Scope{Workflow: "f1", Namespace: "j7", Node: "pure.echo"}
			c := WorkerControl{Origin: server.URL, Client: server.Client(), Bearer: func(context.Context) (string, error) { return "fixture", nil }}
			url, err := c.httpPath(s)
			if err != nil {
				t.Fatal(err)
			}
			ref := restoreledger.ClaimRef{NodeID: s.Node, Attempt: 1, TokenSHA256: restoreledger.SHA256([]byte(oldToken))}
			if _, err := p.resultHTTP(t.Context(), c.Client, url, "fixture", s, staleResultID, ref, oldToken,
				json.RawMessage(`{}`), true); err == nil {
				t.Fatalf("unproved HTTP denial was accepted: %s", tc.name)
			}
		})
	}
}
