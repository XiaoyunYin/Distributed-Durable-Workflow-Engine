package k6sink

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/k6auth"
	"durable-agent-execution-engine/internal/state"
)

var testHTTP = HTTPConfig{
	Worker:           WorkerIdentity{"episode-a", "worker", "worker-uid"},
	EnginePeerURI:    "spiffe://episode-a/worker-control",
	IngressPeerURI:   "spiffe://episode-a/ingress",
	ClockUncertainty: time.Millisecond,
}

const workerSubject = "system:serviceaccount:episode-a:worker"

type reviewerFunc func(context.Context, string) (k6auth.Review, error)

func (f reviewerFunc) Review(ctx context.Context, token string) (k6auth.Review, error) {
	return f(ctx, token)
}

func testReviewer() k6auth.Reviewer {
	return reviewerFunc(func(_ context.Context, _ string) (k6auth.Review, error) {
		var result k6auth.Review
		result.Authenticated = true
		result.Audiences = []string{k6auth.SinkAudience}
		result.User.Username, result.User.UID = workerSubject, "worker-uid"
		return result, nil
	})
}

func workerToken(t *testing.T, mutate func(map[string]any)) string {
	t.Helper()
	claims := map[string]any{
		"iss": k6auth.Issuer, "sub": workerSubject, "aud": []string{k6auth.SinkAudience},
		"iat": time.Now().Add(-time.Minute).Unix(), "exp": time.Now().Add(time.Minute).Unix(),
		"kubernetes.io": map[string]any{
			"namespace": "episode-a", "serviceaccount": map[string]string{"name": "worker", "uid": "worker-uid"},
			"pod": map[string]string{"name": "worker-a", "uid": "pod-uid"},
		},
	}
	if mutate != nil {
		mutate(claims)
	}
	return "test." + base64.RawURLEncoding.EncodeToString(asJSON(t, claims)) + ".signature"
}

type grantFunc func(context.Context, k6auth.ExactGrantRequest) (k6auth.ExactGrant, error)

func (f grantFunc) ReadExactGrant(ctx context.Context, req k6auth.ExactGrantRequest) (k6auth.ExactGrant, error) {
	return f(ctx, req)
}

type testGrants map[string]k6auth.ExactGrant

func (g testGrants) ReadExactGrant(_ context.Context, req k6auth.ExactGrantRequest) (k6auth.ExactGrant, error) {
	grant, ok := g[req.Token]
	if !ok {
		return k6auth.ExactGrant{}, errDenied
	}
	// Return a grant by token alone so the sink's independent validation is
	// exercised even for mismatches normally filtered by the exact reader.
	grant.DBTime = time.Now()
	return grant, nil
}

func (g testGrants) add(t *testing.T, req Request, expires time.Time) Request {
	t.Helper()
	canonicalHash, err := state.CanonicalPayloadHash(req.Arguments)
	if err != nil {
		t.Fatal(err)
	}
	proposal := hash([]byte(req.ResourceID + "\x00" + canonicalHash + "\x00" + req.ExpectedRevision))
	req.ScopeHash = hash([]byte(proposal + "\x00" + req.ExpectedRevision + "\x00" + req.LogicalKey + "\x00" + req.ResourceID))
	g[req.GrantToken] = k6auth.ExactGrant{
		WorkflowID: req.WorkflowID, NodeID: req.NodeID, Iteration: *req.Iteration, IntentID: req.IntentID,
		Target: req.ResourceID, CanonicalArguments: req.Arguments, ExpectedResourceRevision: req.ExpectedRevision,
		ProposalHash: proposal, ScopeHash: req.ScopeHash, ExpiresAt: expires, Decision: "APPROVED", DispatchStatus: "GRANTED",
	}
	return req
}

func testRequest() Request {
	var iteration int32
	return Request{
		IntentID: "01234567-0123-4123-8123-012345678901", GrantToken: "private-grant-a",
		WorkflowID: "workflow", NodeID: "effect", Iteration: &iteration,
		LogicalKey: "logical", ResourceID: "resource", ExpectedRevision: "0",
		Arguments: json.RawMessage(`{"private-state":"DO-NOT-EXPORT","nested":[2,{"é":true}]}`),
		Attempt:   1, MessageID: "message-1", RequestID: "apply-1",
	}
}

type backendFunc func(context.Context, delivery, grantRecheck) (result, error)

func (f backendFunc) execute(ctx context.Context, d delivery, check grantRecheck) (result, error) {
	return f(ctx, d, check)
}

type testClients struct {
	url             string
	engine, ingress *http.Client
	serve           *httptest.Server
}

func newTestServer(t *testing.T, handler http.Handler) testClients {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour)}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err = x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	server := httptest.NewUnstartedServer(handler)
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	server.StartTLS()
	t.Cleanup(server.Close)

	client := func(serial int64, ident string) *http.Client {
		t.Helper()
		uri, err := url.Parse(ident)
		if err != nil {
			t.Fatal(err)
		}
		leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		template := &x509.Certificate{SerialNumber: big.NewInt(serial), URIs: []*url.URL{uri},
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, KeyUsage: x509.KeyUsageDigitalSignature,
			NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour)}
		leafDER, err := x509.CreateCertificate(rand.Reader, template, ca, &leafKey.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		transport := server.Client().Transport.(*http.Transport).Clone()
		transport.DisableKeepAlives = true
		transport.TLSClientConfig.Certificates = []tls.Certificate{{Certificate: [][]byte{leafDER}, PrivateKey: leafKey}}
		t.Cleanup(transport.CloseIdleConnections)
		return &http.Client{Transport: transport, Timeout: 8 * time.Second}
	}
	return testClients{server.URL, client(2, testHTTP.EnginePeerURI), client(3, testHTTP.IngressPeerURI), server}
}

type reply struct {
	status int
	body   []byte
}

func post(client *http.Client, url, token string, body []byte) (reply, error) {
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return reply{}, err
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := client.Do(request)
	if err != nil {
		return reply{}, err
	}
	defer response.Body.Close()
	content, err := io.ReadAll(response.Body)
	return reply{response.StatusCode, content}, err
}

func request(t *testing.T, client *http.Client, base, operation, token string, body []byte, status int) []byte {
	t.Helper()
	response, err := post(client, base+"/v1/"+operation, token, body)
	if err != nil || response.status != status {
		t.Fatalf("%s: status=%d expected=%d err=%v", operation, response.status, status, err)
	}
	return response.body
}

func asJSON(t *testing.T, input any) []byte {
	t.Helper()
	body, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestK6SinkWorkerCannotAllocate(t *testing.T) {
	grants := testGrants{}
	req := grants.add(t, testRequest(), time.Now().Add(time.Minute))
	var allocations atomic.Int32
	handler, err := newHandler(testHTTP, testReviewer(), grants, backendFunc(func(_ context.Context, d delivery, _ grantRecheck) (result, error) {
		if d.operation != "allocate" {
			t.Error("worker reached the wrong peer's store")
		}
		allocations.Add(1)
		return result{outcome: "ALLOCATED", body: []byte(`{"fence":1}`)}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	clients := newTestServer(t, handler)
	body, token := asJSON(t, req), workerToken(t, nil)
	request(t, clients.ingress, clients.url, "allocate", token, body, http.StatusForbidden)
	request(t, clients.engine, clients.url, "allocate", token, body, http.StatusForbidden)
	if _, err := post(clients.serve.Client(), clients.url+"/v1/allocate", token, body); err == nil {
		t.Fatal("client without a peer certificate connected")
	}
	request(t, clients.engine, clients.url, "apply", token, body, http.StatusForbidden)
	request(t, clients.ingress, clients.url, "apply", "", body, http.StatusForbidden)
	request(t, clients.engine, clients.url, "allocate", "", body, http.StatusOK)
	if got := allocations.Load(); got != 1 {
		t.Fatalf("allocation store reached %d times", got)
	}
}

func TestK6SinkPrevalidation(t *testing.T) {
	grants := testGrants{}
	req := grants.add(t, testRequest(), time.Now().Add(time.Minute))
	req.Fence = 1
	noWrites := backendFunc(func(_ context.Context, _ delivery, _ grantRecheck) (result, error) {
		t.Error("pre-validation rejection reached the store")
		return result{}, errUnavailable
	})
	handler, err := newHandler(testHTTP, testReviewer(), grants, noWrites)
	if err != nil {
		t.Fatal(err)
	}
	clients := newTestServer(t, handler)
	token := workerToken(t, nil)
	for _, tc := range []struct {
		name string
		edit func(*Request)
	}{
		{"grant token", func(r *Request) { r.GrantToken = "wrong" }},
		{"intent", func(r *Request) { r.IntentID = "11234567-0123-4123-8123-012345678901" }},
		{"scope", func(r *Request) { r.ScopeHash = strings.Repeat("a", 64) }},
		{"target", func(r *Request) { r.ResourceID = "another" }},
		{"key", func(r *Request) { r.LogicalKey = "another" }},
		{"arguments", func(r *Request) { r.Arguments = json.RawMessage(`{"changed":1}`) }},
		{"revision", func(r *Request) { r.ExpectedRevision = "1" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := req
			tc.edit(&bad)
			response := request(t, clients.ingress, clients.url, "apply", token, asJSON(t, bad), http.StatusForbidden)
			if string(response) != "K6_AUTH_DENIED\n" {
				t.Fatal("grant rejection exposed validation details")
			}
		})
	}
	for _, tc := range []struct {
		name  string
		claim string
		value any
	}{
		{"approval audience", "aud", []string{k6auth.Audience}},
		{"multiple audiences", "aud", []string{k6auth.SinkAudience, "other"}},
		{"wrong issuer", "iss", "https://example.invalid"},
		{"foreign namespace", "sub", "system:serviceaccount:another:worker"},
		{"missing Pod", "kubernetes.io", nil},
		{"expired token", "exp", time.Now().Unix()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := workerToken(t, func(c map[string]any) { c[tc.claim] = tc.value })
			request(t, clients.ingress, clients.url, "lookup", bad, asJSON(t, req), http.StatusForbidden)
		})
	}
	request(t, clients.ingress, clients.url, "apply", token, bytes.Repeat([]byte("a"), maxBody+1), http.StatusRequestEntityTooLarge)
}

func TestK6SinkGrantDBTimeBoundary(t *testing.T) {
	grants := testGrants{}
	expiry := time.Now().Add(time.Minute)
	req := grants.add(t, testRequest(), expiry)
	for _, tc := range []struct {
		sample time.Time
		ok     bool
	}{
		{expiry.Add(-time.Second), true},
		{expiry.Add(-time.Millisecond), false},
		{expiry, false},
		{expiry.Add(time.Microsecond), false},
	} {
		reader := grantFunc(func(_ context.Context, _ k6auth.ExactGrantRequest) (k6auth.ExactGrant, error) {
			grant := grants[req.GrantToken]
			grant.DBTime = tc.sample
			return grant, nil
		})
		if _, err := authorize(context.Background(), reader, req, testHTTP.ClockUncertainty); (err == nil) != tc.ok {
			t.Fatalf("engine DB sample relative to expiry %s returned %v", tc.sample.Sub(expiry), err)
		}
	}
}

func TestK6SinkChangedBodyBinding(t *testing.T) {
	if !sameBody(hash([]byte("same")), hash([]byte("same"))) {
		t.Fatal("byte-identical request body was rejected")
	}
	if sameBody(hash([]byte("original")), hash([]byte("changed"))) {
		t.Fatal("changed request body was accepted")
	}
}

func TestK6SinkCountsJournalInsteadOfEngineReceipts(t *testing.T) {
	engineReceiptCount := 1
	snapshot := Snapshot{Journal: []Mutation{{Revision: 1}, {Revision: 2}}, Receipts: []Receipt{{AppliedRevision: 1}}}
	if snapshot.CheckApplications(engineReceiptCount) == nil {
		t.Fatal("engine receipt count concealed a second sink write")
	}
}

func TestK6SinkRevisionChainDetectsMissingJournal(t *testing.T) {
	resource := ResourceState{ResourceID: "resource", Revision: 1, InitialStateHash: "initial", FinalStateHash: "final"}
	write := Mutation{ResourceID: "resource", Revision: 1, OldStateHash: "initial", StateHash: "final"}
	snapshot := Snapshot{Resources: []ResourceState{resource}, Journal: []Mutation{write}}
	if err := snapshot.CheckRevisionChain(); err != nil {
		t.Fatal(err)
	}
	// Conceal the second write and leave the receipt and journal count at one.
	snapshot.Resources[0].Revision = 2
	if snapshot.CheckApplications(1) != nil || snapshot.CheckRevisionChain() == nil {
		t.Fatal("initial/final revision does not expose the missing mutation")
	}
}
