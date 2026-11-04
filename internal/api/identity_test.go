package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/state"
)

type identityRepository struct {
	*fakeWorkerRepository
	ApprovalRepository
	decision state.ApprovalDecisionInput
	claim    state.ClaimInput
}

func (f *identityRepository) RecordApprovalDecision(_ context.Context, in state.ApprovalDecisionInput) (state.ApprovalIntent, error) {
	f.decision = in
	return state.ApprovalIntent{}, nil
}
func (f *identityRepository) ClaimAttempt(_ context.Context, in state.ClaimInput) (state.ClaimResult, error) {
	f.claim = in
	return state.ClaimResult{}, nil
}

func projectedToken(issuer, subject, audience string, expiry int64) string {
	raw, _ := json.Marshal(map[string]any{"iss": issuer, "sub": subject, "aud": []string{audience}, "exp": expiry})
	return "header." + base64.RawURLEncoding.EncodeToString(raw) + ".signature"
}

type reviewFixture struct {
	redirect                  bool
	authenticated             bool
	audiences                 []string
	username, credential, err string
	status, calls             int
	malformed                 bool
}

// Models trusted API-server responses over TLS, not a cluster/signature proof.
func identityFixture(t *testing.T) (*TokenReviewer, *reviewFixture) {
	t.Helper()
	fixture := &reviewFixture{authenticated: true, audiences: []string{"durable-worker"}, username: "system:serviceaccount:engine:worker", status: 201, credential: "reviewer"}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fixture.calls++
		if r.URL.Path != "/apis/authentication.k8s.io/v1/tokenreviews" || r.Method != "POST" || r.Header.Get("Authorization") != "Bearer "+fixture.credential {
			http.Error(w, "bad review request", 403)
			return
		}
		if fixture.redirect {
			http.Redirect(w, r, "/redirected-review", http.StatusTemporaryRedirect)
			return
		}
		var request struct {
			Spec struct {
				Token     string   `json:"token"`
				Audiences []string `json:"audiences"`
			} `json:"spec"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		if len(request.Spec.Audiences) != 1 || request.Spec.Audiences[0] != "durable-worker" {
			http.Error(w, "missing dedicated audience", 403)
			return
		}
		raw, _ := base64.RawURLEncoding.DecodeString(strings.Split(request.Spec.Token, ".")[1])
		var claims struct {
			Exp int64 `json:"exp"`
		}
		_ = json.Unmarshal(raw, &claims)
		w.WriteHeader(fixture.status)
		if fixture.malformed {
			_, _ = w.Write([]byte("{"))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": map[string]any{
			"authenticated": fixture.authenticated && (claims.Exp == 0 || claims.Exp > time.Now().Unix()),
			"audiences":     fixture.audiences, "user": map[string]string{"username": fixture.username}, "error": fixture.err,
		}})
	}))
	t.Cleanup(server.Close)
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.crt")
	token := filepath.Join(dir, "token")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(token, []byte(fixture.credential), 0600); err != nil {
		t.Fatal(err)
	}
	config := IdentityConfig{APIURL: server.URL, CAFile: ca, ReviewerTokenFile: token, Issuer: "https://cluster.example", Audience: "durable-worker", Namespace: "engine", ServiceAccount: "worker"}
	reviewer, err := NewTokenReviewer(config)
	if err != nil {
		t.Fatal(err)
	}
	return reviewer, fixture
}

func TestK2IdentityGuards(t *testing.T) {
	reviewer, remote := identityFixture(t)
	good := projectedToken(reviewer.config.Issuer, remote.username, "durable-worker", time.Now().Add(time.Hour).Unix())
	cases := []struct {
		name, token string
		configure   func()
	}{
		{"missing", "", func() {}},
		{"malformed", "not-jwt", func() {}},
		{"invalid-base64", "header.!.signature", func() {}},
		{"invalid-claims", "header." + base64.RawURLEncoding.EncodeToString([]byte("{")) + ".signature", func() {}},
		{"wrong-audience", projectedToken(reviewer.config.Issuer, remote.username, "kubernetes", time.Now().Add(time.Hour).Unix()), func() {}},
		{"wrong-ServiceAccount", projectedToken(reviewer.config.Issuer, "system:serviceaccount:engine:attacker", "durable-worker", time.Now().Add(time.Hour).Unix()), func() {}},
		{"wrong-namespace", projectedToken(reviewer.config.Issuer, "system:serviceaccount:other:worker", "durable-worker", time.Now().Add(time.Hour).Unix()), func() {}},
		{"wrong-issuer", projectedToken("https://other", remote.username, "durable-worker", time.Now().Add(time.Hour).Unix()), func() {}},
		{"expired", projectedToken(reviewer.config.Issuer, remote.username, "durable-worker", time.Now().Add(-time.Minute).Unix()), func() {}},
		{"no-expiry", projectedToken(reviewer.config.Issuer, remote.username, "durable-worker", 0), func() {}},
		{"unauthenticated", good, func() { remote.authenticated = false }},
		{"review-error", good, func() { remote.err = "review failed" }},
		{"empty-review-audience", good, func() { remote.audiences = nil }},
		{"wrong-review-audience", good, func() { remote.audiences = []string{"kubernetes"} }},
		{"wrong-review-user", good, func() { remote.username = "system:serviceaccount:engine:attacker" }},
		{"review-unavailable", good, func() { remote.status = 503 }},
		{"review-malformed", good, func() { remote.malformed = true }},
		{"review-redirect", good, func() { remote.redirect = true }},
		{"reviewer-missing", good, func() { reviewer.config.ReviewerTokenFile = filepath.Join(t.TempDir(), "absent") }},
		{"reviewer-empty", good, func() { _ = os.WriteFile(reviewer.config.ReviewerTokenFile, nil, 0600) }},
	}
	config := reviewer.config
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reviewer.config = config
			remote.authenticated = true
			remote.err = ""
			remote.audiences = []string{"durable-worker"}
			remote.username = "system:serviceaccount:engine:worker"
			remote.status = 201
			remote.malformed = false
			remote.redirect = false
			_ = os.WriteFile(config.ReviewerTokenFile, []byte(remote.credential), 0600)
			tc.configure()
			called := false
			handler := reviewer.middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
			req := httptest.NewRequest("POST", "/worker", nil)
			if tc.token != "" {
				req.Header.Set("Authorization", "Bearer "+tc.token)
			}
			beforeCalls := remote.calls
			res := httptest.NewRecorder()
			handler.ServeHTTP(res, req)
			if tc.name == "review-redirect" && remote.calls != beforeCalls+1 {
				t.Fatal("identity guard bypassed: TokenReview redirect followed")
			}
			if strings.HasPrefix(tc.name, "reviewer-") && remote.calls != beforeCalls {
				t.Fatal("identity guard bypassed: unavailable reviewer credential reached API server")
			}
			if called || res.Code != 401 {
				t.Fatalf("identity guard bypassed: %s code=%d", tc.name, res.Code)
			}
		})
	}
	reviewer.config = config
	remote.authenticated = true
	remote.err = ""
	remote.audiences = []string{"durable-worker"}
	remote.username = "system:serviceaccount:engine:worker"
	remote.status = 201
	remote.malformed = false
	remote.credential = "rotated"
	_ = os.WriteFile(config.ReviewerTokenFile, []byte(remote.credential), 0600)
	if id, err := reviewer.verify(context.Background(), good); err != nil || id != remote.username {
		t.Fatalf("rotated token failed: %q %v", id, err)
	}
	reviewer.config.APIURL = "https://127.0.0.1:1"
	if _, err := reviewer.verify(context.Background(), good); err == nil {
		t.Fatal("identity guard bypassed: outage")
	}
}

func TestK2IdentityConfiguration(t *testing.T) {
	reviewer, _ := identityFixture(t)
	good := reviewer.config
	cases := []IdentityConfig{good, good, good, good, good, good, good, good, good, good, good}
	cases[0].APIURL = "http://cluster"
	cases[1].Issuer = ""
	cases[2].Audience = ""
	cases[3].Namespace = ""
	cases[4].ServiceAccount = ""
	cases[5].CAFile = "missing"
	cases[6].ReviewerTokenFile = "missing"
	cases[7].Namespace = "bad:namespace"
	cases[8].APIURL = "https://user@cluster"
	cases[9].APIURL = "https://cluster/path"
	invalidCA := filepath.Join(t.TempDir(), "bad-ca")
	_ = os.WriteFile(invalidCA, []byte("invalid"), 0600)
	cases[10].CAFile = invalidCA
	for i, c := range cases {
		if _, err := NewTokenReviewer(c); err == nil {
			t.Fatalf("identity configuration guard bypassed: %d", i)
		}
	}
	t.Setenv("RUNTIME_TOKEN_AUTH", "enabled")
	if _, err := AuthenticationFromEnvironment(); err == nil {
		t.Fatal("identity configuration guard bypassed: enabled without config")
	}
	for name, value := range map[string]string{
		"RUNTIME_TOKEN_REVIEW_URL": good.APIURL, "RUNTIME_TOKEN_REVIEW_CA": good.CAFile,
		"RUNTIME_TOKEN_REVIEW_TOKEN_FILE": good.ReviewerTokenFile, "RUNTIME_TOKEN_ISSUER": good.Issuer,
		"RUNTIME_TOKEN_AUDIENCE": good.Audience, "RUNTIME_TOKEN_NAMESPACE": good.Namespace, "RUNTIME_TOKEN_SERVICE_ACCOUNT": good.ServiceAccount,
	} {
		t.Setenv(name, value)
	}
	if configured, err := AuthenticationFromEnvironment(); err != nil || configured == nil {
		t.Fatalf("valid identity configuration rejected: %v", err)
	}
	t.Setenv("RUNTIME_TOKEN_AUTH", "typo")
	if _, err := AuthenticationFromEnvironment(); err == nil {
		t.Fatal("identity configuration guard bypassed: typo")
	}
	t.Setenv("RUNTIME_TOKEN_AUTH", "")
	if reviewer, err := AuthenticationFromEnvironment(); err != nil || reviewer != nil {
		t.Fatal("Compose auth default changed")
	}
}

func TestK2RouteSetsAndIdentityBinding(t *testing.T) {
	reviewer, remote := identityFixture(t)
	repo := &identityRepository{fakeWorkerRepository: &fakeWorkerRepository{fakeRepository: &fakeRepository{}}}
	operatorRoutes := []string{"/v1/workflows/w/nodes/n/iterations/0/approval", "/v1/approvals/a/decision", "/v1/approvals/a/apply", "/v1/workflows/w/cancel-request", "/v1/cancellations/c/apply"}
	workerRoutes := []string{"/v1/worker-deliveries", "/v1/workflows/w/nodes/n/iterations/0/claim", "/v1/workflows/w/nodes/n/iterations/0/heartbeat", "/v1/workflows/w/nodes/n/iterations/0/result"}
	for _, routes := range []RouteSet{RoutesWorkerControl, RoutesPublic, RoutesNone, RoutesOperator, RoutesCombined} {
		handler := NewServer(repo).WithRoutes(routes).Handler()
		for _, path := range append(append([]string{}, operatorRoutes...), workerRoutes...) {
			res := httptest.NewRecorder()
			handler.ServeHTTP(res, httptest.NewRequest("POST", path, strings.NewReader("{}")))
			operator := strings.Contains(path, "approval") || strings.Contains(path, "cancel")
			expected := routes == RoutesCombined || (operator && routes == RoutesOperator) || (!operator && routes == RoutesWorkerControl)
			if (res.Code != 404) != expected {
				t.Fatalf("route isolation bypassed: %q %s status=%d", routes, path, res.Code)
			}
		}
	}

	for _, routes := range []RouteSet{RoutesCombined, RoutesPublic, RoutesOperator, RoutesWorkerControl, RoutesNone} {
		handler := NewServer(repo).WithRoutes(routes).Handler()
		for _, path := range []string{"/v1/workflows/w", "/v1/workflows/w/history"} {
			res := httptest.NewRecorder()
			handler.ServeHTTP(res, httptest.NewRequest("GET", path, nil))
			want := routes == RoutesCombined || routes == RoutesPublic || routes == RoutesOperator
			if (res.Code != 404) != want {
				t.Fatalf("route isolation bypassed: read %s %s", routes, path)
			}
		}
	}
	token := projectedToken(reviewer.config.Issuer, remote.username, "durable-worker", time.Now().Add(time.Hour).Unix())
	for _, path := range operatorRoutes {
		req := httptest.NewRequest("POST", path, strings.NewReader(`{"approver_id":"spoof"}`))
		req.Header.Set("Authorization", "Bearer "+token)
		res := httptest.NewRecorder()
		NewServer(repo).WithRoutes(RoutesWorkerControl).WithAuthentication(reviewer).Handler().ServeHTTP(res, req)
		if res.Code != 404 {
			t.Fatal("route isolation bypassed: authenticated worker reached operator")
		}
	}
	req := httptest.NewRequest("POST", "/v1/approvals/a/decision", strings.NewReader(`{"approver_id":"spoof","proposal_hash":"hash","decision":"APPROVED"}`))
	req.Header.Set("Authorization", "Bearer "+token)
	res := httptest.NewRecorder()
	NewServer(repo).WithRoutes(RoutesOperator).WithAuthentication(reviewer).Handler().ServeHTTP(res, req)
	if res.Code != 200 || repo.decision.ApproverID != remote.username {
		t.Fatal("authenticated approver spoofed")
	}
	req = httptest.NewRequest("POST", "/v1/workflows/w/nodes/n/iterations/0/claim", strings.NewReader(`{"worker_id":"spoof","request_id":"request"}`))
	req.Header.Set("Authorization", "Bearer "+token)
	res = httptest.NewRecorder()
	NewServer(repo).WithRoutes(RoutesWorkerControl).WithAuthentication(reviewer).Handler().ServeHTTP(res, req)
	if res.Code != 200 || repo.claim.WorkerID != remote.username {
		t.Fatal("authenticated worker spoofed")
	}
	res = httptest.NewRecorder()
	NewServer(repo).Handler().ServeHTTP(res, httptest.NewRequest("POST", "/v1/approvals/a/decision", strings.NewReader(`{"approver_id":"localhost","proposal_hash":"hash","decision":"APPROVED"}`)))
	if res.Code != 200 || repo.decision.ApproverID != "localhost" {
		t.Fatal("Compose decision behavior changed")
	}
}
