package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/state"
)

const (
	j12IntentID  = "11111111-1111-4111-8111-111111111111"
	j12ForeignID = "22222222-2222-4222-8222-222222222222"
	j12LegacyID  = "33333333-3333-4333-8333-333333333333"
	j12MissingID = "44444444-4444-4444-8444-444444444444"
	j12Grant     = "private-dispatch-grant"
	j12GrantHash = "private-grant-scope"
	j12Submit    = `{"submission_key":"client-key","actor_id":"forged","payload":{},"definition_id":"demo","definition_version":1,"initial_node_id":"root"}`
	j12Decision  = `{"approver_id":"forged","proposal_hash":"proposal","decision":"APPROVED"}`
	j12Cancel    = `{"client_key":"cancel","observed_revision":1}`
)

type j12Repository struct {
	*identityRepository
	cancellation state.CancellationRequestInput
	historyReads int
	decide       func(state.ApprovalDecisionInput) (state.ApprovalIntent, error)
	cancel       func(state.CancellationRequestInput) (state.CancellationRequest, error)
}

func newJ12Repository() *j12Repository {
	return &j12Repository{identityRepository: &identityRepository{fakeWorkerRepository: &fakeWorkerRepository{
		fakeRepository: &fakeRepository{createResult: state.CreateWorkflowResult{Created: true}},
	}}}
}

func (repo *j12Repository) GetWorkflow(_ context.Context, id string) (state.Workflow, error) {
	switch id {
	case "own":
		return state.Workflow{WorkflowID: id, Namespace: "tenant-a", Revision: 1}, nil
	case "foreign":
		return state.Workflow{WorkflowID: id, Namespace: "tenant-b", Revision: 7}, nil
	default:
		return state.Workflow{}, state.ErrWorkflowNotFound
	}
}

func (repo *j12Repository) ListHistory(context.Context, string, int64, int) ([]state.TransitionRecord, error) {
	repo.historyReads++
	return []state.TransitionRecord{{Revision: 1}}, nil
}

func (repo *j12Repository) GetApprovalIntent(_ context.Context, id string) (state.ApprovalIntent, error) {
	if id != j12IntentID {
		return state.ApprovalIntent{}, state.ErrApprovalNotFound
	}
	return state.ApprovalIntent{IntentID: id, WorkflowID: "own", NodeID: "root"}, nil
}

func (repo *j12Repository) RequestCancellation(_ context.Context, input state.CancellationRequestInput) (state.CancellationRequest, error) {
	repo.cancellation = input
	if repo.cancel != nil {
		return repo.cancel(input)
	}
	return state.CancellationRequest{WorkflowID: input.WorkflowID}, nil
}

func (repo *j12Repository) RecordApprovalDecision(_ context.Context, input state.ApprovalDecisionInput) (state.ApprovalIntent, error) {
	repo.decision = input
	if repo.decide != nil {
		return repo.decide(input)
	}
	return state.ApprovalIntent{ApproverID: input.ApproverID, GrantToken: j12Grant, GrantScopeHash: j12GrantHash}, nil
}

func j12Request(handler http.Handler, method, path, token, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

type j12Route struct {
	name, method, path, body string
	listener                 RouteSet
	accepted                 int
}

var j12Routes = []j12Route{
	{"submit", "POST", "/v1/workflows", j12Submit, RoutesPublic, 201},
	{"status", "GET", "/v1/workflows/own", "", RoutesPublic, 200},
	{"status-head", "HEAD", "/v1/workflows/own", "", RoutesPublic, 200},
	{"history", "GET", "/v1/workflows/own/history", "", RoutesPublic, 200},
	{"history-head", "HEAD", "/v1/workflows/own/history", "", RoutesPublic, 200},
	{"cancel-request", "POST", "/v1/workflows/own/cancel-request", j12Cancel, RoutesPublic, 202},
	{"decision", "POST", "/v1/approvals/" + j12IntentID + "/decision", j12Decision, RoutesPublic, 200},
	{"approval", "POST", "/v1/workflows/own/nodes/root/iterations/0/approval", `{"owner_id":"owner","epoch":1}`, RoutesOperator, 201},
	{"approval-apply", "POST", "/v1/approvals/" + j12IntentID + "/apply", `{"workflow_id":"own","node_id":"root","intent_id":"` + j12IntentID + `","owner_id":"owner","epoch":1}`, RoutesOperator, 200},
	{"cancellation-apply", "POST", "/v1/cancellations/" + j12IntentID + "/apply", `{"owner_id":"owner","epoch":1}`, RoutesOperator, 200},
	// Delivery uses the concrete PostgreSQL store. The matching credential
	// reaches that dependency check in this unit test.
	{"delivery", "POST", "/v1/worker-deliveries", `{}`, RoutesWorkerControl, 501},
	{"claim", "POST", "/v1/workflows/own/nodes/root/iterations/0/claim", `{"worker_id":"forged","request_id":"req"}`, RoutesWorkerControl, 200},
	{"heartbeat", "POST", "/v1/workflows/own/nodes/root/iterations/0/heartbeat", `{"attempt_number":1,"claim_token":"token"}`, RoutesWorkerControl, 200},
	{"result", "POST", "/v1/workflows/own/nodes/root/iterations/0/result", `{"attempt_number":1,"claim_token":"token","attempt_state":"SUCCEEDED","payload":{}}`, RoutesWorkerControl, 200},
	{"demand", "GET", "/v1/demand", "", RoutesDemand, 200},
	{"demand-head", "HEAD", "/v1/demand", "", RoutesDemand, 200},
}

func TestJ12MethodListenerCredentialMatrix(t *testing.T) {
	verifier, key := j12JWTFixture(t)
	reviewer, remote := identityFixture(t)
	secret := "registered-demand-fixture-bearer-0123456789"
	file := filepath.Join(t.TempDir(), "demand")
	if err := os.WriteFile(file, []byte(secret), 0600); err != nil {
		t.Fatal(err)
	}
	demand := &DemandBearerAuthenticator{file: file}
	now := time.Now()
	tenantsToken := j12Token(t, key, "current", j12Claims("approver", "tenant-a", now))
	projected := projectedToken(reviewer.config.Issuer, remote.username, reviewer.config.Audience, now.Add(time.Minute).Unix())
	for _, listener := range []RouteSet{RoutesPublic, RoutesOperator, RoutesWorkerControl, RoutesDemand, RoutesNone, RoutesCombined} {
		repo := newJ12Repository()
		sample := &demandFixture{}
		handler := NewServer(repo).WithRoutes(listener).WithPublicAPIAuth(true, verifier).
			WithAuthentication(reviewer).WithDemandAuthentication(demand).WithDemand(sample, "campaign").Handler()
		for _, route := range j12Routes {
			for _, credential := range []struct {
				name, token string
				listeners   []RouteSet
			}{
				{"tenant", tenantsToken, []RouteSet{RoutesPublic}},
				{"projected", projected, []RouteSet{RoutesOperator, RoutesWorkerControl}},
				{"demand", secret, []RouteSet{RoutesDemand}},
				{"missing", "", nil},
			} {
				name := fmt.Sprintf("%q/%s/%s", listener, route.name, credential.name)
				t.Run(name, func(t *testing.T) {
					want := http.StatusNotFound
					if listener == route.listener {
						want = http.StatusUnauthorized
						for _, allowed := range credential.listeners {
							if listener == allowed {
								want = route.accepted
							}
						}
					}
					response := j12Request(handler, route.method, route.path, credential.token, route.body)
					if response.Code != want {
						t.Fatalf("%s %s returned %d, want %d: %s", route.method, route.path, response.Code, want, response.Body)
					}
				})
			}
			if listener == route.listener {
				t.Run(string(listener)+"/"+route.name+"/wrong-method", func(t *testing.T) {
					if got := j12Request(handler, "DELETE", route.path, tenantsToken, "").Code; got != http.StatusNotFound {
						t.Fatalf("DELETE %s returned %d", route.path, got)
					}
				})
			}
		}
		identity := remote.username + ":pod:worker-0:uid:7fa8065c-c4cb-4b9b-a021-380edda831e8"
		if listener == RoutesWorkerControl && repo.claim.WorkerID != identity {
			t.Fatal("worker claim did not use the reviewed pod identity")
		}
		if listener == RoutesOperator && (repo.approval.ActorID != identity || repo.applied.ActorID != identity || repo.cancelActor != identity) {
			t.Fatal("operator mutation did not use the reviewed pod identity")
		}
		if listener == RoutesOperator {
			path := "/v1/approvals/" + j12IntentID + "/apply"
			body := `{"intent_id":"` + j12ForeignID + `","workflow_id":"foreign","owner_id":"owner","epoch":1}`
			repo.applied = state.ApplyApprovalInput{}
			if response := j12Request(handler, "POST", path, projected, body); response.Code != 409 || repo.applied.IntentID != "" {
				t.Fatalf("forged apply target reached storage: %d %+v", response.Code, repo.applied)
			}
			if response := j12Request(handler, "POST", path, projected, `{"owner_id":"owner","epoch":1}`); response.Code != 200 ||
				repo.applied.IntentID != j12IntentID || repo.applied.WorkflowID != "own" || repo.applied.NodeID != "root" {
				t.Fatalf("internal apply did not use stored target: %d %+v", response.Code, repo.applied)
			}
		}
		if listener == RoutesDemand && (sample.calls != 2 || sample.namespace != "campaign") {
			t.Fatalf("demand sampled with the wrong credential or namespace: %+v", sample)
		}
	}
	// A misconfigured or rotated demand file cannot make a tenant JWT valid there.
	j12Write(t, file, []byte(tenantsToken))
	handler := NewServer(nil).WithRoutes(RoutesDemand).WithPublicAPIAuth(true, nil).
		WithDemandAuthentication(demand).WithDemand(&demandFixture{}, "campaign").Handler()
	if demand.ValidateJ12Secret() == nil || j12Request(handler, "GET", "/v1/demand", tenantsToken, "").Code != 401 {
		t.Fatal("demand accepted a JWT as its dedicated credential")
	}
}

func TestJ12TenantRouteBindings(t *testing.T) {
	verifier, key := j12JWTFixture(t)
	now := time.Now()
	requester := j12Token(t, key, "current", j12Claims("requester", "tenant-a", now))
	approver := j12Token(t, key, "current", j12Claims("approver", "tenant-a", now))
	foreign := j12Token(t, key, "current", j12Claims("approver", "tenant-b", now))
	repo := newJ12Repository()
	handler := NewServer(repo).WithRoutes(RoutesPublic).WithPublicAPIAuth(true, verifier).Handler()
	check := func(method, path, token, body string, status int) *httptest.ResponseRecorder {
		t.Helper()
		response := j12Request(handler, method, path, token, body)
		if response.Code != status {
			t.Fatalf("%s %s returned %d, want %d: %s", method, path, response.Code, status, response.Body)
		}
		return response
	}
	check("POST", "/v1/workflows", requester, j12Submit, 201)
	if input := repo.lastCreate; input.Namespace != "tenant-a" || input.ActorID != "requester" ||
		input.Tenant == nil || *input.Tenant != (state.TenantPrincipal{Issuer: j12Issuer, Namespace: "tenant-a", Subject: "requester"}) {
		t.Fatalf("submit did not pass the verified binding: %+v", input)
	}
	repo.lastCreate = state.CreateWorkflowInput{}
	check("POST", "/v1/workflows", foreign, `{"namespace":"tenant-a",`+j12Submit[1:], 403)
	if repo.lastCreate.Namespace != "" {
		t.Fatal("mismatched body namespace reached submission")
	}
	for _, method := range []string{"GET", "HEAD"} {
		for _, suffix := range []string{"?expected_revision=1", "/history?after_revision=19"} {
			foreignResponse := check(method, "/v1/workflows/foreign"+suffix, requester, "", 404)
			missingResponse := check(method, "/v1/workflows/absent"+suffix, requester, "", 404)
			if foreignResponse.Body.String() != missingResponse.Body.String() {
				t.Fatal("foreign workflow response disclosed existence or revision")
			}
		}
	}
	if repo.historyReads != 0 {
		t.Fatal("foreign history was read")
	}
	check("POST", "/v1/workflows/own/cancel-request", requester, `{"workflow_id":"foreign",`+j12Cancel[1:], 400)
	if repo.cancellation.WorkflowID != "" {
		t.Fatal("forged cancellation workflow reached storage")
	}
	repo.cancel = func(in state.CancellationRequestInput) (state.CancellationRequest, error) {
		if in.TenantNamespace != "tenant-a" {
			t.Fatalf("cancellation lacked the verified tenant namespace: %+v", in)
		}
		return state.CancellationRequest{}, state.ErrWorkflowNotFound
	}
	foreignResponse := check("POST", "/v1/workflows/foreign/cancel-request", requester, j12Cancel, 404)
	if repo.cancellation.WorkflowID != "foreign" {
		t.Fatalf("cancellation did not use path ID: %+v", repo.cancellation)
	}
	missingResponse := check("POST", "/v1/workflows/absent/cancel-request", requester, j12Cancel, 404)
	if foreignResponse.Body.String() != missingResponse.Body.String() {
		t.Fatal("foreign cancellation response disclosed existence")
	}
	decisionPath := func(id string) string { return "/v1/approvals/" + id + "/decision" }
	check("POST", decisionPath(j12IntentID), approver, `{"intent_id":"foreign",`+j12Decision[1:], 400)
	if repo.decision.IntentID != "" {
		t.Fatal("forged intent ID reached storage")
	}
	repo.decide = func(in state.ApprovalDecisionInput) (state.ApprovalIntent, error) {
		if in.Tenant == nil || in.Tenant.Issuer != j12Issuer || in.ApproverID != in.Tenant.Subject {
			t.Fatalf("decision used an unverified actor: %+v", in)
		}
		switch {
		case in.IntentID == j12ForeignID || in.IntentID == j12MissingID || in.Tenant.Namespace != "tenant-a":
			return state.ApprovalIntent{}, state.ErrApprovalNotFound
		case in.IntentID == j12LegacyID || in.ApproverID == "requester":
			return state.ApprovalIntent{}, state.ErrApprovalUnauthorized
		default:
			return state.ApprovalIntent{ApproverID: in.ApproverID, GrantToken: j12Grant, GrantScopeHash: j12GrantHash}, nil
		}
	}
	check("POST", decisionPath(j12IntentID), requester, j12Decision, 403)
	check("POST", decisionPath(j12IntentID), foreign, j12Decision, 404)
	check("POST", decisionPath(j12LegacyID), approver, j12Decision, 403)
	foreignResponse = check("POST", decisionPath(j12ForeignID), approver, j12Decision, 404)
	missingResponse = check("POST", decisionPath(j12MissingID), approver, j12Decision, 404)
	if foreignResponse.Body.String() != missingResponse.Body.String() {
		t.Fatal("foreign decision response disclosed existence")
	}
	response := check("POST", decisionPath(j12IntentID), approver, j12Decision, 200)
	if strings.Contains(response.Body.String(), j12Grant) || strings.Contains(response.Body.String(), j12GrantHash) ||
		!strings.Contains(response.Body.String(), `"ApproverID":"approver"`) {
		t.Fatalf("public decision response contains the wrong identity or grant data: %s", response.Body)
	}
}

func TestJ12TenantCredentialFailuresAtHandler(t *testing.T) {
	verifier, key := j12JWTFixture(t)
	other := j12Key(t)
	now := time.Now()
	repo := newJ12Repository()
	handler := NewServer(repo).WithRoutes(RoutesPublic).WithPublicAPIAuth(true, verifier).Handler()
	for _, name := range []string{"missing", "expired", "bad-signature", "wrong-issuer", "wrong-audience", "missing-namespace", "missing-subject", "unknown-kid", "none", "HS256", "missing-jwks"} {
		t.Run(name, func(t *testing.T) {
			claims := j12Claims("approver", "tenant-a", now)
			signer, kid := key, "current"
			switch name {
			case "expired":
				claims["exp"] = now.Add(-time.Minute).Unix()
			case "bad-signature":
				signer = other
			case "wrong-issuer":
				claims["iss"] = "https://foreign.invalid"
			case "wrong-audience":
				claims["aud"] = "foreign"
			case "missing-namespace":
				delete(claims, "namespace")
			case "missing-subject":
				delete(claims, "sub")
			case "unknown-kid":
				kid = "unknown"
			}
			token := j12Token(t, signer, kid, claims)
			switch name {
			case "missing":
				token = ""
			case "none", "HS256":
				token = j12Sign(t, signer, j12JSON(t, map[string]string{"alg": name, "kid": kid}), j12JSON(t, claims))
			case "missing-jwks":
				if err := os.Remove(verifier.jwksFile); err != nil {
					t.Fatal(err)
				}
			}
			response := j12Request(handler, "POST", "/v1/workflows", token, j12Submit)
			if response.Code != 401 || repo.lastCreate.Namespace != "" {
				t.Fatalf("unexpected acceptance of invalid credential: %d %s", response.Code, response.Body)
			}
		})
	}
}
