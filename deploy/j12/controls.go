package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/state"
)

type route struct {
	id, listener, method, credential string
	status                           int
}

// Order only resolves dependencies between positive cells. All wrong cells
// run before the positive for that route, using the same valid request state.
var matrix = []route{
	{"submit", "public", "POST", "tenant", 201},
	{"status", "public", "GET", "tenant", 200}, {"status-head", "public", "HEAD", "tenant", 200},
	{"history", "public", "GET", "tenant", 200}, {"history-head", "public", "HEAD", "tenant", 200},
	{"cancel-request", "public", "POST", "tenant", 202},
	{"decision", "public", "POST", "tenant", 200},
	{"approval-create", "operator", "POST", "operator", 201},
	{"approval-apply", "operator", "POST", "operator", 200},
	{"cancellation-apply", "operator", "POST", "operator", 200},
	{"demand", "demand", "GET", "demand", 200}, {"demand-head", "demand", "HEAD", "demand", 200},
	{"delivery", "worker", "POST", "worker", 200},
	{"claim", "worker", "POST", "worker", 200},
	{"heartbeat", "worker", "POST", "worker", 200},
	{"result", "worker", "POST", "worker", 200},
}

var credentialNames = []string{"tenant", "operator", "worker", "demand", "none"}
var tokenRejections = []string{"expired", "bad-signature", "wrong-issuer", "wrong-audience", "missing-subject", "missing-namespace", "unknown-kid",
	"none", "HS256", "fractional-exp", "fractional-nbf", "fractional-iat"}

var extraControls = []string{
	"offline/fixed-clock", "offline/unset-zero", "offline/switch-values",
	"startup/combined", "startup/operator", "startup/worker-control", "startup/demand", "startup/missing-jwks", "fixtures",
	"wrong-namespace/submit", "wrong-namespace/status", "wrong-namespace/cancel", "wrong-namespace/decision", "forged/cancel-path", "forged/decision-path", "forged/apply-body",
	"foreign/absent-cancel", "foreign/cancel", "foreign/absent-intent", "foreign/intent", "self/APPROVED", "self/REJECTED", "legacy/decision", "approval/replay-after-grant",
	"jwks/initial", "jwks/overlap-old", "jwks/overlap-new", "jwks/removed-old", "jwks/current-new", "jwks/invalid", "jwks/missing", "jwks/restored",
	"fixtures/final",
}

func matrixID(r route, listener, credential string) string {
	return "matrix/" + r.id + "/" + listener + "/" + credential
}

func controlIDs() []string {
	ids := append([]string{}, extraControls...)
	for _, id := range tokenRejections {
		ids = append(ids, "jwt/"+id)
	}
	for _, method := range []string{"GET", "HEAD"} {
		for _, kind := range []string{"status", "history"} {
			for _, scope := range []string{"absent", "foreign"} {
				ids = append(ids, "workflow/"+method+"/"+kind+"/"+scope)
			}
		}
	}
	for _, r := range matrix {
		for _, listener := range listeners {
			for _, credential := range credentialNames {
				ids = append(ids, matrixID(r, listener, credential))
			}
		}
		ids = append(ids, matrixID(r, r.listener, "wrong-method"))
	}
	return ids
}

func expectation(r route, listener, credential string) (int, string) {
	status, code := r.status, ""
	switch {
	case listener != r.listener || credential == "wrong-method":
		status, code = 404, "NOT_FOUND"
	case credential != r.credential:
		status, code = 401, "UNAUTHENTICATED"
	}
	if r.method == "HEAD" && credential != "wrong-method" {
		code = ""
	}
	return status, code
}

func runControls(ctx context.Context, h *host, report *report, c *credentials) error {
	q := campaign{ctx: ctx, host: h, report: report}
	for _, check := range []struct{ id, role, marker string }{
		{"startup/combined", "combined", "RUNTIME_PUBLIC_API_AUTH=1 rejects combined HTTP roles"},
		{"startup/operator", "operator", "require Kubernetes TokenReview"},
		{"startup/worker-control", "worker-control", "require Kubernetes TokenReview"},
		{"startup/demand", "demand", "requires its dedicated bearer credential"},
		{"startup/missing-jwks", "api", "public JWT JWKS unavailable"},
	} {
		if err := q.startup(check.id, check.role, check.marker); err != nil {
			return err
		}
	}
	f, err := prepareFixtures(ctx, h, c)
	row := result{ID: "fixtures", Verdict: "invalid"}
	initial, id, readErr := q.snapshot()
	if readErr == nil {
		row.After = id
		var httpFailure fixtureHTTPFailure
		if errors.As(err, &httpFailure) {
			row.Verdict, row.Want, row.Status, row.Code, row.Target = "fail", 201, httpFailure.status, httpFailure.code, httpFailure.path
		}
		if err == nil && initialFixtures(initial, f) {
			row.Verdict = "pass"
		} else if err == nil {
			err = errors.New("J12 fixtures lack the expected independent SQL state")
		}
	} else {
		err = readErr
	}
	if markErr := q.mark(row); markErr != nil {
		return markErr
	}
	if err != nil {
		return err
	}
	report.Inventory["relay_delivery"] = object{"event_id": f.delivery["event_id"], "topic": f.delivery["topic"],
		"partition": f.delivery["partition"], "offset": f.delivery["offset"], "postgres_broker_acknowledged": true}
	if err := q.authorizationControls(f); err != nil {
		return err
	}
	if err := q.invalidTokens(c); err != nil {
		return err
	}
	if err := q.routeMatrix(f, c); err != nil {
		return err
	}
	if _, err := q.http("approval/replay-after-grant", "tenant", probeRequest{Listener: "public", Method: "POST", Path: approvalPath + f.intents["j12-own"].IntentID + "/decision",
		Token: f.approver, Body: decisionBody(f.intents["j12-own"], "APPROVED")}, 200, "", func(_, after snapshot, reply response) bool {
		var a state.ApprovalIntent
		return json.Unmarshal(reply.Body, &a) == nil && f.grantToken != "" && !bytes.Contains(reply.Body, []byte(f.grantToken)) &&
			a.GrantToken == "" && a.GrantScopeHash == "" && a.GrantExpiresAt == nil && after.approved("j12-own", f.intents["j12-own"].IntentID, "approver", true)
	}); err != nil {
		return err
	}
	if err := q.rotation(c); err != nil {
		return err
	}
	final, digest, err := q.snapshot()
	row = result{ID: "fixtures/final", After: digest, Verdict: "invalid"}
	if err == nil {
		row.Verdict = "pass"
		for _, id := range []string{"j12-self", "j12-legacy", "j12-foreign"} {
			if !pending(final, id, f.intents[id].IntentID) {
				row.Verdict = "fail"
			}
		}
		if !final.approved("j12-own", f.intents["j12-own"].IntentID, "approver", true) {
			row.Verdict = "fail"
		}
	}
	if markErr := q.mark(row); markErr != nil {
		return markErr
	}
	return err
}

func initialFixtures(s snapshot, f *fixtures) bool {
	if !s.requester("j12-own", "tenant-a", "requester") || !s.requester("j12-foreign", "tenant-b", "requester") {
		return false
	}
	for _, r := range s.Requesters {
		if r.Workflow == "j12-legacy" {
			return false
		}
	}
	for _, id := range []string{"j12-own", "j12-self", "j12-foreign", "j12-legacy"} {
		if !pending(s, id, f.intents[id].IntentID) {
			return false
		}
	}
	return true
}

func (q campaign) authorizationControls(f *fixtures) error {
	public := func(id, method, path, token string, body object, want int, code string, check observation) (response, error) {
		return q.http(id, "tenant", probeRequest{Listener: "public", Method: method, Path: path, Token: token, Body: body}, want, code, check)
	}
	for _, method := range []string{"GET", "HEAD"} {
		for _, name := range []string{"status", "history"} {
			suffix := "?expected_revision=1"
			if name == "history" {
				suffix = "/history?after_revision=99"
			}
			code := "NOT_FOUND"
			if method == "HEAD" {
				code = ""
			}
			prefix := "workflow/" + method + "/" + name + "/"
			absent, err := public(prefix+"absent", method, workflowPath+missing+suffix, f.requester, nil, 404, code, nil)
			if err != nil {
				return err
			}
			if _, err := public(prefix+"foreign", method, workflowPath+"j12-foreign"+suffix, f.requester, nil, 404, code, sameDenial(absent)); err != nil {
				return err
			}
		}
	}
	absent, err := public("foreign/absent-cancel", "POST", workflowPath+missing+"/cancel-request", f.requester, cancelBody(), 404, "NOT_FOUND", nil)
	if err != nil {
		return err
	}
	if _, err := public("foreign/cancel", "POST", workflowPath+"j12-foreign/cancel-request", f.requester, cancelBody(), 404, "NOT_FOUND", sameDenial(absent)); err != nil {
		return err
	}
	foreign := f.intents["j12-foreign"]
	absent, err = public("foreign/absent-intent", "POST", approvalPath+missingIntent+"/decision", f.approver, decisionBody(foreign, "APPROVED"), 404, "APPROVAL_NOT_FOUND", nil)
	if err != nil {
		return err
	}
	if _, err := public("foreign/intent", "POST", approvalPath+foreign.IntentID+"/decision", f.approver, decisionBody(foreign, "APPROVED"), 404, "APPROVAL_NOT_FOUND",
		func(b, a snapshot, reply response) bool {
			return sameDenial(absent)(b, a, reply) && pending(a, "j12-foreign", foreign.IntentID)
		}); err != nil {
		return err
	}
	own := f.intents["j12-own"]
	badCancel := cancelBody()
	badCancel["workflow_id"] = "j12-foreign"
	badDecision := decisionBody(own, "APPROVED")
	badDecision["intent_id"] = foreign.IntentID
	for _, test := range []struct {
		id, method, path, token string
		body                    object
		status                  int
		code                    string
	}{
		{"wrong-namespace/submit", "POST", "/v1/workflows", f.foreign, submitBody("j12-wrong-namespace", "tenant-a"), 403, "NAMESPACE_FORBIDDEN"},
		{"wrong-namespace/status", "GET", workflowPath + "j12-own", f.foreign, nil, 404, "NOT_FOUND"},
		{"wrong-namespace/cancel", "POST", workflowPath + "j12-own/cancel-request", f.foreign, cancelBody(), 404, "NOT_FOUND"},
		{"wrong-namespace/decision", "POST", approvalPath + own.IntentID + "/decision", f.foreign, decisionBody(own, "APPROVED"), 404, "APPROVAL_NOT_FOUND"},
		{"forged/cancel-path", "POST", workflowPath + "j12-cancel/cancel-request", f.requester, badCancel, 400, "INVALID_REQUEST"},
		{"forged/decision-path", "POST", approvalPath + own.IntentID + "/decision", f.approver, badDecision, 400, "INVALID_REQUEST"},
	} {
		if _, err := public(test.id, test.method, test.path, test.token, test.body, test.status, test.code, nil); err != nil {
			return err
		}
	}
	for _, check := range []struct{ id, workflow, value, token string }{
		{"self/APPROVED", "j12-self", "APPROVED", f.requester}, {"self/REJECTED", "j12-self", "REJECTED", f.requester},
		{"legacy/decision", "j12-legacy", "APPROVED", f.approver},
	} {
		intent := f.intents[check.workflow]
		if _, err := public(check.id, "POST", approvalPath+intent.IntentID+"/decision", check.token, decisionBody(intent, check.value), 403, "APPROVAL_UNAUTHORIZED",
			func(_, s snapshot, _ response) bool { return pending(s, check.workflow, intent.IntentID) }); err != nil {
			return err
		}
	}
	return nil
}

func sameDenial(absent response) observation {
	return func(_, _ snapshot, actual response) bool { return bytes.Equal(absent.Body, actual.Body) }
}

func (q campaign) invalidTokens(c *credentials) error {
	for _, name := range tokenRejections {
		now := time.Now()
		body := claims("requester", "tenant-a", now)
		kid, signer, alg := "current", "current", "RS256"
		switch name {
		case "expired":
			body["exp"] = now.Add(-time.Minute).Unix()
		case "bad-signature":
			signer = "unsigned-by-jwks"
		case "wrong-issuer":
			body["iss"] = "https://other.invalid"
		case "wrong-audience":
			body["aud"] = "other-audience"
		case "missing-subject":
			delete(body, "sub")
		case "missing-namespace":
			delete(body, "namespace")
		case "unknown-kid":
			kid = "unknown"
		case "none", "HS256":
			alg = name
		default:
			body[strings.TrimPrefix(name, "fractional-")] = float64(now.Add(-time.Second).Unix()) + 0.5
		}
		token, err := c.token(kid, signer, alg, body)
		if err != nil {
			return err
		}
		if _, err := q.http("jwt/"+name, "tenant", probeRequest{Listener: "public", Method: "POST", Path: "/v1/workflows", Token: token,
			Body: submitBody("j12-invalid-token", "tenant-a")}, 401, "UNAUTHENTICATED", nil); err != nil {
			return err
		}
	}
	return nil
}

func (q campaign) routeMatrix(f *fixtures, c *credentials) error {
	for _, r := range matrix {
		if r.id == "approval-apply" {
			body := f.applyBody("j12-own")
			body["intent_id"], body["workflow_id"], body["node_id"] = f.intents["j12-own"].IntentID, "j12-own", node
			if _, err := q.http("forged/apply-body", "operator", probeRequest{Listener: "operator", Method: "POST",
				Path: approvalPath + f.intents["j12-foreign"].IntentID + "/apply", Body: body}, 409, "APPROVAL_CONFLICT", nil); err != nil {
				return err
			}
		}
		request := f.requestFor(r)
		for _, listener := range listeners {
			for _, credential := range credentialNames {
				if listener == r.listener && credential == r.credential {
					continue
				}
				request.Listener, request.Token = listener, ""
				if credential == "tenant" {
					request.Token = f.requester
				}
				if credential == "demand" {
					request.Token = c.demand
				}
				want, code := expectation(r, listener, credential)
				if _, err := q.http(matrixID(r, listener, credential), credential, request, want, code, nil); err != nil {
					return err
				}
			}
		}
		request.Listener, request.Method, request.Token = r.listener, "DELETE", f.requester
		if _, err := q.http(matrixID(r, r.listener, "wrong-method"), "tenant", request, 404, "NOT_FOUND", nil); err != nil {
			return err
		}
		request.Method, request.Token = r.method, ""
		if r.credential == "tenant" {
			request.Token = f.requester
		}
		if r.id == "decision" {
			request.Token = f.approver
		}
		if r.credential == "demand" {
			request.Token = c.demand
		}
		if _, err := q.http(matrixID(r, r.listener, r.credential), r.credential, request, r.status, "", f.positive(r)); err != nil {
			return err
		}
	}
	return nil
}

func (f *fixtures) requestFor(r route) probeRequest {
	req := probeRequest{Method: r.method, Listener: r.listener}
	own := f.intents["j12-own"]
	switch r.id {
	case "submit":
		req.Path, req.Body = "/v1/workflows", submitBody("j12-submit", "tenant-a")
	case "status", "status-head":
		req.Path = workflowPath + "j12-own"
	case "history", "history-head":
		req.Path = workflowPath + "j12-own/history"
	case "cancel-request":
		req.Path, req.Body = workflowPath+"j12-cancel/cancel-request", cancelBody()
	case "decision":
		req.Path, req.Body = approvalPath+own.IntentID+"/decision", decisionBody(own, "APPROVED")
	case "approval-create":
		req.Path, req.Body = nodePath("j12-approval-create", "approval"), f.approvalBody("j12-approval-create")
	case "approval-apply":
		req.Path, req.Body = approvalPath+own.IntentID+"/apply", f.applyBody("j12-own")
	case "cancellation-apply":
		req.Path, req.Body = "/v1/cancellations/"+f.cancelID+"/apply", leaseBody(f.leases["j12-cancel"])
	case "demand", "demand-head":
		req.Path = "/v1/demand"
	case "delivery":
		req.Path, req.Body = "/v1/worker-deliveries", f.delivery
	case "claim":
		req.Path, req.Body = nodePath("j12-worker", "claim"), object{"expected_attempt": f.attempt, "worker_id": "forged-worker", "request_id": "j12-claim", "attempt_lease_ms": 300000}
	case "heartbeat":
		req.Path, req.Body = nodePath("j12-worker", "heartbeat"), object{"attempt_number": f.attempt, "claim_token": f.claimToken, "extension_ms": 360000}
	case "result":
		req.Path, req.Body = nodePath("j12-worker", "result"), object{"attempt_number": f.attempt, "claim_token": f.claimToken, "attempt_state": "SUCCEEDED", "payload": object{"observed": "ok"}}
	}
	return req
}

func (f *fixtures) positive(r route) observation {
	return func(before, after snapshot, reply response) bool {
		own := f.intents["j12-own"].IntentID
		switch r.id {
		case "submit":
			return after.requester("j12-submit", "tenant-a", "requester")
		case "status":
			var status struct {
				Workflow struct {
					ID        string `json:"workflow_id"`
					Namespace string
				}
			}
			return json.Unmarshal(reply.Body, &status) == nil && status.Workflow.ID == "j12-own" && status.Workflow.Namespace == "tenant-a"
		case "history":
			var history struct {
				ID      string `json:"workflow_id"`
				Entries []json.RawMessage
			}
			return json.Unmarshal(reply.Body, &history) == nil && history.ID == "j12-own" && len(history.Entries) > 0
		case "status-head", "history-head", "demand-head":
			return len(reply.Body) == 0
		case "cancel-request":
			var request state.CancellationRequest
			if json.Unmarshal(reply.Body, &request) != nil || request.RequestID == "" || request.WorkflowID != "j12-cancel" {
				return false
			}
			f.cancelID = request.RequestID
			return after.cancelled("j12-cancel", f.cancelID, "PENDING")
		case "decision":
			return after.approved("j12-own", own, "approver", false)
		case "approval-create":
			var intent state.ApprovalIntent
			if json.Unmarshal(reply.Body, &intent) != nil || !pending(after, "j12-approval-create", intent.IntentID) {
				return false
			}
			for _, history := range after.History {
				if history.Workflow == "j12-approval-create" && history.Reason == "APPROVAL_REQUESTED" && history.Actor == f.operatorIdentity {
					return true
				}
			}
		case "approval-apply":
			var grant state.ApprovalGrant
			if json.Unmarshal(reply.Body, &grant) != nil || grant.IntentID != own || grant.GrantToken == "" {
				return false
			}
			f.grantToken = grant.GrantToken
			return after.approved("j12-own", own, "approver", true)
		case "cancellation-apply":
			return after.cancelled("j12-cancel", f.cancelID, "APPLIED")
		case "demand":
			var demand state.DemandSample
			if json.Unmarshal(reply.Body, &demand) != nil {
				return false
			}
			return demand.Namespace == "tenant-a" && demand.ClaimableAttempts == 1 && demand.ActiveClaims == 0 && demand.Demand == 1 &&
				time.Since(demand.SampledAt).Abs() <= 10*time.Second
		case "delivery":
			var delivery struct {
				Disposition string
				Task        *state.RuntimeActivity
			}
			if json.Unmarshal(reply.Body, &delivery) != nil || delivery.Task == nil || delivery.Task.WorkflowID != "j12-worker" || delivery.Disposition != "ACCEPTED" {
				return false
			}
			for _, row := range after.Inbox {
				if row.Event == f.delivery["event_id"] && row.Disposition == "ACCEPTED" && row.Partition == f.delivery["partition"] && row.Offset == f.delivery["offset"] {
					return true
				}
			}
		case "claim":
			var claim struct {
				Token   string `json:"claim_token"`
				Attempt int64  `json:"attempt_number"`
			}
			if json.Unmarshal(reply.Body, &claim) != nil || claim.Token == "" || claim.Attempt != f.attempt {
				return false
			}
			f.claimToken = claim.Token
			for _, row := range after.Attempts {
				if row.Workflow == "j12-worker" && row.State == "CLAIMED" && row.Claim && row.Worker == f.workerIdentity {
					return true
				}
			}
		case "heartbeat":
			for _, old := range before.Attempts {
				for _, row := range after.Attempts {
					if old.Workflow == "j12-worker" && row.Workflow == old.Workflow && row.State == "CLAIMED" && row.Heartbeat != old.Heartbeat {
						return true
					}
				}
			}
		case "result":
			for _, row := range after.Attempts {
				if row.Workflow == "j12-worker" && row.State == "SUCCEEDED" && row.Result && row.Worker == f.workerIdentity {
					return true
				}
			}
		}
		return false
	}
}

func (q campaign) rotation(c *credentials) error {
	old, err := c.token("current", "current", "RS256", claims("requester", "tenant-a", time.Now()))
	if err != nil {
		return err
	}
	next, err := c.token("next", "next", "RS256", claims("requester", "tenant-a", time.Now()))
	if err != nil {
		return err
	}
	for _, item := range []struct {
		name, token string
		update      bool
		keys        []byte
		want        int
	}{
		{"initial", old, false, nil, 200},
		{"overlap-old", old, true, c.jwks("current", "next"), 200}, {"overlap-new", next, false, nil, 200},
		{"removed-old", old, true, c.jwks("next"), 401}, {"current-new", next, false, nil, 200},
		{"invalid", next, true, []byte(`{"keys":[]}`), 401}, {"missing", next, true, nil, 401},
		{"restored", next, true, c.jwks("next"), 200},
	} {
		if item.update {
			if err := q.host.setKeys(item.keys); err != nil {
				return fmt.Errorf("rotate J12 JWKS: %w", err)
			}
		}
		code := ""
		if item.want == 401 {
			code = "UNAUTHENTICATED"
		}
		if _, err := q.http("jwks/"+item.name, "tenant", probeRequest{Listener: "public", Method: "GET", Path: workflowPath + "j12-own", Token: item.token}, item.want, code, nil); err != nil {
			return err
		}
	}
	return nil
}
