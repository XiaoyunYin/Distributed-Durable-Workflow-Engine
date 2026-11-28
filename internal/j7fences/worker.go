package j7fences

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/restoreledger"
	"durable-agent-execution-engine/internal/state"
)

const (
	workerLease   = 60 * time.Second
	claimID       = "j7-worker-claim"
	staleResultID = "j7-worker-stale-result"
	freshResultID = "j7-worker-fresh-result"
)

type WorkerControl struct {
	Origin   string
	Identity string
	Client   *http.Client
	Bearer   func(context.Context) (string, error)
}

type WorkerInput struct {
	Source        SourceClaim
	Target        Target
	ArchivedToken string
	N1Workflow    string
	N1Ack         restoreledger.Exchange
	Result        json.RawMessage
	Control       WorkerControl
}

type WorkerEvidence struct {
	Source         SourceClaim            `json:"source"`
	Target         Target                 `json:"target"`
	N1Request      string                 `json:"n1_request_id"`
	N1SyncedNS     int64                  `json:"n1_ack_synced_ns"`
	ControlOrigin  string                 `json:"worker_control_origin"`
	Worker         string                 `json:"authenticated_worker"`
	Replacement    restoreledger.ClaimRef `json:"replacement"`
	ReplacementSHA string                 `json:"fenced_replacement_sha256,omitempty"`
	BeforeSHA      string                 `json:"before_denial_sha256,omitempty"`
	AfterSHA       string                 `json:"after_denial_sha256,omitempty"`
	DenialCode     string                 `json:"denial_code,omitempty"`
	TerminalSHA    string                 `json:"terminal_sha256,omitempty"`
}

type claimReply struct {
	AttemptNumber int64             `json:"attempt_number"`
	ClaimToken    string            `json:"claim_token"`
	EffectClass   state.EffectClass `json:"effect_class"`
	Deadline      time.Time         `json:"heartbeat_deadline"`
}

func (c WorkerControl) httpPath(scope Scope) (*url.URL, error) {
	u, err := url.Parse(c.Origin)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" ||
		u.Fragment != "" || u.ForceQuery || u.RawPath != "" || (u.Path != "" && u.Path != "/") ||
		c.Client == nil || c.Bearer == nil {
		return nil, errors.New("worker probe requires an authenticated private HTTPS origin")
	}
	u.Path = fmt.Sprintf("/v1/workflows/%s/nodes/%s/iterations/%d", scope.Workflow, scope.Node, scope.Iteration)
	return u, nil
}

func checkWorkerInput(in WorkerInput, authority string, now int64) error {
	x := in.N1Ack
	a := in.Source.Attempt
	w := in.Source.Scope
	identity := strings.Split(in.Control.Identity, ":")
	if len(identity) != 8 || identity[0] != "system" || identity[1] != "serviceaccount" ||
		identity[2] == "" || identity[3] == "" || identity[4] != "pod" ||
		identity[5] == "" || identity[6] != "uid" || identity[7] == "" {
		return errors.New("worker probe requires the fixture worker's pod-bound ServiceAccount identity")
	}
	if !x.Acked() || x.Operation.Kind != restoreledger.Submit || x.Operation.Authority != authority ||
		in.N1Workflow == "" || in.N1Workflow == w.Workflow || x.Operation.WorkflowID != in.N1Workflow ||
		x.Operation.Namespace != w.Namespace || x.Outcome.DurableID != in.N1Workflow ||
		x.Operation.RequestID == "" || x.Outcome.RequestID != x.Operation.RequestID || x.Outcome.HTTPStatus != http.StatusCreated ||
		x.SendSynced <= 0 || x.Outcome.SentNS < x.SendSynced || x.Outcome.CompletedNS < x.Outcome.SentNS ||
		x.OutcomeSync < x.Outcome.CompletedNS ||
		x.OutcomeSync < in.Target.T0NS || x.OutcomeSync > now {
		return errors.New("F1 intake requires the durable N1 acknowledgment")
	}
	if w.Node == "" || strings.ContainsAny(w.Workflow+w.Node, "/?#") || a.Number != 1 ||
		a.Node != w.Node || a.Iteration != w.Iteration || a.State != state.AttemptClaimed ||
		!a.Current || a.Class != state.EffectPure || in.Source.DBTime.IsZero() ||
		!a.Deadline.After(in.Source.DBTime) || in.ArchivedToken == "" ||
		restoreledger.SHA256([]byte(in.ArchivedToken)) != a.TokenSHA || !json.Valid(in.Result) {
		return errors.New("F1 source claim, exact private token or sealed result does not match")
	}
	return nil
}

// RunWorker waits for the scheduler's ordinary fenced replacement. Keep
// normal worker intake for F1 held while this probe claims through the API.
func (p *Probe) RunWorker(ctx context.Context, in WorkerInput) (WorkerEvidence, error) {
	src, scope := in.Source, in.Source.Scope
	out := WorkerEvidence{Source: src, Target: in.Target, N1Request: in.N1Ack.Operation.RequestID,
		N1SyncedNS: in.N1Ack.OutcomeSync, ControlOrigin: in.Control.Origin, Worker: in.Control.Identity}
	ctx, stop, err := p.targetContext(ctx, src.Database, src.Authority, src.BootID, src.Evidence,
		src.StartedNS, src.EndedNS, in.Target)
	if err != nil {
		return out, err
	}
	defer stop()
	authority := p.journal.Header().Target
	now, err := p.journal.NowNS()
	if err != nil {
		return out, err
	}
	if err := checkWorkerInput(in, authority, now); err != nil {
		return out, err
	}
	path, err := in.Control.httpPath(scope)
	if err != nil {
		return out, err
	}
	ready, err := p.wait(ctx, scope, in.Target.Database, false, func(s sample) (bool, error) {
		return replacementReady(s, src)
	})
	if err != nil {
		return out, err
	}
	original := src.Attempt.ref()
	if scope.Node == "pure.echo" {
		n, err := ready.node()
		if err != nil || !resultEqual(n.Input, in.Result) {
			return out, errors.New("sealed F1 echo result differs from its immutable input")
		}
	}
	out.ReplacementSHA, err = p.witness("j7-worker-replacement", authority, "", ready, nil, &original)
	if err != nil {
		return out, err
	}
	token, err := in.Control.Bearer(ctx)
	if err != nil || token == "" || strings.ContainsAny(token, " \t\r\n") {
		return out, errors.New("worker-control probe credential unavailable")
	}
	client := *in.Control.Client
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	claim, err := p.claimHTTP(ctx, &client, path, token, scope)
	if err != nil {
		return out, err
	}
	before, err := p.observe(ctx, scope, in.Target.Database, false, true)
	if err != nil {
		return out, err
	}
	live, err := liveClaim(before, 2)
	if err != nil || live.Worker != in.Control.Identity || live.Request != claimID ||
		live.TokenSHA != restoreledger.SHA256([]byte(claim.ClaimToken)) ||
		!live.Deadline.Equal(claim.Deadline) || live.TokenSHA == original.TokenSHA256 {
		return out, errors.New("authenticated HTTP claim did not establish the live restored F1 replacement")
	}
	out.Replacement = live.ref()
	out.BeforeSHA, err = p.witness("j7-worker-before", authority, claimID, before, nil, &out.Replacement)
	if err != nil {
		return out, err
	}
	code, denialErr := p.resultHTTP(ctx, &client, path, token, scope, staleResultID, original, in.ArchivedToken, in.Result, true)
	out.DenialCode = code
	after, err := p.observe(ctx, scope, in.Target.Database, false, true)
	if err != nil {
		return out, errors.Join(denialErr, err)
	}
	out.AfterSHA, err = p.witness("j7-worker-after-denial", authority, staleResultID, after, nil, &out.Replacement)
	if err = errors.Join(denialErr, err, stable(before, after)); err != nil {
		return out, err
	}
	if _, err := liveClaim(after, 2); err != nil {
		return out, err
	}
	if _, err := p.resultHTTP(ctx, &client, path, token, scope, freshResultID, out.Replacement, claim.ClaimToken, in.Result, false); err != nil {
		return out, err
	}
	start, _ := before.workflow()
	terminal, err := p.wait(ctx, scope, in.Target.Database, false, func(s sample) (bool, error) {
		w, _ := s.workflow()
		if w.State == state.StateSucceeded {
			return true, checkWorkerTerminal(s, out.Replacement, in.Control.Identity, in.Result, start.Revision)
		}
		return false, checkPending(w)
	})
	if err != nil {
		return out, err
	}
	out.TerminalSHA, err = p.witness("j7-worker-terminal", authority, freshResultID, terminal, nil, &out.Replacement)
	return out, err
}

func replacementReady(s sample, source SourceClaim) (bool, error) {
	if err := checkClock(s); err != nil {
		return false, err
	}
	w, _ := s.workflow()
	if w.State != state.StateWaitingActivity {
		return false, errors.New("F1 left the held activity before the replacement probe")
	}
	n, err := s.node()
	if err != nil {
		return false, err
	}
	rows, err := s.attempts()
	if err != nil {
		return false, err
	}
	var old, next Attempt
	count := 0
	for _, a := range rows {
		if a.Node != source.Scope.Node || a.Iteration != source.Scope.Iteration {
			continue
		}
		count++
		switch a.Number {
		case 1:
			old = a
		case 2:
			next = a
		}
	}
	if old.Number != 1 || old.TokenSHA != source.Attempt.TokenSHA || old.Worker != source.Attempt.Worker ||
		old.Request != source.Attempt.Request || old.Deadline.Before(source.Attempt.Deadline) ||
		old.Class != state.EffectPure || old.Deadline.IsZero() || count > 2 {
		return false, errors.New("archived F1 claim is missing, changed or has extra replacements")
	}
	if count == 1 && n.Current == 1 && old.Current && old.State == state.AttemptClaimed {
		return false, nil
	}
	if count != 2 || n.Current != 2 || old.Current || old.State != state.AttemptTimedOut ||
		old.Deadline.After(s.DBTime) || next.Number != 2 || !next.Current ||
		next.State != state.AttemptDispatchable || next.TokenSHA != "" || next.Worker != "" || next.Request != "" ||
		next.Class != state.EffectPure || !next.Deadline.After(s.DBTime) {
		return false, errors.New("F1 replacement did not follow natural expiry or was claimed early")
	}
	rowsHistory, err := historyRows(s)
	if err != nil {
		return false, err
	}
	var timeoutRevision int64
	for _, h := range rowsHistory {
		if h.Reason != "TIMEOUT_REPLACEMENT" {
			continue
		}
		if timeoutRevision != 0 || h.ActorKind != "scheduler" || h.Epoch <= 0 || h.Node != s.Scope.Node ||
			h.Iteration != s.Scope.Iteration || h.Attempt != 1 || h.At.Before(old.Deadline) {
			return false, errors.New("F1 times out without one fenced scheduler history entry after the archived deadline")
		}
		timeoutRevision = h.Revision
	}
	if timeoutRevision == 0 {
		return false, errors.New("missing the fenced F1 replacement history")
	}
	return replacementPublished(s, timeoutRevision)
}

func replacementPublished(s sample, revision int64) (bool, error) {
	var outboxRows []outbox
	if err := json.Unmarshal(s.Rows["outbox"], &outboxRows); err != nil {
		return false, err
	}
	var eventID string
	for _, row := range outboxRows {
		if row.Revision != revision {
			continue
		}
		var notice struct {
			Workflow  string `json:"workflow_id"`
			Node      string `json:"node_id"`
			Iteration int    `json:"iteration"`
			Attempt   int64  `json:"attempt_number"`
		}
		if eventID != "" || row.Kind != "attempt.dispatch" || json.Unmarshal(row.Payload, &notice) != nil ||
			notice.Workflow != s.Scope.Workflow || notice.Node != s.Scope.Node || notice.Iteration != s.Scope.Iteration || notice.Attempt != 2 {
			return false, errors.New("F1 replacement dispatch does not match the ordinary outbox record")
		}
		eventID = row.ID
	}
	if eventID == "" {
		return false, errors.New("missing F1 replacement dispatch outbox row")
	}
	var pubs []struct {
		Event string `json:"event_id"`
		Ack   bool   `json:"broker_acknowledged"`
	}
	if err := json.Unmarshal(s.Rows["publications"], &pubs); err != nil {
		return false, err
	}
	for _, pub := range pubs {
		if pub.Ack && pub.Event == eventID {
			return true, nil
		}
	}
	return false, nil
}

func postWorker(ctx context.Context, client *http.Client, path *url.URL, token, action, id string, raw []byte) (int, []byte, error) {
	url := *path
	url.Path += "/" + action
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url.String(), bytes.NewReader(raw))
	if err != nil {
		return 0, nil, err
	}
	req.GetBody = nil
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Request-ID", id)
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, errors.New("worker-control request did not complete")
	}
	defer resp.Body.Close()
	const max = 1 << 20
	body, err := io.ReadAll(io.LimitReader(resp.Body, max+1))
	if err != nil || len(body) > max {
		return resp.StatusCode, nil, errors.New("worker-control response was incomplete")
	}
	return resp.StatusCode, body, nil
}

func (p *Probe) claimHTTP(ctx context.Context, client *http.Client, path *url.URL, token string, s Scope) (claimReply, error) {
	var claim claimReply
	raw := payload(struct {
		Worker  string `json:"worker_id"`
		Request string `json:"request_id"`
		Lease   int64  `json:"attempt_lease_ms"`
		Attempt int64  `json:"expected_attempt"`
	}{"j7-fence-probe", claimID, workerLease.Milliseconds(), 2})
	op, err := p.send(restoreledger.Operation{RequestID: claimID, Kind: restoreledger.Claim,
		Phase: "j7-worker-fence", WorkflowID: s.Workflow, Namespace: s.Namespace}, raw)
	if err != nil {
		return claim, err
	}
	code, body, err := postWorker(ctx, client, path, token, "claim", claimID, raw)
	if err != nil || code != http.StatusOK || json.Unmarshal(body, &claim) != nil ||
		claim.AttemptNumber != 2 || claim.ClaimToken == "" || claim.EffectClass != state.EffectPure || claim.Deadline.IsZero() {
		return claim, errors.Join(errors.New("F1 replacement claim did not receive a typed success"), err, recordHTTPFailure(op, code, body))
	}
	ref := restoreledger.ClaimRef{NodeID: s.Node, Iteration: s.Iteration, Attempt: claim.AttemptNumber,
		TokenSHA256: restoreledger.SHA256([]byte(claim.ClaimToken))}
	return claim, op.outcome(restoreledger.Outcome{Disposition: restoreledger.Acknowledged, HTTPStatus: code,
		DurableID: s.Workflow, Claim: &ref}, body)
}

func (p *Probe) resultHTTP(ctx context.Context, client *http.Client, path *url.URL, token string, s Scope,
	id string, ref restoreledger.ClaimRef, claimToken string, result json.RawMessage, stale bool) (string, error) {
	raw := payload(struct {
		Attempt int64              `json:"attempt_number"`
		Token   string             `json:"claim_token"`
		State   state.AttemptState `json:"attempt_state"`
		Payload json.RawMessage    `json:"payload"`
	}{ref.Attempt, claimToken, state.AttemptSucceeded, result})
	op, err := p.send(restoreledger.Operation{RequestID: id, Kind: restoreledger.Result, Phase: "j7-worker-fence",
		WorkflowID: s.Workflow, Namespace: s.Namespace, Claim: &ref}, raw)
	if err != nil {
		return "", err
	}
	status, body, err := postWorker(ctx, client, path, token, "result", id, raw)
	code := apiErrorCode(body)
	if err == nil && status == http.StatusConflict && (code == "STALE_CLAIM" || code == "STALE_ATTEMPT") {
		recordErr := op.outcome(restoreledger.Outcome{Disposition: restoreledger.Rejected, HTTPStatus: status, Code: code}, body)
		if stale {
			return code, recordErr
		}
		return code, errors.Join(errors.New("live F1 claim was rejected"), recordErr)
	}
	var reply state.ResultReceipt
	if err == nil && status == http.StatusOK && json.Unmarshal(body, &reply) == nil &&
		reply.Disposition == state.ResultAccepted && reply.WorkflowID == s.Workflow && reply.NodeID == s.Node &&
		reply.Iteration == s.Iteration && reply.AttemptNumber == ref.Attempt &&
		reply.AttemptState == state.AttemptSucceeded && resultEqual(reply.Payload, result) {
		logErr := op.outcome(restoreledger.Outcome{Disposition: restoreledger.Acknowledged, HTTPStatus: status, DurableID: s.Workflow}, body)
		if stale {
			return "", errors.Join(errors.New("archived worker claim successfully submitted a result"), logErr)
		}
		return "", logErr
	}
	return code, errors.Join(errors.New("worker result did not produce the required typed receipt"), err, recordHTTPFailure(op, status, body))
}

func apiErrorCode(body []byte) string {
	var out struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &out) == nil && out.Error.Code != "" && len(out.Error.Code) <= 80 &&
		strings.IndexFunc(out.Error.Code, func(r rune) bool { return !(r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_') }) == -1 {
		return out.Error.Code
	}
	return "UNPROVED_HTTP_RECEIPT"
}

func recordHTTPFailure(op operation, status int, body []byte) error {
	if body != nil && status >= 400 && status < 500 {
		return op.outcome(restoreledger.Outcome{Disposition: restoreledger.Rejected, HTTPStatus: status, Code: apiErrorCode(body)}, body)
	}
	return op.outcome(restoreledger.Outcome{Disposition: restoreledger.Ambiguous, HTTPStatus: status, Code: "UNPROVED_HTTP_RECEIPT"}, body)
}

func checkWorkerTerminal(s sample, fresh restoreledger.ClaimRef, identity string, result json.RawMessage, previousRevision int64) error {
	w, _ := s.workflow()
	n, err := s.node()
	if err != nil || n.Current != 0 || !resultEqual(n.Result, result) || w.Revision <= previousRevision {
		return errors.New("F1 terminal state lacks the accepted replacement result")
	}
	attempts, err := s.attempts()
	if err != nil {
		return err
	}
	var count int
	for _, a := range attempts {
		if a.Node == fresh.NodeID && a.Iteration == fresh.Iteration && a.Number == fresh.Attempt &&
			a.TokenSHA == fresh.TokenSHA256 && a.Worker == identity && a.Request == claimID &&
			a.State == state.AttemptSucceeded && !a.Current && a.Outcome == "KNOWN_SUCCESS" && resultEqual(a.Result, result) {
			count++
		}
	}
	rows, err := historyRows(s)
	if err != nil {
		return err
	}
	var recorded, consumed int64
	for _, h := range rows {
		if h.Revision <= previousRevision {
			continue
		}
		switch h.Reason {
		case "ATTEMPT_RESULT_RECORDED":
			if recorded != 0 || h.ActorKind != "worker" || h.Actor != identity ||
				h.Attempt != fresh.Attempt || h.Node != fresh.NodeID || h.Iteration != fresh.Iteration {
				return errors.New("F1 result history does not match its fresh claim")
			}
			recorded = h.Revision
		case "RESULT_CONSUMED":
			if consumed != 0 || h.ActorKind != "scheduler" || h.Epoch <= 0 || h.NewState != state.StateSucceeded ||
				h.Attempt != fresh.Attempt || h.Node != fresh.NodeID || h.Iteration != fresh.Iteration {
				return errors.New("F1 result consumption is not the expected fenced success")
			}
			consumed = h.Revision
		}
	}
	if count != 1 || recorded == 0 || consumed <= recorded || consumed != w.Revision {
		return errors.New("F1 lacks one recorded and consumed successful replacement result")
	}
	return nil
}
