package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"durable-agent-execution-engine/internal/engine"
	"durable-agent-execution-engine/internal/j7markers"
	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/restoreledger"
	"durable-agent-execution-engine/internal/state"
)

type fixtureWorkload struct {
	work        drillWorkload
	markers     j7markers.Plan
	requests    map[string][]byte
	definitions []state.DefinitionInput
	effect      engine.K6Operation
}

func buildFixtureWorkload(campaign, drill, namespace string, clock orphanClock) (fixtureWorkload, error) {
	w := fixtureWorkload{requests: map[string][]byte{}}
	for _, def := range []struct{ id, node, class string }{
		{"j7-echo", "pure.echo", "PURE_ACTIVITY"}, {"j7-effect", state.K6Activity, "COOPERATING_EFFECT"},
	} {
		graph := json.RawMessage(fmt.Sprintf(`{"entry":%q,"nodes":[{"id":%q,"kind":"activity","next":"done"},{"id":"done","kind":"success"}]}`, def.node, def.node))
		versions := json.RawMessage(fmt.Sprintf(`{%q:"v1"}`, def.node))
		classes := json.RawMessage(fmt.Sprintf(`{%q:%q}`, def.node, def.class))
		hash := restoreledger.SHA256([]byte(string(graph) + string(versions) + string(classes)))
		w.definitions = append(w.definitions, state.DefinitionInput{DefinitionID: def.id, Version: 1, DefinitionHash: hash,
			Graph: graph, ActivityVersions: versions, EffectClasses: classes})
	}
	f := j7markers.Fixture{Campaign: campaign, Drill: drill, Namespace: namespace,
		DefinitionID: w.definitions[0].DefinitionID, DefinitionSHA256: w.definitions[0].DefinitionHash}
	p, err := j7markers.NewPlan(f)
	if err != nil {
		return w, err
	}
	w.markers = p
	w.work = drillWorkload{Schema: "j7.workload.v1", Fixture: f, Clock: clock}
	used := map[uint64]bool{}
	for _, m := range p.Markers {
		i, _ := partition.ID(m.WorkflowID)
		used[i] = true
	}
	prefix := restoreledger.SHA256([]byte(campaign + "\x00" + drill))
	for _, role := range []string{"d1", "d2", "f1", "e1", "n1", "n2", "n3", "n4", "l1"} {
		id := "j7-" + prefix + "-" + role
		if role == "l1" {
			// Keep the held owner partition out of the marker and remaining cohort paths.
			found := false
			for n := 0; n < 1024; n++ {
				candidate := fmt.Sprintf("%s-%d", id, n)
				part, _ := partition.ID(candidate)
				if !used[part] {
					id = candidate
					found = true
					break
				}
			}
			if !found {
				return w, errors.New("J7 cohort does not leave an independent L1 partition")
			}
		} else {
			// Reserve one deterministic free partition for L1 before choosing the
			// next non-marker ID, including the four target-only submissions.
			for n := 0; n < 1024; n++ {
				part, _ := partition.ID(id)
				if used[part] || uint64(len(used)) < partition.PartitionCount-1 {
					used[part] = true
					break
				}
				id = fmt.Sprintf("j7-%s-%s-%d", prefix, role, n)
			}
		}
		def, node := w.definitions[0].DefinitionID, "pure.echo"
		if role == "e1" {
			def, node = w.definitions[1].DefinitionID, state.K6Activity
		}
		input := json.RawMessage(fmt.Sprintf(`{"cohort":%q,"drill":%q}`, role, drill))
		s := plannedSubmission{Role: role, ID: id, Key: id, RequestID: id + "-submit", Definition: def, InitialNode: node, Input: input}
		body, err := json.Marshal(struct {
			ID         string          `json:"workflow_id"`
			Namespace  string          `json:"namespace"`
			Key        string          `json:"submission_key"`
			Payload    json.RawMessage `json:"payload"`
			Definition string          `json:"definition_id"`
			Version    int             `json:"definition_version"`
			Node       string          `json:"initial_node_id"`
			Input      json.RawMessage `json:"initial_input"`
		}{id, namespace, id, input, def, 1, node, input})
		if err != nil {
			return w, err
		}
		s.RequestSHA = restoreledger.SHA256(body)
		w.requests[role] = body
		w.work.NonMarkers = append(w.work.NonMarkers, s)
		if role == "e1" {
			w.effect = engine.K6Operation{SubmissionKey: id, DefinitionID: def, DefinitionVersion: 1, NodeID: node,
				Resource: "j7-resource-" + prefix, ExpectedRevision: "0", Arguments: json.RawMessage(`{"fixture":"j7"}`), LogicalKey: "j7-effect-" + prefix}
		}
	}
	s := w.work.NonMarkers
	// The checker binds the source/target population in this acquisition order.
	w.work.NonMarkers = append(append(append([]plannedSubmission{}, s[:3]...), s[8]), s[3:8]...)
	return w, nil
}

func (w fixtureWorkload) submission(role string) plannedSubmission {
	for _, s := range w.work.NonMarkers {
		if s.Role == role {
			return s
		}
	}
	return plannedSubmission{}
}

type fixtureHTTP struct {
	client        *http.Client
	origin, token string
}

func (c fixtureHTTP) post(ctx context.Context, path, id string, raw []byte) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.origin+path, bytes.NewReader(raw))
	if err != nil {
		return 0, nil, err
	}
	req.GetBody = nil
	req.Header.Set("Content-Type", "application/json")
	if id != "" {
		req.Header.Set("X-Request-ID", id)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil || len(data) > 1<<20 {
		return 0, nil, errors.New("fixture API response could not be read")
	}
	return resp.StatusCode, data, nil
}

func submitFixture(ctx context.Context, j j7markers.Journal, c fixtureHTTP, authority, namespace string,
	s plannedSubmission, body []byte) (restoreledger.Exchange, error) {
	var x restoreledger.Exchange
	if s.ID == "" || s.RequestSHA != restoreledger.SHA256(body) {
		return x, errors.New("submission differs from predeclared fixture")
	}
	op := restoreledger.Operation{RequestID: s.RequestID, Kind: restoreledger.Submit, Phase: "fixture", Authority: authority,
		WorkflowID: s.ID, Namespace: namespace, SubmissionKey: s.Key, RequestSHA256: s.RequestSHA}
	x.Operation = op
	var err error
	x.SendSynced, err = j.RecordSend(op)
	if err != nil {
		return x, err
	}
	start, err := j.NowNS()
	if err != nil {
		return x, err
	}
	status, response, requestErr := c.post(ctx, "/v1/workflows", s.RequestID, body)
	end, err := j.NowNS()
	if err != nil {
		return x, err
	}
	o := restoreledger.Outcome{RequestID: s.RequestID, SentNS: start, CompletedNS: end, HTTPStatus: status}
	if requestErr != nil {
		o.Disposition, o.Code = restoreledger.Ambiguous, "HTTP_INCOMPLETE"
		o.CompletedNS = 0
	} else {
		o.ResponseSHA256 = restoreledger.SHA256(response)
		var r struct {
			Created  bool `json:"created"`
			Workflow struct {
				ID         string `json:"workflow_id"`
				Namespace  string `json:"namespace"`
				Key        string `json:"submission_key"`
				Definition string `json:"definition_id"`
				Version    int    `json:"definition_version"`
				Hash       string `json:"payload_hash"`
				Partition  *int16 `json:"partition_id"`
				State      string `json:"state"`
				Revision   int    `json:"revision"`
			} `json:"workflow"`
		}
		part, _ := partition.ID(s.ID)
		decodeErr := json.Unmarshal(response, &r)
		v := r.Workflow
		hash, ok := bytes.CutPrefix([]byte(v.Hash), []byte("sub-v1:"))
		switch {
		case status >= 400 && status < 500:
			o.Disposition, o.Code = restoreledger.Rejected, "HTTP_REJECTED"
		case status == http.StatusCreated && decodeErr == nil && r.Created && v.ID == s.ID && v.Namespace == namespace && v.Key == s.Key && v.Definition == s.Definition &&
			v.Version == 1 && v.State == "RUNNABLE" && v.Revision == 1 && v.Partition != nil && *v.Partition == int16(part) &&
			ok && restoreledger.IsSHA256(string(hash)):
			o.Disposition, o.DurableID, o.PayloadHash = restoreledger.Acknowledged, s.ID, v.Hash
		default:
			o.Disposition, o.Code = restoreledger.Ambiguous, "CREATION_IDENTITY_MISMATCH"
		}
	}
	x.Outcome = &o
	x.OutcomeSync, err = j.RecordOutcome(o)
	if err != nil {
		return x, err
	}
	if !x.Acked() {
		return x, fmt.Errorf("fixture submission %s did not acknowledge (%s)", s.Role, o.Code)
	}
	return x, nil
}

func newFixtureHTTP(origin, token string, transport *http.Transport) fixtureHTTP {
	return fixtureHTTP{origin: origin, token: token,
		client: &http.Client{Timeout: 4 * time.Second, Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}
