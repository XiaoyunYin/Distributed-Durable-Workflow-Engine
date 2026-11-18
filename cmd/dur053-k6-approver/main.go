// dur053-k6-approver presents a private credential to the local decision socket.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/k6auth"
	"durable-agent-execution-engine/internal/k6auth/smokeplan"
	"durable-agent-execution-engine/internal/state"
)

type input struct {
	Plan     smokeplan.Plan
	IntentID string
	Choice   *string
	Actor    *string
}

func readPrivate(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Size() > 64<<10 {
		return nil, errors.New("APPROVER_PRIVATE_INPUT")
	}
	return os.ReadFile(path)
}

func present(socket string, request input, token string) error {
	hash, err := state.K6ProposalHash(request.Plan.Target, request.Plan.Arguments, request.Plan.ResourceRevision)
	if err != nil {
		return errors.New("APPROVER_PLAN_HASH")
	}
	iteration := int32(0)
	body, err := json.Marshal(k6auth.DecisionRequest{
		WorkflowID: request.Plan.WorkflowID, NodeID: request.Plan.NodeID, Iteration: &iteration,
		IntentID: request.IntentID, ProposalHash: hash, Decision: request.Choice, Actor: request.Actor,
	})
	if err != nil {
		return errors.New("APPROVER_REQUEST")
	}
	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		}},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	req, err := http.NewRequest(http.MethodPost, "http://localhost/v1/approval-decision", bytes.NewReader(body))
	if err != nil {
		return errors.New("APPROVER_REQUEST")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return errors.New("APPROVER_SOCKET_REQUEST")
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusUnprocessableEntity {
		data, err := io.ReadAll(io.LimitReader(resp.Body, 64))
		code := strings.TrimSpace(string(data))
		if err != nil || (resp.StatusCode == http.StatusForbidden && code != k6auth.ErrDenied.Error()) ||
			(resp.StatusCode == http.StatusUnprocessableEntity && code != k6auth.ErrExpired.Error()) {
			return errors.New("APPROVER_DENIAL_RESPONSE")
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"status": resp.StatusCode, "code": code})
	}
	var identity k6auth.DecisionIdentity
	if resp.StatusCode != http.StatusOK || json.NewDecoder(io.LimitReader(resp.Body, 8192)).Decode(&identity) != nil ||
		identity.ProposalHash != hash || identity.IntentID == "" {
		return errors.New("APPROVER_DECISION_RESPONSE")
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"status": resp.StatusCode, "identity": identity})
}

func run() error {
	if len(os.Args) != 4 {
		return errors.New("APPROVER_USAGE")
	}
	body, err := readPrivate(os.Args[2])
	var request input
	if err != nil || json.Unmarshal(body, &request) != nil {
		return errors.New("APPROVER_PRIVATE_PLAN")
	}
	var token []byte
	if os.Args[3] != "-" {
		token, err = readPrivate(os.Args[3])
		if err != nil || len(token) == 0 {
			return errors.New("APPROVER_PRIVATE_TOKEN")
		}
	}
	return present(os.Args[1], request, string(token))
}

func main() {
	if err := run(); err != nil {
		// Never pass OS, JSON or HTTP parser diagnostics across the boundary.
		fmt.Fprintln(os.Stderr, "K6_APPROVER_FAILED")
		os.Exit(1)
	}
}
