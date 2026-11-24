package k6sink

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/k6auth"
	"durable-agent-execution-engine/internal/state"
	"github.com/google/uuid"
)

var errDenied = k6auth.ErrDenied

type GrantReader interface {
	ReadExactGrant(context.Context, k6auth.ExactGrantRequest) (k6auth.ExactGrant, error)
}

// WorkerIdentity comes from trusted episode setup. Each projected Pod token
// also carries its individual binding, which Kubernetes checks on TokenReview.
type WorkerIdentity struct {
	Namespace, ServiceAccount, ServiceAccountUID string
}

func (w WorkerIdentity) authenticate(ctx context.Context, reviewer k6auth.Reviewer, token string, now func() time.Time) (time.Time, error) {
	if token == "" || len(token) > 16<<10 || strings.ContainsAny(token, " \t\r\n") {
		return time.Time{}, errDenied
	}
	review, err := reviewer.Review(ctx, token)
	subject := "system:serviceaccount:" + w.Namespace + ":" + w.ServiceAccount
	if err != nil || review.Error != "" || !review.Authenticated || !sinkAudience(review.Audiences) ||
		review.User.Username != subject || review.User.UID != w.ServiceAccountUID {
		return time.Time{}, errDenied
	}
	// TokenReview authenticated these claims on the same token.
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] == "" || parts[2] == "" {
		return time.Time{}, errDenied
	}
	payload, err := base64.RawURLEncoding.Strict().DecodeString(parts[1])
	if err != nil {
		return time.Time{}, errDenied
	}
	var claims struct {
		Issuer    string   `json:"iss"`
		Subject   string   `json:"sub"`
		Audiences []string `json:"aud"`
		IssuedAt  int64    `json:"iat"`
		ExpiresAt int64    `json:"exp"`
		Binding   struct {
			Namespace           string `json:"namespace"`
			ServiceAccount, Pod struct{ Name, UID string }
		} `json:"kubernetes.io"`
	}
	if json.Unmarshal(payload, &claims) != nil || claims.Issuer != k6auth.Issuer ||
		claims.Subject != subject || !sinkAudience(claims.Audiences) ||
		claims.Binding.Namespace != w.Namespace || claims.Binding.ServiceAccount.Name != w.ServiceAccount ||
		claims.Binding.ServiceAccount.UID != w.ServiceAccountUID || claims.Binding.Pod.Name == "" ||
		claims.Binding.Pod.UID == "" || claims.IssuedAt <= 0 || claims.ExpiresAt <= claims.IssuedAt {
		return time.Time{}, errDenied
	}
	at := now()
	expiry := time.Unix(claims.ExpiresAt, 0)
	if at.Before(time.Unix(claims.IssuedAt, 0)) || !at.Before(expiry) {
		return time.Time{}, errDenied
	}
	return expiry, nil
}

func sinkAudience(audiences []string) bool {
	return len(audiences) == 1 && audiences[0] == k6auth.SinkAudience
}

// Request is the exact grant and attempt presented on each operation. The
// allocator channel authenticates worker-control as the authority for Attempt.
type Request struct {
	IntentID         string          `json:"intent_id"`
	GrantToken       string          `json:"grant_token"`
	WorkflowID       string          `json:"workflow_id"`
	NodeID           string          `json:"node_id"`
	Iteration        *int32          `json:"iteration"`
	LogicalKey       string          `json:"logical_key"`
	ScopeHash        string          `json:"scope_hash"`
	ResourceID       string          `json:"resource_id"`
	ExpectedRevision string          `json:"expected_revision"`
	Arguments        json.RawMessage `json:"arguments"`
	Attempt          int64           `json:"attempt"`
	Fence            int64           `json:"fence,omitempty"`
	MessageID        string          `json:"message_id"`
	RequestID        string          `json:"request_id"`
}

func (r Request) exactGrant() k6auth.ExactGrantRequest {
	return k6auth.ExactGrantRequest{
		IntentID: r.IntentID, Token: r.GrantToken, WorkflowID: r.WorkflowID, NodeID: r.NodeID,
		Iteration: *r.Iteration, LogicalKey: r.LogicalKey, ScopeHash: r.ScopeHash, Target: r.ResourceID,
	}
}

type authorization struct {
	grant         k6auth.ExactGrant
	revision      int64
	canonicalHash string
	grantHash     string
	deadline      time.Time
	sinkCutoff    time.Time
}

func hash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func identifier(s string) bool {
	return len(s) > 0 && len(s) <= 512 && strings.IndexFunc(s, func(r rune) bool {
		return r <= ' ' || r == 127
	}) < 0
}

func authorize(ctx context.Context, reader GrantReader, request Request, uncertainty time.Duration) (authorization, error) {
	intent, err := uuid.Parse(request.IntentID)
	if err != nil || intent.String() != request.IntentID || request.Iteration == nil || *request.Iteration < 0 ||
		request.Attempt <= 0 || !identifier(request.GrantToken) || !identifier(request.WorkflowID) ||
		!identifier(request.NodeID) || !identifier(request.LogicalKey) || !identifier(request.ResourceID) ||
		!identifier(request.MessageID) || !identifier(request.RequestID) || len(request.ScopeHash) != 64 {
		return authorization{}, errDenied
	}
	revision, err := strconv.ParseInt(request.ExpectedRevision, 10, 64)
	if err != nil || revision < 0 || strconv.FormatInt(revision, 10) != request.ExpectedRevision ||
		state.ValidateK6Arguments(request.Arguments) != nil {
		return authorization{}, errDenied
	}
	canonicalHash, err := state.CanonicalPayloadHash(request.Arguments)
	if err != nil {
		return authorization{}, errDenied
	}
	before := time.Now()
	grant, err := reader.ReadExactGrant(ctx, request.exactGrant())
	if err != nil || grant.IntentID != request.IntentID || grant.WorkflowID != request.WorkflowID ||
		grant.NodeID != request.NodeID || grant.Iteration != *request.Iteration || grant.Target != request.ResourceID ||
		grant.Decision != "APPROVED" || (grant.DispatchStatus != "GRANTED" && grant.DispatchStatus != "DISPATCHED") ||
		grant.ExpectedResourceRevision != request.ExpectedRevision || grant.ScopeHash != request.ScopeHash {
		return authorization{}, errDenied
	}
	storedHash, err := state.CanonicalPayloadHash(grant.CanonicalArguments)
	proposal, proposalErr := state.K6ProposalHash(grant.Target, grant.CanonicalArguments, grant.ExpectedResourceRevision)
	scope := state.K6GrantScopeHash(proposal, grant.ExpectedResourceRevision, request.LogicalKey, grant.Target)
	if err != nil || proposalErr != nil || canonicalHash != storedHash || grant.ProposalHash != proposal || grant.ScopeHash != scope {
		return authorization{}, errDenied
	}
	// The engine sample lies inside the request bracket. Anchor the remaining
	// validity to its earliest possible instant using local monotonic time.
	cutoff := grant.ExpiresAt.Add(-uncertainty)
	deadline := before.Add(cutoff.Sub(grant.DBTime))
	if grant.DBTime.IsZero() || !grant.DBTime.Before(cutoff) || !time.Now().Before(deadline) {
		return authorization{}, errDenied
	}
	return authorization{grant, revision, storedHash, hash([]byte(request.GrantToken)), deadline, cutoff}, nil
}

func recheck(ctx context.Context, reader GrantReader, request Request, auth authorization, uncertainty time.Duration) (authorization, error) {
	next, err := authorize(ctx, reader, request, uncertainty)
	if err != nil || !next.grant.ExpiresAt.Equal(auth.grant.ExpiresAt) ||
		next.canonicalHash != auth.canonicalHash || !time.Now().Before(auth.deadline) {
		return authorization{}, errDenied
	}
	return next, nil
}

var errUnavailable = errors.New("K6_SINK_UNAVAILABLE")
