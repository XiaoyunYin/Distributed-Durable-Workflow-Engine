// Package k6auth implements the isolated DUR-053 episode authorization boundary.
package k6auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const (
	Audience  = "dur053-k6-approval"
	Issuer    = "https://kubernetes.default.svc.cluster.local"
	maxToken  = 16 << 10
	maxBody   = 8 << 10
	approver  = "k6-approver"
	operation = "/v1/approval-decision"
)

var (
	ErrDenied  = errors.New("K6_AUTH_DENIED")
	ErrExpired = errors.New("K6_AUTH_EXPIRED")
)

type Episode struct {
	Namespace, ServiceAccountUID, SecretName, SecretUID string
	WarmupStart, LastPlannedArrival                     time.Time
	TwoL                                                time.Duration
}

func (e Episode) subject() string { return "system:serviceaccount:" + e.Namespace + ":" + approver }

// ValidateTokenWindow is also used at issuance to check the returned token
// lifetime against the episode manifest. The endpoint must remain usable at
// the final required instant, so equality at exp is insufficient.
func ValidateTokenWindow(now, issuedAt, expiresAt, warmupStart, lastPlannedArrival time.Time, twoL time.Duration) error {
	if warmupStart.IsZero() || issuedAt.IsZero() || now.Before(warmupStart) || issuedAt.Before(warmupStart.Truncate(time.Second)) ||
		issuedAt.After(now) || !now.Before(expiresAt) || lastPlannedArrival.Before(warmupStart) || twoL <= 0 {
		return ErrDenied
	}
	if !expiresAt.After(lastPlannedArrival.Add(twoL)) {
		return ErrDenied
	}
	return nil
}

type Review struct {
	Authenticated bool     `json:"authenticated"`
	Audiences     []string `json:"audiences"`
	Error         string   `json:"error"`
	User          struct {
		Username string `json:"username"`
		UID      string `json:"uid"`
	} `json:"user"`
}

type Reviewer interface {
	Review(context.Context, string) (Review, error)
}

type DecisionRequest struct {
	IntentID     string  `json:"intent_id"`
	WorkflowID   string  `json:"workflow_id"`
	NodeID       string  `json:"node_id"`
	Iteration    *int32  `json:"iteration"`
	ProposalHash string  `json:"proposal_hash"`
	Decision     *string `json:"decision,omitempty"`
	Actor        *string `json:"actor,omitempty"`
}

type DecisionIdentity struct {
	IntentID     string     `json:"intent_id"`
	ProposalHash string     `json:"proposal_hash"`
	Decision     string     `json:"decision"`
	Subject      *string    `json:"subject,omitempty"`
	DecidedAt    *time.Time `json:"decided_at,omitempty"`
}

type DecisionWriter interface {
	Decide(context.Context, DecisionRequest, string) (DecisionIdentity, error)
}

type Adapter struct {
	episode  Episode
	reviewer Reviewer
	writer   DecisionWriter
	now      func() time.Time
}

func NewAdapter(episode Episode, reviewer Reviewer, writer DecisionWriter) (*Adapter, error) {
	if episode.Namespace == "" || episode.ServiceAccountUID == "" || episode.SecretName == "" ||
		episode.SecretUID == "" || episode.WarmupStart.IsZero() || episode.LastPlannedArrival.Before(episode.WarmupStart) ||
		episode.TwoL <= 0 || reviewer == nil || writer == nil {
		return nil, ErrDenied
	}
	return &Adapter{episode: episode, reviewer: reviewer, writer: writer, now: time.Now}, nil
}

type tokenClaims struct {
	Issuer    string   `json:"iss"`
	Subject   string   `json:"sub"`
	Audiences []string `json:"aud"`
	IssuedAt  int64    `json:"iat"`
	ExpiresAt int64    `json:"exp"`
	Binding   struct {
		Namespace      string `json:"namespace"`
		ServiceAccount struct {
			Name string `json:"name"`
			UID  string `json:"uid"`
		} `json:"serviceaccount"`
		Secret struct {
			Name string `json:"name"`
			UID  string `json:"uid"`
		} `json:"secret"`
	} `json:"kubernetes.io"`
}

func soleAudience(audiences []string) bool {
	return len(audiences) == 1 && audiences[0] == Audience
}

func (a *Adapter) subject(ctx context.Context, token string) (string, error) {
	if token == "" || len(token) > maxToken || strings.ContainsAny(token, " \t\r\n") {
		return "", ErrDenied
	}
	review, err := a.reviewer.Review(ctx, token)
	if err != nil || review.Error != "" || !review.Authenticated || !soleAudience(review.Audiences) ||
		review.User.Username != a.episode.subject() || review.User.UID != a.episode.ServiceAccountUID {
		return "", ErrDenied
	}
	// The successful TokenReview authenticates these claims on the same token.
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] == "" || parts[2] == "" {
		return "", ErrDenied
	}
	payload, err := base64.RawURLEncoding.Strict().DecodeString(parts[1])
	if err != nil {
		return "", ErrDenied
	}
	var claims tokenClaims
	if json.Unmarshal(payload, &claims) != nil || claims.Issuer != Issuer || claims.Subject != review.User.Username ||
		!soleAudience(claims.Audiences) || claims.IssuedAt <= 0 || claims.ExpiresAt <= 0 ||
		claims.Binding.Namespace != a.episode.Namespace || claims.Binding.ServiceAccount.Name != approver ||
		claims.Binding.ServiceAccount.UID != review.User.UID || claims.Binding.Secret.Name != a.episode.SecretName ||
		claims.Binding.Secret.UID != a.episode.SecretUID {
		return "", ErrDenied
	}
	if err := ValidateTokenWindow(a.now(), time.Unix(claims.IssuedAt, 0), time.Unix(claims.ExpiresAt, 0),
		a.episode.WarmupStart, a.episode.LastPlannedArrival, a.episode.TwoL); err != nil {
		return "", err
	}
	return review.User.Username, nil
}

func (a *Adapter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != operation || r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	auth := r.Header.Values("Authorization")
	if len(auth) != 1 || !strings.HasPrefix(auth[0], "Bearer ") {
		http.Error(w, ErrDenied.Error(), http.StatusForbidden)
		return
	}
	subject, err := a.subject(r.Context(), strings.TrimPrefix(auth[0], "Bearer "))
	if err != nil {
		http.Error(w, ErrDenied.Error(), http.StatusForbidden)
		return
	}
	var request DecisionRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&request) != nil || decoder.Decode(new(any)) != io.EOF || request.Iteration == nil ||
		*request.Iteration < 0 || (request.Decision != nil && request.IntentID == "") ||
		request.WorkflowID == "" || request.NodeID == "" || request.ProposalHash == "" {
		http.Error(w, ErrDenied.Error(), http.StatusForbidden)
		return
	}
	if request.Actor != nil {
		http.Error(w, ErrDenied.Error(), http.StatusForbidden)
		return
	}
	identity, err := a.writer.Decide(r.Context(), request, subject)
	if errors.Is(err, ErrExpired) {
		http.Error(w, ErrExpired.Error(), http.StatusUnprocessableEntity)
		return
	}
	if err != nil {
		http.Error(w, ErrDenied.Error(), http.StatusForbidden)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(identity)
}

// Serve creates a fresh private directory beneath a trusted runner directory.
// It never removes or reuses a preexisting path.
func (a *Adapter) Serve(ctx context.Context, directory string) error {
	if runtime.GOOS == "windows" {
		return ErrDenied // Unix permission bits are required for this socket.
	}
	if err := os.Mkdir(directory, 0700); err != nil {
		return ErrDenied
	}
	defer os.Remove(directory)
	if err := os.Chmod(directory, 0700); err != nil {
		return ErrDenied
	}
	path := filepath.Join(directory, "decision.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return ErrDenied
	}
	defer listener.Close()
	if err := os.Chmod(path, 0600); err != nil {
		return ErrDenied
	}
	server := &http.Server{
		Handler: a, ErrorLog: log.New(io.Discard, "", 0), MaxHeaderBytes: 2 * maxToken,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second,
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	stop := context.AfterFunc(ctx, func() { _ = server.Close() })
	defer stop()
	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return ErrDenied
	}
	return nil
}
