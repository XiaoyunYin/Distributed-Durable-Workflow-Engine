package api

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type RouteSet string

const (
	RoutesCombined      RouteSet = ""
	RoutesPublic        RouteSet = "public"
	RoutesOperator      RouteSet = "operator"
	RoutesWorkerControl RouteSet = "worker-control"
	RoutesNone          RouteSet = "none"
)

func (s *Server) WithRoutes(routes RouteSet) *Server { s.routes = routes; return s }
func (s *Server) WithAuthentication(reviewer *TokenReviewer) *Server {
	s.authenticator = reviewer
	return s
}

// IdentityConfig is explicit: no issuer discovery or trust of token-provided URLs.
type IdentityConfig struct {
	APIURL, CAFile, ReviewerTokenFile, Issuer, Audience, Namespace, ServiceAccount string
}
type TokenReviewer struct {
	config IdentityConfig
	client *http.Client
}

func NewTokenReviewer(c IdentityConfig) (*TokenReviewer, error) {
	u, err := url.Parse(c.APIURL)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, errors.New("TokenReview requires an HTTPS API server origin")
	}
	if c.Issuer == "" || c.Audience == "" || c.Namespace == "" || c.ServiceAccount == "" || strings.ContainsAny(c.Namespace+c.ServiceAccount, ":/ \t\r\n") {
		return nil, errors.New("TokenReview requires issuer, audience, namespace and ServiceAccount")
	}
	ca, err := os.ReadFile(c.CAFile)
	if err != nil {
		return nil, fmt.Errorf("TokenReview CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("invalid TokenReview CA")
	}
	token, err := os.ReadFile(c.ReviewerTokenFile)
	if err != nil || strings.TrimSpace(string(token)) == "" {
		return nil, errors.New("TokenReview reviewer credential unavailable")
	}
	return &TokenReviewer{config: c, client: &http.Client{
		Timeout:       3 * time.Second,
		Transport:     &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}},
		CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("TokenReview redirects forbidden") },
	}}, nil
}

// AuthenticationFromEnvironment fails startup on partial/invalid enabled configuration.
func AuthenticationFromEnvironment() (*TokenReviewer, error) {
	enabled := os.Getenv("RUNTIME_TOKEN_AUTH")
	if enabled == "" || enabled == "disabled" {
		return nil, nil
	}
	if enabled != "enabled" {
		return nil, errors.New("RUNTIME_TOKEN_AUTH must be enabled or disabled")
	}
	return NewTokenReviewer(IdentityConfig{
		APIURL: os.Getenv("RUNTIME_TOKEN_REVIEW_URL"), CAFile: os.Getenv("RUNTIME_TOKEN_REVIEW_CA"),
		ReviewerTokenFile: os.Getenv("RUNTIME_TOKEN_REVIEW_TOKEN_FILE"), Issuer: os.Getenv("RUNTIME_TOKEN_ISSUER"),
		Audience: os.Getenv("RUNTIME_TOKEN_AUDIENCE"), Namespace: os.Getenv("RUNTIME_TOKEN_NAMESPACE"),
		ServiceAccount: os.Getenv("RUNTIME_TOKEN_SERVICE_ACCOUNT"),
	})
}

type identityKey struct{}

func authenticatedIdentity(ctx context.Context) string {
	v, _ := ctx.Value(identityKey{}).(string)
	return v
}

func (v *TokenReviewer) verify(ctx context.Context, token string) (string, error) {
	// Claims are restrictions, never proof. Only a successful trusted TokenReview
	// authenticates their signature, issuer, expiry and bound Kubernetes objects.
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", errors.New("invalid projected token")
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", err
	}
	var claims struct {
		Issuer   string   `json:"iss"`
		Subject  string   `json:"sub"`
		Expiry   int64    `json:"exp"`
		Audience []string `json:"aud"`
	}
	if err := json.Unmarshal(raw, &claims); err != nil {
		return "", err
	}
	identity := "system:serviceaccount:" + v.config.Namespace + ":" + v.config.ServiceAccount
	if claims.Issuer != v.config.Issuer {
		return "", errors.New("wrong issuer")
	}
	if claims.Expiry <= 0 {
		return "", errors.New("projected expiry required")
	}
	if !hasAudience(claims.Audience, v.config.Audience) {
		return "", errors.New("wrong audience")
	}
	if claims.Subject != identity {
		return "", errors.New("wrong ServiceAccount")
	}
	body, _ := json.Marshal(map[string]any{"apiVersion": "authentication.k8s.io/v1", "kind": "TokenReview",
		"spec": map[string]any{"token": token, "audiences": []string{v.config.Audience}}})
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(v.config.APIURL, "/")+"/apis/authentication.k8s.io/v1/tokenreviews", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	// Re-read projected credentials on every call, including kubelet rotation.
	credential, err := os.ReadFile(v.config.ReviewerTokenFile)
	if err != nil || strings.TrimSpace(string(credential)) == "" {
		return "", errors.New("reviewer credential unavailable")
	}
	request.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(credential)))
	request.Header.Set("Content-Type", "application/json")
	response, err := v.client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusOK {
		return "", errors.New("TokenReview rejected")
	}
	var review struct {
		Status struct {
			Authenticated bool     `json:"authenticated"`
			Audiences     []string `json:"audiences"`
			Error         string   `json:"error"`
			User          struct {
				Username string `json:"username"`
			} `json:"user"`
		} `json:"status"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&review); err != nil {
		return "", err
	}
	if !review.Status.Authenticated || review.Status.Error != "" {
		return "", errors.New("unauthenticated TokenReview")
	}
	if !hasAudience(review.Status.Audiences, v.config.Audience) {
		return "", errors.New("TokenReview audience not confirmed")
	}
	if review.Status.User.Username != identity {
		return "", errors.New("TokenReview ServiceAccount mismatch")
	}
	return identity, nil
}
func hasAudience(audiences []string, expected string) bool {
	for _, a := range audiences {
		if a == expected {
			return true
		}
	}
	return false
}
func (v *TokenReviewer) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Preserve route absence even for authenticated workers, and avoid contacting
		// Kubernetes for a route this listener does not implement.
		if mux, ok := next.(*http.ServeMux); ok {
			_, pattern := mux.Handler(r)
			if pattern == "/v1/" {
				next.ServeHTTP(w, r)
				return
			}
		}
		fields := strings.Fields(r.Header.Get("Authorization"))
		if len(fields) != 2 || fields[0] != "Bearer" {
			writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "projected token required", nil)
			return
		}
		identity, err := v.verify(r.Context(), fields[1])
		if err != nil {
			writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "token rejected", nil)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), identityKey{}, identity)))
	})
}
