package k6auth

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"
)

type BoundObjectReference struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	UID        string `json:"uid"`
}

type TokenRequest struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Spec       struct {
		Audiences         []string              `json:"audiences"`
		ExpirationSeconds int                   `json:"expirationSeconds"`
		BoundObjectRef    *BoundObjectReference `json:"boundObjectRef,omitempty"`
	} `json:"spec"`
}

func BuildTokenRequest(secret, uid string) (TokenRequest, error) {
	if secret == "" || uid == "" {
		return TokenRequest{}, ErrDenied
	}
	request := TokenRequest{APIVersion: reviewVersion, Kind: "TokenRequest"}
	request.Spec.Audiences = []string{Audience}
	request.Spec.ExpirationSeconds = 600
	request.Spec.BoundObjectRef = &BoundObjectReference{APIVersion: "v1", Kind: "Secret", Name: secret, UID: uid}
	return request, nil
}

// ValidateIssuedToken checks setup's response from the trusted API server.
// The adapter still authenticates the presented token via TokenReview.
func ValidateIssuedToken(episode Episode, token string, expiration, now time.Time) error {
	parts := strings.Split(token, ".")
	if len(token) > maxToken || len(parts) != 3 || parts[0] == "" || parts[2] == "" {
		return ErrDenied
	}
	payload, err := base64.RawURLEncoding.Strict().DecodeString(parts[1])
	var claims tokenClaims
	if err != nil || json.Unmarshal(payload, &claims) != nil ||
		claims.Issuer != Issuer || claims.Subject != episode.subject() || !soleAudience(claims.Audiences) ||
		claims.Binding.Namespace != episode.Namespace || claims.Binding.ServiceAccount.Name != approver ||
		claims.Binding.ServiceAccount.UID != episode.ServiceAccountUID || claims.Binding.Secret.Name != episode.SecretName ||
		claims.Binding.Secret.UID != episode.SecretUID || claims.IssuedAt <= 0 || claims.ExpiresAt <= 0 {
		return ErrDenied
	}
	// Kubernetes samples the status and JWT clocks separately. Use whichever
	// expiry is earlier so the response can never extend the JWT's window.
	jwtExpiry := time.Unix(claims.ExpiresAt, 0)
	if jwtExpiry.Before(expiration) {
		expiration = jwtExpiry
	}
	return ValidateTokenWindow(now, time.Unix(claims.IssuedAt, 0), expiration,
		episode.WarmupStart, episode.LastPlannedArrival, episode.TwoL)
}
