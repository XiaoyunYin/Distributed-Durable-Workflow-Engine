package k6auth

import (
	"reflect"
	"testing"
	"time"
)

func TestK6BoundTokenRequest(t *testing.T) {
	request, err := BuildTokenRequest("episode-binding", "episode-secret-uid")
	if err != nil || request.Kind != "TokenRequest" || request.APIVersion != "authentication.k8s.io/v1" ||
		request.Spec.ExpirationSeconds != 600 {
		t.Fatal("incorrect TokenRequest type or duration")
	}
	expected := &BoundObjectReference{APIVersion: "v1", Kind: "Secret", Name: "episode-binding", UID: "episode-secret-uid"}
	if !reflect.DeepEqual(request.Spec.BoundObjectRef, expected) {
		t.Fatal("TokenRequest must bind exact episode Secret kind, name and UID")
	}
	if !reflect.DeepEqual(request.Spec.Audiences, []string{"dur053-k6-approval"}) {
		t.Fatal("TokenRequest must use the sole registered approval audience")
	}
	if _, err := BuildTokenRequest("episode-binding", ""); err == nil {
		t.Fatal("TokenRequest accepted an empty Secret UID")
	}
}

func TestK6IssuedTokenWindow(t *testing.T) {
	episode, now, claims, _ := identityFixture()
	expiration := time.Unix(claims.ExpiresAt, 0)
	if err := ValidateIssuedToken(episode, jwt(t, claims), expiration, now); err != nil {
		t.Fatal("trusted issuance response with sufficient validity was refused")
	}
	for _, test := range []struct {
		name   string
		change func(*tokenClaims, *time.Time, *time.Time)
	}{
		{"expired", func(c *tokenClaims, n, e *time.Time) { *n = *e }},
		{"response too short", func(c *tokenClaims, n, e *time.Time) {
			*e = episode.LastPlannedArrival.Add(episode.TwoL)
		}},
		{"claim too short", func(c *tokenClaims, n, e *time.Time) {
			c.ExpiresAt = episode.LastPlannedArrival.Add(episode.TwoL).Unix()
		}},
		{"early", func(c *tokenClaims, n, e *time.Time) { c.IssuedAt-- }},
		{"wrong issuer", func(c *tokenClaims, n, e *time.Time) { c.Issuer += "/" }},
		{"wrong binding", func(c *tokenClaims, n, e *time.Time) { c.Binding.Secret.UID = "another-uid" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			modified, at, expiry := claims, now, expiration
			test.change(&modified, &at, &expiry)
			if err := ValidateIssuedToken(episode, jwt(t, modified), expiry, at); err == nil {
				t.Fatal("invalid issuance was allowed")
			}
		})
	}
}
