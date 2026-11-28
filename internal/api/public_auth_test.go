package api

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/state"
)

const (
	j12Issuer   = "https://j12-fixture.invalid"
	j12Audience = "durable-j12-api"
)

func j12Key(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func j12JWK(kid string, key *rsa.PrivateKey) map[string]any {
	return map[string]any{"kid": kid, "kty": "RSA", "alg": "RS256", "use": "sig", "e": "AQAB",
		"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes())}
}

func j12JSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func j12Write(t *testing.T, path string, raw []byte) {
	t.Helper()
	staged := path + ".new"
	if err := os.WriteFile(staged, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(staged, path); err != nil {
		t.Fatal(err)
	}
}

func j12JWKS(t *testing.T, path string, keys ...map[string]any) {
	t.Helper()
	j12Write(t, path, j12JSON(t, map[string]any{"keys": keys}))
}

func j12JWTFixture(t *testing.T) (*TenantJWTVerifier, *rsa.PrivateKey) {
	t.Helper()
	key := j12Key(t)
	path := filepath.Join(t.TempDir(), "jwks.json")
	j12JWKS(t, path, j12JWK("current", key))
	t.Setenv("RUNTIME_PUBLIC_JWT_ISSUER", j12Issuer)
	t.Setenv("RUNTIME_PUBLIC_JWT_AUDIENCE", j12Audience)
	t.Setenv("RUNTIME_PUBLIC_JWT_JWKS_FILE", path)
	verifier, err := NewTenantJWTVerifierFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	return verifier, key
}

func j12Claims(subject, namespace string, now time.Time) map[string]any {
	return map[string]any{"iss": j12Issuer, "aud": j12Audience, "sub": subject,
		"namespace": namespace, "exp": now.Add(5 * time.Minute).Unix()}
}

func j12Sign(t *testing.T, key *rsa.PrivateKey, header, claims []byte) string {
	t.Helper()
	unsigned := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(claims)
	if key == nil {
		return unsigned + "."
	}
	digest := sha256.Sum256([]byte(unsigned))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func j12Token(t *testing.T, key *rsa.PrivateKey, kid string, claims map[string]any) string {
	t.Helper()
	header := j12JSON(t, map[string]string{"alg": "RS256", "kid": kid, "typ": "JWT"})
	return j12Sign(t, key, header, j12JSON(t, claims))
}

func TestJ12TenantJWTClaims(t *testing.T) {
	verifier, key := j12JWTFixture(t)
	now := time.Unix(1800000000, 0)
	for _, tc := range []struct {
		name, claim string
		value       any
		omit, pass  bool
	}{
		{name: "required-only", pass: true},
		{name: "audience-array", claim: "aud", value: []string{"other", j12Audience}, pass: true},
		{name: "wrong-issuer", claim: "iss", value: "https://other.invalid"},
		{name: "wrong-audience", claim: "aud", value: "other"},
		{name: "missing-audience", claim: "aud", omit: true},
		{name: "malformed-audience", claim: "aud", value: []any{j12Audience, nil}},
		{name: "missing-namespace", claim: "namespace", omit: true},
		{name: "empty-namespace", claim: "namespace", value: ""},
		{name: "namespace-not-string", claim: "namespace", value: []string{"tenant-a"}},
		{name: "invalid-namespace", claim: "namespace", value: "tenant/a"},
		{name: "missing-subject", claim: "sub", omit: true},
		{name: "bad-subject", claim: "sub", value: "space separated"},
		{name: "missing-expiry", claim: "exp", omit: true},
		{name: "null-expiry", claim: "exp", value: nil},
		{name: "fractional-expiry", claim: "exp", value: json.Number("1800000050.1")},
		{name: "exponent-expiry", claim: "exp", value: json.Number("1.80000005e9")},
		{name: "exp-string", claim: "exp", value: "1800000060"},
		{name: "expired-within-skew", claim: "exp", value: now.Unix() - 29, pass: true},
		{name: "expired-at-skew", claim: "exp", value: now.Unix() - 30},
		{name: "expired-beyond-skew", claim: "exp", value: now.Unix() - 31},
		{name: "nbf-at-skew", claim: "nbf", value: now.Unix() + 30, pass: true},
		{name: "nbf-beyond-skew", claim: "nbf", value: now.Unix() + 31},
		{name: "nbf-null", claim: "nbf", value: nil},
		{name: "nbf-fractional", claim: "nbf", value: json.Number("1800000000.5")},
		{name: "iat-at-skew", claim: "iat", value: now.Unix() + 30, pass: true},
		{name: "iat-beyond-skew", claim: "iat", value: now.Unix() + 31},
		{name: "iat-zero", claim: "iat", value: 0},
		{name: "iat-null", claim: "iat", value: nil},
		{name: "iat-fractional", claim: "iat", value: json.Number("1800000000.5")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			claims := j12Claims("requester", "tenant-a", now)
			if tc.omit {
				delete(claims, tc.claim)
			} else if tc.claim != "" {
				claims[tc.claim] = tc.value
			}
			got, err := verifier.verify(j12Token(t, key, "current", claims), now)
			if (err == nil) != tc.pass {
				t.Fatalf("JWT accepted=%t, want %t (%v)", err == nil, tc.pass, err)
			}
			if tc.pass && got != (state.TenantPrincipal{Issuer: j12Issuer, Namespace: "tenant-a", Subject: "requester"}) {
				t.Fatalf("incorrect principal: %+v", got)
			}
		})
	}
	claims := j12Claims("requester", "tenant-a", now)
	claims["exp"], claims["iat"] = now.Unix()+1, now.Unix()+1
	if _, err := verifier.verify(j12Token(t, key, "current", claims), now); err == nil {
		t.Fatal("iat equal to expiry accepted")
	}
}

func TestJ12TenantJWTRejectsUntrustedJWS(t *testing.T) {
	verifier, key := j12JWTFixture(t)
	other := j12Key(t)
	now := time.Now()
	claims := j12JSON(t, j12Claims("requester", "tenant-a", now))
	for _, tc := range []struct {
		name, header string
		key          *rsa.PrivateKey
	}{
		{"bad-signature", `{"alg":"RS256","kid":"current"}`, other},
		{"unknown-kid", `{"alg":"RS256","kid":"other"}`, key},
		{"missing-kid", `{"alg":"RS256"}`, key},
		{"none", `{"alg":"none","kid":"current"}`, nil},
		{"HS256", `{"alg":"HS256","kid":"current"}`, key},
		{"ES256", `{"alg":"ES256","kid":"current"}`, key},
		{"wrong-typ", `{"alg":"RS256","kid":"current","typ":"at+jwt"}`, key},
		{"duplicate-alg", `{"alg":"RS256","alg":"RS256","kid":"current"}`, key},
		{"case-variant-alg", `{"ALG":"RS256","kid":"current"}`, key},
		{"embedded-jwk", `{"alg":"RS256","kid":"current","jwk":{}}`, key},
		{"remote-jku", `{"alg":"RS256","kid":"current","jku":"https://other.invalid/keys"}`, key},
		{"unsupported-crit", `{"alg":"RS256","kid":"current","crit":["extension"]}`, key},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := verifier.verify(j12Sign(t, tc.key, []byte(tc.header), claims), now); err == nil {
				t.Fatal("untrusted JWS accepted")
			}
		})
	}
	duplicate := append([]byte(`{"namespace":"tenant-b",`), claims[1:]...)
	if _, err := verifier.verify(j12Sign(t, key, []byte(`{"alg":"RS256","kid":"current"}`), duplicate), now); err == nil {
		t.Fatal("duplicate namespace accepted")
	}
	for _, token := range []string{"", "bad", "a.!.c", strings.Repeat("a", maxTenantTokenBytes+1)} {
		if _, err := verifier.verify(token, now); err == nil {
			t.Fatal("malformed token accepted")
		}
	}
}

func TestJ12TenantJWKSFailClosedAndRotation(t *testing.T) {
	verifier, first := j12JWTFixture(t)
	second := j12Key(t)
	now := time.Now()
	claims := j12Claims("requester", "tenant-a", now)
	oldToken := j12Token(t, first, "current", claims)
	newToken := j12Token(t, second, "next", claims)
	oldKey, newKey := j12JWK("current", first), j12JWK("next", second)
	handler := NewServer(newJ12Repository()).WithRoutes(RoutesPublic).WithPublicAPIAuth(true, verifier).Handler()
	check := func(oldOK, newOK bool) {
		t.Helper()
		for _, tc := range []struct {
			name, token string
			accepted    bool
		}{{"old", oldToken, oldOK}, {"new", newToken, newOK}} {
			_, err := verifier.verify(tc.token, now)
			status := j12Request(handler, "GET", "/v1/workflows/own", tc.token, "").Code
			want := 401
			if tc.accepted {
				want = 200
			}
			if (err == nil) != tc.accepted || status != want {
				t.Fatalf("%s key returned %v and HTTP %d, want HTTP %d", tc.name, err, status, want)
			}
		}
	}
	check(true, false)
	j12JWKS(t, verifier.jwksFile, oldKey, newKey)
	check(true, true)
	j12JWKS(t, verifier.jwksFile, newKey)
	check(false, true)
	j12JWKS(t, verifier.jwksFile, newKey, newKey)
	check(false, false)
	for _, raw := range []string{`{"keys":[]}`, `{"keys":[] , "keys":[]}`, `{`, strings.Repeat("x", maxTenantJWKSBytes+1)} {
		j12Write(t, verifier.jwksFile, []byte(raw))
		check(false, false)
		if _, err := NewTenantJWTVerifierFromEnvironment(); err == nil {
			t.Fatal("startup accepted invalid JWKS")
		}
	}
	if err := os.Remove(verifier.jwksFile); err != nil {
		t.Fatal(err)
	}
	check(false, false)
	if _, err := NewTenantJWTVerifierFromEnvironment(); err == nil {
		t.Fatal("startup accepted missing JWKS")
	}
}
