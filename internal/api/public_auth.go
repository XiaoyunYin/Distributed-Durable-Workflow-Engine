package api

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"durable-agent-execution-engine/internal/state"
)

const (
	maxTenantTokenBytes = 16 << 10
	maxTenantJWKSBytes  = 64 << 10
	tenantClockSkew     = 30 // Unix seconds
)

var (
	tenantNamespace = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	jwtIdentityPart = regexp.MustCompile(`^[!-~]{1,255}$`)
	errTenantToken  = errors.New("tenant token rejected")
	errTenantJWKS   = errors.New("public JWT JWKS unavailable or invalid")
)

type TenantJWTVerifier struct {
	issuer   string
	audience string
	jwksFile string
}

func NewTenantJWTVerifierFromEnvironment() (*TenantJWTVerifier, error) {
	verifier := &TenantJWTVerifier{
		issuer:   os.Getenv("RUNTIME_PUBLIC_JWT_ISSUER"),
		audience: os.Getenv("RUNTIME_PUBLIC_JWT_AUDIENCE"),
		jwksFile: os.Getenv("RUNTIME_PUBLIC_JWT_JWKS_FILE"),
	}
	if !jwtIdentityPart.MatchString(verifier.issuer) || !jwtIdentityPart.MatchString(verifier.audience) {
		return nil, errors.New("public JWT requires a configured issuer and audience")
	}
	if _, err := verifier.loadKeys(); err != nil {
		return nil, err
	}
	return verifier, nil
}

func (v *TenantJWTVerifier) loadKeys() (map[string]*rsa.PublicKey, error) {
	file, err := os.Open(v.jwksFile)
	if err != nil {
		return nil, errTenantJWKS
	}
	defer file.Close()
	// Only read regular files. Projected volume symlinks open their regular target.
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > maxTenantJWKSBytes {
		return nil, errTenantJWKS
	}
	raw, err := io.ReadAll(io.LimitReader(file, maxTenantJWKSBytes+1))
	if err != nil || len(raw) > maxTenantJWKSBytes {
		return nil, errTenantJWKS
	}
	set := jwtObject(raw)
	var entries []json.RawMessage
	if json.Unmarshal(set["keys"], &entries) != nil || len(entries) == 0 {
		return nil, errTenantJWKS
	}
	keys := make(map[string]*rsa.PublicKey, len(entries))
	for _, rawKey := range entries {
		// The enclosing document already passed duplicate-member validation.
		var jwk map[string]json.RawMessage
		if json.Unmarshal(rawKey, &jwk) != nil {
			return nil, errTenantJWKS
		}
		kid := jwtString(jwk["kid"])
		if !jwtIdentityPart.MatchString(kid) || keys[kid] != nil ||
			jwtString(jwk["kty"]) != "RSA" || jwtString(jwk["alg"]) != "RS256" || jwtString(jwk["e"]) != "AQAB" {
			return nil, errTenantJWKS
		}
		if use, present := jwk["use"]; present && jwtString(use) != "sig" {
			return nil, errTenantJWKS
		}
		if ops, present := jwk["key_ops"]; present {
			var names []string
			if json.Unmarshal(ops, &names) != nil || len(names) != 1 || names[0] != "verify" {
				return nil, errTenantJWKS
			}
		}
		modulus, err := decodeJWTPart(jwtString(jwk["n"]))
		if err != nil || len(modulus) < 256 || len(modulus) > 512 || modulus[0] == 0 {
			return nil, errTenantJWKS
		}
		n := new(big.Int).SetBytes(modulus)
		if n.BitLen() < 2048 || n.Bit(0) != 1 {
			return nil, errTenantJWKS
		}
		keys[kid] = &rsa.PublicKey{N: n, E: 65537}
	}
	return keys, nil
}

func decodeJWTPart(encoded string) ([]byte, error) {
	// The base64 decoder otherwise ignores line endings, including in strict mode.
	if encoded == "" || strings.ContainsAny(encoded, "\r\n") {
		return nil, errTenantToken
	}
	return base64.RawURLEncoding.Strict().DecodeString(encoded)
}

func jwtObject(raw []byte) map[string]json.RawMessage {
	if !utf8.Valid(raw) || validateJSONDocument(raw) != nil {
		return nil
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil {
		return nil
	}
	return object
}

func jwtString(raw json.RawMessage) string {
	var value string
	if json.Unmarshal(raw, &value) != nil {
		return ""
	}
	return value
}

func jwtNumericDate(raw json.RawMessage) int64 {
	var value int64
	if json.Unmarshal(raw, &value) != nil {
		return 0
	}
	return value
}

func (v *TenantJWTVerifier) verify(token string, now time.Time) (state.TenantPrincipal, error) {
	if len(token) > maxTenantTokenBytes {
		return state.TenantPrincipal{}, errTenantToken
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return state.TenantPrincipal{}, errTenantToken
	}
	rawHeader, err := decodeJWTPart(parts[0])
	if err != nil {
		return state.TenantPrincipal{}, errTenantToken
	}
	header := jwtObject(rawHeader)
	kid := jwtString(header["kid"])
	if jwtString(header["alg"]) != "RS256" || !jwtIdentityPart.MatchString(kid) {
		return state.TenantPrincipal{}, errTenantToken
	}
	for field, value := range header {
		if field != "alg" && field != "kid" && (field != "typ" || jwtString(value) != "JWT") {
			return state.TenantPrincipal{}, errTenantToken
		}
	}
	keys, err := v.loadKeys()
	if err != nil {
		return state.TenantPrincipal{}, errTenantJWKS
	}
	key := keys[kid]
	signature, err := decodeJWTPart(parts[2])
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if key == nil || err != nil || rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], signature) != nil {
		return state.TenantPrincipal{}, errTenantToken
	}
	rawClaims, err := decodeJWTPart(parts[1])
	if err != nil {
		return state.TenantPrincipal{}, errTenantToken
	}
	claims := jwtObject(rawClaims)
	subject, namespace := jwtString(claims["sub"]), jwtString(claims["namespace"])
	if jwtString(claims["iss"]) != v.issuer || !tenantAudience(claims["aud"], v.audience) ||
		!jwtIdentityPart.MatchString(subject) || !tenantNamespace.MatchString(namespace) {
		return state.TenantPrincipal{}, errTenantToken
	}
	exp := jwtNumericDate(claims["exp"])
	if exp <= 0 || exp <= now.Unix()-tenantClockSkew {
		return state.TenantPrincipal{}, errTenantToken
	}
	for _, claim := range []string{"nbf", "iat"} {
		if raw, present := claims[claim]; present {
			value := jwtNumericDate(raw)
			if value <= 0 || value >= exp || value > now.Unix()+tenantClockSkew {
				return state.TenantPrincipal{}, errTenantToken
			}
		}
	}
	return state.TenantPrincipal{Issuer: v.issuer, Namespace: namespace, Subject: subject}, nil
}

func tenantAudience(raw json.RawMessage, expected string) bool {
	if audience := jwtString(raw); audience != "" {
		return audience == expected
	}
	var audiences []string
	if json.Unmarshal(raw, &audiences) != nil {
		return false
	}
	for _, audience := range audiences {
		if audience == "" {
			return false
		}
	}
	return hasAudience(audiences, expected)
}

type tenantIdentityKey struct{}

func tenantIdentity(ctx context.Context) *state.TenantPrincipal {
	principal, _ := ctx.Value(tenantIdentityKey{}).(*state.TenantPrincipal)
	return principal
}

func (v *TenantJWTVerifier) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		headers := r.Header.Values("Authorization")
		if len(headers) == 1 {
			fields := strings.Fields(headers[0])
			if len(fields) == 2 && strings.EqualFold(fields[0], "Bearer") {
				principal, err := v.verify(fields[1], time.Now())
				if err == nil {
					ctx := context.WithValue(r.Context(), tenantIdentityKey{}, &principal)
					next.ServeHTTP(w, r.WithContext(ctx))
					return
				}
			}
		}
		writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "tenant token rejected", nil)
	})
}

func (s *Server) getVisibleWorkflow(ctx context.Context, workflowID string) (state.Workflow, error) {
	workflow, err := s.repository.GetWorkflow(ctx, workflowID)
	if err == nil {
		if tenant := tenantIdentity(ctx); tenant != nil && workflow.Namespace != tenant.Namespace {
			return state.Workflow{}, state.ErrWorkflowNotFound
		}
	}
	return workflow, err
}

func (s *Server) j12CredentialHandler(mux *http.ServeMux) http.Handler {
	var authenticated http.Handler
	switch s.routes {
	case RoutesPublic:
		if s.tenantJWT != nil {
			authenticated = s.tenantJWT.middleware(mux)
		}
	case RoutesOperator, RoutesWorkerControl:
		if s.authenticator != nil {
			authenticated = s.authenticator.middleware(mux)
		}
	case RoutesDemand:
		if s.demandAuth != nil {
			authenticated = s.demandAuth.middleware(mux, true)
		}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, pattern := mux.Handler(r); pattern == "" || pattern == "/v1/" {
			mux.ServeHTTP(w, r)
			return
		}
		if authenticated == nil {
			writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "listener credential unavailable", nil)
			return
		}
		authenticated.ServeHTTP(w, r)
	})
}
