package api

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"net/http"
	"os"
	"regexp"
	"strings"
)

const minimumDemandBearerBytes = 32

// DemandBearerAuthenticator protects the read-only demand endpoint with a
// campaign-scoped secret. The mounted file is read on every request so
// Kubernetes Secret volume rotation takes effect without restarting runtime.
type DemandBearerAuthenticator struct {
	file string
}

func NewDemandBearerAuthenticatorFromEnvironment() (*DemandBearerAuthenticator, error) {
	path := os.Getenv("RUNTIME_DEMAND_BEARER_TOKEN_FILE")
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("RUNTIME_DEMAND_BEARER_TOKEN_FILE is required when demand authentication is enabled")
	}
	auth := &DemandBearerAuthenticator{file: path}
	if _, err := auth.readSecret(); err != nil {
		return nil, errors.New("demand bearer secret unavailable or invalid")
	}
	return auth, nil
}

func (a *DemandBearerAuthenticator) readSecret() ([]byte, error) {
	secret, err := os.ReadFile(a.file)
	if err != nil {
		return nil, err
	}
	secret = []byte(strings.TrimSpace(string(secret)))
	if len(secret) < minimumDemandBearerBytes {
		return nil, errors.New("demand bearer secret must contain at least 32 bytes")
	}
	return secret, nil
}

var opaqueDemandSecret = regexp.MustCompile(`^[A-Za-z0-9_-]{32,}$`)

func (a *DemandBearerAuthenticator) ValidateJ12Secret() error {
	secret, err := a.readSecret()
	if err != nil || !opaqueDemandSecret.Match(secret) {
		return errors.New("J12 demand requires an opaque base64url bearer secret")
	}
	return nil
}

func (a *DemandBearerAuthenticator) middleware(next http.Handler, j12 bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fields := strings.Fields(r.Header.Get("Authorization"))
		if len(fields) != 2 || fields[0] != "Bearer" || fields[1] == "" {
			writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "campaign bearer credential required", nil)
			return
		}
		secret, err := a.readSecret()
		providedHash, expectedHash := sha256.Sum256([]byte(fields[1])), sha256.Sum256(secret)
		if err != nil || (j12 && !opaqueDemandSecret.Match(secret)) || subtle.ConstantTimeCompare(providedHash[:], expectedHash[:]) != 1 {
			writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "campaign bearer credential rejected", nil)
			return
		}
		next.ServeHTTP(w, r)
	})
}
