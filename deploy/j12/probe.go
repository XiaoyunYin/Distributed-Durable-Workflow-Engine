package main

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strings"
	"time"
)

type credentials struct {
	keys   map[string]*rsa.PrivateKey
	demand string
}

func newCredentials() (*credentials, error) {
	fixture := &credentials{keys: map[string]*rsa.PrivateKey{}}
	for _, kid := range []string{"current", "next", "unsigned-by-jwks"} {
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			return nil, err
		}
		fixture.keys[kid] = key
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return nil, err
	}
	fixture.demand = base64.RawURLEncoding.EncodeToString(secret)
	return fixture, nil
}

func (c *credentials) jwks(kids ...string) []byte {
	var keys []object
	for _, kid := range kids {
		keys = append(keys, object{"kid": kid, "kty": "RSA", "alg": "RS256", "use": "sig", "e": "AQAB",
			"n": base64.RawURLEncoding.EncodeToString(c.keys[kid].N.Bytes())})
	}
	raw, _ := json.Marshal(object{"keys": keys})
	return raw
}

func claims(subject, namespace string, now time.Time) object {
	return object{"iss": issuer, "aud": audience, "sub": subject, "namespace": namespace, "exp": now.Add(30 * time.Minute).Unix()}
}

func (c *credentials) token(kid, signer, alg string, claims object) (string, error) {
	header, _ := json.Marshal(object{"alg": alg, "kid": kid, "typ": "JWT"})
	body, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	wire := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(body) + "."
	if alg == "none" {
		return wire, nil
	}
	sum := sha256.Sum256([]byte(strings.TrimSuffix(wire, ".")))
	key := c.keys[signer]
	if key == nil {
		return "", errors.New("unknown J12 fixture signer")
	}
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, sum[:])
	if err != nil {
		return "", err
	}
	return wire + base64.RawURLEncoding.EncodeToString(sig), nil
}

type probeRequest struct {
	Listener, Method, Path string
	Token                  string
	Projected              bool
	Loopback               bool
	Body                   object
	Startup, Marker        string
}

type response struct {
	Status int             `json:"status,omitempty"`
	Body   json.RawMessage `json:"body,omitempty"`
	Exit   int             `json:"exit,omitempty"`
	Marker bool            `json:"marker,omitempty"`
}

func podProbe(input io.Reader, output io.Writer) error {
	var request probeRequest
	if err := json.NewDecoder(io.LimitReader(input, 128<<10)).Decode(&request); err != nil {
		return errors.New("decode J12 probe request")
	}
	var answer response
	if request.Startup != "" {
		answer = probeStartup(request)
	} else {
		if !slices.Contains(listeners, request.Listener) || !strings.HasPrefix(request.Path, "/v1/") {
			return errors.New("unknown J12 probe destination")
		}
		if request.Projected {
			secret, err := os.ReadFile("/caller/token")
			if err != nil || len(bytes.TrimSpace(secret)) == 0 {
				return errors.New("read projected caller token")
			}
			request.Token = strings.TrimSpace(string(secret))
		}
		var payload []byte
		if request.Body != nil {
			var err error
			payload, err = json.Marshal(request.Body)
			if err != nil {
				return errors.New("encode J12 request body")
			}
		}
		destination := request.Listener
		if request.Loopback {
			destination = "127.0.0.1"
		}
		r, err := http.NewRequest(request.Method, "http://"+destination+":8080"+request.Path, bytes.NewReader(payload))
		if err != nil {
			return errors.New("construct J12 HTTP request")
		}
		if request.Token != "" {
			r.Header.Set("Authorization", "Bearer "+request.Token)
		}
		r.Header.Set("Content-Type", "application/json")
		client := http.Client{Timeout: 8 * time.Second, Transport: &http.Transport{},
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		reply, err := client.Do(r)
		if err != nil {
			return errors.New("J12 probe transport failed")
		}
		defer reply.Body.Close()
		raw, err := io.ReadAll(io.LimitReader(reply.Body, 64<<10+1))
		if err != nil || len(raw) > 64<<10 {
			return errors.New("read bounded J12 response")
		}
		answer.Status = reply.StatusCode
		if len(raw) > 0 && json.Valid(raw) {
			answer.Body = raw
		}
	}
	return json.NewEncoder(output).Encode(answer)
}

func probeStartup(request probeRequest) response {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/runtime", "--addr", "127.0.0.1:0", "--roles", request.Startup)
	cmd.Env = []string{authSwitch + "=1", relaySwitch + "=1", "RUNTIME_SHUTDOWN_MODE=drain", "RUNTIME_TOKEN_AUTH=disabled",
		"RUNTIME_PUBLIC_JWT_ISSUER=" + issuer, "RUNTIME_PUBLIC_JWT_AUDIENCE=" + audience, "RUNTIME_PUBLIC_JWT_JWKS_FILE=/absent/jwks.json",
		"DATABASE_URL=postgresql://offline@127.0.0.1:1/offline", "RUNTIME_NAMESPACE=tenant-a"}
	raw, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	answer := response{}
	if errors.As(err, &exit) {
		answer.Exit = exit.ExitCode()
	}
	answer.Marker = ctx.Err() == nil && strings.Contains(string(raw), request.Marker)
	return answer
}

func (h *host) request(ctx context.Context, caller string, request probeRequest) (response, error) {
	var answer response
	pod := placeProbe(caller, &request)
	raw, err := json.Marshal(request)
	if err != nil {
		return answer, err
	}
	raw, err = h.kubectl(ctx, "in-cluster probe", raw, "exec", "-i", pod, "--", "/j12", "probe")
	if err != nil {
		return answer, err
	}
	if err := json.Unmarshal(raw, &answer); err != nil {
		return answer, errors.New("unreadable J12 probe output")
	}
	return answer, nil
}

func placeProbe(caller string, request *probeRequest) string {
	if caller == "operator" || caller == "worker" {
		request.Projected = request.Startup == ""
		return caller + "-probe"
	}
	// JWT and demand values travel in the authenticated exec stream. The
	// actual HTTP request carrying them stays on the listener pod's loopback.
	request.Loopback = true
	return request.Listener
}
