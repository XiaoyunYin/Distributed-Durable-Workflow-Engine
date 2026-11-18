package k6auth

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net/http"
	"strings"
	"testing"
	"time"
)

type reviewTransport func(*http.Request) (*http.Response, error)

func (f reviewTransport) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func TestK6KubernetesTokenReviewRequest(t *testing.T) {
	_, now, claims, expected := identityFixture()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "runner test root"},
		NotBefore: now, NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, pub, priv)
	if err != nil {
		t.Fatal(err)
	}
	const credential = "reviewer-credential"
	reviewer, err := NewKubernetesReviewer("https://api.episode.test", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), credential)
	if err != nil {
		t.Fatal(err)
	}
	tls := reviewer.client.Transport.(*http.Transport).TLSClientConfig
	if tls.RootCAs == nil || tls.InsecureSkipVerify || reviewer.client.CheckRedirect(nil, nil) != http.ErrUseLastResponse {
		t.Fatal("reviewer did not pin trust roots and prohibit redirects")
	}
	token := jwt(t, claims)
	var calls int
	responseStatus := http.StatusCreated
	reviewer.client.Transport = reviewTransport(func(request *http.Request) (*http.Response, error) {
		calls++
		var payload tokenReview
		if request.Method != http.MethodPost || request.URL.String() != "https://api.episode.test/apis/authentication.k8s.io/v1/tokenreviews" ||
			request.Header.Get("Authorization") != "Bearer "+credential || request.Header.Get("Content-Type") != "application/json" ||
			json.NewDecoder(request.Body).Decode(&payload) != nil || payload.APIVersion != reviewVersion || payload.Kind != "TokenReview" ||
			payload.Spec == nil || payload.Spec.Token != token || !soleAudience(payload.Spec.Audiences) {
			t.Fatal("TokenReview request altered the assertion, audience, API route or reviewer identity")
		}
		body, err := json.Marshal(tokenReview{APIVersion: reviewVersion, Kind: "TokenReview", Status: &expected})
		if err != nil {
			t.Fatal(err)
		}
		return &http.Response{StatusCode: responseStatus, Body: io.NopCloser(strings.NewReader(string(body)))}, nil
	})
	if actual, err := reviewer.Review(context.Background(), token); err != nil || !actual.Authenticated || actual.User != expected.User {
		t.Fatal("valid TokenReview response was rejected")
	}
	responseStatus = http.StatusForbidden
	if _, err := reviewer.Review(context.Background(), token); !errors.Is(err, ErrDenied) {
		t.Fatal("reviewer accepted a forbidden TokenReview response")
	}
	if _, err := reviewer.Review(context.Background(), credential); !errors.Is(err, ErrDenied) || calls != 2 {
		t.Fatal("reviewer used its reviewer credential as the assertion")
	}
}

func TestK6DatabaseRoleRequiresVerifiedTLS(t *testing.T) {
	for _, dsn := range []string{
		"postgresql://wrong:secret@localhost/episode?sslmode=verify-full",
		"postgresql://k6_sink_grant_reader:secret@localhost/episode?sslmode=disable",
		"postgresql://k6_sink_grant_reader:secret@localhost/episode?sslmode=require",
		"postgresql://k6_sink_grant_reader:secret@localhost/episode?sslmode=verify-ca",
	} {
		reader, err := OpenGrantReader(context.Background(), dsn)
		if reader != nil || !errors.Is(err, ErrDenied) || strings.Contains(err.Error(), "secret") {
			t.Fatal("unverified TLS or wrong DB identity reached the reader pool")
		}
	}
}
