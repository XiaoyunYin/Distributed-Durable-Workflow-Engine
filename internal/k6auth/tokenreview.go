package k6auth

import (
	"bytes"
	"context"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const reviewVersion = "authentication.k8s.io/v1"

type KubernetesReviewer struct {
	endpoint   string
	credential string
	client     *http.Client
}

func NewKubernetesReviewer(apiServer string, caPEM []byte, reviewerCredential string) (*KubernetesReviewer, error) {
	server, err := url.Parse(apiServer)
	roots := x509.NewCertPool()
	if err != nil || server.Scheme != "https" || server.Hostname() == "" || server.User != nil ||
		(server.Path != "" && server.Path != "/") || server.RawQuery != "" || server.Fragment != "" ||
		reviewerCredential == "" || strings.ContainsAny(reviewerCredential, " \t\r\n") || !roots.AppendCertsFromPEM(caPEM) {
		return nil, ErrDenied
	}
	server.Path = "/apis/" + reviewVersion + "/tokenreviews"
	return &KubernetesReviewer{
		endpoint: server.String(), credential: reviewerCredential,
		client: &http.Client{
			Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}},
			Timeout:   5 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

type tokenReview struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Spec       *struct {
		Token     string   `json:"token"`
		Audiences []string `json:"audiences"`
	} `json:"spec,omitempty"`
	Status *Review `json:"status,omitempty"`
}

func (r *KubernetesReviewer) Review(ctx context.Context, token string) (Review, error) {
	if token == "" || subtle.ConstantTimeCompare([]byte(token), []byte(r.credential)) == 1 {
		return Review{}, ErrDenied
	}
	request := tokenReview{APIVersion: reviewVersion, Kind: "TokenReview"}
	request.Spec = &struct {
		Token     string   `json:"token"`
		Audiences []string `json:"audiences"`
	}{Token: token, Audiences: []string{Audience}}
	body, err := json.Marshal(request)
	if err != nil {
		return Review{}, ErrDenied
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, r.endpoint, bytes.NewReader(body))
	if err != nil {
		return Review{}, ErrDenied
	}
	httpRequest.Header.Set("Authorization", "Bearer "+r.credential)
	httpRequest.Header.Set("Content-Type", "application/json")
	response, err := r.client.Do(httpRequest)
	if err != nil {
		return Review{}, ErrDenied
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		return Review{}, ErrDenied
	}
	const maxReview = 64 << 10
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, maxReview+1))
	var result tokenReview
	if err != nil || len(responseBody) > maxReview || json.Unmarshal(responseBody, &result) != nil ||
		result.Kind != "TokenReview" || result.APIVersion != reviewVersion || result.Status == nil {
		return Review{}, ErrDenied
	}
	return *result.Status, nil
}
