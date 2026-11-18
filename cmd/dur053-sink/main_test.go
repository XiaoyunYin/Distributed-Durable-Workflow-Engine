package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestK6SinkServerTLSRequiresVerifiedClient(t *testing.T) {
	fixture := httptest.NewTLSServer(http.NotFoundHandler())
	defer fixture.Close()
	cert := fixture.TLS.Certificates[0]
	key, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	certificate := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]})
	cfg := serveConfig{ClientCA: certificate, ServerCertificate: certificate,
		ServerKey: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key})}
	got, err := serverTLS(cfg)
	if err != nil || got.ClientAuth != tls.RequireAndVerifyClientCert ||
		got.MinVersion != tls.VersionTLS13 || got.ClientCAs == nil || len(got.Certificates) != 1 {
		t.Fatal("sink TLS must require a verified client certificate")
	}
	cfg.ClientCA = nil
	if _, err := serverTLS(cfg); err == nil {
		t.Fatal("sink configured TLS without a client trust root")
	}
}
