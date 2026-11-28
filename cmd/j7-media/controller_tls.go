package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"time"

	"durable-agent-execution-engine/internal/k6ingress"
)

type controllerCA struct {
	cert *x509.Certificate
	key  ed25519.PrivateKey
	pem  []byte
}

func newControllerCA(now time.Time) (controllerCA, error) {
	var ca controllerCA
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return ca, err
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "J7 drill CA"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(60 * time.Minute), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	raw, err := x509.CreateCertificate(rand.Reader, cert, cert, pub, key)
	if err != nil {
		return ca, err
	}
	ca.cert, err = x509.ParseCertificate(raw)
	ca.key, ca.pem = key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: raw})
	return ca, err
}

func (ca controllerCA) certificate(name, peer string) (k6ingress.TLSFiles, error) {
	out := k6ingress.TLSFiles{CA: ca.pem}
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return out, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		return out, err
	}
	cert := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: name},
		NotBefore: ca.cert.NotBefore, NotAfter: ca.cert.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:    []string{name}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	if peer != "" {
		u, err := url.Parse(peer)
		if err != nil {
			return out, err
		}
		cert.URIs = []*url.URL{u}
	}
	raw, err := x509.CreateCertificate(rand.Reader, cert, ca.cert, pub, ca.key)
	if err != nil {
		return out, err
	}
	private, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return out, err
	}
	out.Certificate = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: raw})
	out.Key = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private})
	return out, nil
}
