// dur053-sink runs the standalone K6 effect sink or exports its database.
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"durable-agent-execution-engine/internal/effects/k6sink"
	"durable-agent-execution-engine/internal/k6auth"
	"durable-agent-execution-engine/internal/k6hook"
)

type serveConfig struct {
	Address                                string
	Worker                                 k6sink.WorkerIdentity
	APIServer                              string
	APICA                                  []byte
	ReviewerToken                          string
	EnginePeerURI, IngressPeerURI          string
	ServerCertificate, ServerKey, ClientCA []byte
	SinkDSN, GrantReaderDSN                string
	ClockUncertaintyMicros                 int64
	ReceiptSigningSeed                     []byte
	ObserveTimings                         bool
}

func loadPrivateConfig(path string, config any) error {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 128<<10 {
		return errors.New("K6_SINK_PRIVATE_CONFIG")
	}
	f, err := os.Open(path)
	if err != nil {
		return errors.New("K6_SINK_CONFIG")
	}
	defer f.Close()
	body, err := io.ReadAll(io.LimitReader(f, (128<<10)+1))
	if err != nil || len(body) > 128<<10 || json.Unmarshal(body, config) != nil {
		return errors.New("K6_SINK_CONFIG")
	}
	return nil
}

func serverTLS(cfg serveConfig) (*tls.Config, error) {
	roots := x509.NewCertPool()
	certificate, err := tls.X509KeyPair(cfg.ServerCertificate, cfg.ServerKey)
	if err != nil || !roots.AppendCertsFromPEM(cfg.ClientCA) {
		return nil, errors.New("K6_SINK_TLS_CONFIG")
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate},
		ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots,
	}, nil
}

func serve(ctx context.Context, path string) error {
	var cfg serveConfig
	if err := loadPrivateConfig(path, &cfg); err != nil {
		return err
	}
	if cfg.Address == "" || cfg.ClockUncertaintyMicros <= 0 ||
		cfg.ClockUncertaintyMicros > int64((1<<63-1)/time.Microsecond) {
		return errors.New("K6_SINK_CONFIG")
	}
	tlsConfig, err := serverTLS(cfg)
	if err != nil {
		return err
	}
	reviewer, err := k6auth.NewSinkKubernetesReviewer(cfg.APIServer, cfg.APICA, cfg.ReviewerToken)
	if err != nil {
		return errors.New("K6_SINK_REVIEWER_CONFIG")
	}
	initial, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	reader, err := k6auth.OpenGrantReader(initial, cfg.GrantReaderDSN)
	if err != nil {
		return errors.New("K6_SINK_GRANT_DB")
	}
	defer reader.Close()
	store, err := k6sink.OpenStore(initial, cfg.SinkDSN)
	if err != nil {
		return errors.New("K6_SINK_OWN_DB")
	}
	defer store.Close()
	var receiptKey ed25519.PrivateKey
	if len(cfg.ReceiptSigningSeed) != 0 {
		if len(cfg.ReceiptSigningSeed) != ed25519.SeedSize {
			return errors.New("K6_SINK_RECEIPT_CONFIG")
		}
		receiptKey = ed25519.NewKeyFromSeed(cfg.ReceiptSigningSeed)
	}
	hook, err := k6hook.FromEnvironment("sink", "")
	if err != nil {
		return err
	}
	handler, err := k6sink.NewHandler(k6sink.HTTPConfig{
		Worker: cfg.Worker, EnginePeerURI: cfg.EnginePeerURI, IngressPeerURI: cfg.IngressPeerURI,
		ClockUncertainty: time.Duration(cfg.ClockUncertaintyMicros) * time.Microsecond,
		ReceiptKey:       receiptKey,
		ObserveTimings:   cfg.ObserveTimings,
		Hook:             hook,
	}, reviewer, reader, store)
	if err != nil {
		return errors.New("K6_SINK_IDENTITY_CONFIG")
	}
	server := &http.Server{
		Addr: cfg.Address, Handler: handler, TLSConfig: tlsConfig, ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout: 4 * time.Second, WriteTimeout: k6sink.ResponseTimeout(hook), IdleTimeout: 10 * time.Second,
		MaxHeaderBytes: 24 << 10, ErrorLog: log.New(io.Discard, "", 0),
	}
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServeTLS("", "") }()
	select {
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close()
			return errors.New("K6_SINK_SHUTDOWN")
		}
	case <-done:
		return errors.New("K6_SINK_LISTENER")
	}
	return nil
}

func export(ctx context.Context, config string, output io.Writer) error {
	var cfg struct{ ExportDSN string }
	if err := loadPrivateConfig(config, &cfg); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	snapshot, err := k6sink.Export(ctx, cfg.ExportDSN)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(output).Encode(snapshot); err != nil {
		return errors.New("K6_SINK_EXPORT_OUTPUT")
	}
	return nil
}

func main() {
	if len(os.Args) != 3 || (os.Args[1] != "serve" && os.Args[1] != "export") {
		fmt.Fprintln(os.Stderr, "usage: dur053-sink {serve|export} PRIVATE_CONFIG")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	if os.Args[1] == "serve" {
		err = serve(ctx, os.Args[2])
	} else {
		err = export(ctx, os.Args[2], os.Stdout)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
