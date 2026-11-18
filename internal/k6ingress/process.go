package k6ingress

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"net"
	"os"
	"sync"
)

type TLSFiles struct {
	Certificate []byte
	Key         []byte
	CA          []byte
}

func (f TLSFiles) certificate() (tls.Certificate, *x509.CertPool, error) {
	cert, err := tls.X509KeyPair(f.Certificate, f.Key)
	roots := x509.NewCertPool()
	if err != nil || !roots.AppendCertsFromPEM(f.CA) {
		return tls.Certificate{}, nil, errors.New("K6_INGRESS_TLS_CONFIG")
	}
	return cert, roots, nil
}

func (f TLSFiles) Server(mutual bool) (*tls.Config, error) {
	cert, roots, err := f.certificate()
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, ClientCAs: roots}
	if mutual {
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}

func (f TLSFiles) Client(server string) (*tls.Config, error) {
	cert, roots, err := f.certificate()
	if err != nil || server == "" {
		return nil, errors.New("K6_INGRESS_TLS_CONFIG")
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert},
		RootCAs: roots, ServerName: server}, nil
}

func ReadConfig(path string, target any) error {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 128<<10 {
		return errors.New("K6_INGRESS_PRIVATE_CONFIG")
	}
	data, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(data, target) != nil {
		return errors.New("K6_INGRESS_CONFIG")
	}
	return nil
}

// Serve stops admitting connections on termination, then drains all admitted
// handlers before the caller seals the session or closes the journal.
func Serve(ctx context.Context, address string, config *tls.Config, handle func(*tls.Conn)) error {
	listener, err := tls.Listen("tcp", address, config)
	if err != nil {
		return errors.New("K6_INGRESS_LISTENER")
	}
	defer listener.Close()
	stop := context.AfterFunc(ctx, func() { _ = listener.Close() })
	defer stop()
	var active sync.WaitGroup
	defer active.Wait()
	slots := make(chan struct{}, 32)
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return errors.New("K6_INGRESS_ACCEPT")
		}
		select {
		case slots <- struct{}{}:
		default:
			_ = conn.Close()
			continue
		}
		active.Add(1)
		go func() {
			defer active.Done()
			defer func() { <-slots }()
			handle(conn.(*tls.Conn))
		}()
	}
}
