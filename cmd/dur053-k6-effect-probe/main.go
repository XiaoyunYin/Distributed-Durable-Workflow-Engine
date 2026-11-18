// dur053-k6-effect-probe sends engineering fixture requests without logging credentials.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"durable-agent-execution-engine/internal/k6ingress"
)

type request struct {
	Address, Name, Path, TokenFile, Bearer, Invocation string
	CA, Certificate, Key, Body                         []byte
	StartAt                                            time.Time
	CorruptToken                                       bool
}

func dialCode(err error) string {
	var network net.Error
	switch {
	case errors.As(err, &network) && network.Timeout():
		return "TCP_DIAL_TIMEOUT"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "TCP_CONNECTION_REFUSED"
	case errors.Is(err, syscall.EHOSTUNREACH), errors.Is(err, syscall.ENETUNREACH):
		return "TCP_UNREACHABLE"
	default:
		return "TCP_DIAL_FAILED"
	}
}

func probeHTTP(conn net.Conn, in request) (map[string]any, error) {
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(in.CA) {
		return nil, errors.New("K6_PROBE_CA")
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS13, ServerName: in.Name, RootCAs: roots}
	if len(in.Certificate) != 0 {
		cert, err := tls.X509KeyPair(in.Certificate, in.Key)
		if err != nil {
			return nil, errors.New("K6_PROBE_CERTIFICATE")
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	client := tls.Client(conn, cfg)
	if err := client.Handshake(); err != nil {
		return map[string]any{"code": "TLS_HANDSHAKE_DENIED"}, nil
	}
	state := client.ConnectionState()
	binding, err := state.ExportKeyingMaterial(k6ingress.BindingLabel, nil, 32)
	if err != nil {
		return nil, errors.New("K6_PROBE_BINDING")
	}
	request, err := http.NewRequest(http.MethodPost, "https://"+in.Name+in.Path, bytes.NewReader(in.Body))
	if err != nil {
		return nil, errors.New("K6_PROBE_REQUEST")
	}
	if in.TokenFile != "" {
		token, err := os.ReadFile(in.TokenFile)
		if err != nil {
			return nil, errors.New("K6_PROBE_PROJECTED_TOKEN")
		}
		in.Bearer = strings.TrimSpace(string(token))
		if in.CorruptToken {
			signature := strings.LastIndexByte(in.Bearer, '.') + 1
			if signature <= 0 || signature >= len(in.Bearer) {
				return nil, errors.New("K6_PROBE_PROJECTED_SIGNATURE")
			}
			replacement := "A"
			if in.Bearer[signature] == 'A' {
				replacement = "B"
			}
			in.Bearer = in.Bearer[:signature] + replacement + in.Bearer[signature+1:]
		}
	}
	if in.Bearer != "" {
		request.Header.Set("Authorization", "Bearer "+in.Bearer)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Dur053-Invocation", in.Invocation)
	request.Close = true
	if !in.StartAt.IsZero() {
		delay := time.Until(in.StartAt)
		if delay <= 0 || delay > 6*time.Second {
			return nil, errors.New("K6_PROBE_START_WINDOW")
		}
		time.Sleep(delay)
	}
	started := time.Now()
	if err := request.Write(client); err != nil {
		return nil, errors.New("K6_PROBE_SEND")
	}
	response, err := http.ReadResponse(bufio.NewReader(client), request)
	if err != nil {
		return nil, errors.New("K6_PROBE_REPLY")
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, k6ingress.MaxBody+1))
	if err != nil || len(body) > k6ingress.MaxBody {
		return nil, errors.New("K6_PROBE_REPLY_BODY")
	}
	out := map[string]any{
		"code": "HTTP_RESPONSE", "status": response.StatusCode,
		"request_hash": k6ingress.Hash(in.Body), "response_hash": k6ingress.Hash(body),
		"response_length": len(body), "connection_hash": k6ingress.Hash(binding),
		"started_ns": started.UnixNano(), "finished_ns": time.Now().UnixNano(),
	}
	if in.Path == "/v1/allocate" && response.StatusCode == 200 {
		var allocation struct{ Fence int64 }
		if json.Unmarshal(body, &allocation) != nil || allocation.Fence <= 0 {
			return nil, errors.New("K6_PROBE_ALLOCATION")
		}
		out["fence"] = allocation.Fence
	}
	return out, nil
}

func probe(mode string) error {
	var in request
	if json.NewDecoder(io.LimitReader(os.Stdin, 128<<10)).Decode(&in) != nil || in.Address == "" {
		return errors.New("K6_PROBE_CONFIG")
	}
	conn, err := net.DialTimeout("tcp", in.Address, 2*time.Second)
	if err != nil {
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"code": dialCode(err)})
	}
	defer conn.Close()
	out := map[string]any{"code": "TCP_CONNECTED"}
	if mode == "http" {
		if conn.SetDeadline(time.Now().Add(22*time.Second)) != nil {
			return errors.New("K6_PROBE_DEADLINE")
		}
		out, err = probeHTTP(conn, in)
		if err != nil {
			return err
		}
	}
	return json.NewEncoder(os.Stdout).Encode(out)
}

func install(source, target string) error {
	body, err := os.ReadFile(source)
	if err != nil || len(body) > 128<<10 {
		return errors.New("K6_PROBE_INSTALL_READ")
	}
	// An init-container retry can reuse its emptyDir. Replace a prior
	// complete file atomically and keep the final mode owner-only.
	temp := target + ".tmp"
	f, err := os.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return errors.New("K6_PROBE_INSTALL_OPEN")
	}
	_, err = f.Write(body)
	if f.Close() != nil || err != nil || os.Rename(temp, target) != nil {
		return errors.New("K6_PROBE_INSTALL_WRITE")
	}
	return nil
}

func run(args []string) error {
	if len(args) >= 3 && len(args)%2 == 1 && args[0] == "install" {
		for i := 1; i < len(args); i += 2 {
			if err := install(args[i], args[i+1]); err != nil {
				return err
			}
		}
		return nil
	}
	if len(args) == 1 {
		switch args[0] {
		case "wait":
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			<-ctx.Done()
			return nil
		case "tcp", "http":
			return probe(args[0])
		}
	}
	return errors.New("K6_PROBE_USAGE")
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
