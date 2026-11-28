package main

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type syntheticTimeout struct{}

func (syntheticTimeout) Error() string   { return "synthetic timeout" }
func (syntheticTimeout) Timeout() bool   { return true }
func (syntheticTimeout) Temporary() bool { return true }

func TestClassifyConnectError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "wrapped context deadline",
			err:  fmt.Errorf("outer: %w", context.DeadlineExceeded),
			want: "TCP_DIAL_TIMEOUT",
		},
		{
			name: "dial timeout",
			err:  &net.OpError{Op: "dial", Net: "tcp", Err: syntheticTimeout{}},
			want: "TCP_DIAL_TIMEOUT",
		},
		{
			name: "connection refused",
			err:  &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED},
			want: "TCP_DIAL_REFUSED",
		},
		{
			name: "host unreachable",
			err:  &net.OpError{Op: "dial", Net: "tcp", Err: syscall.EHOSTUNREACH},
			want: "TCP_DIAL_UNREACHABLE",
		},
		{
			name: "network unreachable",
			err:  &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ENETUNREACH},
			want: "TCP_DIAL_UNREACHABLE",
		},
		{
			name: "postgres SQLSTATE",
			err:  &pgconn.PgError{Code: "28P01"},
			want: "28P01",
		},
		{
			name: "unknown certificate authority",
			err:  x509.UnknownAuthorityError{Cert: &x509.Certificate{}},
			want: "TLS_VERIFY_FAILED",
		},
		{
			name: "other connect error",
			err:  errors.New("synthetic failure"),
			want: "CONNECT_FAILED",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := classifyConnectError(test.err); got != test.want {
				t.Fatalf("classifyConnectError() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestConnectDiagnosticIsSanitized(t *testing.T) {
	err := &net.OpError{
		Op:  "dial",
		Net: "tcp",
		Err: &osSyscallErrorForTest{err: syscall.ECONNREFUSED},
	}
	diagnostic := connectDiagnostic(err)
	if diagnostic["op"] != "dial" || diagnostic["errno"] != "ECONNREFUSED" || diagnostic["timeout"] != false {
		t.Fatalf("unexpected diagnostic: %#v", diagnostic)
	}
	if _, ok := diagnostic["error_types"].([]string); !ok {
		t.Fatalf("error types missing: %#v", diagnostic)
	}
}

func TestProbeWitnessBindsAuthenticationFailureToSocket(t *testing.T) {
	for _, code := range []string{"28P01", "28000"} {
		t.Run(code, func(t *testing.T) {
			listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			deadline := time.Now().Add(3 * time.Second)
			if err := listener.SetDeadline(deadline); err != nil {
				t.Fatal(err)
			}
			type observation struct {
				peer   *net.TCPAddr
				params map[string]string
				err    error
			}
			observed := make(chan observation, 1)
			go func() {
				conn, err := listener.AcceptTCP()
				if err != nil {
					observed <- observation{err: err}
					return
				}
				defer conn.Close()
				if err := conn.SetDeadline(deadline); err != nil {
					observed <- observation{err: err}
					return
				}
				server := pgproto3.NewBackend(conn, conn)
				message, err := server.ReceiveStartupMessage()
				if err != nil {
					observed <- observation{err: err}
					return
				}
				startup, ok := message.(*pgproto3.StartupMessage)
				if !ok {
					observed <- observation{err: fmt.Errorf("unexpected startup message %T", message)}
					return
				}
				server.Send(&pgproto3.ErrorResponse{Severity: "FATAL", Code: code})
				observed <- observation{conn.RemoteAddr().(*net.TCPAddr), startup.Parameters, server.Flush()}
			}()
			ctx, cancel := context.WithDeadline(context.Background(), deadline)
			defer cancel()
			dsn := "postgresql://probe_role:test@" + listener.Addr().String() +
				"/postgres?sslmode=disable&gssencmode=disable&application_name=probe-app"
			result := sqlProbe(ctx, input{DSN: dsn, Probe: "update", TraceSocket: true})
			server := <-observed
			if server.err != nil {
				t.Fatal(server.err)
			}
			if result["code"] != code {
				t.Fatalf("probe got %#v, want %s", result, code)
			}
			witness, ok := result["rejected_connection"].(map[string]any)
			if code != "28P01" {
				if ok {
					t.Fatalf("unexpected socket witness for %s: %#v", code, witness)
				}
				return
			}
			if witness["client_addr"] != server.peer.IP.String() || witness["client_port"] != server.peer.Port ||
				witness["role"] != server.params["user"] || witness["application"] != server.params["application_name"] {
				t.Fatalf("probe witness %#v does not match socket %v and startup %v", witness, server.peer, server.params)
			}
		})
	}
}

type osSyscallErrorForTest struct{ err error }

func (e *osSyscallErrorForTest) Error() string { return "sensitive detail must not be serialized" }
func (e *osSyscallErrorForTest) Unwrap() error { return e.err }
