package main

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
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

type osSyscallErrorForTest struct{ err error }

func (e *osSyscallErrorForTest) Error() string { return "sensitive detail must not be serialized" }
func (e *osSyscallErrorForTest) Unwrap() error { return e.err }
