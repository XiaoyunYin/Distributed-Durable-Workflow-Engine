package faults

import (
	"context"
	"errors"
	"net"
	"testing"
)

func TestK1CanceledControllerDial(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	t.Setenv("DURABLE_FAULT_ENDPOINT", listener.Addr().String())
	t.Setenv("DURABLE_FAULT_TOKEN", "k1-test-token")
	t.Setenv("DURABLE_FAULT_SEED", "1")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client, err := FromContext(ctx)
	if client != nil {
		_ = client.Close()
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled startup dial did not stop: %v", err)
	}
}
