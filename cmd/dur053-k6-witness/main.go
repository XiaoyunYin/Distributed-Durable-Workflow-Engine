// dur053-k6-witness receives and fsyncs ingress records on the runner.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"durable-agent-execution-engine/internal/k6ingress"
)

func run(ctx context.Context, path string) error {
	var config struct {
		Address, Journal, RelayPeer, SinkPeer string
		TLS                                   k6ingress.TLSFiles
	}
	if err := k6ingress.ReadConfig(path, &config); err != nil {
		return err
	}
	serverTLS, err := config.TLS.Server(true)
	if err != nil {
		return err
	}
	witness, err := k6ingress.OpenReceiver(config.Journal, config.RelayPeer, config.SinkPeer)
	if err != nil {
		return err
	}
	defer witness.Close()
	return k6ingress.Serve(ctx, config.Address, serverTLS, witness.Handle)
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: dur053-k6-witness PRIVATE_CONFIG")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
