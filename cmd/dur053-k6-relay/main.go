// dur053-k6-relay forwards one witnessed apply or lookup per connection.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"durable-agent-execution-engine/internal/k6hook"
	"durable-agent-execution-engine/internal/k6ingress"
)

const usage = "usage: dur053-k6-relay PRIVATE_CONFIG"

func run(ctx context.Context, args []string) error {
	if len(args) != 1 || args[0] == "" || args[0][0] == '-' {
		return errors.New(usage)
	}
	var config struct {
		Address, SinkAddress, SinkName, SinkPeer, RelayPeer, WitnessAddress, WitnessName string
		WorkerTLS, ClientTLS                                                             k6ingress.TLSFiles
		ObserveTimings                                                                   bool
	}
	if err := k6ingress.ReadConfig(args[0], &config); err != nil {
		return err
	}
	workerTLS, err := config.WorkerTLS.Server(false)
	if err != nil {
		return err
	}
	sinkTLS, err := config.ClientTLS.Client(config.SinkName)
	if err != nil {
		return err
	}
	witnessTLS, err := config.ClientTLS.Client(config.WitnessName)
	if err != nil {
		return err
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return errors.New("K6_RELAY_SESSION_ID")
	}
	witness := &k6ingress.Client{
		Address: config.WitnessAddress, TLS: witnessTLS, Session: hex.EncodeToString(nonce[:]),
		RelayPeer: config.RelayPeer, SinkPeer: config.SinkPeer,
		ObserveTimings: config.ObserveTimings,
	}
	relay := &k6ingress.Relay{SinkAddress: config.SinkAddress, SinkPeer: config.SinkPeer, SinkTLS: sinkTLS, Witness: witness}
	relay.Hook, err = k6hook.FromEnvironment("relay", witness.Session)
	if err != nil {
		return err
	}
	if err := k6ingress.Serve(ctx, config.Address, workerTLS, relay.Handle); err != nil {
		return err
	}
	return witness.Seal()
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
