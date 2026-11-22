// dur053-k6-decision serves the restricted episode decision adapter.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"durable-agent-execution-engine/internal/invariants/k6v1"
	"durable-agent-execution-engine/internal/k6auth"
	"durable-agent-execution-engine/internal/k6observation"
)

type config struct {
	Episode     k6auth.Episode
	APIServer   string
	CA          []byte
	Reviewer    string
	DecisionDSN string
	Evidence    *struct {
		Path    string
		Binding k6v1.SourceBinding
	}
}

func run(ctx context.Context, path, socketDir string) error {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Size() > 64<<10 {
		return errors.New("ADAPTER_PRIVATE_CONFIG")
	}
	body, err := os.ReadFile(path)
	var cfg config
	if err != nil || json.Unmarshal(body, &cfg) != nil {
		return errors.New("ADAPTER_CONFIG")
	}
	reviewer, err := k6auth.NewKubernetesReviewer(cfg.APIServer, cfg.CA, cfg.Reviewer)
	if err != nil {
		return errors.New("ADAPTER_REVIEWER_CONFIG")
	}
	connect, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	writer, err := k6auth.OpenDecisionWriter(connect, cfg.DecisionDSN)
	if err != nil {
		return errors.New("ADAPTER_DECISION_DB")
	}
	defer writer.Close()
	adapter, err := k6auth.NewAdapter(cfg.Episode, reviewer, writer)
	if err != nil {
		return errors.New("ADAPTER_EPISODE")
	}
	var journal *k6observation.Journal[k6v1.DecisionRow]
	if cfg.Evidence != nil {
		if cfg.Evidence.Binding.Namespace != cfg.Episode.Namespace {
			return errors.New("ADAPTER_EVIDENCE_IDENTITY")
		}
		journal, err = k6observation.Open[k6v1.DecisionRow](cfg.Evidence.Path, cfg.Evidence.Binding, "decisions", "decision-adapter")
		if err != nil {
			return err
		}
		adapter.Observe = func(id k6auth.DecisionIdentity) error {
			row := k6v1.DecisionRow{Intent: id.IntentID, ProposalHash: id.ProposalHash, Outcome: id.Decision,
				Subject:           "system:serviceaccount:" + cfg.Episode.Namespace + ":k6-approver",
				ServiceAccountUID: cfg.Episode.ServiceAccountUID, BoundUID: cfg.Episode.SecretUID,
				Audience: k6auth.Audience, IssuerHash: k6v1.Hash([]byte(k6auth.Issuer))}
			if id.DecidedAt != nil {
				row.DecidedUTC = k6v1.UTC(*id.DecidedAt)
			}
			if !row.Valid() {
				return errors.New("ADAPTER_EVIDENCE_SCHEMA")
			}
			return journal.Append(row)
		}
	}
	if err := adapter.Serve(ctx, socketDir); err != nil {
		return errors.New("ADAPTER_SOCKET")
	}
	if journal != nil {
		return journal.Checkpoint(true)
	}
	return nil
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: dur053-k6-decision PRIVATE_CONFIG FRESH_SOCKET_DIRECTORY")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1], os.Args[2]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
