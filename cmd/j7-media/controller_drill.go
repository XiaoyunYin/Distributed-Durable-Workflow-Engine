package main

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"
)

type drillStep string

const (
	stepProvision       drillStep = "provision"
	stepLedger          drillStep = "bind-ledger"
	stepBackup          drillStep = "prepare"
	stepSourcePreflight drillStep = "source-preflight"
	stepMarkers         drillStep = "source-markers"
	stepLoss            drillStep = "source-loss"
	stepRestore         drillStep = "restore"
	stepBroker          drillStep = "fresh-kafka"
	stepEndpoint        drillStep = "target-endpoint"
	stepReader          drillStep = "orphan-rebind"
	stepRecover         drillStep = "reconcile"
	stepWait            drillStep = "terminal-and-drained"
	stepQuietStart      drillStep = "quiet-start"
	stepQuietWait       drillStep = "quiet-window"
	stepJournal         drillStep = "seal-effect-ingress"
	stepOrphan          drillStep = "verify-orphan"
	stepQuietEnd        drillStep = "quiet-end"
	stepRedispatch      drillStep = "verify-redispatch"
	stepResources       drillStep = "seal-resource-samples"
	stepSeal            drillStep = "seal-acknowledgments"
)

var drillSequence = []drillStep{stepProvision, stepLedger, stepBackup, stepSourcePreflight, stepMarkers, stepLoss, stepRestore,
	stepBroker, stepEndpoint, stepReader, stepRecover, stepWait, stepQuietStart, stepQuietWait, stepJournal, stepOrphan, stepQuietEnd,
	stepRedispatch, stepResources, stepSeal}

// A failure leaves artifacts from completed stages in place for the workflow's
// partial evidence index. All later gates, including the final seal, are skipped.
func driveDrill(ctx context.Context, step func(context.Context, drillStep) error) error {
	for _, phase := range drillSequence {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := step(ctx, phase); err != nil {
			return fmt.Errorf("J7 %s: %w", phase, err)
		}
	}
	return nil
}

type drillPaths struct {
	evidence, secret, restore, orphan, bin string
}

func ownedDrillPaths(h fixtureHost, id, evidence string) (drillPaths, error) {
	if !validControllerDrill(id) || evidence != filepath.Join(h.private, id) {
		return drillPaths{}, errors.New("J7 requires the exact owned drill evidence directory")
	}
	secret := filepath.Join(h.private, id+"-private")
	return drillPaths{evidence: evidence, secret: secret, restore: filepath.Join(secret, "restored"),
		orphan: filepath.Join(secret, "orphan"), bin: filepath.Join(secret, "pg-bin")}, nil
}

func (p drillPaths) raw(name string) string { return filepath.Join(p.evidence, name) }
func (p drillPaths) cfg(name string) string { return filepath.Join(p.secret, name) }

func waitControllerUntil(ctx context.Context, now func() (int64, error), until int64) error {
	for {
		at, err := now()
		if err != nil {
			return err
		}
		if at >= until {
			return ctx.Err()
		}
		timer := time.NewTimer(time.Duration(until - at))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
