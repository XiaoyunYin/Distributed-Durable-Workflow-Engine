package main

import (
	"context"
	"errors"
	"path/filepath"
	"time"

	"durable-agent-execution-engine/internal/restoreledger"
)

type endpointObservation struct {
	OldDeniedNS, ForeignDeniedNS, OpenNS int64
	SystemID                             uint64
	Timeline                             uint32
}

type endpointJournal interface {
	Header() restoreledger.Header
	RecordWitness(restoreledger.Witness) error
}

// Admission consumes authenticated live observations supplied by the endpoint
// driver. The resource transitions and the ledger witness must agree before
// the driver can expose the endpoint to application writers.
func bindControllerEndpoint(dir string, ledger endpointJournal, restoreSHA, freshSHA string, live endpointObservation,
	save func(string, readyEndpoint) (string, error)) error {
	if err := checkedEvidenceDir(dir); err != nil {
		return err
	}
	var restore restoreReceipt
	var fresh freshKafkaReceipt
	if err := errors.Join(readBoundJSON(filepath.Join(dir, "restore.json"), restoreSHA, &restore),
		readBoundJSON(filepath.Join(dir, "kafka-fresh.json"), freshSHA, &fresh)); err != nil {
		return err
	}
	h := ledger.Header()
	if restore.Schema != "j7.isolated-restore.v1" || fresh.Schema != "j7.fresh-kafka.v1" ||
		restore.BootID != h.BootID || fresh.BootID != h.BootID || restore.Source != h.Source ||
		restore.Target != h.Target || fresh.Authority != h.Target || fresh.RestoreSHA != restoreSHA ||
		fresh.T0NS <= 0 || restore.CompletedNS < fresh.T0NS || fresh.CompletedNS < restore.CompletedNS ||
		!restore.State.Restricted || restore.State.SystemID == 0 || restore.State.Timeline <= restore.SourceTimeline ||
		live.SystemID != restore.State.SystemID || live.Timeline != restore.State.Timeline ||
		live.OldDeniedNS < restore.CompletedNS || live.ForeignDeniedNS < restore.CompletedNS ||
		live.OpenNS < max(live.OldDeniedNS, live.ForeignDeniedNS, fresh.CompletedNS) ||
		live.OpenNS-fresh.T0NS > int64(300*time.Second) {
		return errors.New("J7 target endpoint does not bind the admitted restore, fresh broker and denial observations")
	}
	p := readyEndpoint{Schema: "j7.target-endpoint.v1", Authority: h.Target, BootID: h.BootID,
		RestoreSHA: restoreSHA, KafkaSHA: freshSHA, SystemID: live.SystemID, Timeline: live.Timeline,
		OldDeniedNS: live.OldDeniedNS, ForeignDeniedNS: live.ForeignDeniedNS, OpenNS: live.OpenNS}
	digest, err := save(filepath.Join(dir, "endpoint.json"), p)
	if err != nil {
		return err
	}
	return ledger.RecordWitness(restoreledger.Witness{
		Kind: restoreledger.StateProbe, Phase: "j7-target-endpoint", Authority: h.Target, Timeline: p.Timeline,
		StartedNS: min(p.OldDeniedNS, p.ForeignDeniedNS), CompletedNS: p.OpenNS, EvidenceSHA: digest,
	})
}

// The endpoint file is fsynced before the ledger can refer to its digest.
func recordControllerEndpoint(ctx context.Context, dir string, ledger endpointJournal, restoreSHA, freshSHA string, live endpointObservation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return bindControllerEndpoint(dir, ledger, restoreSHA, freshSHA, live, func(path string, p readyEndpoint) (string, error) {
		_, digest, err := faultReceipt(path, func() (readyEndpoint, error) { return p, nil })
		return digest, err
	})
}
