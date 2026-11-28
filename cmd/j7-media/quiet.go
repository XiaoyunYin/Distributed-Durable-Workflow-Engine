package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"path/filepath"
	"time"

	"durable-agent-execution-engine/internal/invariants"
	"durable-agent-execution-engine/internal/restoreledger"
	"durable-agent-execution-engine/internal/state"
	"github.com/jackc/pgx/v5/pgxpool"
)

type quietConfig struct {
	EngineDSN  string `json:"engine_export_dsn_file"`
	SinkDSN    string `json:"sink_export_dsn_file"`
	Namespace  string `json:"namespace"`
	Ledger     string `json:"ledger"`
	Restore    string `json:"restore"`
	FreshKafka string `json:"fresh_kafka"`
	Evidence   string `json:"evidence_directory"`
}

const quietSQL = `
SELECT (SELECT system_identifier FROM pg_control_system()), (SELECT timeline_id FROM pg_control_checkpoint()),
       clock_timestamp(), pg_database_size(current_database()),
       (SELECT sum(pg_database_size(oid)) FROM pg_database WHERE has_database_privilege(oid,'CONNECT')),
       (SELECT COALESCE(sum(size),0) FROM pg_ls_waldir()),
       current_setting('transaction_read_only')='on' AND NOT pg_is_in_recovery()
`

func runQuietObservation(ctx context.Context, args []string, out, diagnostics io.Writer) error {
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	flags.SetOutput(diagnostics)
	config := flags.String("config", "", "absolute private quiet observation config")
	phase := flags.String("phase", "", "start or end")
	restoreSHA := flags.String("restore-sha256", "", "digest of isolated restore record")
	freshSHA := flags.String("fresh-sha256", "", "digest of fresh broker admission")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 || !filepath.IsAbs(*config) || (*phase != "start" && *phase != "end") {
		return errors.New("quiet observation requires an absolute config and the start or end phase")
	}
	now, boot, err := restoreledger.HostClock()
	if err != nil {
		return err
	}
	var c quietConfig
	if _, err := readFaultJSON(*config, &c); err != nil {
		return err
	}
	if err := checkedEvidenceDir(c.Evidence); err != nil {
		return err
	}
	for _, name := range []string{c.EngineDSN, c.SinkDSN, c.Ledger, c.Restore, c.FreshKafka} {
		if !filepath.IsAbs(name) || filepath.Clean(name) != name {
			return errors.New("quiet inputs require canonical absolute paths")
		}
	}
	report, err := readLiveLedger(c.Ledger, boot)
	if err != nil {
		return err
	}
	var restore restoreReceipt
	var fresh freshKafkaReceipt
	if err := errors.Join(readBoundJSON(c.Restore, *restoreSHA, &restore), readBoundJSON(c.FreshKafka, *freshSHA, &fresh)); err != nil {
		return err
	}
	if !fixtureName.MatchString(c.Namespace) || restore.Target != report.Header.Target || restore.BootID != boot ||
		fresh.RestoreSHA != *restoreSHA || fresh.Authority != report.Header.Target || fresh.BootID != boot ||
		!restore.State.Restricted || fresh.CompletedNS < restore.CompletedNS {
		return errors.New("quiet observation is not bound to the verified isolated target and fresh Kafka")
	}
	at, err := now()
	if err != nil {
		return err
	}
	ctx, stop, err := phaseDeadline(ctx, at, fresh.T0NS, 600*time.Second)
	if err != nil {
		return err
	}
	defer stop()
	engine, sink := "engine-quiet-start.json", "sink-quiet-start.json"
	if *phase == "end" {
		engine, sink = "engine-final.json", "final-sink.json"
	}
	_, digest, err := faultReceipt(filepath.Join(c.Evidence, "quiet-"+*phase+".json"), func() (quietObservation, error) {
		return captureQuiet(ctx, c, restore, fresh, now, engine, sink, *phase)
	})
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(map[string]string{"evidence_sha256": digest})
}

func captureQuiet(ctx context.Context, c quietConfig, restore restoreReceipt, fresh freshKafkaReceipt,
	now restoreledger.Clock, engineName, sinkName, phase string) (quietObservation, error) {
	r := quietObservation{Schema: "j7.quiet-observation.v1", BootID: restore.BootID, Authority: restore.Target}
	dsn, err := readPrivateDSN(c.EngineDSN)
	if err != nil {
		return r, err
	}
	engineCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil || engineCfg.ConnConfig.User != observerRole || engineCfg.ConnConfig.TLSConfig == nil ||
		engineCfg.ConnConfig.TLSConfig.InsecureSkipVerify || engineCfg.ConnConfig.TLSConfig.ServerName == "" ||
		len(engineCfg.ConnConfig.Fallbacks) != 0 {
		return r, errors.New("quiet observation requires a TLS-verified read-only engine observer")
	}
	store, err := state.NewCheckerFromURL(ctx, dsn)
	if err != nil {
		return r, errors.New("open quiet engine observer")
	}
	defer store.Close()
	if r.StartedNS, err = now(); err != nil {
		return r, err
	}
	var readOnly bool
	err = store.Pool().QueryRow(ctx, quietSQL).Scan(&r.SystemID, &r.Timeline, &r.DBTime, &r.Database, &r.Cluster, &r.WAL, &readOnly)
	if err != nil || !readOnly || r.SystemID != restore.State.SystemID || r.Timeline != restore.State.Timeline {
		return r, errors.New("quiet engine observer did not verify the restored read-only authority")
	}
	t, err := invariants.LoadNamespace(ctx, store, c.Namespace)
	if err != nil {
		return r, errors.New("read quiet namespace trace")
	}
	if r.EngineSHA, err = savePrivateJSON(filepath.Join(c.Evidence, engineName), t); err != nil {
		return r, err
	}
	sinkDSN, err := readPrivateDSN(c.SinkDSN)
	if err != nil {
		return r, err
	}
	sinkStore, err := state.NewCheckerFromURL(ctx, sinkDSN)
	if err != nil {
		return r, errors.New("open quiet sink size observer")
	}
	defer sinkStore.Close()
	if err := sinkStore.Pool().QueryRow(ctx, "SELECT pg_database_size(current_database())").Scan(&r.SinkBytes); err != nil {
		return r, errors.New("read quiet sink database size")
	}
	snapshot, err := exportRetainedSink(ctx, c.SinkDSN)
	if err != nil {
		return r, err
	}
	if phase == "end" {
		// E1 reconciliation already wrote this file. A fresh quiet export must
		// match it byte for byte rather than claiming the older read as new.
		raw, err := json.Marshal(snapshot)
		if err != nil {
			return r, err
		}
		r.SinkSHA = restoreledger.SHA256(append(raw, '\n'))
		sha, err := fileSHA(c.Evidence, sinkName)
		if err != nil || sha != r.SinkSHA {
			return r, errors.New("retained sink changed between E1 reconciliation and the final observation")
		}
	} else if r.SinkSHA, err = savePrivateJSON(filepath.Join(c.Evidence, sinkName), snapshot); err != nil {
		return r, err
	}
	broker, err := openJ7Broker(fresh.Broker.Endpoint)
	if err != nil {
		return r, err
	}
	defer broker.ids.Close()
	r.Broker, err = broker.snapshot(ctx, now, true)
	if err != nil {
		return r, err
	}
	if err := verifyKafkaIdentity(fresh.Broker, r.Broker); err != nil {
		return r, err
	}
	r.CompletedNS, err = now()
	return r, err
}
