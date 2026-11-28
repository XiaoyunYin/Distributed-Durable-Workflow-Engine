package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"time"

	"durable-agent-execution-engine/internal/effects/k6sink"
	"durable-agent-execution-engine/internal/k6ingress"
	"durable-agent-execution-engine/internal/restoreledger"
	"durable-agent-execution-engine/internal/state"
)

const (
	orphanSourcePhase = "j7-e1-source-hold"
	orphanRebindPhase = "j7-e1-reader-rebound"
	orphanSourceFile  = "source-engine.json"
	orphanRebindFile  = "reader-rebind.json"
)

type orphanConfig struct {
	Clock      orphanClock `json:"clock"`
	Restore    string      `json:"restore_config"`
	Cohorts    string      `json:"pre_cut_cohorts"`
	Ledger     string      `json:"ack_ledger"`
	SourceDSN  string      `json:"source_observer_dsn_file"`
	TargetDSN  string      `json:"target_observer_dsn_file"`
	SinkDSN    string      `json:"sink_export_dsn_file"`
	SinkClient string      `json:"sink_grant_reader_client_address"`
	RelayPeer  string      `json:"effect_relay_peer"`
	SinkPeer   string      `json:"effect_sink_peer"`
	Ingress    string      `json:"effect_ingress_journal"`
	Namespace  string      `json:"namespace"`
	Evidence   string      `json:"new_evidence_directory"`
}

type orphanInputs struct {
	cfg       orphanConfig
	sha       string
	boot      string
	now       restoreledger.Clock
	hold      effectAtCut
	cohortSHA string
	header    restoreledger.Header
}

func loadOrphanInputs(path, boot string, now restoreledger.Clock) (orphanInputs, error) {
	o := orphanInputs{boot: boot, now: now}
	var err error
	o.sha, err = readFaultJSON(path, &o.cfg)
	if err != nil {
		return o, err
	}
	c := o.cfg
	paths := []string{path, c.Cohorts, c.Ledger, c.Restore, c.SourceDSN, c.TargetDSN, c.SinkDSN, c.Ingress, c.Evidence}
	seen := map[string]bool{}
	for _, p := range paths {
		if !filepath.IsAbs(p) || filepath.Clean(p) != p || seen[p] || (p != c.Evidence && within(c.Evidence, p)) {
			return o, errors.New("E1 requires distinct canonical controller inputs and private evidence storage")
		}
		seen[p] = true
		parent, err := filepath.EvalSymlinks(filepath.Dir(p))
		if err != nil || parent != filepath.Dir(p) {
			return o, errors.New("E1 controller paths must have canonical parents")
		}
	}
	at, err := now()
	client, ipErr := netip.ParseAddr(c.SinkClient)
	if err != nil || ipErr != nil || client.String() != c.SinkClient || client.IsUnspecified() ||
		!fixtureName.MatchString(c.Namespace) || c.Clock.StartNS <= 0 || at < c.Clock.StartNS ||
		c.Clock.StartUTC.IsZero() || c.Clock.StartUTC.Nanosecond()%1000 != 0 ||
		c.RelayPeer == "" || c.SinkPeer == "" || c.RelayPeer == c.SinkPeer {
		return o, errors.New("E1 configuration is missing the frozen fixture, clock or private sink identity")
	}
	var cohorts cohortsAtCut
	o.cohortSHA, err = readFaultJSON(c.Cohorts, &cohorts)
	if err != nil {
		return o, err
	}
	o.hold = cohorts.E1
	h := o.hold
	if cohorts.Schema != "j7.cohorts-at-cut.v1" || h.Attempt.NodeID != state.K6Activity ||
		h.Attempt.WorkflowID == "" || h.Attempt.Iteration < 0 || h.Attempt.Attempt != 1 || h.Attempt.Deadline.IsZero() ||
		h.Intent == "" || h.Resource == "" || h.LogicalKey == "" || h.Fence <= 0 ||
		!restoreledger.IsSHA256(h.Attempt.ClaimSHA) || !restoreledger.IsSHA256(h.CanonicalHash) ||
		!restoreledger.IsSHA256(h.ScopeHash) || !restoreledger.IsSHA256(h.GrantSHA) {
		return o, errors.New("E1 cohort lacks its exact grant, fence or first claim")
	}
	report, err := readLiveLedger(c.Ledger, boot)
	if err != nil {
		return o, err
	}
	o.header = report.Header
	return o, nil
}

type reboundInputs struct {
	restoreInputs
	restored   restoreReceipt
	restoreSHA string
	source     orphanObservation
	sourceSHA  string
	before     k6sink.Snapshot
}

func (o orphanInputs) loadRebound(sha string) (reboundInputs, error) {
	var i reboundInputs
	in, err := loadRestoreInputs(o.cfg.Restore, o.boot)
	if err != nil {
		return i, err
	}
	i.restoreInputs = in
	if in.fault.Ledger != o.cfg.Ledger || in.config.Cohorts != o.cfg.Cohorts || in.config.SinkDSNFile != o.cfg.SinkDSN ||
		in.header != o.header || in.cut.Cohorts.EvidenceSHA != o.cohortSHA || in.cohorts.E1 != o.hold ||
		o.cfg.Namespace != in.markers[0].Namespace || o.cfg.Clock.StartNS > in.fault.OriginNS ||
		within(in.fault.Archive, o.cfg.Evidence) || within(in.fault.BackupDir, o.cfg.Evidence) ||
		within(in.binding.Volume.Mountpoint, o.cfg.Evidence) {
		return i, errors.New("E1 evidence is unbound to the sealed recovery drill")
	}
	i.restoreSHA, err = readFaultJSON(filepath.Join(in.config.Evidence, "restore.json"), &i.restored)
	if err != nil {
		return i, err
	}
	r := i.restored
	if !restoreledger.IsSHA256(sha) || i.restoreSHA != sha || r.Schema != "j7.isolated-restore.v1" ||
		r.BootID != o.boot || r.Source != o.header.Source || r.Target != o.header.Target ||
		r.LossSHA != in.config.LossSHA || r.ConfigSHA != in.configSHA ||
		r.CompletedNS < in.loss.ArchiveSealNS || r.CompletedNS-in.loss.T0NS > int64(restoreWindow) ||
		r.Verdict.Orphan != in.proof.Receipt || len(r.Verdict.RetainedWorkflows) != 17 ||
		!slices.Contains(r.Verdict.RetainedWorkflows, o.hold.Attempt.WorkflowID) {
		return i, errors.New("E1 requires the completed isolated restore and its listed orphan")
	}
	if err := r.verifyReplay(in.backup.Source); err != nil {
		return i, err
	}
	if err := readBoundJSON(filepath.Join(in.config.Evidence, "sink-pre-write.json"), r.SinkSHA, &i.before); err != nil {
		return i, err
	}
	i.sourceSHA, err = readFaultJSON(filepath.Join(o.cfg.Evidence, orphanSourceFile), &i.source)
	if err != nil {
		return i, err
	}
	s := i.source
	if s.Schema != "j7.e1-source-hold.v1" || s.ConfigSHA != o.sha || s.BootID != o.boot || s.Authority != o.header.Source ||
		s.DB.SystemID != in.backup.Source.SystemID || s.DB.Timeline != in.backup.Source.Timeline ||
		s.DB.Database != in.config.Database || s.CompletedNS < s.DB.EndedNS || s.CompletedNS > in.cut.StartedNS {
		return i, errors.New("E1 source hold does not match the archived source")
	}
	report, err := readLossLedger(in.fault.Ledger, o.boot, in.loss.LedgerSHA256)
	if err != nil {
		return i, err
	}
	w, err := phaseWitness(report, orphanSourcePhase, o.cfg.Clock.StartNS, in.cut.StartedNS)
	if err != nil || w.EvidenceSHA != i.sourceSHA || w.Timeline != s.DB.Timeline ||
		w.StartedNS != s.DB.StartedNS || w.CompletedNS != s.CompletedNS || w.LSN != s.DB.FlushLSN {
		return i, errors.New("E1 source hold digest is missing from the durable pre-cut ledger")
	}
	return i, errors.Join(s.DB.checkGrant(o.cfg.Clock, o.hold, o.cfg.Namespace, "GRANTED"),
		s.DB.checkHeld(o.hold, true), s.DB.checkReader(o.cfg.SinkClient, o.cfg.Clock.StartUTC))
}

func readBoundJSON[T any](path, expected string, value *T) error {
	digest, err := readFaultJSON(path, value)
	if err != nil {
		return err
	}
	if !restoreledger.IsSHA256(expected) || digest != expected {
		return fmt.Errorf("E1 evidence digest mismatch for %s", filepath.Base(path))
	}
	return nil
}

func (o orphanInputs) sample(ctx context.Context, dsn string) (orphanDB, error) {
	sampleCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	return readOrphanDB(sampleCtx, dsn, o.cfg.Namespace, o.hold.Attempt.WorkflowID, o.hold.Attempt.Iteration, o.now)
}

func (o orphanInputs) captureSource(ctx context.Context) (orphanObservation, error) {
	r := orphanObservation{Schema: "j7.e1-source-hold.v1", Authority: o.header.Source, ConfigSHA: o.sha, BootID: o.boot}
	var err error
	r.DB, err = o.sample(ctx, o.cfg.SourceDSN)
	if err != nil {
		return r, err
	}
	if err := errors.Join(r.DB.checkGrant(o.cfg.Clock, o.hold, o.cfg.Namespace, "GRANTED"),
		r.DB.checkReader(o.cfg.SinkClient, o.cfg.Clock.StartUTC), r.DB.checkHeld(o.hold, true)); err != nil {
		return r, err
	}
	sink, err := exportRetainedSink(ctx, o.cfg.SinkDSN)
	if err != nil {
		return r, err
	}
	r.SinkSHA, err = savePrivateJSON(filepath.Join(o.cfg.Evidence, "source-sink.json"), sink)
	if err != nil {
		return r, err
	}
	r.CompletedNS, err = o.now()
	return r, errors.Join(err, verifySourceSink(sink, o.hold))
}

func (o orphanInputs) checkRebind(db orphanDB, i reboundInputs) error {
	old := i.source.DB
	if db.Address == old.Address || db.SystemID != i.restored.State.SystemID ||
		db.Timeline != i.restored.State.Timeline || db.Database != old.Database ||
		db.StartedNS < i.restored.CompletedNS || db.Grant != old.Grant {
		return errors.New("E1 sink reader has not moved to the verified target with the original approval")
	}
	return errors.Join(db.checkGrant(o.cfg.Clock, o.hold, o.cfg.Namespace, "GRANTED"),
		db.checkHeld(o.hold, false), db.checkReader(o.cfg.SinkClient, o.cfg.Clock.wall(i.restored.CompletedNS)))
}

func (o orphanInputs) captureRebind(ctx context.Context, i reboundInputs) (orphanObservation, error) {
	r := orphanObservation{Schema: "j7.e1-reader-rebind.v1", Authority: o.header.Target,
		ConfigSHA: o.sha, BootID: o.boot, SourceSHA: i.sourceSHA, RestoreSHA: i.restoreSHA}
	var err error
	r.DB, err = o.sample(ctx, o.cfg.TargetDSN)
	if err != nil {
		return r, err
	}
	r.CompletedNS, err = o.now()
	if r.CompletedNS-i.loss.T0NS > int64(300*time.Second) {
		err = errors.New("E1 sink reader rebind missed the endpoint gate")
	}
	return r, errors.Join(err, o.checkRebind(r.DB, i))
}

func (o orphanInputs) loadRebind(i reboundInputs) (orphanObservation, string, error) {
	var r orphanObservation
	sha, err := readFaultJSON(filepath.Join(o.cfg.Evidence, orphanRebindFile), &r)
	if err != nil {
		return r, sha, err
	}
	if r.Schema != "j7.e1-reader-rebind.v1" || r.Authority != o.header.Target || r.ConfigSHA != o.sha ||
		r.BootID != o.boot || r.SourceSHA != i.sourceSHA || r.RestoreSHA != i.restoreSHA ||
		r.CompletedNS < r.DB.EndedNS || r.CompletedNS-i.loss.T0NS > int64(300*time.Second) {
		return r, sha, errors.New("E1 reader-rebind receipt is incomplete or bound to another restore")
	}
	if err := o.checkRebind(r.DB, i); err != nil {
		return r, sha, err
	}
	report, err := readLiveLedger(o.cfg.Ledger, o.boot)
	if err != nil {
		return r, sha, err
	}
	count := 0
	for _, w := range report.Witnesses {
		if w.Phase == orphanRebindPhase {
			if w.Kind != restoreledger.AuthorityChange || w.Authority != o.header.Target || w.Former != o.header.Source ||
				w.RequestID != "" || w.StartedNS != r.DB.StartedNS || w.CompletedNS != r.CompletedNS ||
				w.EvidenceSHA != sha || w.Timeline != r.DB.Timeline || w.LSN != r.DB.FlushLSN {
				return r, sha, errors.New("E1 reader rebind differs from its authority-change witness")
			}
			count++
		}
	}
	if count != 1 {
		return r, sha, errors.New("E1 reader rebind needs exactly one durable authority-change witness")
	}
	return r, sha, nil
}

func (o orphanInputs) loadIngress() ([]k6ingress.Record, string, error) {
	file, err := openRegular(o.cfg.Ingress, os.O_RDONLY)
	if err != nil {
		return nil, "", err
	}
	defer file.Close()
	const max = 64 << 10
	raw, err := io.ReadAll(io.LimitReader(file, max+1))
	if err != nil || len(raw) > max {
		return nil, "", errors.New("E1 independent ingress journal is unreadable or unexpectedly large")
	}
	rows, err := k6ingress.ReadJournal(bytes.NewReader(raw), o.cfg.RelayPeer, o.cfg.SinkPeer)
	return rows, restoreledger.SHA256(raw), err
}

func (o orphanInputs) verify(ctx context.Context, i reboundInputs) (orphanVerdict, error) {
	r := orphanVerdict{Schema: "j7.e1-reconciliation.v1", BootID: o.boot, Workflow: o.hold.Attempt.WorkflowID,
		RestoreSHA: i.restoreSHA, SourceSHA: i.sourceSHA, ApplicationID: i.proof.Receipt.ApplicationID,
		OriginalResultSHA: i.loss.FinalSource.EffectSHA256, Unresolved: []string{o.hold.Attempt.WorkflowID}}
	rebind, sha, err := o.loadRebind(i)
	r.RebindSHA = sha
	if err != nil {
		return r, err
	}
	var source k6sink.Snapshot
	if err := readBoundJSON(filepath.Join(o.cfg.Evidence, "source-sink.json"), i.source.SinkSHA, &source); err != nil {
		return r, err
	}
	rows, sha, err := o.loadIngress()
	r.IngressSHA = sha
	if err != nil {
		return r, err
	}
	final, err := o.sample(ctx, o.cfg.TargetDSN)
	if err != nil {
		return r, err
	}
	r.EngineSHA, err = savePrivateJSON(filepath.Join(o.cfg.Evidence, "final-engine.json"), final)
	if err != nil {
		return r, err
	}
	pinned := rebind.DB
	expectedGrant := pinned.Grant
	expectedGrant.Status = "DISPATCHED"
	if final.Address != pinned.Address || final.Timeline != pinned.Timeline || final.SystemID != pinned.SystemID ||
		final.Database != pinned.Database || final.StartedNS < rebind.CompletedNS || final.Grant != expectedGrant ||
		!slices.ContainsFunc(final.Sessions, func(s readerSession) bool { return slices.Contains(pinned.Sessions, s) }) {
		return r, errors.New("E1 final observation lost the verified target or its retained grant-reader session")
	}
	if err := errors.Join(final.checkGrant(o.cfg.Clock, o.hold, o.cfg.Namespace, "DISPATCHED"),
		final.checkReader(o.cfg.SinkClient, o.cfg.Clock.wall(i.restored.CompletedNS))); err != nil {
		return r, err
	}
	after, err := exportRetainedSink(ctx, o.cfg.SinkDSN)
	if err != nil {
		return r, err
	}
	r.SinkSHA, err = savePrivateJSON(filepath.Join(o.cfg.Evidence, "final-sink.json"), after)
	if err != nil {
		return r, err
	}
	key, _ := hex.DecodeString(i.config.SinkKey) // sealed restore inputs validated the public key
	t0 := o.cfg.Clock.wall(i.loss.T0NS)
	replacement, err := verifyRecoveredEffect(final, o.hold, i.proof, r.OriginalResultSHA,
		ed25519.PublicKey(key), pinned, t0.Add(-clockBound), t0.Add(480*time.Second-clockBound), &r)
	if err != nil {
		return r, err
	}
	original, lookup, err := verifyRetainedSink(source, i.before, after, o.hold, i.proof.Receipt, replacement)
	if err != nil {
		return r, err
	}
	if err := checkIngress(rows, o.hold, original, lookup, o.cfg.Clock.wall(i.cut.CompletedNS),
		o.cfg.Clock.wall(i.loss.KillNS), o.cfg.Clock.wall(rebind.CompletedNS), o.cfg.Clock.wall(final.StartedNS)); err != nil {
		return r, err
	}
	if original.ObservedAt.Before(rows[1].Time.Add(-clockBound)) ||
		original.ObservedAt.After(rows[2].Time.Add(clockBound)) ||
		lookup.ObservedAt.Before(rows[6].Time.Add(-clockBound)) ||
		lookup.ObservedAt.After(rows[7].Time.Add(clockBound)) ||
		!lookup.ObservedAt.Before(replacement.Deadline) || !lookup.ObservedAt.Before(r.ConsumedAt.Add(clockBound)) ||
		final.DBTime.Sub(r.ConsumedAt) < 30*time.Second {
		return r, errors.New("E1 lookup missed its replacement claim or final quiet observation")
	}
	r.CompletedNS, err = o.now()
	if err != nil || r.CompletedNS-i.loss.T0NS > int64(600*time.Second) || ctx.Err() != nil {
		return r, errors.New("E1 evidence missed the final observation gate")
	}
	zero := 0
	r.ReplacementApplies = &zero
	r.RestoredResultSHA, r.LookupCallID = final.ResultSHA, lookup.CallID
	r.ReplacementAttempt, r.ReplacementFence, r.ReplacementClaim = replacement.Attempt, replacement.Fence, replacement.ClaimSHA
	r.Mutations, r.Revision, r.StateHash = len(after.Journal), after.Resources[0].Revision, after.Resources[0].FinalStateHash
	r.Unresolved = []string{}
	return r, nil
}

func runOrphanStage(ctx context.Context, args []string, out, diagnostics io.Writer) error {
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	flags.SetOutput(diagnostics)
	config := flags.String("config", "", "absolute private E1 drill configuration")
	restoreSHA := flags.String("restore-sha256", "", "digest of the completed isolated restore")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 || !filepath.IsAbs(*config) || (args[0] == "orphan-source") != (*restoreSHA == "") {
		return errors.New("E1 stage requires --config and the isolated restore digest for target stages")
	}
	now, boot, err := restoreledger.HostClock()
	if err != nil {
		return err
	}
	o, err := loadOrphanInputs(*config, boot, now)
	if err != nil {
		return err
	}
	digest := ""
	if args[0] == "orphan-source" {
		if err := os.Mkdir(o.cfg.Evidence, 0o700); err != nil {
			return err
		}
		if err := syncDirectory(filepath.Dir(o.cfg.Evidence)); err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		_, digest, err = faultReceipt(filepath.Join(o.cfg.Evidence, orphanSourceFile), func() (orphanObservation, error) {
			return o.captureSource(ctx)
		})
	} else {
		info, err := os.Lstat(o.cfg.Evidence)
		if err != nil || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
			return errors.New("E1 target stages require the existing private source evidence directory")
		}
		i, err := o.loadRebound(*restoreSHA)
		if err != nil {
			return err
		}
		at, err := now()
		if err != nil {
			return err
		}
		window := 600 * time.Second
		if args[0] == "orphan-rebind" {
			window = 300 * time.Second
		}
		bounded, cancel, err := phaseDeadline(ctx, at, i.loss.T0NS, window)
		if err != nil {
			return err
		}
		defer cancel()
		if args[0] == "orphan-rebind" {
			_, digest, err = faultReceipt(filepath.Join(o.cfg.Evidence, orphanRebindFile), func() (orphanObservation, error) {
				return o.captureRebind(bounded, i)
			})
		} else {
			_, digest, err = faultReceipt(filepath.Join(o.cfg.Evidence, "reconciliation.json"), func() (orphanVerdict, error) {
				return o.verify(bounded, i)
			})
		}
		if err != nil {
			return err
		}
	}
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(map[string]string{"evidence_sha256": digest})
}
