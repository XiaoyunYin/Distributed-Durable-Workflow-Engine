package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"

	"durable-agent-execution-engine/internal/invariants"
	"durable-agent-execution-engine/internal/j7fences"
	"durable-agent-execution-engine/internal/restoreledger"
	"durable-agent-execution-engine/internal/state"
)

type liveDrill struct {
	h                    fixtureHost
	p                    drillPaths
	id, boot, revision   string
	now                  restoreledger.Clock
	ledger               *restoreledger.Writer
	work                 fixtureWorkload
	db                   drillDatabases
	apps                 liveApps
	effect               liveEffect
	sampler              controllerSampler
	stamp                orphanClock
	depPhase             atomic.Int32
	freezing             atomic.Bool
	media                stageFiles
	backup               backupReceipt
	loss                 outageReceipt
	restored             restoreReceipt
	fresh                freshKafkaReceipt
	ownerSource          j7fences.SourceOwner
	workerSource         j7fences.SourceClaim
	f1, e1               fixtureClaim
	cohorts              cohortsAtCut
	effectDone           chan effectCommit
	quietStart, quietEnd quietObservation
}

func (h fixtureHost) runDrill(ctx context.Context, drill, evidence, tool, revision, candidate string) error {
	p, err := ownedDrillPaths(h, drill, evidence)
	if err != nil {
		return err
	}
	if tool != filepath.Join(h.private, "j7-media") {
		return errors.New("drill requires the bootstrapped media binary")
	}
	if err := checkedEvidenceDir(evidence); err != nil {
		return err
	}
	if err := os.Mkdir(p.secret, 0o700); err != nil {
		return err
	}
	now, boot, err := restoreledger.HostClock()
	if err != nil {
		return err
	}
	var b bootstrapWitness
	if _, err := readFaultJSON(filepath.Join(h.private, "bootstrap.json"), &b); err != nil || b.Schema != "j7.bootstrap.v1" ||
		b.Boot != boot || b.CandidateSHA != candidate || b.Revision != revision {
		return errors.New("drill requires the exact live bootstrap witness")
	}
	d := &liveDrill{h: h, p: p, id: drill, boot: boot, revision: revision, now: now, db: initDrillDB(h, drill, p)}
	d.media = stageFiles{Root: p.evidence, ConfigRoot: p.secret, Drill: drill, PGBin: p.bin}
	stageCtx, stop := context.WithCancelCause(ctx)
	defer stop(nil)
	defer d.apps.close()
	defer d.effect.close()
	defer d.sampler.stop()
	capacity, err := h.hostCapacity(stageCtx, false)
	if err != nil {
		return err
	}
	d.sampler = controllerSampler{now: now, collect: d.sample,
		window: resourceWindow{Schema: "j7.resource-samples.v1", CPUs: capacity.CPUs, InstalledKiB: capacity.InstalledKiB,
			StartupFreeKiB: min(capacity.Filesystems["workspace"], capacity.Filesystems["docker"]),
			EngineLimit:    h.plan.Work.EngineConnections, SinkLimit: h.plan.Work.SinkConnections, RoleLimits: h.plan.Work.Roles}}
	if err := d.sampler.start(stageCtx, stop); err != nil {
		return err
	}
	defer func() {
		if d.ledger != nil {
			_ = d.ledger.Close()
		}
	}()
	if err := driveDrill(stageCtx, d.step); err != nil {
		d.sampler.stop()
		var retainErr error
		if _, statErr := os.Stat(d.p.raw("resource-samples.json")); errors.Is(statErr, os.ErrNotExist) {
			retainErr = writeControllerJSON(d.p.raw("resource-samples.json"), d.sampler.window)
		}
		return errors.Join(err, context.Cause(stageCtx), retainErr)
	}
	return nil
}

func (d *liveDrill) step(ctx context.Context, s drillStep) error {
	switch s {
	case stepProvision:
		return d.provision(ctx)
	case stepLedger:
		return d.createLedger()
	case stepBackup:
		return d.prepare(ctx)
	case stepSourcePreflight:
		_, err := d.mediaCall(ctx, "source-preflight")
		return err
	case stepMarkers:
		return d.runMarkers(ctx)
	case stepLoss:
		return d.lose(ctx)
	case stepRestore:
		return d.restore(ctx)
	case stepBroker:
		return d.newBroker(ctx)
	case stepEndpoint:
		return d.openTarget(ctx)
	case stepReader:
		return d.rebind(ctx)
	case stepRecover:
		return d.reconcile(ctx)
	case stepWait:
		return d.waitForDrain(ctx)
	case stepQuietStart:
		return d.quiet(ctx, "start", &d.quietStart)
	case stepQuietWait:
		return waitControllerUntil(ctx, d.now, d.quietStart.CompletedNS+int64(30*time.Second))
	case stepJournal:
		return d.effect.witness.Seal()
	case stepOrphan:
		_, err := d.mediaCall(ctx, "verify-orphan")
		if err != nil {
			return err
		}
		return d.copyEvidence(d.p.orphan, map[string]string{"reconciliation.json": "orphan.json",
			"final-engine.json": "final-engine.json", "final-sink.json": "final-sink.json"})
	case stepQuietEnd:
		return d.quiet(ctx, "end", &d.quietEnd)
	case stepRedispatch:
		var err error
		d.media.Engine, err = fileSHA(d.p.evidence, "engine-final.json")
		if err != nil {
			return err
		}
		_, err = d.mediaCall(ctx, "verify-redispatch")
		return err
	case stepResources:
		window, err := d.sampler.finish(ctx, d.stamp.StartNS, d.quietEnd.CompletedNS)
		if err != nil {
			return err
		}
		return writeControllerJSON(d.p.raw("resource-samples.json"), window)
	case stepSeal:
		_, err := d.ledger.Seal()
		return err
	default:
		return errors.New("unknown J7 drill stage")
	}
}

func (d *liveDrill) mediaCall(ctx context.Context, name string) ([]byte, error) {
	var out, diag bytes.Buffer
	if err := invokeControllerStage(ctx, d.media, name, &out, &diag); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func (d *liveDrill) provision(ctx context.Context) error {
	start, err := d.now()
	if err != nil {
		return err
	}
	d.stamp = orphanClock{StartUTC: time.Now().UTC().Truncate(time.Microsecond), StartNS: start}
	w, err := buildFixtureWorkload(d.h.campaign, d.id, kubeNamespace(d.id), d.stamp)
	if err != nil {
		return err
	}
	d.work = w
	ca, err := newControllerCA(d.stamp.StartUTC)
	if err != nil {
		return err
	}
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		return err
	}
	d.work.work.SinkKey = hex.EncodeToString(key.Public().(ed25519.PublicKey))
	d.work.work.RelayPeer, d.work.work.SinkPeer = relayURI, sinkURI
	d.apps = liveApps{h: d.h, p: d.p, ns: w.work.Fixture.Namespace, ca: ca, db: &d.db, work: &d.work, receiptKey: key}
	if err := d.db.start(ctx, d.h, d.id, d.p, ca, d.work, d.stamp); err != nil {
		return err
	}
	d.depPhase.Store(1)
	if err := d.apps.init(ctx); err != nil {
		return err
	}
	if err := d.apps.startSink(ctx, false); err != nil {
		return err
	}
	e, err := openLiveEffect(ctx, ca, d.p.raw("effect-ingress.jsonl"))
	if err != nil {
		return err
	}
	d.effect = e
	return d.apps.startRuntime(ctx, false)
}

func (d *liveDrill) createLedger() error {
	run, _ := strconv.ParseUint(d.h.runID, 10, 64)
	now, err := d.now()
	if err != nil {
		return err
	}
	// Leave a fixed preparation window for the verified backup and both WAL anchors.
	d.work.work.MarkerOrigin = now + int64(120*time.Second)
	work, err := savePrivateJSON(d.p.raw("workload.json"), d.work.work)
	if err != nil {
		return err
	}
	topology, err := savePrivateJSON(d.p.raw("topology.json"), struct {
		Work           controllerWorkload `json:"workload"`
		Source, Target controllerOverlay
		Dependencies   []controllerDependency `json:"dependencies"`
		AppImage       string                 `json:"observed_application_image_digest"`
	}{d.h.plan.Work, d.h.plan.Source, d.h.plan.Target, d.h.dependencies(d.id), d.apps.imageDigest})
	if err != nil {
		return err
	}
	policy, err := savePrivateJSON(d.p.raw("policy.json"), map[string]int{
		"rto_seconds": 600, "last_terminal_seconds": 480, "marker_spacing_seconds": 5, "approval_minutes": 20})
	if err != nil {
		return err
	}
	if _, err := savePrivateJSON(d.p.raw("marker-plan.json"), d.work.markers); err != nil {
		return err
	}
	markers, err := d.work.markers.Digest()
	if err != nil {
		return err
	}
	d.ledger, err = restoreledger.Create(d.p.raw("ack.jsonl"), restoreledger.Header{
		Schema: restoreledger.Schema, Campaign: d.h.campaign, Drill: d.id, RunID: run, RunAttempt: 1, BootID: d.boot,
		Source: d.db.source.Endpoint, Target: d.db.target.Endpoint, Revision: d.revision,
		WorkloadSHA256: work, TopologySHA256: topology, PolicySHA256: policy, MarkersSHA256: markers})
	return err
}

func (d *liveDrill) prepare(ctx context.Context) error {
	if err := d.sampler.take(ctx, "backup"); err != nil {
		return err
	}
	raw, err := d.mediaCall(ctx, "prepare")
	if err != nil {
		return err
	}
	if err := evidenceJSON(raw, &d.backup); err != nil {
		return err
	}
	if err := secureWrite(d.p.raw("backup.json"), raw, 0o600); err != nil {
		return err
	}
	manifest, err := evidenceFile(d.p.raw("media/base"), "backup_manifest")
	if err != nil || restoreledger.SHA256(manifest) != d.backup.ManifestSHA256 {
		return errors.New("base backup manifest not bound to its physical receipt")
	}
	if err := secureWrite(d.p.raw("backup-manifest.json"), manifest, 0o600); err != nil {
		return err
	}
	f := faultFiles{PGBin: d.p.bin, Source: "j7-source", Archive: d.p.raw("media/archive"), BackupDir: d.p.raw("media/base"),
		Backup: d.p.raw("backup.json"), BackupSHA256: restoreledger.SHA256(raw), Ledger: d.p.raw("ack.jsonl"), Plan: d.p.raw("marker-plan.json"),
		OriginNS: d.work.work.MarkerOrigin, Binding: d.p.raw("source-binding.json"), Cut: d.p.raw("cut.json"), Outage: d.p.raw("source-loss.json"),
		EffectID: d.work.submission("e1").ID, Resources: faultConfig{Container: d.db.ids[sourceRole],
			SourceVolume: d.db.source.Volume, TargetVolume: d.db.target.Volume}}
	if err := writeControllerJSON(d.media.config("fault"), f); err != nil {
		return err
	}
	return writeControllerJSON(d.media.config("kafka"), kafkaConfig{
		Ledger: d.p.raw("ack.jsonl"), Image: d.h.plan.Lock.Kafka, SourceID: d.db.ids[sourceKafkaRole],
		Source:        kafkaPlacement{Volume: d.db.broker.Volume, Alias: d.db.broker.Endpoint, Port: d.db.broker.HostPort},
		Target:        kafkaPlacement{Volume: d.db.fresh.Volume, Alias: d.db.fresh.Endpoint, Port: d.db.fresh.HostPort},
		SourceReceipt: d.p.raw("kafka-source.json"), RestoreConfig: d.media.config("restore"),
		TargetConsumers: d.p.raw("consumers.json"), FreshReceipt: d.p.raw("kafka-fresh.json")})
}

func (d *liveDrill) lose(ctx context.Context) error {
	d.apps.stopForwards()
	// The source application clients remain frozen if this stage fails.
	d.depPhase.Store(2)
	if _, err := d.mediaCall(ctx, "source-loss"); err != nil {
		return err
	}
	sha, err := readFaultJSON(d.p.raw("source-loss.json"), &d.loss)
	if err != nil {
		return err
	}
	if err := d.sampler.take(ctx, "outage"); err != nil {
		return err
	}
	if _, err := d.h.kubectl(ctx, "-n", d.apps.ns, "delete", "pod/source-api", "--grace-period=1", "--wait=true", "--timeout=6s"); err != nil {
		return errors.New("source request listener was not retired before restoring the target")
	}
	return writeControllerJSON(d.media.config("restore"), restoreConfig{
		FaultConfig: d.media.config("fault"), LossSHA: sha, Cohorts: d.p.cfg("cohorts.json"), SourceProof: d.p.raw("source-proof.json"),
		SinkDSNFile: d.db.sinkExportPath, SinkKey: d.work.work.SinkKey, Database: engineDatabase, Evidence: d.p.restore})
}
func (d *liveDrill) restore(ctx context.Context) error {
	if _, err := d.mediaCall(ctx, "restore"); err != nil {
		return err
	}
	sha, err := readFaultJSON(filepath.Join(d.p.restore, "restore.json"), &d.restored)
	if err != nil {
		return err
	}
	d.media.Restore = sha
	d.depPhase.Store(3)
	if err := d.sampler.take(ctx, "replay"); err != nil {
		return err
	}
	return d.copyEvidence(d.p.restore, map[string]string{
		"restore.json": "restore.json", "engine-pre-write.json": "engine-pre-write.json", "sink-pre-write.json": "sink-pre-write.json"})
}
func (d *liveDrill) newBroker(ctx context.Context) error {
	consumer := renderedTargetConsumers(d.h, d.db.fresh.Endpoint+":19092")
	if err := verifyGenerationRender(consumer, d.db.fresh.Endpoint+":19092"); err != nil {
		return err
	}
	if err := writeControllerJSON(d.p.raw("consumers.json"), consumer); err != nil {
		return err
	}
	if _, err := d.mediaCall(ctx, "fresh-kafka"); err != nil {
		return err
	}
	sha, err := readFaultJSON(d.p.raw("kafka-fresh.json"), &d.fresh)
	if err != nil {
		return err
	}
	d.db.ids[targetKafkaRole], d.media.Fresh = d.fresh.Target.ID, sha
	return nil
}
func (d *liveDrill) openTarget(ctx context.Context) error {
	var w bootstrapWitness
	if _, err := readFaultJSON(filepath.Join(d.h.private, "bootstrap.json"), &w); err != nil {
		return err
	}
	d.depPhase.Store(4)
	id, err := d.h.restartVerifiedTarget(ctx, d.id, d.p.cfg(targetSecurityDir), d.db.target, d.restored, w.Images[d.db.target.Image])
	if err != nil {
		return err
	}
	d.db.ids[targetRole] = id
	observer := d.db.observer.local(d.db.target, engineDatabase, d.db.ca, true)
	conn, err := waitPG(ctx, observer)
	if err != nil {
		return err
	}
	migrationsErr := checkEngineMigrations(ctx, conn, 23)
	_ = conn.Close(ctx)
	if migrationsErr != nil {
		return migrationsErr
	}
	d.depPhase.Store(5)
	return nil
}

func (d *liveDrill) rebind(ctx context.Context) error {
	if err := d.apps.startSink(ctx, true); err != nil {
		return err
	}
	if _, err := d.mediaCall(ctx, "orphan-rebind"); err != nil {
		return err
	}
	if err := d.copyEvidence(d.p.orphan, map[string]string{"reader-rebind.json": "reader-rebind.json"}); err != nil {
		return err
	}
	var r orphanObservation
	sha, err := readFaultJSON(d.p.raw("reader-rebind.json"), &r)
	if err != nil {
		return err
	}
	h := d.ledger.Header()
	if err := d.ledger.RecordWitness(restoreledger.Witness{Kind: restoreledger.AuthorityChange, Phase: orphanRebindPhase,
		Authority: h.Target, Former: h.Source, Timeline: r.DB.Timeline, StartedNS: r.DB.StartedNS, CompletedNS: r.CompletedNS, EvidenceSHA: sha}); err != nil {
		return err
	}
	live, err := observeReopenedEndpoint(ctx, d.now, liveEndpointProbes(d.db.source.HostPort,
		d.db.sourceWriter.local(d.db.target, engineDatabase, d.db.ca, false), d.db.observer.local(d.db.target, engineDatabase, d.db.ca, true)))
	if err != nil {
		return err
	}
	if err := recordControllerEndpoint(ctx, d.p.evidence, d.ledger, d.media.Restore, d.media.Fresh, live); err != nil {
		return err
	}
	d.media.OpenNS = live.OpenNS
	return nil
}
func (d *liveDrill) quiet(ctx context.Context, phase string, q *quietObservation) error {
	if phase == "start" {
		c := quietConfig{EngineDSN: d.db.targetObserverPath, SinkDSN: d.db.sinkExportPath, Namespace: d.apps.ns, Ledger: d.p.raw("ack.jsonl"),
			Restore: d.p.raw("restore.json"), FreshKafka: d.p.raw("kafka-fresh.json"), Evidence: d.p.evidence}
		if err := writeControllerJSON(d.media.config("quiet"), c); err != nil {
			return err
		}
	}
	if _, err := d.mediaCall(ctx, "quiet-"+phase); err != nil {
		return err
	}
	sha, err := readFaultJSON(d.p.raw("quiet-"+phase+".json"), q)
	if err != nil {
		return err
	}
	return d.witness("quiet-"+phase, q.Authority, q.Timeline, q.StartedNS, q.CompletedNS, sha)
}

func (d *liveDrill) copyEvidence(base string, files map[string]string) error {
	for source, target := range files {
		raw, err := evidenceFile(base, source)
		if err != nil {
			return err
		}
		if err := saveFixtureBytes(d.p.raw(target), raw); err != nil {
			return err
		}
	}
	return nil
}
func saveFixtureBytes(path string, raw []byte) error {
	if err := secureWrite(path, raw, 0o600); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}
func (d *liveDrill) witness(phase, authority string, timeline uint32, start, end int64, sha string) error {
	return d.ledger.RecordWitness(restoreledger.Witness{Kind: restoreledger.StateProbe, Phase: phase, Authority: authority, Timeline: timeline,
		EvidenceSHA: sha, StartedNS: start, CompletedNS: end})
}
func (d *liveDrill) probe(store *state.Store) (*j7fences.Probe, error) {
	return j7fences.New(store, d.ledger, func(name string, data []byte) error { return saveFixtureBytes(d.p.raw(name), data) })
}
func (d *liveDrill) sourceStore(ctx context.Context) (*state.Store, error) {
	return state.NewK6RuntimeFromURL(ctx, d.db.sourceWriter.local(d.db.source, engineDatabase, d.db.ca, false))
}
func (d *liveDrill) targetStore(ctx context.Context) (*state.Store, error) {
	return state.NewK6RuntimeFromURL(ctx, d.db.targetWriter.local(d.db.target, engineDatabase, d.db.ca, false))
}
func (d *liveDrill) submit(ctx context.Context, role string, target bool) (restoreledger.Exchange, error) {
	authority := d.ledger.Header().Source
	if target {
		authority = d.ledger.Header().Target
	}
	return submitFixture(ctx, d.ledger, d.apps.api, authority, d.apps.ns, d.work.submission(role), d.work.requests[role])
}

// Source and target runtime state is read from PostgreSQL. Poll bounded
// transitions instead of sleeping through a scheduler or broker gate.
func pollFixture(ctx context.Context, timeout time.Duration, check func(context.Context) (bool, error)) error {
	probe, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		ok, err := check(probe)
		if ok || err != nil {
			return err
		}
		select {
		case <-probe.Done():
			return probe.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}
func (d *liveDrill) trace(ctx context.Context, store *state.Store) (invariants.Trace, error) {
	return invariants.LoadNamespace(ctx, store, d.apps.ns)
}
func (d *liveDrill) waitForDrain(ctx context.Context) error {
	now, err := d.now()
	if err != nil {
		return err
	}
	bounded, cancel, err := phaseDeadline(ctx, now, d.loss.T0NS, 480*time.Second)
	if err != nil {
		return err
	}
	defer cancel()
	store, err := d.targetStore(bounded)
	if err != nil {
		return err
	}
	defer store.Close()
	return pollFixture(bounded, 480*time.Second, func(ctx context.Context) (bool, error) {
		var total, success, pending int
		err := store.Pool().QueryRow(ctx, `SELECT
			(SELECT count(*) FROM engine.workflow_executions WHERE namespace=$1),
			(SELECT count(*) FROM engine.workflow_executions WHERE namespace=$1 AND state='SUCCEEDED'),
			(SELECT count(*) FROM engine.outbox o JOIN engine.workflow_executions w ON w.workflow_id=o.workflow_id
			WHERE w.namespace=$1 AND o.publish_state<>'PUBLISHED')`, d.apps.ns).Scan(&total, &success, &pending)
		if err != nil {
			return false, err
		}
		if total > 21 {
			return false, errors.New("target admitted an unexpected workflow")
		}
		if total != 21 || success != 21 || pending != 0 {
			return false, nil
		}
		return d.brokerDrained(ctx, store)
	})
}
