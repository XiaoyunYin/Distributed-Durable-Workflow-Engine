package main

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/effects/k6sink"
	"durable-agent-execution-engine/internal/j7markers"
	"durable-agent-execution-engine/internal/k6receipt"
	"durable-agent-execution-engine/internal/restoreledger"
)

const restoreWindow = 180 * time.Second

type restoreConfig struct {
	FaultConfig string `json:"fault_config"`
	LossSHA     string `json:"source_loss_sha256"`
	Cohorts     string `json:"pre_cut_cohorts"`
	SourceProof string `json:"source_effect_proof"`
	SinkDSNFile string `json:"sink_export_dsn_file"`
	SinkKey     string `json:"sink_receipt_public_key_hex"`
	Database    string `json:"engine_database"`
	Evidence    string `json:"new_evidence_directory"`
}

type restoreInputs struct {
	config    restoreConfig
	configSHA string
	fault     faultFiles
	backup    backupReceipt
	loss      outageReceipt
	binding   sourceBinding
	cut       cutReceipt
	markers   []markerExpected
	header    restoreledger.Header
	cohorts   cohortsAtCut
	proof     k6receipt.Proof
}

type restoreReceipt struct {
	Schema          string          `json:"schema"`
	BootID          string          `json:"boot_id"`
	Source          string          `json:"source_instance"`
	Target          string          `json:"target_instance"`
	SourceTimeline  uint32          `json:"source_timeline"`
	LossSHA         string          `json:"source_loss_sha256"`
	ConfigSHA       string          `json:"restore_config_sha256"`
	StartedNS       int64           `json:"started_ns"`
	Volume          sourceVolume    `json:"target_volume"`
	VolumeCreatedNS int64           `json:"target_volume_created_ns,omitempty"`
	VolumeBytes     int64           `json:"target_data_directory_bytes,omitempty"`
	StagingID       string          `json:"staging_container_id,omitempty"`
	TargetID        string          `json:"target_container_id,omitempty"`
	Endpoint        string          `json:"observer_endpoint,omitempty"`
	WAL             walProof        `json:"wal_verification"`
	CopyLog         string          `json:"backup_copy_verification,omitempty"`
	ReplayLogs      string          `json:"replay_logs,omitempty"`
	FirstReplayNS   int64           `json:"first_replay_observed_ns,omitempty"`
	ReadyObservedNS int64           `json:"database_ready_observed_ns,omitempty"`
	History         string          `json:"target_timeline_history,omitempty"`
	RecoveredNS     int64           `json:"replay_verified_ns,omitempty"`
	State           targetState     `json:"target_state"`
	SnapshotSHA     string          `json:"pre_write_engine_snapshot_sha256,omitempty"`
	SinkSHA         string          `json:"pre_write_sink_snapshot_sha256,omitempty"`
	Verdict         prewriteVerdict `json:"pre_write_verdict"`
	CompletedNS     int64           `json:"completed_ns,omitempty"`
}

func loadRestoreInputs(configPath string, boot string) (restoreInputs, error) {
	var in restoreInputs
	c := &in.config
	var err error
	in.configSHA, err = readFaultJSON(configPath, c)
	if err != nil {
		return in, err
	}
	paths := []string{configPath, c.FaultConfig, c.Cohorts, c.SourceProof, c.SinkDSNFile, c.Evidence}
	seen := make(map[string]bool)
	for _, name := range paths {
		if !filepath.IsAbs(name) || filepath.Clean(name) != name || strings.ContainsAny(name, ",\n\r") || seen[name] {
			return in, errors.New("restore input paths must be distinct, absolute and canonical")
		}
		seen[name] = true
		parent, err := filepath.EvalSymlinks(filepath.Dir(name))
		if err != nil || parent != filepath.Dir(name) {
			return in, errors.New("restore paths must have canonical parents")
		}
	}
	key, err := hex.DecodeString(c.SinkKey)
	if err != nil || len(key) != ed25519.PublicKeySize || !fixtureName.MatchString(c.Database) || !restoreledger.IsSHA256(c.LossSHA) {
		return in, errors.New("restore requires a fixture database, sink key and source-loss digest")
	}
	faultSHA, err := readFaultJSON(c.FaultConfig, &in.fault)
	if err != nil {
		return in, err
	}
	f := in.fault
	if err := f.validate(); err != nil {
		return in, err
	}
	if strings.ContainsAny(f.Archive+f.BackupDir, ",\n\r") || within(f.Archive, c.Evidence) || within(f.BackupDir, c.Evidence) {
		return in, errors.New("restore evidence must be separate from both mounted media directories")
	}
	backupSHA, err := readFaultJSON(f.Backup, &in.backup)
	if err != nil {
		return in, err
	}
	bindingSHA, err := readFaultJSON(f.Binding, &in.binding)
	if err != nil {
		return in, err
	}
	cutSHA, err := readFaultJSON(f.Cut, &in.cut)
	if err != nil {
		return in, err
	}
	lossSHA, err := readFaultJSON(f.Outage, &in.loss)
	if err != nil {
		return in, err
	}
	if err := in.backup.Source.validate(); err != nil {
		return in, err
	}
	if lossSHA != c.LossSHA || in.loss.Schema != "j7.source-loss.v1" || in.loss.BootID != boot ||
		in.binding.BootID != boot || in.binding.ConfigSHA256 != faultSHA || in.binding.Run != f.Resources ||
		!dockerID.MatchString(strings.TrimPrefix(in.binding.Image, "sha256:")) ||
		!strings.HasPrefix(in.binding.Image, "sha256:") || in.binding.Schema != "j7.source-binding.v1" ||
		backupSHA != f.BackupSHA256 || in.binding.BackupSHA256 != backupSHA ||
		in.loss.BackupSHA256 != backupSHA || in.loss.BindingSHA256 != bindingSHA || in.loss.CutSHA256 != cutSHA ||
		within(in.binding.Volume.Mountpoint, c.Evidence) {
		return in, errors.New("source loss, binding and backup do not match the sealed restore input")
	}
	if err := verifyLossProof(in.loss, in.binding); err != nil {
		return in, err
	}
	report, err := readLossLedger(f.Ledger, boot, in.loss.LedgerSHA256)
	if err != nil {
		return in, err
	}
	in.header = report.Header
	var plan j7markers.Plan
	if _, err := readFaultJSON(f.Plan, &plan); err != nil {
		return in, err
	}
	ack, err := j7markers.BoundaryAck(plan, f.OriginNS, report, true)
	if err != nil {
		return in, err
	}
	if ack != in.loss.M15AckNS || report.Digest != in.loss.LedgerSHA256 ||
		report.Header.Source != in.loss.Source || in.binding.Authority != report.Header.Source ||
		!matchesLabels(in.binding.Labels, faultLabels(report.Header)) {
		return in, errors.New("live ledger does not match the confirmed source outage")
	}
	if _, err := verifyOutageCut(in.cut, cutSHA, backupSHA, in.backup, plan, report); err != nil {
		return in, err
	}
	effect, err := phaseWitness(report, "post-cut-effect", in.cut.CompletedNS, in.loss.KillNS)
	if err != nil || effect != in.loss.EffectProof {
		return in, errors.New("source loss receipt differs from the retained effect witness")
	}
	freeze, err := phaseWitness(report, "writers-frozen", in.cut.CompletedNS, in.loss.KillNS)
	if err != nil || freeze != in.loss.WriterFreeze {
		return in, errors.New("source loss receipt differs from the writer-freeze witness")
	}
	in.markers = ledgerMarkers(plan, report)
	cohortSHA, err := readFaultJSON(c.Cohorts, &in.cohorts)
	if err != nil {
		return in, err
	}
	proofSHA, err := readFaultJSON(c.SourceProof, &in.proof)
	if err != nil {
		return in, err
	}
	e := in.cohorts.E1
	claim := k6receipt.Claim{NodeID: e.Attempt.NodeID, Iteration: e.Attempt.Iteration, Attempt: e.Attempt.Attempt, Fence: e.Fence}
	if cohortSHA != in.cut.Cohorts.EvidenceSHA || proofSHA != in.loss.EffectProof.EvidenceSHA ||
		in.cohorts.Schema != "j7.cohorts-at-cut.v1" || f.EffectID != e.Attempt.WorkflowID ||
		in.proof.Operation != "apply" || !k6receipt.Verify(ed25519.PublicKey(key), claim, in.proof) {
		return in, errors.New("held source cohorts or original signed effect receipt do not match the ledger")
	}
	return in, nil
}

// The journal is append-only. Verify the exact complete prefix seen by the
// loss command even if the controller has since added observation rows.
func readLossLedger(path, boot, digest string) (restoreledger.Report, error) {
	if !restoreledger.IsSHA256(digest) {
		return restoreledger.Report{}, errors.New("source-loss ledger digest is invalid")
	}
	file, err := openRegular(path, os.O_RDONLY)
	if err != nil {
		return restoreledger.Report{}, err
	}
	defer file.Close()
	reader := bufio.NewReaderSize(file, 256<<10)
	hash := sha256.New()
	var size int64
	for {
		raw, err := reader.ReadSlice('\n')
		if err != nil {
			return restoreledger.Report{}, errors.New("source-loss ledger prefix is missing or incomplete")
		}
		size += int64(len(raw))
		_, _ = hash.Write(raw)
		if hex.EncodeToString(hash.Sum(nil)) != digest {
			continue
		}
		report, err := restoreledger.Read(io.NewSectionReader(file, 0, size), digest, true)
		if err != nil {
			return restoreledger.Report{}, err
		}
		if report.Sealed || report.Header.BootID != boot {
			return restoreledger.Report{}, errors.New("source-loss ledger prefix is from another boot or phase")
		}
		return report, nil
	}
}

func verifyLossProof(r outageReceipt, b sourceBinding) error {
	const ns = int64(time.Second)
	if r.M15AckNS <= 0 || r.KillNS < r.M15AckNS || r.KillNS-r.M15AckNS >= 2*ns ||
		r.KillAckNS < r.KillNS || r.LastSuccessNS < r.M15AckNS ||
		r.SQLDown.LastSuccessNS != r.LastSuccessNS || r.SQLDown.FirstFailedStartNS < r.KillAckNS ||
		r.SQLDown.FirstFailedStartNS < r.LastSuccessNS || r.SQLDown.FirstFailedNS < r.SQLDown.FirstFailedStartNS ||
		r.SQLDown.FirstFailedNS-r.LastSuccessNS > 3*ns || r.SQLDown.NoResponseNS < r.SQLDown.FirstFailedNS ||
		r.SQLDown.PsqlExitCode != 2 || r.SQLDown.PgIsReadyExitCode != 2 ||
		r.Exit.ObservedNS < r.KillAckNS || !killedSource(r.Exit.State, b.StartedAt) ||
		r.T0NS != max(r.Exit.ObservedNS, r.SQLDown.NoResponseNS) || r.T0NS-r.KillNS > 2*ns ||
		r.ContainerGone < r.T0NS || r.VolumeRemoved < r.ContainerGone || r.VolumeAbsent < r.VolumeRemoved ||
		r.VolumeAbsent-r.T0NS > 30*ns || r.ArchiveSealNS < r.VolumeAbsent || len(r.Inventory) < 3 {
		return errors.New("source deletion and confirmed outage proof are incomplete or late")
	}
	return nil
}

func runRestore(ctx context.Context, args []string, out, diagnostics io.Writer) error {
	flags := flag.NewFlagSet("restore", flag.ContinueOnError)
	flags.SetOutput(diagnostics)
	config := flags.String("config", "", "absolute path of the private isolated restore configuration")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || !filepath.IsAbs(*config) {
		return errors.New("restore requires --config with an absolute path")
	}
	now, boot, err := restoreledger.HostClock()
	if err != nil {
		return err
	}
	in, err := loadRestoreInputs(*config, boot)
	if err != nil {
		return err
	}
	binary, err := os.Executable()
	if err != nil {
		return err
	}
	binary, err = filepath.EvalSymlinks(binary)
	if err != nil {
		return err
	}
	info, err := os.Stat(binary)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&5 != 5 ||
		strings.ContainsAny(binary, ",\r\n") || within(in.binding.Volume.Mountpoint, binary) {
		return errors.New("restore requires the private absolute Linux helper binary readable and executable in the container")
	}
	if err := os.Mkdir(in.config.Evidence, 0o700); err != nil {
		return err
	}
	if err := syncDirectory(filepath.Dir(in.config.Evidence)); err != nil {
		return err
	}
	source := mediaSource{pgBin: in.fault.PGBin, archive: in.fault.Archive, run: execTool, now: now}
	recovery := restoreDocker{localDocker: localDocker{run: execTool}, binary: binary,
		logs: localDocker{run: func(ctx context.Context, name string, args ...string) ([]byte, error) {
			return exec.CommandContext(ctx, name, args...).CombinedOutput()
		}}}
	_, sha, err := faultReceipt(filepath.Join(in.config.Evidence, "restore.json"), func() (restoreReceipt, error) {
		return restoreIsolated(ctx, in, source, recovery)
	})
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(map[string]string{"restore_receipt_sha256": sha})
}

func restoreIsolated(ctx context.Context, in restoreInputs, source mediaSource, d restoreDocker) (restoreReceipt, error) {
	r := restoreReceipt{Schema: "j7.isolated-restore.v1", BootID: in.header.BootID, Source: in.header.Source,
		Target: in.header.Target, SourceTimeline: in.backup.Source.Timeline, LossSHA: in.config.LossSHA, ConfigSHA: in.configSHA}
	var err error
	r.StartedNS, err = source.now()
	if err != nil {
		return r, err
	}
	if r.StartedNS < in.loss.ArchiveSealNS {
		return r, errors.New("isolated restore precedes sealed source-volume loss")
	}
	ctx, cancel, err := phaseDeadline(ctx, r.StartedNS, in.loss.T0NS, restoreWindow)
	if err != nil {
		return r, err
	}
	defer cancel()
	r.WAL, err = verifyReplayMedia(ctx, source, in.backup, in.cut, in.loss, in.fault.BackupDir)
	if err != nil {
		return r, err
	}
	manifest := filepath.Join(in.config.Evidence, "replay-files.json")
	m := replayManifest{Files: in.loss.Inventory, Timeline: in.backup.Source.Timeline,
		Database: in.config.Database, Backup: in.backup.ManifestSHA256}
	if err := writeReplayManifest(manifest, m); err != nil {
		return r, err
	}
	err = d.startIsolated(ctx, in, manifest, source.now, &r)
	if err != nil {
		return r, err
	}
	err = d.awaitReplay(ctx, in.cut.PreSwitch.Timeline, in.config.Database, source.now, &r)
	if err != nil {
		return r, err
	}
	raw, err := d.command(ctx, "container", "exec", "--user=postgres", r.TargetID, targetBinary, "observe-target",
		in.config.Database, in.markers[0].Namespace)
	if err != nil {
		return r, fmt.Errorf("read isolated PostgreSQL snapshot: %w", err)
	}
	var observed observedTarget
	if err := json.Unmarshal(raw, &observed); err != nil {
		return r, err
	}
	r.State = observed.Target
	r.SnapshotSHA, err = savePrivateJSON(filepath.Join(in.config.Evidence, "engine-pre-write.json"), observed.Trace)
	if err != nil {
		return r, err
	}
	historyName := fmt.Sprintf("%08X.history", r.State.Timeline)
	raw, err = d.command(ctx, "container", "exec", "--user=postgres", r.TargetID, "cat", targetData+"/pg_wal/"+historyName)
	if err != nil {
		return r, fmt.Errorf("read recovered timeline history: %w", err)
	}
	r.History = string(raw)
	if err := r.verifyReplay(in.backup.Source); err != nil {
		return r, err
	}
	r.RecoveredNS, err = source.now()
	if err != nil {
		return r, err
	}
	raw, err = d.command(ctx, "container", "exec", "--user=postgres", r.TargetID, "du", "--summarize", "--bytes", targetData)
	if err != nil {
		return r, fmt.Errorf("measure isolated target storage: %w", err)
	}
	size, dir, found := strings.Cut(strings.TrimSpace(string(raw)), "\t")
	r.VolumeBytes, err = strconv.ParseInt(size, 10, 64)
	if !found || err != nil || dir != targetData || r.VolumeBytes < r.State.DatabaseBytes+r.State.WALBytes {
		return r, errors.New("isolated target cluster size evidence is invalid")
	}
	sink, err := exportRetainedSink(ctx, in.config.SinkDSNFile)
	if err != nil {
		return r, err
	}
	r.SinkSHA, err = savePrivateJSON(filepath.Join(in.config.Evidence, "sink-pre-write.json"), sink)
	if err != nil {
		return r, err
	}
	r.Verdict, err = verifyPrewrite(observed.Trace, in.markers, in.cohorts, in.proof, sink)
	if err != nil {
		return r, err
	}
	if err := d.verifyIsolated(ctx, in, manifest, &r); err != nil {
		return r, err
	}
	end, err := parseLSN(r.WAL.End)
	if err != nil || end < walSegmentBytes {
		return r, errors.New("invalid WAL range after replay")
	}
	files, err := inventory(source.archive, in.backup, end/walSegmentBytes-1)
	if err != nil || !slices.Equal(files, in.loss.Inventory) {
		return r, errors.New("archive changed during isolated replay")
	}
	r.CompletedNS, err = source.now()
	if err == nil && (r.CompletedNS-in.loss.T0NS > int64(restoreWindow) || ctx.Err() != nil) {
		err = errors.New("isolated restore and pre-write snapshot missed 180-second gate")
	}
	return r, err
}

func savePrivateJSON(path string, value any) (string, error) {
	_, digest, err := faultReceipt(path, func() (any, error) { return value, nil })
	return digest, err
}

func writeReplayManifest(path string, m replayManifest) error {
	if _, err := savePrivateJSON(path, m); err != nil {
		return err
	}
	// Only names and public digests are in this file. The unprivileged
	// PostgreSQL process needs to read the single-file bind.
	return os.Chmod(path, 0o444)
}

func exportRetainedSink(ctx context.Context, path string) (k6sink.Snapshot, error) {
	dsn, err := readPrivateDSN(path)
	if err != nil {
		return k6sink.Snapshot{}, err
	}
	s, err := k6sink.Export(ctx, dsn)
	if err != nil {
		return s, errors.New("retained sink read-only export failed")
	}
	return s, nil
}

func readPrivateDSN(path string) (string, error) {
	file, err := openRegular(path, os.O_RDONLY)
	if err != nil {
		return "", err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || info.Mode().Perm()&0o077 != 0 || info.Size() > 4096 {
		return "", errors.New("database credentials must be a private fixture file")
	}
	raw, err := io.ReadAll(io.LimitReader(file, 4097))
	if err != nil || len(raw) > 4096 || strings.TrimSpace(string(raw)) == "" {
		return "", errors.New("read private fixture database connection")
	}
	return strings.TrimSpace(string(raw)), nil
}

func (r restoreReceipt) verifyReplay(s sourceState) error {
	t := r.State
	replayed, replayErr := parseLSN(t.ReplayLSN)
	end, endErr := parseLSN(r.WAL.End)
	_, selected, err := replayLogStatus(r.ReplayLogs, s.Timeline, r.WAL)
	if err != nil {
		return err
	}
	if t.SystemID != s.SystemID || t.Version != serverVersion || t.Timeline <= s.Timeline ||
		(t.PreviousTimeline != s.Timeline && t.PreviousTimeline != t.Timeline) || t.DataDirectory != targetData ||
		replayErr != nil || endErr != nil || replayed != end || !t.Recovered || !t.SocketOnly || !t.ReadOnly || !t.Restricted ||
		t.DatabaseBytes <= 0 || t.DatabaseBytes > maxDatabaseSize || t.ClusterBytes < t.DatabaseBytes || t.WALBytes < walSegmentBytes ||
		r.FirstReplayNS <= 0 || r.ReadyObservedNS < r.FirstReplayNS || selected != t.Timeline {
		return errors.New("target identity, isolation or replay end does not match the sealed source")
	}
	return matchTimelineHistory(r.History, s.Timeline, end)
}

func matchTimelineHistory(history string, parent uint32, end uint64) error {
	var last []string
	for _, line := range strings.Split(history, "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "#") {
			last = strings.Fields(line)
		}
	}
	if len(last) < 2 {
		return errors.New("missing target timeline history")
	}
	p, parentErr := strconv.ParseUint(last[0], 10, 32)
	fork, forkErr := parseLSN(last[1])
	if parentErr != nil || forkErr != nil || p != uint64(parent) || end != fork {
		return errors.New("target timeline history has unexpected parent or fork LSN")
	}
	return nil
}
