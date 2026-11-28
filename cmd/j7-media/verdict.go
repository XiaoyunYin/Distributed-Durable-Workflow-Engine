package main

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"time"

	"durable-agent-execution-engine/internal/effects/k6sink"
	"durable-agent-execution-engine/internal/invariants"
	"durable-agent-execution-engine/internal/j7markers"
	"durable-agent-execution-engine/internal/k6receipt"
	"durable-agent-execution-engine/internal/restoreledger"
)

type plannedSubmission struct {
	Role, ID, Key, RequestID, RequestSHA, Definition string
	InitialNode                                      string
	Input                                            json.RawMessage
}

type drillWorkload struct {
	Schema       string              `json:"schema"`
	Clock        orphanClock         `json:"clock"`
	MarkerOrigin int64               `json:"marker_origin_ns"`
	SinkKey      string              `json:"sink_public_key"`
	RelayPeer    string              `json:"effect_relay_peer"`
	SinkPeer     string              `json:"effect_sink_peer"`
	Fixture      j7markers.Fixture   `json:"fixture"`
	NonMarkers   []plannedSubmission `json:"non_markers"`
}

type readyEndpoint struct {
	Schema          string `json:"schema"`
	BootID          string `json:"boot_id"`
	Authority       string `json:"authority"`
	RestoreSHA      string `json:"restore_sha256"`
	KafkaSHA        string `json:"fresh_kafka_sha256"`
	OpenNS          int64  `json:"opened_ns"`
	OldDeniedNS     int64  `json:"old_endpoint_denied_ns"`
	ForeignDeniedNS int64  `json:"foreign_endpoint_denied_ns"`
	SystemID        uint64 `json:"system_identifier"`
	Timeline        uint32 `json:"timeline"`
}

type quietObservation struct {
	Schema      string        `json:"schema"`
	BootID      string        `json:"boot_id"`
	Authority   string        `json:"authority"`
	SystemID    uint64        `json:"system_identifier"`
	Timeline    uint32        `json:"timeline"`
	StartedNS   int64         `json:"started_ns"`
	CompletedNS int64         `json:"completed_ns"`
	DBTime      time.Time     `json:"database_time"`
	EngineSHA   string        `json:"engine_sha256"`
	SinkSHA     string        `json:"sink_sha256"`
	Database    int64         `json:"database_bytes"`
	Cluster     int64         `json:"cluster_bytes"`
	WAL         int64         `json:"wal_bytes"`
	SinkBytes   int64         `json:"sink_database_bytes"`
	Broker      kafkaSnapshot `json:"broker"`
}

type markerRPO struct {
	Acknowledged    int      `json:"acknowledged"`
	Recovered       int      `json:"recovered_prefix"`
	Missing         []string `json:"missing_ids"`
	AckLossNS       *int64   `json:"acknowledged_loss_ns"`
	CommitMinNS     *int64   `json:"commit_difference_min_ns"`
	CommitMaxNS     *int64   `json:"commit_difference_max_ns"`
	MaxSendJitterNS int64    `json:"max_send_jitter_ns"`
	MaxResponseNS   int64    `json:"max_response_ns"`
	ArchiveEnd      string   `json:"archive_end_lsn"`
	SourceFlush     string   `json:"source_flush_lsn"`
	WALLossBytes    uint64   `json:"wal_bytes_past_archive"`
}

type drillVerdict struct {
	Schema     string           `json:"schema"`
	Status     string           `json:"status"`
	Reason     string           `json:"reason,omitempty"`
	Revision   string           `json:"revision"`
	Campaign   string           `json:"campaign"`
	Drill      string           `json:"drill"`
	CheckerNS  int64            `json:"checker_ns,omitempty"`
	RTONS      int64            `json:"rto_ns,omitempty"`
	RPO        markerRPO        `json:"rpo"`
	PhaseNS    map[string]int64 `json:"phase_ns"`
	Bytes      map[string]int64 `json:"bytes"`
	Retained   []string         `json:"retained_workflows"`
	Fresh      []string         `json:"fresh_workflows"`
	Incomplete []string         `json:"incomplete_workflows"`
	Unresolved []string         `json:"unresolved_outcomes"`
	Ambiguous  []string         `json:"ambiguous_requests"`
}

type drillInputs struct {
	b                evidenceBundle
	ledger           restoreledger.Report
	work             drillWorkload
	plan             j7markers.Plan
	backup           backupReceipt
	binding          sourceBinding
	cut              cutReceipt
	loss             outageReceipt
	restore          restoreReceipt
	cohorts          cohortsAtCut
	proof            k6receipt.Proof
	sinkBefore       k6sink.Snapshot
	oldKafka         sourceKafkaReceipt
	freshKafka       freshKafkaReceipt
	watch            redispatchWatch
	redispatch       redispatchReceipt
	endpoint         readyEndpoint
	start, end       quietObservation
	pre, first, last invariants.Trace
}

func readDrill(b evidenceBundle) (drillInputs, error) {
	d := drillInputs{b: b}
	raw, err := b.read("ack.jsonl")
	if err != nil {
		return d, err
	}
	d.ledger, err = restoreledger.Read(bytes.NewReader(raw), b.original("ack.jsonl"), false)
	if err != nil {
		return d, err
	}
	h := d.ledger.Header
	if h.Revision != b.index.Revision || h.RunID != b.index.Run || h.RunAttempt != b.index.Attempt ||
		h.Campaign != b.index.Campaign || h.Drill != b.index.Drill ||
		h.WorkloadSHA256 != b.original("workload.json") || h.TopologySHA256 != b.original("topology.json") ||
		h.PolicySHA256 != b.original("policy.json") {
		return d, errors.New("sealed ledger and indexed candidate inputs disagree")
	}
	for _, entry := range []struct {
		name string
		out  any
	}{
		{"workload.json", &d.work}, {"marker-plan.json", &d.plan}, {"backup.json", &d.backup},
		{"source-binding.json", &d.binding}, {"cut.json", &d.cut}, {"source-loss.json", &d.loss},
		{"restore.json", &d.restore}, {"cohorts.json", &d.cohorts}, {"source-proof.json", &d.proof},
		{"sink-pre-write.json", &d.sinkBefore}, {"kafka-source.json", &d.oldKafka}, {"kafka-fresh.json", &d.freshKafka},
		{"redispatch-watch.json", &d.watch}, {"redispatch.json", &d.redispatch}, {"endpoint.json", &d.endpoint},
		{"quiet-start.json", &d.start}, {"quiet-end.json", &d.end}, {"engine-pre-write.json", &d.pre},
		{"engine-quiet-start.json", &d.first}, {"engine-final.json", &d.last},
	} {
		if err := b.decode(entry.name, entry.out); err != nil {
			return d, err
		}
	}
	if d.work.Schema != "j7.workload.v1" || d.work.Fixture != d.plan.Fixture || d.work.Clock.StartNS <= 0 ||
		d.work.Clock.StartUTC.IsZero() || d.work.MarkerOrigin < d.work.Clock.StartNS {
		return d, errors.New("workload does not bind the marker fixture and shared clock")
	}
	return d, nil
}

func (d drillInputs) checkMedia(v *drillVerdict) error {
	h, b := d.ledger.Header, d.b
	if err := j7markers.Verify(d.plan, d.work.MarkerOrigin, d.ledger); err != nil {
		return err
	}
	if err := d.backup.Source.validate(); err != nil {
		return err
	}
	if err := d.loss.FinalSource.check(d.backup.Source, ledgerMarkers(d.plan, d.ledger), 8); err != nil {
		return err
	}
	if d.backup.ManifestSHA256 != b.original("backup-manifest.json") ||
		d.backup.BackupBytes <= 0 || d.backup.BackupBytes > maxBackupBytes {
		return errors.New("base backup manifest or size differs")
	}
	r, loss := d.restore, d.loss
	if d.binding.BootID != h.BootID || d.binding.Authority != h.Source ||
		d.binding.BackupSHA256 != b.original("backup.json") ||
		loss.Schema != "j7.source-loss.v1" || loss.BootID != h.BootID || loss.Source != h.Source ||
		loss.BindingSHA256 != b.original("source-binding.json") || loss.CutSHA256 != b.original("cut.json") ||
		loss.BackupSHA256 != b.original("backup.json") || loss.EffectProof.EvidenceSHA != b.original("source-proof.json") ||
		d.cut.Cohorts.EvidenceSHA != b.original("cohorts.json") || !slices.Equal(d.cut.Inventory, loss.Inventory) ||
		r.Schema != "j7.isolated-restore.v1" || r.BootID != h.BootID || r.Source != h.Source || r.Target != h.Target ||
		r.LossSHA != b.original("source-loss.json") || r.SnapshotSHA != b.original("engine-pre-write.json") ||
		r.SinkSHA != b.original("sink-pre-write.json") || r.Volume.Name == d.binding.Volume.Name ||
		r.Volume.Mountpoint == d.binding.Volume.Mountpoint || r.TargetID == "" || r.TargetID == d.binding.Run.Container ||
		r.VolumeCreatedNS <= loss.VolumeAbsent || r.VolumeCreatedNS > r.FirstReplayNS ||
		r.FirstReplayNS > r.ReadyObservedNS || r.ReadyObservedNS > r.RecoveredNS || r.RecoveredNS > r.CompletedNS {
		return errors.New("archive, source outage and isolated restore do not form one witnessed lineage")
	}
	if err := verifyLossProof(loss, d.binding); err != nil {
		return err
	}
	ackPath := filepath.Join(b.dir, "ack.jsonl")
	atCut, err := readLossLedger(ackPath, h.BootID, d.cut.LedgerSHA256)
	if err != nil || atCut.Header != h {
		return errors.New("archive cut input ledger prefix is missing")
	}
	ack12, err := j7markers.BoundaryAck(d.plan, d.work.MarkerOrigin, atCut, false)
	if err != nil || ack12 != d.cut.AckNS {
		return errors.New("archive cut input ledger does not contain exactly M01-M12")
	}
	atLoss, err := readLossLedger(ackPath, h.BootID, loss.LedgerSHA256)
	if err != nil || atLoss.Header != h {
		return errors.New("source outage input ledger prefix is missing")
	}
	ack15, err := j7markers.BoundaryAck(d.plan, d.work.MarkerOrigin, atLoss, true)
	if err != nil || ack15 != loss.M15AckNS {
		return errors.New("outage input ledger does not contain exactly the 15 markers")
	}
	effect, effectErr := phaseWitness(atLoss, "post-cut-effect", d.cut.CompletedNS, loss.KillNS)
	freeze, freezeErr := phaseWitness(atLoss, "writers-frozen", d.cut.CompletedNS, loss.KillNS)
	if effectErr != nil || freezeErr != nil || effect != loss.EffectProof || freeze != loss.WriterFreeze {
		return errors.New("outage input ledger lacks the signed effect and writer freeze")
	}
	if _, err := verifyOutageCut(d.cut, loss.CutSHA256, loss.BackupSHA256, d.backup, d.plan, d.ledger); err != nil {
		return err
	}
	if err := r.verifyReplay(d.backup.Source); err != nil {
		return err
	}
	_, end, err := cutGeometry(d.cut.Point, d.backup.Source.Timeline, d.cut.PreSwitch.FlushLSN)
	finalFlush, flushErr := parseLSN(loss.FinalSource.FlushLSN)
	finalInsert, insertErr := parseLSN(loss.FinalSource.InsertLSN)
	if err != nil || flushErr != nil || insertErr != nil || finalFlush <= end || finalInsert < finalFlush ||
		finalInsert >= end+walSegmentBytes ||
		finalFlush-end != loss.FlushPastCut || r.WAL.NextSegment != segmentName(d.backup.Source.Timeline, end/walSegmentBytes) {
		return errors.New("WAL loss is outside the witnessed next open segment")
	}
	var archiveBytes int64
	archiveBytes, err = checkArchivedInventory(loss.Inventory, d.backup, end)
	if err != nil {
		return err
	}
	if loss.FinalSource.SystemID != r.State.SystemID ||
		loss.FinalSource.Timeline != d.backup.Source.Timeline ||
		loss.M15AckNS != lastMarker(d.ledger).OutcomeSync || !restoreledger.IsSHA256(loss.FinalSource.EffectSHA256) {
		return errors.New("source visibility, capacity or M15 acknowledgment is unproved")
	}
	v.RPO.ArchiveEnd, v.RPO.SourceFlush, v.RPO.WALLossBytes = d.cut.EndLSN, loss.FinalSource.FlushLSN, finalFlush-end
	v.Bytes["backup"], v.Bytes["archive"] = d.backup.BackupBytes, archiveBytes
	v.Bytes["database_at_backup"], v.Bytes["database_before_kill"] = d.backup.Source.DatabaseBytes, loss.FinalSource.DatabaseBytes
	v.Bytes["cluster_before_kill"], v.Bytes["wal_before_kill"] = loss.FinalSource.ClusterBytes, loss.FinalSource.WALBytes
	v.Bytes["database_on_restore"], v.Bytes["cluster_on_restore"], v.Bytes["wal_on_restore"] = r.State.DatabaseBytes, r.State.ClusterBytes, r.State.WALBytes
	return nil
}

func checkArchivedInventory(files []archivedFile, backup backupReceipt, end uint64) (int64, error) {
	var total int64
	names := map[string]bool{}
	segments := []string{}
	for _, f := range files {
		limit, err := archiveSizeLimit(f.Name)
		if err != nil || len(f.Name) < 24 || f.Size <= 0 || f.Size > limit ||
			(len(f.Name) == 24 && f.Size != walSegmentBytes) || !restoreledger.IsSHA256(f.SHA256) || names[f.Name] ||
			!bytes.HasPrefix([]byte(f.Name), fmt.Appendf(nil, "%08X", backup.Source.Timeline)) {
			return 0, errors.New("invalid source archive entry")
		}
		names[f.Name] = true
		if len(f.Name) == 24 {
			segments = append(segments, f.Name)
		}
		total += f.Size
	}
	slices.Sort(segments)
	if len(segments) < 3 || total > maxArchiveBytes || uint64(len(segments)) > end/walSegmentBytes ||
		len(backup.RequiredWAL) != 1 || backup.RequiredWAL[0].Timeline != backup.Source.Timeline {
		return 0, errors.New("WAL_ARCHIVE_GAP: cut inventory is not complete")
	}
	first := end/walSegmentBytes - uint64(len(segments))
	start, err := parseLSN(backup.RequiredWAL[0].Start)
	if err != nil || first > start/walSegmentBytes {
		return 0, errors.New("WAL_ARCHIVE_GAP: required backup WAL is missing")
	}
	for i, name := range segments {
		if name != segmentName(backup.Source.Timeline, first+uint64(i)) {
			return 0, errors.New("WAL_ARCHIVE_GAP: cut inventory is not contiguous")
		}
	}
	for name := range names {
		if !names[name[:24]] {
			return 0, errors.New("archive metadata has no matching segment")
		}
	}
	for _, prior := range backup.Archive {
		if !slices.Contains(files, prior) {
			return 0, errors.New("prepared backup archive changed before the outage")
		}
	}
	return total, nil
}

func lastMarker(r restoreledger.Report) restoreledger.Exchange {
	for i := len(r.Exchanges) - 1; i >= 0; i-- {
		if r.Exchanges[i].Operation.Phase == "markers" {
			return r.Exchanges[i]
		}
	}
	return restoreledger.Exchange{}
}

func measureRPO(r restoreledger.Report, kept int, v *markerRPO) error {
	v.AckLossNS, v.CommitMinNS, v.CommitMaxNS = nil, nil, nil
	v.MaxSendJitterNS, v.MaxResponseNS = 0, 0
	markers := []restoreledger.Exchange{}
	for _, x := range r.Exchanges {
		if x.Operation.Phase != "markers" {
			continue
		}
		if !x.Acked() || x.Operation.Sequence != len(markers)+1 {
			return errors.New("RPO requires a contiguous acknowledged marker schedule")
		}
		markers = append(markers, x)
		delta := x.Outcome.SentNS - x.Operation.ScheduledNS
		v.MaxSendJitterNS = max(v.MaxSendJitterNS, delta, -delta)
		v.MaxResponseNS = max(v.MaxResponseNS, x.Outcome.CompletedNS-x.Outcome.SentNS)
	}
	if kept < 0 || kept > len(markers) {
		return errors.New("recovered marker prefix is outside the acknowledged population")
	}
	v.Acknowledged, v.Recovered = len(markers), kept
	v.Missing = []string{}
	for _, x := range markers[kept:] {
		v.Missing = append(v.Missing, x.Operation.WorkflowID)
	}
	if kept == 0 {
		return nil
	}
	var ack, low, high int64
	if kept < len(markers) {
		k, n := markers[kept-1].Outcome, markers[len(markers)-1].Outcome
		ack = n.CompletedNS - k.CompletedNS
		low = max(0, n.SentNS-k.CompletedNS)
		high = max(0, n.CompletedNS-k.SentNS)
	}
	v.AckLossNS, v.CommitMinNS, v.CommitMaxNS = &ack, &low, &high
	return nil
}

func checkProof(h effectAtCut, proof k6receipt.Proof, operation string, attempt, fence int64, key string) error {
	k, err := hex.DecodeString(key)
	claim := k6receipt.Claim{NodeID: h.Attempt.NodeID, Iteration: h.Attempt.Iteration, Attempt: attempt, Fence: fence}
	if err != nil || proof.Operation != operation || !k6receipt.Verify(ed25519.PublicKey(k), claim, proof) {
		return errors.New("signed effect proof does not match its witness claim")
	}
	return nil
}

func sameBefore(pre prewriteVerdict, r restoreReceipt) bool {
	return slices.Equal(pre.RecoveredMarkers, r.Verdict.RecoveredMarkers) &&
		slices.Equal(pre.MissingMarkers, r.Verdict.MissingMarkers) &&
		slices.Equal(pre.RetainedWorkflows, r.Verdict.RetainedWorkflows) && pre.Orphan == r.Verdict.Orphan
}

func (d drillInputs) checkPrewrite(v *drillVerdict) (prewriteVerdict, error) {
	if err := checkProof(d.cohorts.E1, d.proof, "apply", 1, d.cohorts.E1.Fence, d.work.SinkKey); err != nil {
		return prewriteVerdict{}, err
	}
	pre, err := verifyPrewrite(d.pre, ledgerMarkers(d.plan, d.ledger), d.cohorts, d.proof, d.sinkBefore)
	if err != nil {
		return pre, err
	}
	if !sameBefore(pre, d.restore) {
		return pre, errors.New("independent pre-write replay differs from the restore receipt")
	}
	if err := measureRPO(d.ledger, len(pre.RecoveredMarkers), &v.RPO); err != nil {
		return pre, err
	}
	if len(v.RPO.Missing) != 3 || !slices.Equal(v.RPO.Missing, pre.MissingMarkers) ||
		v.RPO.AckLossNS == nil || *v.RPO.AckLossNS > int64(16_200*time.Millisecond) ||
		*v.RPO.CommitMaxNS-*v.RPO.CommitMinNS > int64(2*time.Second) {
		return pre, errors.New("three-marker loss and acknowledgment-time bound do not both hold")
	}
	v.Retained = pre.RetainedWorkflows
	return pre, checkKafkaPrewrite(d.pre)
}

func runDrillVerdict(args []string, out, diagnostics io.Writer) error {
	return runDrillVerdictWithClock(args, out, diagnostics, restoreledger.HostClock)
}

func runDrillVerdictWithClock(args []string, out, diagnostics io.Writer, hostClock func() (restoreledger.Clock, string, error)) error {
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	flags.SetOutput(diagnostics)
	dir := flags.String("dir", "", "indexed evidence directory")
	index := flags.String("index-sha256", "", "independently sealed evidence index digest")
	replay := flags.Bool("replay", false, "replay a retained verdict on a different host")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("verify-drill accepts flags only")
	}
	b, err := openBundle(*dir, *index)
	if err != nil {
		return err
	}
	v := drillVerdict{Schema: "j7.drill-verdict.v1", Status: "invalid",
		Revision: b.index.Revision, Campaign: b.index.Campaign, Drill: b.index.Drill,
		PhaseNS: map[string]int64{}, Bytes: map[string]int64{},
		Incomplete: []string{}, Unresolved: []string{}, Ambiguous: []string{}}
	var original drillVerdict
	var now restoreledger.Clock
	boot := ""
	if *replay {
		if err := b.decode("verdict.json", &original); err != nil {
			return err
		}
		now = func() (int64, error) { return original.CheckerNS, nil }
	} else {
		now, boot, err = hostClock()
		if err != nil || b.index.Projection {
			return errors.New("live verdict requires the original private evidence on the outage boot")
		}
	}
	err = evaluateDrill(b, boot, now, &v)
	if err != nil {
		v.Reason = err.Error()
	}
	if *replay {
		calculated, _ := json.Marshal(v)
		saved, _ := json.Marshal(original)
		if !bytes.Equal(calculated, saved) {
			return errors.New("independent replay differs from the retained verdict")
		}
	}
	return errors.Join(err, json.NewEncoder(out).Encode(v))
}

func evaluateDrill(b evidenceBundle, boot string, now restoreledger.Clock, v *drillVerdict) error {
	for _, name := range b.index.Missing {
		switch name {
		case "verdict.json", "qualification-controls.json", "cleanup.json":
			continue
		default:
			return fmt.Errorf("independent verdict lacks required evidence %s", name)
		}
	}
	d, err := readDrill(b)
	if err != nil {
		return err
	}
	if boot != "" && d.ledger.Header.BootID != boot {
		return errors.New("live checker is on a different boot from the fault")
	}
	if err := d.checkMedia(v); err != nil {
		return err
	}
	if err := d.checkClosedEvidence(v); err != nil {
		return err
	}
	var resources resourceWindow
	if err := b.decode("resource-samples.json", &resources); err != nil {
		return err
	}
	if err := checkResources(resources, d.work.Clock.StartNS, d.end.CompletedNS); err != nil {
		return err
	}
	checker, err := now()
	if err != nil || checker < d.end.CompletedNS || checker-d.loss.T0NS > int64(600*time.Second) {
		return fmt.Errorf("independent checker missed the 600-second RTO cutoff")
	}
	v.Status, v.CheckerNS, v.RTONS = "passed", checker, checker-d.loss.T0NS
	v.PhaseNS["checker_pass"] = checker
	return nil
}
