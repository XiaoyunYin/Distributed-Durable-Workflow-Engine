package main

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/restoreledger"
	"durable-agent-execution-engine/internal/state"
)

type drillFixture struct {
	t      *testing.T
	files  map[string][]byte
	header restoreledger.Header
	clock  orphanClock
	ledger fixtureLedger
}

func (f *drillFixture) put(name string, value any) string {
	f.t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		f.t.Fatal(err)
	}
	f.files[name] = append(raw, '\n')
	return restoreledger.SHA256(f.files[name])
}

func (f *drillFixture) ns(seconds int64) int64 { return f.clock.StartNS + seconds*int64(time.Second) }

// Mirror the on-disk row framing only. Every row is read back by the
// production ledger reader, including both prefixes and the final seal.
type fixtureLedgerRow struct {
	Number uint64                   `json:"number"`
	Prior  string                   `json:"previous_sha256"`
	Mono   int64                    `json:"monotonic_ns"`
	UTC    time.Time                `json:"utc"`
	Header *restoreledger.Header    `json:"header,omitempty"`
	Send   *restoreledger.Operation `json:"send,omitempty"`
	Result *restoreledger.Outcome   `json:"outcome,omitempty"`
	Sync   uint64                   `json:"synced_row,omitempty"`
	Probe  *restoreledger.Witness   `json:"witness,omitempty"`
	Seal   *fixtureLedgerSeal       `json:"seal,omitempty"`
}

type fixtureLedgerSeal struct {
	Rows uint64 `json:"rows"`
	Hash string `json:"content_sha256"`
}

type fixtureLedger struct {
	f     *drillFixture
	data  bytes.Buffer
	rows  uint64
	prior string
	last  int64
}

func (l *fixtureLedger) append(at int64, row fixtureLedgerRow) {
	l.f.t.Helper()
	if at < l.last {
		l.f.t.Fatalf("synthetic ledger time went backwards: %d < %d", at, l.last)
	}
	l.rows++
	row.Number, row.Prior, row.Mono, row.UTC = l.rows, l.prior, at, l.f.clock.wall(at)
	line, err := json.Marshal(row)
	if err != nil {
		l.f.t.Fatal(err)
	}
	line = append(line, '\n')
	l.data.Write(line)
	l.prior, l.last = restoreledger.SHA256(line), at
}

func (l *fixtureLedger) exchange(at int64, op restoreledger.Operation, result restoreledger.Outcome) int64 {
	l.append(at-2, fixtureLedgerRow{Send: &op})
	l.append(at-1, fixtureLedgerRow{Sync: l.rows})
	result.RequestID, result.SentNS, result.CompletedNS = op.RequestID, at, at+1
	if result.ResponseSHA256 == "" {
		result.ResponseSHA256 = testDigest
	}
	l.append(at+2, fixtureLedgerRow{Result: &result})
	l.append(at+3, fixtureLedgerRow{Sync: l.rows})
	return at + 3
}

func (l *fixtureLedger) digest() string { return restoreledger.SHA256(l.data.Bytes()) }

func (f *drillFixture) witness(authority, phase, sha string, timeline uint32, start, end int64) {
	f.ledger.append(end+1, fixtureLedgerRow{Probe: &restoreledger.Witness{
		Kind: restoreledger.StateProbe, Phase: phase, Authority: authority, Timeline: timeline,
		StartedNS: start, CompletedNS: end, EvidenceSHA: sha,
	}})
}

func buildDrillFixture(t *testing.T) *drillFixture {
	t.Helper()
	orphan := makeOrphanFixture()
	media := fixtureForFault(t, 15)
	f := &drillFixture{t: t, files: map[string][]byte{}, clock: orphan.clock}
	f.ledger.f = f
	f.header = restoreledger.Header{Schema: restoreledger.Schema, Campaign: media.plan.Fixture.Campaign,
		Drill: media.plan.Fixture.Drill, Source: "source-r1", Target: "target-r1", BootID: "fixture-boot",
		RunID: 318, RunAttempt: 1, Revision: strings.Repeat("1", 40), MarkersSHA256: media.report.Header.MarkersSHA256}
	h := &f.header
	topology := f.put("topology.json", map[string]int{"kind_nodes": 1})
	policy := f.put("policy.json", map[string]int{"rto_seconds": 600})
	f.put("marker-plan.json", media.plan)
	work := drillWorkload{Schema: "j7.workload.v1", Clock: f.clock, MarkerOrigin: f.ns(4), Fixture: media.plan.Fixture,
		SinkKey:   hex.EncodeToString(orphan.signing.Public().(ed25519.PublicKey)),
		RelayPeer: orphan.rows[0].RelayPeer, SinkPeer: orphan.rows[0].SinkPeer}
	for _, role := range strings.Fields("d1 d2 f1 l1 e1 n1 n2 n3 n4") {
		node := "pure.echo"
		if role == "e1" {
			node = state.K6Activity
		}
		work.NonMarkers = append(work.NonMarkers, plannedSubmission{Role: role, ID: role, Key: role,
			RequestID: role + "-submit", RequestSHA: restoreledger.SHA256([]byte(role)), Definition: "marker",
			InitialNode: node, Input: json.RawMessage(`{"fixture":1}`)})
	}
	h.WorkloadSHA256, h.TopologySHA256, h.PolicySHA256 = f.put("workload.json", work), topology, policy
	f.ledger.append(f.ns(0), fixtureLedgerRow{Header: h})
	send := func(s plannedSubmission, at int64, authority string) int64 {
		return f.ledger.exchange(at, restoreledger.Operation{RequestID: s.RequestID, Kind: restoreledger.Submit,
			Phase: "fixture", Authority: authority, WorkflowID: s.ID, Namespace: work.Fixture.Namespace,
			SubmissionKey: s.Key, RequestSHA256: s.RequestSHA}, restoreledger.Outcome{Disposition: restoreledger.Acknowledged,
			HTTPStatus: 201, DurableID: s.ID, PayloadHash: "sub-v1:" + testDigest})
	}
	for i, s := range work.NonMarkers[:5] {
		send(s, f.ns(2)+int64(i*10), h.Source)
	}
	sendMarker := func(i int) int64 {
		x := media.report.Exchanges[i]
		x.Operation.ScheduledNS = work.MarkerOrigin + media.plan.Markers[i].OffsetNS
		return f.ledger.exchange(x.Operation.ScheduledNS, x.Operation,
			restoreledger.Outcome{Disposition: restoreledger.Acknowledged, HTTPStatus: 201,
				DurableID: x.Operation.WorkflowID, PayloadHash: x.Outcome.PayloadHash})
	}
	for i := range 11 {
		sendMarker(i)
	}
	cohorts, oldKafka, freshKafka, watchKafka, finalKafka := f.transportFixture(orphan)
	cohortSHA := f.put("cohorts.json", cohorts)
	owner, worker := f.sourceFenceFixtures(cohorts)
	orphanRebind, orphanRecord := f.orphanFixtures(orphan)
	cohortWitness := restoreledger.Witness{Kind: restoreledger.StateProbe, Phase: "pre-cut-cohorts",
		Authority: h.Source, StartedNS: f.ns(58) + 100_000, CompletedNS: f.ns(58) + 200_000, EvidenceSHA: cohortSHA}
	f.ledger.append(cohortWitness.CompletedNS+1, fixtureLedgerRow{Probe: &cohortWitness})
	ack12 := sendMarker(11)
	pre, final := f.engineFixtures(media, work, cohorts, watchKafka)
	b := media.backup
	b.Source.SystemID = orphan.end.SystemID
	b.BackupBytes = 4 << 20
	b.ManifestSHA256 = f.put("backup-manifest.json", map[string]any{"System-Identifier": b.Source.SystemID})
	backSHA := f.put("backup.json", b)
	binding := sourceBinding{BootID: h.BootID, Authority: h.Source, BackupSHA256: backSHA,
		StartedAt: "source-start", Run: faultConfig{Container: strings.Repeat("2", 64)},
		Volume: sourceVolume{Name: "j7-source", Mountpoint: "/source"}}
	bindSHA := f.put("source-binding.json", binding)
	cut := cutReceipt{Schema: "j7.archive-cut.v1", BootID: h.BootID, Source: h.Source, BackupSHA256: backSHA,
		StartedNS: f.ns(59) + int64(time.Millisecond), CompletedNS: f.ns(60), AckNS: ack12, LedgerSHA256: f.ledger.digest(),
		Cohorts: cohortWitness, Point: anchor{Name: "fixture-cut", LSN: "0/3002048", SwitchLSN: "0/3002060", Segment: segmentName(1, 3)},
		EndLSN: "0/4000000", Inventory: append(b.Archive, archivedFile{segmentName(1, 3), walSegmentBytes, testDigest})}
	cut.PreSwitch = media.sample(12, "0/3002028")
	cut.PreSwitch.SystemID = b.Source.SystemID
	cutSHA := f.put("cut.json", cut)
	w := archiveCutWitness(cut, cutSHA, media.plan.Markers[11].RequestID)
	f.ledger.append(cut.CompletedNS+1, fixtureLedgerRow{Probe: &w})
	sendMarker(12)
	proofSHA := f.put("source-proof.json", orphan.apply)
	proof := restoreledger.Witness{Kind: restoreledger.StateProbe, Phase: "post-cut-effect", Authority: h.Source,
		StartedNS: f.ns(65), CompletedNS: f.ns(66), EvidenceSHA: proofSHA}
	f.ledger.append(proof.CompletedNS+1, fixtureLedgerRow{Probe: &proof})
	sendMarker(13)
	ack15 := sendMarker(14)
	freeze := restoreledger.Witness{Kind: restoreledger.StateProbe, Phase: "writers-frozen", Authority: h.Source,
		StartedNS: f.ns(74) + 100, CompletedNS: f.ns(74) + 200, EvidenceSHA: testDigest}
	f.ledger.append(freeze.CompletedNS+1, fixtureLedgerRow{Probe: &freeze})
	loss := outageReceipt{Schema: "j7.source-loss.v1", BootID: h.BootID, Source: h.Source, BackupSHA256: backSHA,
		BindingSHA256: bindSHA, CutSHA256: cutSHA, LedgerSHA256: f.ledger.digest(), Inventory: cut.Inventory,
		EffectProof: proof, WriterFreeze: freeze, FlushPastCut: 16, M15AckNS: ack15, KillNS: f.ns(75), KillAckNS: f.ns(75) + 1,
		LastSuccessNS: f.ns(75) - 1, T0NS: f.ns(76), ContainerGone: f.ns(77), VolumeRemoved: f.ns(78),
		VolumeAbsent: f.ns(79), ArchiveSealNS: f.ns(80), Exit: exitObservation{ObservedNS: f.ns(76),
			State: containerState{Status: "exited", ExitCode: 137, StartedAt: binding.StartedAt, FinishedAt: "source-finished"}},
		SQLDown: sqlDown{FirstFailedStartNS: f.ns(75) + 2, FirstFailedNS: f.ns(75) + 3, NoResponseNS: f.ns(75) + 4,
			LastSuccessNS: f.ns(75) - 1, PsqlExitCode: 2, PgIsReadyExitCode: 2}}
	loss.FinalSource = media.sample(15, "0/4000010")
	loss.FinalSource.SystemID, loss.FinalSource.EffectSHA256 = b.Source.SystemID, orphan.end.ResultSHA
	loss.FinalSource.StartedNS, loss.FinalSource.CompletedNS = loss.LastSuccessNS-100, loss.LastSuccessNS
	lossSHA := f.put("source-loss.json", loss)
	wal, logs := completeReplayFixture()
	r := restoreReceipt{Schema: "j7.isolated-restore.v1", BootID: h.BootID, Source: h.Source, Target: h.Target,
		SourceTimeline: 1, LossSHA: lossSHA, Volume: sourceVolume{Name: "j7-target", Mountpoint: "/target"},
		VolumeCreatedNS: f.ns(81), TargetID: strings.Repeat("3", 64), WAL: wal, ReplayLogs: logs,
		FirstReplayNS: f.ns(84), ReadyObservedNS: f.ns(85), RecoveredNS: f.ns(86), CompletedNS: f.ns(91),
		History: "1\t0/4000000\tno recovery target specified\n", SnapshotSHA: f.put("engine-pre-write.json", pre),
		SinkSHA: f.put("sink-pre-write.json", orphan.pre),
		State: targetState{Version: serverVersion, SystemID: b.Source.SystemID, Timeline: 2, PreviousTimeline: 1,
			ReplayLSN: wal.End, DataDirectory: targetData, DatabaseBytes: 25, ClusterBytes: 50, WALBytes: walSegmentBytes,
			Recovered: true, SocketOnly: true, ReadOnly: true, Restricted: true}}
	r.Verdict.RecoveredMarkers, r.Verdict.MissingMarkers = []string{}, []string{}
	for i, marker := range media.plan.Markers {
		if i < 12 {
			r.Verdict.RecoveredMarkers = append(r.Verdict.RecoveredMarkers, marker.WorkflowID)
		} else {
			r.Verdict.MissingMarkers = append(r.Verdict.MissingMarkers, marker.WorkflowID)
		}
	}
	r.Verdict.RetainedWorkflows = append(append([]string(nil), r.Verdict.RecoveredMarkers...), "d1", "d2", "f1", "l1", "e1")
	r.Verdict.Orphan = orphan.apply.Receipt
	restoreSHA := f.put("restore.json", r)
	orphanRebind.RestoreSHA = restoreSHA
	orphanRecord.RebindSHA = f.put("reader-rebind.json", orphanRebind)
	orphanRecord.RestoreSHA = restoreSHA
	f.put("orphan.json", orphanRecord)
	f.ledger.append(orphanRebind.CompletedNS+1, fixtureLedgerRow{Probe: &restoreledger.Witness{
		Kind: restoreledger.AuthorityChange, Phase: orphanRebindPhase, Authority: h.Target, Former: h.Source,
		StartedNS: orphanRebind.DB.StartedNS, CompletedNS: orphanRebind.CompletedNS, Timeline: 2, EvidenceSHA: orphanRecord.RebindSHA,
	}})
	freshKafka.SourceSHA = f.put("kafka-source.json", oldKafka)
	freshKafka.RestoreSHA = restoreSHA
	freshSHA := f.put("kafka-fresh.json", freshKafka)
	f.exerciseControllerEndpoint(r, restoreSHA, freshSHA)
	for i, s := range work.NonMarkers[5:] {
		at := send(s, f.ns(96)+int64(i*10), h.Target)
		if i == 0 {
			worker.N1SyncedNS = at
		}
	}
	f.targetFences(owner, worker)
	watchKafka.FreshSHA = freshSHA
	watchSHA := f.put("redispatch-watch.json", watchKafka)
	_, end := f.quietFixtures(final, finalKafka, orphanRecord.SinkSHA)
	d1, err := verifyRedispatchedWorkflow(final, cohorts.D1, *watchKafka.D1, finalKafka, watchKafka.OpenNS)
	if err != nil {
		t.Fatal(err)
	}
	d2, err := verifyRedispatchedWorkflow(final, cohorts.D2, *watchKafka.D2, finalKafka, watchKafka.OpenNS)
	if err != nil {
		t.Fatal(err)
	}
	finalKafka.BeforeNS, finalKafka.AfterNS = f.ns(150), f.ns(150)+100
	f.put("redispatch.json", redispatchReceipt{Schema: "j7.redispatch.v1", BootID: h.BootID, FreshSHA: freshSHA,
		WatchSHA: watchSHA, EngineSHA: end.EngineSHA, Broker: finalKafka, D1: d1, D2: d2, CompletedNS: f.ns(151)})
	f.ledger.append(f.ns(152), fixtureLedgerRow{Seal: &fixtureLedgerSeal{Rows: f.ledger.rows, Hash: f.ledger.digest()}})
	f.files["ack.jsonl"] = f.ledger.data.Bytes()
	f.resourcesFixture(end.CompletedNS)
	return f
}

func (f *drillFixture) writeRaw(dir string) evidenceIndex {
	f.t.Helper()
	idx := evidenceIndex{Schema: "j7.evidence-index.v1", Campaign: f.header.Campaign, Drill: f.header.Drill,
		Revision: f.header.Revision, Run: f.header.RunID, Attempt: f.header.RunAttempt}
	entries := map[string]evidenceEntry{}
	for _, name := range evidenceNames {
		raw, ok := f.files[name]
		if !ok {
			continue
		}
		f.write(dir, name, raw)
		entry := evidenceEntry{Name: name, SHA: restoreledger.SHA256(raw), Bytes: int64(len(raw))}
		idx.Files = append(idx.Files, entry)
		entries[name] = entry
	}
	idx.Missing = missingEvidence(entries)
	return idx
}

func (f *drillFixture) write(dir, name string, bytes []byte) {
	f.t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), bytes, 0o600); err != nil {
		f.t.Fatal(err)
	}
}

func (f *drillFixture) index(dir string, idx evidenceIndex) string {
	f.t.Helper()
	raw, err := json.Marshal(idx)
	if err != nil {
		f.t.Fatal(err)
	}
	f.write(dir, "index.json", raw)
	return restoreledger.SHA256(raw)
}

func TestJ7CompleteDrillAndProjectedReplay(t *testing.T) {
	f := buildDrillFixture(t)
	rawDir, publicDir := filepath.Clean(t.TempDir()), filepath.Clean(t.TempDir())
	idx := f.writeRaw(rawDir)
	sha := f.index(rawDir, idx)
	run := func(dir, index string, replay bool, out io.Writer) error {
		args := []string{"verify-drill", "--dir", dir, "--index-sha256", index}
		if replay {
			args = append(args, "--replay")
		}
		return runDrillVerdictWithClock(args, out, io.Discard, func() (restoreledger.Clock, string, error) {
			return func() (int64, error) { return f.ns(153), nil }, f.header.BootID, nil
		})
	}
	var live, replay bytes.Buffer
	if err := run(rawDir, sha, false, &live); err != nil {
		t.Fatalf("verify-drill: %v\n%s", err, live.String())
	}
	var v drillVerdict
	if err := json.Unmarshal(live.Bytes(), &v); err != nil || v.Status != "passed" || v.RTONS != f.ns(153)-f.ns(76) ||
		len(v.Retained) != 17 || len(v.Fresh) != 4 || len(v.RPO.Missing) != 3 {
		t.Fatalf("incomplete positive verdict: %s (%v)", live.String(), err)
	}
	f.files["verdict.json"] = live.Bytes()
	f.files["qualification-controls.json"] = []byte("{\"healthy_sibling\":true}\n")
	f.files["cleanup.json"] = []byte("{\"verified\":true,\"residuals\":[]}\n")
	idx = f.writeRaw(rawDir)
	sha = f.index(rawDir, idx)
	b, err := openBundle(rawDir, sha)
	if err != nil {
		t.Fatal(err)
	}
	projected, err := projectBundle(b, func(name string, data []byte) error {
		f.write(publicDir, name, data)
		return nil
	})
	if err != nil || len(projected.Missing) != 0 {
		t.Fatalf("evidence projection: missing %v: %v", projected.Missing, err)
	}
	pubSHA := f.index(publicDir, projected)
	if err := run(publicDir, pubSHA, true, &replay); err != nil || !bytes.Equal(live.Bytes(), replay.Bytes()) {
		t.Fatalf("projected replay: %v\nlive=%s\nreplay=%s", err, &live, &replay)
	}
	t.Run("index-seal", func(t *testing.T) {
		err := run(publicDir, testDigest, true, io.Discard)
		if err == nil || !strings.Contains(err.Error(), "index differs") {
			t.Fatalf("mutated outer seal: %v", err)
		}
	})
	for i, entry := range projected.Files {
		t.Run("digest/"+entry.Name, func(t *testing.T) {
			mutant := projected
			mutant.Files = append([]evidenceEntry(nil), projected.Files...)
			mutant.Files[i].SHA = restoreledger.SHA256([]byte(entry.Name))
			badSHA := f.index(publicDir, mutant)
			if err := run(publicDir, badSHA, true, io.Discard); err == nil || !strings.Contains(err.Error(), entry.Name) {
				t.Fatalf("mutated bound input %s: %v", entry.Name, err)
			}
		})
	}
}

// Re-index the changed file to ensure the substantive proof still rejects it.
func TestJ7CompleteDrillRejectsResealedInvalidEvidence(t *testing.T) {
	f := buildDrillFixture(t)
	dir := filepath.Clean(t.TempDir())
	baseline := f.writeRaw(dir)
	for _, tc := range []struct {
		name  string
		field string
		value json.RawMessage
	}{
		{"backup.json", "backup_bytes", json.RawMessage("999999999")},
		{"endpoint.json", "timeline", json.RawMessage("44")},
		{"policy.json", "rto_seconds", json.RawMessage("601")},
		{"resource-samples.json", "logical_cpu_count", json.RawMessage("3")},
		{"workload.json", "marker_origin_ns", json.RawMessage("123")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := f.files[tc.name]
			defer f.write(dir, tc.name, original)
			var value map[string]json.RawMessage
			if err := json.Unmarshal(original, &value); err != nil {
				t.Fatal(err)
			}
			if _, ok := value[tc.field]; !ok {
				t.Fatalf("fixture missing field %s", tc.field)
			}
			value[tc.field] = tc.value
			data, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			f.write(dir, tc.name, data)
			idx := baseline
			idx.Files = append([]evidenceEntry(nil), baseline.Files...)
			for i, e := range idx.Files {
				if e.Name == tc.name {
					idx.Files[i].SHA, idx.Files[i].Bytes = restoreledger.SHA256(data), int64(len(data))
				}
			}
			var outcome bytes.Buffer
			args := []string{"verify-drill", "--dir", dir, "--index-sha256", f.index(dir, idx)}
			err = runDrillVerdictWithClock(args, &outcome, io.Discard, func() (restoreledger.Clock, string, error) {
				return func() (int64, error) { return f.ns(153), nil }, f.header.BootID, nil
			})
			var v drillVerdict
			if err == nil || json.Unmarshal(outcome.Bytes(), &v) != nil || v.Status != "invalid" {
				t.Fatalf("re-sealed invalid field %s.%s: %v, %s", tc.name, tc.field, err, outcome.String())
			}
		})
	}
}
