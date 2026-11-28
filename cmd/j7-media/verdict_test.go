package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/restoreledger"
)

func TestJ7RPOReportsAckAndCommitBounds(t *testing.T) {
	f := fixtureForFault(t, 15)
	var r markerRPO
	if err := measureRPO(f.report, 12, &r); err != nil {
		t.Fatal(err)
	}
	k, n := f.report.Exchanges[11].Outcome, f.report.Exchanges[14].Outcome
	if r.Recovered != 12 || r.Acknowledged != 15 || len(r.Missing) != 3 ||
		r.Missing[0] != f.plan.Markers[12].WorkflowID ||
		r.Missing[2] != f.plan.Markers[14].WorkflowID ||
		*r.AckLossNS != n.CompletedNS-k.CompletedNS ||
		*r.CommitMinNS != n.SentNS-k.CompletedNS || *r.CommitMaxNS != n.CompletedNS-k.SentNS {
		t.Fatalf("unexpected RPO %+v", r)
	}
	if err := measureRPO(f.report, 0, &r); err != nil || len(r.Missing) != 15 ||
		r.AckLossNS != nil || r.CommitMinNS != nil || r.CommitMaxNS != nil {
		t.Fatalf("empty recovered prefix: %+v %v", r, err)
	}
	if err := measureRPO(f.report, 15, &r); err != nil || *r.AckLossNS != 0 || len(r.Missing) != 0 {
		t.Fatalf("full recovered prefix: %+v %v", r, err)
	}
	f.report.Exchanges[13].OutcomeSync = 0
	if err := measureRPO(f.report, 12, &r); err == nil {
		t.Fatal("accepted a gap in the acknowledged marker sequence")
	}
}

func TestJ7ForecastIncludesObservedStagesAndFullFinalReserve(t *testing.T) {
	cases := []struct {
		name, mode    string
		elapsed, done int
		bootstrapped  bool
		observed      stageMinutes
		reserve       int
		stops         bool
	}{
		{name: "qualification", mode: "qualification", reserve: 105},
		{name: "measurement", mode: "measurement", reserve: 140},
		{name: "observed maxima", mode: "measurement", elapsed: 45, bootstrapped: true,
			observed: stageMinutes{Drill: 28, Between: 9, Final: 50}, reserve: 152},
		{name: "exact cap", mode: "measurement", elapsed: 295, done: 2, bootstrapped: true, reserve: 55},
		{name: "one minute short", mode: "measurement", elapsed: 296, done: 2, bootstrapped: true, reserve: 55, stops: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			n, err := forecast(c.mode, c.elapsed, c.done, c.bootstrapped, c.observed)
			if n != c.reserve || (err != nil) != c.stops {
				t.Fatalf("forecast = %d, %v", n, err)
			}
		})
	}
}

func TestJ7AdmissionRequiresBothRegisteredProfilesAndExactPassingQualification(t *testing.T) {
	switches := map[string]registeredSwitch{"EXAMPLE_REPAIRED_RELAY": {
		Task: "J9a", Profiles: []string{"J7_target", "J7_source"},
	}}
	got, _, err := selectRelay(switches)
	if err != nil || got != "EXAMPLE_REPAIRED_RELAY" {
		t.Fatalf("registered mode: %q %v", got, err)
	}
	for _, bad := range []map[string]registeredSwitch{nil, {"ONLY_TARGET": {Task: "J9a", Profiles: []string{"J7_target"}}}} {
		if _, _, err := selectRelay(bad); err == nil || !strings.Contains(err.Error(), "J9A_ADMISSION") {
			t.Fatalf("admitted unregistered mode: %v", err)
		}
	}
	revision := strings.Repeat("a", 40)
	q := qualificationSeal{Schema: "j7.qualification.v1", Run: 123, Attempt: 1, Revision: revision,
		Candidate: testDigest, DrillIndex: testDigest, Status: "passed", DrillStatus: "passed",
		ReplayPassed: true, CleanupOK: true, Controls: qualificationControls}
	if err := verifyQualification(q, 123, testDigest, revision); err != nil {
		t.Fatal(err)
	}
	if err := verifyQualification(q, 123, testDigest, strings.Repeat("b", 40)); err == nil {
		t.Fatal("qualification authorized a different candidate")
	}
	q.Controls = q.Controls[:3]
	if err := verifyQualification(q, 123, testDigest, revision); err == nil {
		t.Fatal("qualification authorized a missing control")
	}
}

func TestJ7EvidenceIndexRequiresExactMissingFilesAndBytes(t *testing.T) {
	dir := t.TempDir()
	const evidence = "{\"schema\":\"j7.workload.v1\"}\n"
	if err := os.WriteFile(filepath.Join(dir, "workload.json"), []byte(evidence), 0o600); err != nil {
		t.Fatal(err)
	}
	entry := evidenceEntry{Name: "workload.json", Bytes: int64(len(evidence)), SHA: restoreledger.SHA256([]byte(evidence))}
	i := evidenceIndex{Schema: "j7.evidence-index.v1", Revision: strings.Repeat("a", 40), Campaign: "j7", Drill: "r1", Run: 1, Attempt: 1,
		Files: []evidenceEntry{entry}, Missing: missingEvidence(map[string]evidenceEntry{entry.Name: entry})}
	data, _ := json.Marshal(i)
	if err := os.WriteFile(filepath.Join(dir, "index.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	sha := restoreledger.SHA256(data)
	if _, err := openBundle(dir, sha); err != nil {
		t.Fatal(err)
	}
	if _, err := openBundle(dir, testDigest); err == nil {
		t.Fatal("used an unbound index")
	}
	if err := os.WriteFile(filepath.Join(dir, "workload.json"), []byte(evidence+" "), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := openBundle(dir, sha); err == nil {
		t.Fatal("used modified evidence")
	}
}

func TestJ7PublicEvidencePreservesCredentialJoinWithoutSecret(t *testing.T) {
	var item any
	if err := json.Unmarshal([]byte(`{"attempt":{"claim_token":"private claim"},"grant_token":"private grant"}`), &item); err != nil {
		t.Fatal(err)
	}
	if safeEvidenceValue(item) || !redactTokens(item) || !safeEvidenceValue(item) {
		t.Fatal("raw token admitted or redacted token rejected")
	}
	got := item.(map[string]any)["grant_token"].(string)
	if credentialDigest(got) != restoreledger.SHA256([]byte("private grant")) {
		t.Fatal("redaction changed the portable grant join")
	}
	for _, raw := range []string{
		`{"dsn":"postgresql://user:password@host/db"}`,
		`{"settings":[{"name":"PGPASSWORD","value":"value"}]}`,
		`{"settings":[{"name":"KAFKA_BOOTSTRAP_SERVERS","valueFrom":{"secretKeyRef":{}}}]}`,
	} {
		if err := json.Unmarshal([]byte(raw), &item); err != nil || safeEvidenceValue(item) {
			t.Fatalf("unsafe upload field was accepted: %v", err)
		}
	}
}

func TestJ7StaleFenceSampleRequiresTemporalBracketingAndUnchangedRows(t *testing.T) {
	send := int64(time.Second)
	s := retainedFenceSample{StartedNS: send - 2, CompletedNS: send - 1,
		Rows: map[string]json.RawMessage{"outbox": json.RawMessage(`[]`)}}
	later := s
	later.StartedNS, later.CompletedNS = send+3, send+4
	x := restoreledger.Exchange{Outcome: &restoreledger.Outcome{SentNS: send}, OutcomeSync: send + 2}
	if !unchangedFence(s, later, x) {
		t.Fatal("stable fenced observation rejected")
	}
	later.Rows = map[string]json.RawMessage{"outbox": json.RawMessage(`[{"event":"unexpected"}]`)}
	if unchangedFence(s, later, x) {
		t.Fatal("stale write unexpectedly changed the outbox")
	}
	later.Rows = s.Rows
	later.StartedNS = send
	if unchangedFence(s, later, x) {
		t.Fatal("accepted sample taken before the denial was durable")
	}
}

func TestJ7ArchivedInventoryAcceptsBackupMetadataButRequiresEverySegment(t *testing.T) {
	backup := backupReceipt{Source: sourceState{Timeline: 1},
		RequiredWAL: []walRange{{Timeline: 1, Start: "0/1000010", End: "0/2000000"}}}
	var files []archivedFile
	for n := uint64(1); n <= 3; n++ {
		files = append(files, archivedFile{Name: segmentName(1, n), Size: walSegmentBytes, SHA256: testDigest})
	}
	files = append(files, archivedFile{Name: segmentName(1, 1) + ".00000010.backup", Size: 40, SHA256: testDigest})
	total, err := checkArchivedInventory(files, backup, 4*walSegmentBytes)
	if err != nil || total != 3*walSegmentBytes+40 {
		t.Fatalf("closed archive: %d %v", total, err)
	}
	files = append(files[:1], files[2:]...)
	if _, err := checkArchivedInventory(files, backup, 4*walSegmentBytes); err == nil || !strings.Contains(err.Error(), "WAL_ARCHIVE_GAP") {
		t.Fatalf("archive with interior gap: %v", err)
	}
}

func TestJ7ResourcesCheckReserveAndSampleContinuity(t *testing.T) {
	window := resourceWindow{Schema: "j7.resource-samples.v1", CPUs: 4, InstalledKiB: 12 << 20, StartupFreeKiB: 8 << 20,
		EngineLimit: 100, SinkLimit: 16, RoleLimits: map[string]quota{
			"engine-db": {1000, 1 << 30}, "sink-db": {500, 512 << 20}, "kafka": {1000, 1536 << 20},
			"api": {500, 256 << 20}, "scheduler": {500, 256 << 20}, "relay": {500, 256 << 20},
			"worker-1": {500, 512 << 20}, "worker-2": {500, 512 << 20}, "sink": {500, 512 << 20},
		}}
	for i, phase := range []string{"backup", "outage", "replay", "export"} {
		window.Samples = append(window.Samples, resourceSample{AtNS: int64(time.Duration(i+1) * 5 * time.Second),
			Phase: phase, FreeKiB: 4 << 20, AvailableKiB: 2 << 20, EngineSessions: 75, SinkSessions: 12})
	}
	start, final := int64(5*time.Second), int64(20*time.Second)
	if err := checkResources(window, start, final); err != nil {
		t.Fatal(err)
	}
	window.Samples[2].SinkSessions++
	if err := checkResources(window, start, final); err == nil {
		t.Fatal("passed with less than 25 percent sink reserve")
	}
	window.Samples[2].SinkSessions--
	window.Samples[2].AtNS++
	if err := checkResources(window, start, final); err == nil {
		t.Fatal("passed an unwitnessed resource interval")
	}
}
