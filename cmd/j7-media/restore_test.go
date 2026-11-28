package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/effects/k6sink"
	"durable-agent-execution-engine/internal/invariants"
	"durable-agent-execution-engine/internal/k6receipt"
	"durable-agent-execution-engine/internal/restoreledger"
)

func TestTerminalWALProof(t *testing.T) {
	name := "j7-final-cut"
	point := anchor{Name: name, Segment: segmentName(1, 3), LSN: "0/3002048", SwitchLSN: "0/3002060"}
	named := "rmgr: XLOG        len (rec/tot):    104/   104, tx:          0, lsn: 0/03002000, prev 0/03001000, desc: RESTORE_POINT " + name + "\n"
	switched := "rmgr: XLOG        len (rec/tot):     24/    24, tx:          0, lsn: 0/03002048, prev 0/03002000, desc: SWITCH \n"
	if final, err := checkedFinalRecord(named+switched, point, 4*walSegmentBytes); err != nil || final != "0/03002048" {
		t.Fatalf("expected exact terminal switch, got %q %v", final, err)
	}
	for _, tc := range []struct{ name, dump string }{
		{"missing_point", switched},
		{"different_point", strings.Replace(named, name, "other-name", 1) + switched},
		{"duplicate_point", named + named + switched},
		{"another_final_record", named + switched + strings.Replace(switched, "SWITCH", "NOOP", 1)},
		{"switch_in_next_segment", named + strings.Replace(switched, "03002048", "04002048", 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := checkedFinalRecord(tc.dump, point, 4*walSegmentBytes); err == nil {
				t.Fatal("invalid WAL proof succeeded")
			}
		})
	}
}

func TestReplayCommandUsesOnlySealedWAL(t *testing.T) {
	archive, pgdata := t.TempDir(), t.TempDir()
	t.Chdir(pgdata)
	if err := os.Mkdir("pg_wal", 0o700); err != nil {
		t.Fatal(err)
	}
	testSegment(t, archive, 1, walSegmentBytes)
	testSegment(t, archive, 2, walSegmentBytes)
	allowed, unlisted := segmentName(1, 1), segmentName(1, 2)
	files, err := readArchive(archive, []string{allowed})
	if err != nil {
		t.Fatal(err)
	}
	m := replayManifest{Files: files}
	dest := "pg_wal/RECOVERYXLOG"
	if err := replayWAL(archive, allowed, dest, m); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(dest); err != nil || info.Size() != walSegmentBytes {
		t.Fatalf("restore side file: %v %v", info, err)
	}
	if err := replayWAL(archive, unlisted, dest, m); !errors.Is(err, errWALAbsent) {
		t.Fatalf("unlisted existing WAL must remain unavailable: %v", err)
	}
	for _, candidate := range []string{"../RECOVERYXLOG", filepath.Join("pg_wal", allowed)} {
		if err := replayWAL(archive, allowed, candidate, m); err == nil {
			t.Fatalf("accepted unregistered destination %s", candidate)
		}
	}
	testSegment(t, archive, 1, 17)
	if err := replayWAL(archive, allowed, dest, m); err == nil || errors.Is(err, errWALAbsent) {
		t.Fatalf("required corrupt WAL must be fatal: %v", err)
	}
	if info, err := os.Stat(dest); err != nil || info.Size() != walSegmentBytes {
		t.Fatalf("failed copy replaced verified destination: %v %v", info, err)
	}
}

func TestRecoveredTimelineAndRange(t *testing.T) {
	wal, logs := completeReplayFixture()
	r := restoreReceipt{
		State: targetState{SystemID: 123, Version: serverVersion, Timeline: 2, PreviousTimeline: 1, DataDirectory: targetData,
			ReplayLSN: "0/4000000", Recovered: true, SocketOnly: true, ReadOnly: true, Restricted: true,
			DatabaseBytes: 12, ClusterBytes: 37, WALBytes: walSegmentBytes},
		WAL: wal, ReplayLogs: logs, FirstReplayNS: 123, ReadyObservedNS: 125,
		History: "1\t0/4000000\tno recovery target specified\n",
	}
	src := sourceState{SystemID: 123, Timeline: 1}
	if err := r.verifyReplay(src); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		change func(*restoreReceipt)
	}{
		{"old_timeline", func(r *restoreReceipt) { r.State.Timeline = 1 }},
		{"wrong_system_identifier", func(r *restoreReceipt) { r.State.SystemID++ }},
		{"replay_stopped_before_switch", func(r *restoreReceipt) { r.State.ReplayLSN = r.WAL.FinalRecord }},
		{"fork_before_C", func(r *restoreReceipt) { r.History = "1\t0/3002028\tunexpected\n" }},
		{"different_last_record", func(r *restoreReceipt) { r.ReplayLogs = strings.Replace(r.ReplayLogs, "0/3002048", "0/3002040", 1) }},
		{"missing_ready_observation", func(r *restoreReceipt) { r.ReadyObservedNS = 0 }},
		{"writer_access", func(r *restoreReceipt) { r.State.SocketOnly = false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mutant := r
			tc.change(&mutant)
			if err := mutant.verifyReplay(src); err == nil {
				t.Fatal("invalid target replay was accepted")
			}
		})
	}
}

func completeReplayFixture() (walProof, string) {
	wal := walProof{Start: "0/1000028", End: "0/4000000", FinalRecord: "0/3002048", NextSegment: segmentName(1, 4)}
	logs := "LOG: redo starts at 0/1000028\n"
	for i := uint64(1); i < 4; i++ {
		logs += "j7-archive restored " + segmentName(1, i) + "\n"
	}
	logs += "j7-archive absent " + wal.NextSegment + "\n" +
		"LOG: redo done at 0/3002048 system usage: example\n" +
		"LOG: selected new timeline ID: 2\n" +
		"LOG: archive recovery complete\n" +
		"LOG: database system is ready to accept connections\n"
	return wal, logs
}

func TestReplayLogReadiness(t *testing.T) {
	wal, complete := completeReplayFixture()
	const accepting = "LOG: database system is ready to accept connections\n"
	for _, tc := range []struct {
		name, logs string
		started    bool
		timeline   uint32
		fail       bool
	}{
		{name: "before_redo", logs: "LOG: starting archive recovery\n"},
		{name: "standby_start_returned", logs: "LOG: redo starts at 0/1000028\n", started: true},
		{name: "read_only_standby", logs: "LOG: redo starts at 0/1000028\nLOG: database system is ready to accept read-only connections\n", started: true},
		{name: "archive_complete_socket_not_ready", logs: strings.TrimSuffix(complete, accepting), started: true},
		{name: "normal_acceptance_before_archive_complete", logs: strings.Replace(complete, "LOG: archive recovery complete\n"+accepting, accepting+"LOG: archive recovery complete\n", 1), started: true, fail: true},
		{name: "missing_interior_segment", logs: strings.Replace(complete, "j7-archive restored "+segmentName(1, 2)+"\n", "", 1), started: true, fail: true},
		{name: "no_end_of_archive_probe", logs: strings.Replace(complete, "j7-archive absent "+wal.NextSegment+"\n", "", 1), started: true, fail: true},
		{name: "wrong_last_record", logs: strings.Replace(complete, wal.FinalRecord, "0/3002000", 1), started: true, fail: true},
		{name: "fatal_replay_error", logs: complete + "FATAL: recovery failed\n", fail: true},
		{name: "sealed_end_and_normal_acceptance", logs: complete, started: true, timeline: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			started, timeline, err := replayLogStatus(tc.logs, 1, wal)
			if (err != nil) != tc.fail || started != tc.started || timeline != tc.timeline {
				t.Fatalf("readiness = %t, %d, %v; want %t, %d, error=%t", started, timeline, err, tc.started, tc.timeline, tc.fail)
			}
		})
	}
}

func TestAwaitArchiveReplayRequiresReadySocket(t *testing.T) {
	wal, complete := completeReplayFixture()
	var now int64 = 10
	clock := func() (int64, error) { now++; return now, nil }
	for _, tc := range []struct {
		name      string
		socketErr error
	}{
		{name: "accepts_connections"},
		{name: "rejects_connections", socketErr: errors.New("socket not accepting")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := restoreReceipt{WAL: wal}
			reads := 0
			err := awaitArchiveReplay(t.Context(), 1, clock, &r, func(context.Context) ([]byte, error) {
				reads++
				if reads == 1 {
					return []byte("LOG: redo starts at 0/1000028\n"), nil
				}
				return []byte(complete), nil
			}, func(context.Context) error {
				if reads != 2 || r.FirstReplayNS != now || r.ReadyObservedNS != 0 {
					t.Fatal("socket probe ran before the sealed replay witness")
				}
				return tc.socketErr
			})
			if !errors.Is(err, tc.socketErr) {
				t.Fatalf("wait for replay: %v, want %v", err, tc.socketErr)
			}
			if tc.socketErr == nil && r.ReadyObservedNS <= r.FirstReplayNS ||
				tc.socketErr != nil && r.ReadyObservedNS != 0 || r.RecoveredNS != 0 {
				t.Fatalf("invalid observations: first=%d ready=%d recovered=%d", r.FirstReplayNS, r.ReadyObservedNS, r.RecoveredNS)
			}
		})
	}
}

func TestIsolatedTargetRejectsNetworkAndMountDrift(t *testing.T) {
	var in restoreInputs
	in.binding.Image = "sha256:" + strings.Repeat("1", 64)
	in.fault.Resources.TargetVolume, in.fault.Archive = "j7-target", "/runner/archive"
	r := restoreReceipt{TargetID: strings.Repeat("1", 64), Volume: sourceVolume{Mountpoint: "/docker/volume"}}
	c := isolatedContainer{ID: r.TargetID, Image: in.binding.Image, Path: "postgres", Args: []string{"-D", targetData}}
	c.Config.Labels, c.Config.User = targetLabels(in.header), "postgres"
	c.HostConfig.NetworkMode, c.HostConfig.RestartPolicy.Name = "none", "no"
	c.HostConfig.ReadonlyRootfs, c.HostConfig.Memory, c.HostConfig.NanoCpus = true, 1<<30, 1_000_000_000
	c.State.Status = "created"
	binary, manifest := "/runner/j7-media", "/runner/replay.json"
	c.Mounts = []dockerMount{
		{Type: "volume", Name: in.fault.Resources.TargetVolume, Source: r.Volume.Mountpoint,
			Destination: targetVolumeDir, Driver: "local", RW: true},
		{Type: "bind", Source: in.fault.Archive, Destination: archiveDest},
		{Type: "bind", Source: binary, Destination: targetBinary},
		{Type: "bind", Source: manifest, Destination: targetManifest},
	}
	if err := checkIsolatedContainer(c, in, manifest, binary, &r, false); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		change func(*isolatedContainer)
	}{
		{"bridge", func(c *isolatedContainer) { c.HostConfig.NetworkMode = "kind" }},
		{"publish", func(c *isolatedContainer) { c.HostConfig.PortBindings = map[string][]any{"5432/tcp": {}} }},
		{"writable_archive", func(c *isolatedContainer) { c.Mounts[1].RW = true }},
		{"original_data", func(c *isolatedContainer) { c.Mounts[0].Source = "/docker/old-data" }},
		{"extra_host_mount", func(c *isolatedContainer) { c.Mounts = append(c.Mounts, dockerMount{Source: "/runner/source"}) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mutant := c
			mutant.Mounts = slices.Clone(c.Mounts)
			tc.change(&mutant)
			if err := checkIsolatedContainer(mutant, in, manifest, binary, &r, false); err == nil {
				t.Fatal("unsafe target isolation was accepted")
			}
		})
	}
}

func TestRecoveredMarkerPrefixRejectsLossAndInclusion(t *testing.T) {
	f := fixtureForFault(t, 15)
	want := ledgerMarkers(f.plan, f.report)
	var workflows []workflowAtRestore
	var nodes []nodeAtRestore
	for _, m := range want {
		workflows = append(workflows, workflowAtRestore{ID: m.ID, Namespace: m.Namespace, Key: m.Key,
			PayloadHash: m.PayloadSHA256, Definition: m.Definition, Version: 1, State: "SUCCEEDED"})
		nodes = append(nodes, nodeAtRestore{ID: m.ID, Node: "pure.echo", Input: m.Input})
	}
	trace := invariants.Trace{Snapshot: map[string]invariants.SnapshotTable{}}
	kept, lost, err := checkMarkerPrefix(trace, workflows[:12], nodes[:12], want)
	if err != nil || len(kept) != 12 || len(lost) != 3 || lost[2] != want[14].ID {
		t.Fatalf("expected full contiguous prefix, got %v %v %v", kept, lost, err)
	}
	gap := slices.Delete(slices.Clone(workflows[:12]), 6, 7)
	_, _, err = checkMarkerPrefix(trace, gap, nodes[:12], want)
	if err == nil || !strings.Contains(err.Error(), "PRE_CUT_MARKER_MISSING: "+want[6].ID) {
		t.Fatalf("expected semantic marker gap rejection, got %v", err)
	}
	extra := append(slices.Clone(workflows[:12]), workflows[14])
	_, _, err = checkMarkerPrefix(trace, extra, nodes[:12], want)
	if err == nil || !strings.Contains(err.Error(), "POST_CUT_MARKER_PRESENT: "+want[14].ID) {
		t.Fatalf("expected semantic lost identity rejection, got %v", err)
	}
	raw, err := json.Marshal(nodes[13])
	if err != nil {
		t.Fatal(err)
	}
	trace.Snapshot["engine.node_instances"] = invariants.SnapshotTable{Rows: []json.RawMessage{raw}}
	_, _, err = checkMarkerPrefix(trace, workflows[:12], nodes[:12], want)
	if err == nil || !strings.Contains(err.Error(), "POST_CUT_MARKER_PRESENT: "+want[13].ID) {
		t.Fatalf("expected rejection of a related row for lost workflow, got %v", err)
	}
}

func TestPrewriteOrphanRequiresOneRetainedMutation(t *testing.T) {
	e := effectAtCut{Attempt: attemptAtCut{WorkflowID: "e1", NodeID: "effect", Attempt: 1, ClaimSHA: restoreledger.SHA256([]byte("claim")),
		Deadline: time.Unix(1_800_000_000, 0)}, Intent: "intent", Resource: "resource", LogicalKey: "key",
		ScopeHash: "scope", CanonicalHash: "args", GrantSHA: restoreledger.SHA256([]byte("grant")), Fence: 1}
	attempts := []attemptAtRestore{{WorkflowID: "e1", NodeID: "effect", Attempt: 1, Claim: "claim", Worker: "worker", Request: "request",
		Deadline: e.Attempt.Deadline.Add(5 * time.Second), State: "CLAIMED", Class: "COOPERATING_EFFECT", Current: true, Outcome: "NONE",
		Fence: e.Fence, LogicalKey: e.LogicalKey, ScopeHash: e.ScopeHash}}
	trace := invariants.Trace{Approvals: []invariants.ApprovalRecord{{WorkflowID: "e1", NodeID: "effect", IntentID: e.Intent,
		ExpectedResourceRevision: "0", Decision: "APPROVED", DispatchStatus: "GRANTED", GrantToken: "grant",
		CanonicalArgumentHash: e.CanonicalHash, GrantScopeHash: e.ScopeHash}}}
	receipt := k6receipt.Receipt{WorkflowID: "e1", IntentID: e.Intent, LogicalKey: e.LogicalKey, ResourceID: e.Resource,
		CanonicalHash: e.CanonicalHash, ScopeHash: e.ScopeHash, AppliedRevision: 1, ApplicationID: "applied"}
	sink := k6sink.Snapshot{Receipts: []k6sink.Receipt{receipt}, Allocations: []k6sink.Allocation{{IntentID: e.Intent,
		ResourceID: e.Resource, Fence: e.Fence, Attempt: 1, GrantHash: e.GrantSHA}},
		Resources: []k6sink.ResourceState{{ResourceID: e.Resource, Revision: 1, InitialStateHash: "before", FinalStateHash: "after"}},
		Journal: []k6sink.Mutation{{ResourceID: e.Resource, Revision: 1, OldStateHash: "before", StateHash: "after",
			IntentID: e.Intent, LogicalKey: e.LogicalKey}}}
	proof := k6receipt.Proof{Receipt: receipt}
	if got, err := checkPrewriteOrphan(trace, nil, attempts, e, proof, sink); err != nil || got != receipt {
		t.Fatalf("expected exact orphan join, got %+v %v", got, err)
	}
	trace.Approvals[0].DispatchStatus = "DISPATCHED"
	if _, err := checkPrewriteOrphan(trace, nil, attempts, e, proof, sink); err == nil {
		t.Fatal("accepted the source's post-result grant disposition before recovery")
	}
	trace.Approvals[0].DispatchStatus = "GRANTED"
	if _, err := checkPrewriteOrphan(trace, []nodeAtRestore{{ID: "e1", Result: json.RawMessage("{}")}}, attempts, e, proof, sink); err == nil {
		t.Fatal("accepted surviving source result")
	}
	older := slices.Clone(attempts)
	older[0].Deadline = e.Attempt.Deadline.Add(-time.Second)
	if _, err := checkPrewriteOrphan(trace, nil, older, e, proof, sink); err == nil {
		t.Fatal("accepted an older claim deadline")
	}
	sink.Journal = append(sink.Journal, sink.Journal[0])
	if _, err := checkPrewriteOrphan(trace, nil, attempts, e, proof, sink); err == nil {
		t.Fatal("accepted an unexplained second mutation")
	}
}

func TestRestoreReadsExactlyTheLossLedgerPrefix(t *testing.T) {
	header := restoreledger.Header{Schema: restoreledger.Schema, Campaign: "campaign", Drill: "r1",
		RunID: 1, RunAttempt: 1, BootID: "boot-id", Source: "source", Target: "target",
		Revision: strings.Repeat("a", 40), WorkloadSHA256: testDigest, TopologySHA256: testDigest, PolicySHA256: testDigest}
	row := struct {
		Number    int                  `json:"number"`
		Previous  string               `json:"previous_sha256"`
		Monotonic int64                `json:"monotonic_ns"`
		UTC       time.Time            `json:"utc"`
		Header    restoreledger.Header `json:"header"`
	}{1, "", 1, time.Unix(1_800_000_000, 0).UTC(), header}
	first, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	first = append(first, '\n')
	sha := restoreledger.SHA256(first)
	path := filepath.Join(t.TempDir(), "ledger.jsonl")
	if err := os.WriteFile(path, append(first, []byte("later append")...), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := readLossLedger(path, "boot-id", sha)
	if err != nil || got.Header != header || got.Digest != sha {
		t.Fatalf("source-loss prefix could not be replayed: %+v, %v", got, err)
	}
	if _, err := readLossLedger(path, "different-boot", sha); err == nil {
		t.Fatal("accepted another host boot")
	}
	if _, err := readLossLedger(path, "boot-id", restoreledger.SHA256(first[:len(first)-1])); err == nil {
		t.Fatal("accepted torn prefix")
	}
}

func TestRestoreChecksInteriorArchiveGapBeforeRunningTools(t *testing.T) {
	f := fixtureForFault(t, 12)
	cut := cutReceipt{Point: anchor{Name: "cut", Segment: segmentName(1, 3), LSN: "0/3002048", SwitchLSN: "0/3002060"},
		PreSwitch: f.sample(12, "0/3002028"), EndLSN: "0/4000000"}
	if err := os.Remove(filepath.Join(f.source.archive, segmentName(1, 2))); err != nil {
		t.Fatal(err)
	}
	// The tool adapter is nil. The archive gap must be diagnosed before it
	// can reach external tools or allocate a target.
	_, err := verifyReplayMedia(t.Context(), f.source, f.backup, cut, outageReceipt{}, "")
	if err == nil || !strings.Contains(err.Error(), "WAL_ARCHIVE_GAP") {
		t.Fatalf("missing interior segment was not rejected: %v", err)
	}
}
