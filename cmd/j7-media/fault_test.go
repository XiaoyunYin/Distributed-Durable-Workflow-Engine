package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/j7markers"
	"durable-agent-execution-engine/internal/restoreledger"
)

const testOrigin = int64(100 * time.Second)

var testDigest = strings.Repeat("a", 64)

type faultFixture struct {
	backup backupReceipt
	plan   j7markers.Plan
	report restoreledger.Report
	source mediaSource
	clock  atomic.Int64
}

func fixtureForFault(t *testing.T, count int) *faultFixture {
	t.Helper()
	archive := t.TempDir()
	for index := uint64(1); index <= 3; index++ {
		testSegment(t, archive, index, walSegmentBytes)
	}
	files, err := readArchive(archive, []string{segmentName(1, 1), segmentName(1, 2)})
	if err != nil {
		t.Fatal(err)
	}
	f := &faultFixture{backup: backupReceipt{
		Source: sourceState{Version: serverVersion, SystemID: 42, Timeline: 1, WALLevel: "replica",
			ArchiveMode: "on", ArchiveTimeout: "0", SegmentBytes: walSegmentBytes, Fsync: "on",
			SyncCommit: "on", FullPageWrites: "on", HasArchiveCmd: true,
			DataDirectory: "/var/lib/postgresql/18/docker", DatabaseBytes: 25},
		RequiredWAL: []walRange{{Timeline: 1, Start: "0/1000028", End: "0/1001000"}},
		Anchors:     []anchor{{LSN: "0/1001048"}, {LSN: "0/2001048"}},
		Archive:     files,
	}}
	f.plan, err = j7markers.NewPlan(j7markers.Fixture{Campaign: "campaign", Drill: "r1", Namespace: "j7",
		DefinitionID: "marker", DefinitionSHA256: testDigest})
	if err != nil {
		t.Fatal(err)
	}
	digest, err := f.plan.Digest()
	if err != nil {
		t.Fatal(err)
	}
	f.report = restoreledger.Report{Header: restoreledger.Header{
		BootID: "boot-1", Campaign: f.plan.Fixture.Campaign, Drill: f.plan.Fixture.Drill, Source: "source-r1",
		MarkersSHA256: digest, RunID: 123, RunAttempt: 1,
	}, Digest: testDigest}
	f.report.Witnesses = append(f.report.Witnesses, testPhaseWitness("pre-cut-cohorts", testOrigin-10))
	for _, marker := range f.plan.Markers[:count] {
		at := testOrigin + marker.OffsetNS
		f.report.Exchanges = append(f.report.Exchanges, restoreledger.Exchange{
			Operation: restoreledger.Operation{RequestID: marker.RequestID, Kind: restoreledger.Submit,
				Phase: "markers", Authority: f.report.Header.Source, Sequence: marker.Sequence,
				WorkflowID: marker.WorkflowID, Namespace: f.plan.Fixture.Namespace,
				SubmissionKey: marker.SubmissionKey, RequestSHA256: marker.RequestSHA256, ScheduledNS: at},
			Outcome: &restoreledger.Outcome{Disposition: restoreledger.Acknowledged, SentNS: at,
				CompletedNS: at + 10, PayloadHash: "sub-v1:" + testDigest},
			OutcomeSync: at + 15,
		})
	}
	f.clock.Store(f.report.Exchanges[count-1].OutcomeSync + 10)
	f.source = mediaSource{archive: archive, pgBin: "bin", service: "source",
		now:     func() (int64, error) { return f.clock.Add(int64(time.Millisecond)), nil },
		syncDir: func(string) error { return nil },
	}
	return f
}

func testPhaseWitness(phase string, at int64) restoreledger.RecordedWitness {
	return restoreledger.RecordedWitness{Witness: restoreledger.Witness{
		Kind: restoreledger.StateProbe, Phase: phase, Authority: "source-r1",
		StartedNS: at - 1, CompletedNS: at, EvidenceSHA: testDigest,
	}, RecordedNS: at}
}

func testSegment(t *testing.T, dir string, index uint64, bytes int64) {
	t.Helper()
	f, err := os.Create(filepath.Join(dir, segmentName(1, index)))
	if err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(f.Truncate(bytes), f.Close()); err != nil {
		t.Fatal(err)
	}
}

func (f *faultFixture) sample(count int, flush string) sourceSample {
	s := sourceSample{SystemID: f.backup.Source.SystemID, Timeline: 1, DataDirectory: f.backup.Source.DataDirectory,
		FlushLSN: flush, InsertLSN: flush, DatabaseBytes: 25, ClusterBytes: 50, WALBytes: 3 * walSegmentBytes}
	for _, marker := range f.plan.Markers[:count] {
		s.Markers = append(s.Markers, markerSeen{ID: marker.WorkflowID, State: "SUCCEEDED", Matches: true})
	}
	return s
}

func TestPhysicalCut(t *testing.T) {
	cases := []struct {
		name, fail string
		change     func(*testing.T, *faultFixture)
	}{
		{name: "archive all required segments"},
		{name: "expired before SQL", fail: "phase deadline", change: func(t *testing.T, f *faultFixture) {
			f.clock.Add(int64(3 * time.Second))
		}},
		{name: "held cohorts absent", fail: "pre-cut-cohorts", change: func(t *testing.T, f *faultFixture) {
			f.report.Witnesses = nil
		}},
		{name: "archived interior gap", fail: "WAL_ARCHIVE_GAP", change: func(t *testing.T, f *faultFixture) {
			if err := os.Remove(filepath.Join(f.source.archive, segmentName(1, 2))); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "unexpected future segment", fail: "unexpected WAL beyond cut", change: func(t *testing.T, f *faultFixture) {
			testSegment(t, f.source.archive, 4, walSegmentBytes)
		}},
		{name: "partial segment", fail: "incomplete", change: func(t *testing.T, f *faultFixture) {
			testSegment(t, f.source.archive, 3, walSegmentBytes-1)
		}},
		{name: "changed backup WAL", fail: "prepared archive segment changed", change: func(t *testing.T, f *faultFixture) {
			f.backup.Archive[0].SHA256 = testDigest
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := fixtureForFault(t, 12)
			if tc.change != nil {
				tc.change(t, f)
			}
			calls := 0
			f.source.run = func(_ context.Context, tool string, args ...string) ([]byte, error) {
				calls++
				if filepath.Base(tool) != "psql" {
					t.Fatalf("unexpected command %q", tool)
				}
				switch calls {
				case 1:
					if !strings.Contains(args[len(args)-1], "n.input = expected->'input'") {
						t.Fatal("probe did not check archived marker inputs")
					}
					return json.Marshal(f.sample(12, "0/3002028"))
				case 2:
					if !strings.Contains(args[len(args)-2], "pg_create_restore_point") ||
						!strings.Contains(args[len(args)-1], "pg_switch_wal()") {
						t.Fatal("expected one restore point and switch, in order")
					}
					return []byte("{\"lsn\":\"0/3002048\",\"segment\":\"" + segmentName(1, 3) + "\"}\n0/3002060\n"), nil
				default:
					t.Fatal("unexpected second restore point or WAL switch")
					return nil, nil
				}
			}
			r, err := closeBoundary(t.Context(), f.source, f.backup, testDigest, f.plan, f.report, testOrigin)
			if tc.fail != "" {
				if err == nil || !strings.Contains(err.Error(), tc.fail) || r.CompletedNS != 0 {
					t.Fatalf("cut %+v, error %v, want %s", r, err, tc.fail)
				}
				if (tc.fail == "phase deadline" || tc.fail == "pre-cut-cohorts") && calls != 0 {
					t.Fatal("unarmed cut touched PostgreSQL")
				}
				return
			}
			if err != nil || r.EndLSN != "0/4000000" || len(r.Inventory) != 3 || calls != 2 ||
				r.Point.Name == "" || r.PreSwitch.Markers[7].State != "SUCCEEDED" {
				t.Fatalf("incomplete successful cut %+v, %v", r, err)
			}
		})
	}
}

type dryDocker struct {
	f        *faultFixture
	cfg      faultConfig
	state    sourceContainer
	volume   sourceVolume
	calls    []string
	killed   atomic.Bool
	removed  bool
	gone     bool
	mutation func(*dryDocker)
	afterRM  func()
}

func makeDryDocker(f *faultFixture) *dryDocker {
	d := &dryDocker{f: f, cfg: faultConfig{Container: strings.Repeat("c", 64),
		SourceVolume: "j7-r1-source", TargetVolume: "j7-r1-target"}}
	d.volume = sourceVolume{Name: d.cfg.SourceVolume, Driver: "local", Scope: "local",
		Mountpoint: "/var/lib/docker/volumes/j7-r1-source/_data", CreatedAt: "2026-01-01T12:00:00Z",
		Labels: faultLabels(f.report.Header)}
	d.state.ID, d.state.Image = d.cfg.Container, "sha256:"+testDigest
	d.state.Config.Labels = faultLabels(f.report.Header)
	d.state.HostConfig.RestartPolicy.Name = "no"
	d.state.State = containerState{Running: true, Status: "running", Pid: 1, StartedAt: "2026-01-01T12:01:00Z"}
	d.state.Mounts = []dockerMount{
		{Type: "volume", Name: d.cfg.SourceVolume, Source: d.volume.Mountpoint,
			Destination: "/var/lib/postgresql", Driver: "local", RW: true},
		{Type: "bind", Source: f.source.archive, Destination: archiveDest, RW: true},
	}
	return d
}

func (d *dryDocker) run(_ context.Context, tool string, args ...string) ([]byte, error) {
	if tool != "docker" || args[0] != "--host=unix:///var/run/docker.sock" {
		return nil, errors.New("expected local Docker command")
	}
	d.calls = append(d.calls, strings.Join(args[1:], " "))
	switch strings.Join(args[1:3], " ") {
	case "container inspect":
		return json.Marshal([]sourceContainer{d.state})
	case "volume inspect":
		return json.Marshal([]sourceVolume{d.volume})
	case "container exec":
		switch args[4] {
		case "cat":
			return []byte("postgres\n"), nil
		case "realpath":
			return []byte(d.f.backup.Source.DataDirectory), nil
		}
	case "volume ls":
		if d.gone {
			return nil, nil
		}
		return []byte(d.cfg.SourceVolume), nil
	case "container ls":
		if d.removed {
			return nil, nil
		}
		return []byte(d.cfg.Container), nil
	case "container kill":
		if len(args) != 5 || args[3] != "--signal=SIGKILL" || args[4] != d.cfg.Container {
			return nil, errors.New("unsafe kill")
		}
		d.killed.Store(true)
		d.state.State.Running, d.state.State.ExitCode, d.state.State.Pid = false, 137, 0
		d.state.State.Status, d.state.State.FinishedAt = "exited", "2026-01-01T12:03:00Z"
		if d.mutation != nil {
			d.mutation(d)
		}
		return []byte(d.cfg.Container), nil
	case "container rm":
		if len(args) != 4 || args[3] != d.cfg.Container || !d.killed.Load() {
			return nil, errors.New("unsafe container removal")
		}
		d.removed = true
		return []byte(d.cfg.Container), nil
	case "volume rm":
		if len(args) != 4 || args[3] != d.cfg.SourceVolume || !d.removed {
			return nil, errors.New("unsafe volume removal")
		}
		d.gone = true
		if d.afterRM != nil {
			d.afterRM()
		}
		return []byte(d.cfg.SourceVolume), nil
	}
	return nil, fmt.Errorf("unexpected Docker command %v", args)
}

type testExit int

func (e testExit) Error() string { return fmt.Sprintf("test subprocess exit %d", e) }
func (e testExit) ExitCode() int { return int(e) }

func TestSourceLoss(t *testing.T) {
	cases := []struct {
		name, fail     string
		change         func(*testing.T, *faultFixture, *dryDocker, *sourceSample, *int, *cutReceipt)
		kills, deletes bool
	}{
		{name: "verified kill, remove and archive seal", kills: true, deletes: true},
		{name: "unarchived tail crossed into later segment", fail: "single next open segment",
			change: func(_ *testing.T, _ *faultFixture, _ *dryDocker, s *sourceSample, _ *int, _ *cutReceipt) {
				s.InsertLSN = "0/5000028"
			}},
		{name: "source effect result absent", fail: "cooperating result",
			change: func(_ *testing.T, _ *faultFixture, _ *dryDocker, s *sourceSample, _ *int, _ *cutReceipt) {
				s.EffectSHA256 = ""
			}},
		{name: "retained sink receipt witness missing", fail: "post-cut-effect",
			change: func(_ *testing.T, f *faultFixture, _ *dryDocker, _ *sourceSample, _ *int, _ *cutReceipt) {
				f.report.Witnesses = append(f.report.Witnesses[:2], f.report.Witnesses[3:]...)
			}},
		{name: "writer freeze missing", fail: "writers-frozen",
			change: func(_ *testing.T, f *faultFixture, _ *dryDocker, _ *sourceSample, _ *int, _ *cutReceipt) {
				f.report.Witnesses = f.report.Witnesses[:len(f.report.Witnesses)-1]
			}},
		{name: "M15 kill window expired", fail: "phase deadline",
			change: func(_ *testing.T, f *faultFixture, _ *dryDocker, _ *sourceSample, _ *int, _ *cutReceipt) {
				f.clock.Add(int64(killWindow))
			}},
		{name: "SQL error is not unavailability", fail: "connection error", kills: true,
			change: func(_ *testing.T, _ *faultFixture, _ *dryDocker, _ *sourceSample, status *int, _ *cutReceipt) {
				*status = 3
			}},
		{name: "sealed volume was replaced", fail: "changed before deletion", kills: true,
			change: func(_ *testing.T, _ *faultFixture, d *dryDocker, _ *sourceSample, _ *int, _ *cutReceipt) {
				d.mutation = func(d *dryDocker) { d.volume.CreatedAt = "2026-01-01T12:03:00Z" }
			}},
		{name: "source OOM is not the expected fault", fail: "expected SIGKILL", kills: true,
			change: func(_ *testing.T, _ *faultFixture, d *dryDocker, _ *sourceSample, _ *int, _ *cutReceipt) {
				d.mutation = func(d *dryDocker) { d.state.State.OOMKilled = true }
			}},
		{name: "extra WAL after loss invalidates cut", fail: "unexpected WAL beyond cut", kills: true, deletes: true,
			change: func(t *testing.T, f *faultFixture, d *dryDocker, _ *sourceSample, _ *int, _ *cutReceipt) {
				d.afterRM = func() { testSegment(t, f.source.archive, 4, walSegmentBytes) }
			}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := fixtureForFault(t, 15)
			d := makeDryDocker(f)
			docker := localDocker{run: d.run}
			binding, err := docker.bind(t.Context(), f.source, d.cfg, f.report.Header, f.backup.Source, testDigest)
			if err != nil {
				t.Fatal(err)
			}
			binding.ObservedNS = testOrigin - 100
			d.calls = nil
			m12 := f.report.Exchanges[11]
			inventory, err := inventory(f.source.archive, f.backup, 3)
			if err != nil {
				t.Fatal(err)
			}
			cut := cutReceipt{Schema: "j7.archive-cut.v1", BootID: f.report.Header.BootID, Source: f.report.Header.Source,
				BackupSHA256: testDigest, LedgerSHA256: testDigest,
				AckNS: m12.OutcomeSync, StartedNS: m12.OutcomeSync + 10, CompletedNS: m12.OutcomeSync + 100,
				PreSwitch: f.sample(12, "0/3002028"), Inventory: inventory, EndLSN: "0/4000000",
				Point: anchor{LSN: "0/3002048", Segment: segmentName(1, 3), SwitchLSN: "0/3002060"}}
			cutProof := archiveCutWitness(cut, testDigest, m12.Operation.RequestID)
			f.report.Witnesses = append(f.report.Witnesses,
				restoreledger.RecordedWitness{Witness: cutProof, RecordedNS: cutProof.CompletedNS + 10},
				testPhaseWitness("post-cut-effect", cutProof.CompletedNS+20),
				testPhaseWitness("writers-frozen", f.report.Exchanges[14].OutcomeSync+1))
			last := f.sample(15, "0/4003000")
			last.EffectSHA256 = testDigest
			sqlStatus := 2
			if tc.change != nil {
				tc.change(t, f, d, &last, &sqlStatus, &cut)
			}
			f.source.run = func(_ context.Context, tool string, _ ...string) ([]byte, error) {
				if !d.killed.Load() {
					return json.Marshal(last)
				}
				if filepath.Base(tool) == "pg_isready" {
					return nil, testExit(2)
				}
				return nil, testExit(sqlStatus)
			}
			r, err := loseSource(t.Context(), f.source, docker, d.cfg, f.backup, testDigest,
				binding, testDigest, cut, testDigest, f.plan, f.report, testOrigin, "e1")
			if tc.fail == "" {
				if err != nil || r.T0NS != max(r.Exit.ObservedNS, r.SQLDown.NoResponseNS) ||
					r.T0NS <= r.KillNS || r.VolumeAbsent <= r.T0NS || r.ArchiveSealNS <= r.VolumeAbsent ||
					r.FlushPastCut != 0x3000 || len(r.Inventory) != 3 {
					t.Fatalf("incomplete source loss %+v, %v", r, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.fail) || r.ArchiveSealNS != 0 {
				t.Fatalf("source loss %+v, error %v, want %s", r, err, tc.fail)
			}
			if d.killed.Load() != tc.kills || d.gone != tc.deletes || d.removed != tc.deletes {
				t.Fatalf("inappropriate Docker mutation: %v", d.calls)
			}
		})
	}
}

func TestOwnedMountsRejectWrongFaultTarget(t *testing.T) {
	f := fixtureForFault(t, 12)
	for _, name := range []string{"foreign label", "restart policy", "wrong postgres directory", "nested mount"} {
		t.Run(name, func(t *testing.T) {
			d := makeDryDocker(f)
			switch name {
			case "foreign label":
				d.volume.Labels["j7.restore.run"] = "321"
			case "restart policy":
				d.state.HostConfig.RestartPolicy.Name = "always"
			case "wrong postgres directory":
				d.state.Mounts[0].Destination = "/unrelated"
			case "nested mount":
				d.state.Mounts = append(d.state.Mounts, dockerMount{Type: "bind",
					Destination: f.backup.Source.DataDirectory + "/pg_wal"})
			}
			if _, err := (localDocker{run: d.run}).bind(t.Context(), f.source, d.cfg,
				f.report.Header, f.backup.Source, testDigest); err == nil {
				t.Fatal("accepted unsafe source fault binding")
			}
		})
	}
}
