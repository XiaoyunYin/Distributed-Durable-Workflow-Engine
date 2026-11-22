package k6v1

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"testing/fstest"
	"time"
)

func fixtureAt(second int64) Observer {
	return Observer{UTC(fixed.Add(time.Duration(second) * time.Second)), second * int64(time.Second)}
}

func fixturePlan(m Manifest) (Plan, Arrivals) {
	last := fixed.Add(-time.Minute)
	plan := Plan{Schema: PlanVersion, Episode: m.Episode, ResetID: m.ResetID, Namespace: m.Namespace,
		FaultMode: "control", SealedUTC: UTC(last.Add(-2 * time.Minute))}
	arrivals := Arrivals{Schema: ArrivalsVersion, Episode: m.Episode, ResetID: m.ResetID, SealedUTC: UTC(last)}
	reason := "overload"
	target := "pure.echo"
	if m.Sink != nil {
		target = "resource"
	}
	for i, name := range []string{"warmup", "measured"} {
		key := fmt.Sprintf("dur053-%s-%d-000000", name, m.Seed)
		body := SubmissionBody{DefinitionID: "definition", DefinitionVersion: 1, InitialNodeID: "pure.echo", InitialInput: []byte("{}"), Payload: []byte("{}")}
		item := Submission{Key: key, ScheduledUTC: UTC(last.Add(time.Duration(i-1) * time.Minute)), Body: body, BodySHA256: body.Digest(),
			Request: WireDigest{1, fixtureHash("request")}, Target: target, Expected: []byte("{}")}
		plan.Windows = append(plan.Windows, PlanWindow{Name: name, LoadRunID: name, Submissions: []Submission{item}})
		arrivals.Rows = append(arrivals.Rows, Arrival{Window: name, Key: key, Disposition: "unsent", UnsentReason: &reason, Attempts: []Send{}})
	}
	return plan, arrivals
}

func throughFixture(e *External, end string) {
	e.Roster.Seal.ThroughUTC = end
	e.Broker.Seal.ThroughUTC = end
	if e.Topology != nil {
		e.Topology.Seal.ThroughUTC = end
	}
	if e.Ingress != nil {
		e.Ingress.Seal.ThroughUTC = end
		e.Decisions.Seal.ThroughUTC = end
	}
}

func writeFixture(t *testing.T, bundle fstest.MapFS, path string, obj any) string {
	t.Helper()
	raw, err := Canonical(obj)
	if err != nil {
		t.Fatal(err)
	}
	bundle[path] = &fstest.MapFile{Data: raw}
	return Hash(raw)
}

func indexFixture(t *testing.T, bundle fstest.MapFS) {
	t.Helper()
	index := Index{Schema: IndexVersion}
	for _, path := range sortedKeys(bundle) {
		if path != IndexPath && path != "checker.json" {
			raw := bundle[path].Data
			index.Files = append(index.Files, File{path, int64(len(raw)), Hash(raw)})
		}
	}
	writeFixture(t, bundle, IndexPath, index)
}

func attachQuietData(t *testing.T, m Manifest, e External, pre, final int64) (fstest.MapFS, Barrier, ExternalSamples) {
	t.Helper()
	plan, arrivals := fixturePlan(m)
	throughFixture(&e, fixtureAt(final).UTC)
	// Positive complete bundles use explicit unsent arrivals and empty DB
	// cuts. The effect-traffic fixture exercises nonempty joins separately.
	bundle := mapExternal(t, e)
	m.PlanSHA256 = writeFixture(t, bundle, PlanPath, plan)
	arrivals.PlanSHA256 = m.PlanSHA256
	writeFixture(t, bundle, ArrivalsPath, arrivals)
	manifestHash := writeFixture(t, bundle, ManifestPath, m)
	b := Barrier{Schema: BarrierVersion, ManifestSHA256: manifestHash, BarrierID: m.BarrierID, ResetID: m.ResetID,
		DeadlineUTC: UTC(plan.Deadline(arrivals)), SelectedGeneration: 1}
	attempt := CutAttempt{Generation: 1, Started: fixtureAt(pre), Finished: fixtureAt(pre + 1), Reasons: []string{}}
	bracket := func(second int64) Bracket {
		at := fixtureAt(second)
		return Bracket{at, at, at.UTC}
	}
	var dbMarks []QuietDatabase
	for i, db := range m.Databases() {
		identity := m.Identity(db)
		version := SnapshotVersion
		if db == "sink" {
			version = SinkSnapshotVersion
		}
		snap := Snapshot{Schema: version, ManifestSHA256: manifestHash, BarrierID: m.BarrierID, ResetID: m.ResetID,
			Database: identity.Database, Instance: identity.Instance, DatabaseOID: uint32(i + 1), Generation: 1,
			Visibility: "1:1:", TransactionUTC: fixtureAt(pre).UTC, Open: bracket(pre), Close: bracket(pre),
			Commit: "committed", CommitAfter: fixtureAt(pre)}
		var marks []Watermark
		for _, spec := range m.Relations(db) {
			bundle[RelationPath(spec.Name)] = &fstest.MapFile{}
			empty, n := Hash(nil), int64(0)
			kind := "r"
			if spec.Name == SinkResources {
				kind = "v"
			}
			snap.Relations = append(snap.Relations, Inventory{spec.Name, spec.Keys, 0, 0, empty})
			snap.Census = append(snap.Census, CensusEntry{Name: spec.Name, Kind: kind, Classification: Exported, Count: &n, SHA256: &empty, Columns: spec.SourceColumns()})
			marks = append(marks, NewWatermark(spec).Finish())
		}
		if db == "sink" {
			n := int64(0)
			snap.Census = append(snap.Census, CensusEntry{Name: "k6_sink.resources", Kind: "r", Classification: Excluded,
				Reason: "safe_resource_projection", Count: &n, Columns: ResourceSourceColumns()})
			slices.SortFunc(snap.Census, func(a, b CensusEntry) int { return strings.Compare(a.Name, b.Name) })
		}
		snapHash := writeFixture(t, bundle, db+"/snapshot.json", snap)
		before := WatermarkSample{Terminal: true, Database: identity.Database, Instance: identity.Instance, DatabaseOID: snap.DatabaseOID,
			ResetID: m.ResetID, Visibility: snap.Visibility, Open: bracket(pre), CommitAfter: fixtureAt(pre), Relations: marks}
		after := before
		after.Open, after.CommitAfter = bracket(pre+1), fixtureAt(pre+1)
		attempt.Databases = append(attempt.Databases, DatabaseCut{db, before, marks, after, snapHash})
		dbMarks = append(dbMarks, QuietDB(db, marks))
	}
	b.Attempts = append(b.Attempts, attempt)
	writeFixture(t, bundle, BarrierPath, b)
	samples := ExternalSamples{Schema: SamplesVersion, Binding: Binding(m), Observer: "observer"}
	prefixes := e.prefixes()
	appendAt := func(sec int64, phase string) {
		at := fixtureAt(sec)
		var marks []ExternalMark
		for _, path := range sortedKeys(prefixes) {
			p := prefixes[path]
			idx := 0
			for idx+1 < len(p.utc) && p.utc[idx+1] <= at.UTC {
				idx++
			}
			mark := p.marks[idx]
			mark.ThroughUTC = min(at.UTC, p.seal.ThroughUTC)
			mark.SealMode = p.seal.Mode
			if isSessionPath(path) && int64(idx) < p.seal.Last {
				mark.SealMode = "checkpoint"
			}
			marks = append(marks, mark)
		}
		samples.Rows = append(samples.Rows, QuietSample{int64(len(samples.Rows) + 1), 1, phase, at, true, marks, dbMarks})
	}
	for sec := int64(0); sec < pre; sec += 2 {
		appendAt(sec, "quiet")
	}
	appendAt(pre, "pre")
	appendAt(pre+1, "post")
	appendAt(final, "export")
	if err := samples.SealAt(fixtureAt(final).UTC); err != nil {
		t.Fatal(err)
	}
	writeFixture(t, bundle, SamplesPath, samples)
	indexFixture(t, bundle)
	return bundle, b, samples
}

func quietFixture(t *testing.T, effect bool) (Manifest, External) {
	t.Helper()
	m, e, _ := fixtureExternal(t, effect)
	first := e.Sessions[firstSession]
	first.Rows = first.Rows[:1]
	resealFixture(&first)
	e.Sessions[firstSession] = first
	if effect {
		e.Decisions.Rows, e.Ingress.Rows = e.Decisions.Rows[:0], e.Ingress.Rows[:0]
		resealFixture(e.Decisions)
		resealFixture(e.Ingress)
	}
	return m, e
}

func TestK6QuietPositiveBundles(t *testing.T) {
	for _, effect := range []bool{false, true} {
		m, e := quietFixture(t, effect)
		bundle, _, _ := attachQuietData(t, m, e, 30, 32)
		r := Verify(bundle)
		if len(r.Findings) != 0 || r.ExitCode() != Incomplete {
			t.Fatalf("clean complete fixture: %+v", r.Findings)
		}
		paths := append(requiredExternal(m), SessionPath(deadSession), SessionPath(idleSession), SessionPath(restartedSession))
		for _, path := range paths {
			t.Run(path, func(t *testing.T) {
				saved := bundle[path]
				delete(bundle, path)
				defer func() { bundle[path] = saved }()
				wantExternal(t, Verify(bundle), "required_proof_missing:"+path, path)
			})
		}
	}
}

func withActivity(e *External, sec int64, kind string) {
	l := e.Sessions[firstSession]
	l.Rows = append(l.Rows, Record[WorkerRow]{Position{2, fixtureAt(sec).UTC}, WorkerRow{Kind: kind}})
	l.Seal.ThroughUTC = fixtureAt(sec).UTC
	resealFixture(&l)
	e.Sessions[firstSession] = l
}

func TestK6QuietRestart(t *testing.T) {
	m, e := quietFixture(t, false)
	s := e.Sessions[firstSession]
	a := AttemptIdentity{"workflow", "node", 0, 1, fixtureHash("claim"), "request"}
	s.Rows = append(s.Rows, Record[WorkerRow]{Position{2, fixtureAt(14).UTC}, WorkerRow{Kind: "start", Invocation: "inv", Attempt: &a}},
		Record[WorkerRow]{Position{3, fixtureAt(14).UTC}, WorkerRow{Kind: "finish", Invocation: "inv", Attempt: &a}})
	s.Seal.ThroughUTC = fixtureAt(14).UTC
	resealFixture(&s)
	e.Sessions[firstSession] = s
	for _, pre := range []int64{30, 44} {
		_, b, samples := attachQuietData(t, m, e, pre, pre+2)
		throughFixture(&e, fixtureAt(pre+2).UTC)
		p, a := fixturePlan(m)
		issues := verifyExternalQuiet(m, p, a, b, e, samples, nil)
		want := pre == 30
		if slices.ContainsFunc(issues, func(f Finding) bool { return f.Code == "EXTERNAL_QUIET_WINDOW" }) != want || !want && len(issues) != 0 {
			t.Fatalf("quiet restart at %d: %+v", pre, issues)
		}
	}
}

func TestK6QuietBetweenCuts(t *testing.T) {
	m, e := quietFixture(t, false)
	for _, kind := range []string{"idle_probe", "lease_renewal"} {
		t.Run(kind, func(t *testing.T) {
			m, e := quietFixture(t, false)
			withActivity(&e, 31, kind)
			bundle, _, _ := attachQuietData(t, m, e, 30, 32)
			if issues := Verify(bundle).Findings; len(issues) != 0 {
				t.Fatalf("idle progress raced the cut: %+v", issues)
			}
		})
	}
	m, e = quietFixture(t, false)
	next := e.Broker.Rows[0]
	next.Position = Position{2, fixtureAt(31).UTC}
	next.Data.Generation++
	next.Data.Members = slices.Clone(next.Data.Members)
	next.Data.Members[0].Generation++
	e.Broker.Rows = append(e.Broker.Rows, next)
	bundle, _, _ := attachQuietData(t, m, e, 30, 32)
	wantExternal(t, Verify(bundle), "EXTERNAL_CUT_ACTIVITY", SamplesPath)
}

func TestK6QuietRegression(t *testing.T) {
	m, e := quietFixture(t, false)
	withActivity(&e, 12, "idle_probe")
	bundle, _, samples := attachQuietData(t, m, e, 30, 32)
	marks := slices.Clone(samples.Rows[5].Marks)
	for i := range marks {
		if marks[i].Path != SessionPath(firstSession) {
			marks[i].ThroughUTC = samples.Rows[7].At.UTC
		}
	}
	samples.Rows[7].Marks = marks
	_ = samples.SealAt(fixtureAt(32).UTC)
	writeFixture(t, bundle, SamplesPath, samples)
	indexFixture(t, bundle)
	wantExternal(t, Verify(bundle), "EXTERNAL_REGRESSION", SessionPath(firstSession))
}

func TestK6QuietMutableFence(t *testing.T) {
	m, e := quietFixture(t, false)
	bundle, _, samples := attachQuietData(t, m, e, 30, 32)
	for i := range samples.Rows[0].Marks {
		if samples.Rows[0].Marks[i].Path == BrokerPath {
			samples.Rows[0].Marks[i].SealMode = "checkpoint"
		}
	}
	_ = samples.SealAt(fixtureAt(32).UTC)
	writeFixture(t, bundle, SamplesPath, samples)
	indexFixture(t, bundle)
	wantExternal(t, Verify(bundle), "EXTERNAL_MUTABLE_WRITER_UNFENCED", SamplesPath)
}

func TestK6IngressResealedWithProducerPass(t *testing.T) {
	m, e := quietFixture(t, true)
	_, nonempty, _ := fixtureExternal(t, true)
	e.Ingress.Rows = nonempty.Ingress.Rows[:1]
	e.Ingress.Rows[0].Data.Acknowledged = false
	resealFixture(e.Ingress)
	bundle, _, _ := attachQuietData(t, m, e, 30, 32)
	bundle["checker.json"] = &fstest.MapFile{Data: []byte(`{"status":"PASS"}`)}
	result := Verify(bundle)
	wantExternal(t, result, "INGRESS_UNACKNOWLEDGED", IngressPath)
	if result.Safety != "FAIL" || result.Labels[7].Outcome != "covered/fail" {
		t.Fatalf("resealed raw contradiction escaped: %+v", result)
	}
}
