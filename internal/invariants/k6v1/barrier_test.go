package k6v1

import (
	"encoding/json"
	"slices"
	"testing"
	"testing/fstest"
	"time"
)

func TestK6CutWatermarkClasses(t *testing.T) {
	d := Hash(nil)
	for _, tc := range []struct {
		name, db, relation string
	}{
		{"AppendOnly", "sink", SinkBindings},
		{"Quiescent", "sink", SinkResources},
		{"IdleChurn", "engine", "engine.partition_leases"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := Relation{Name: tc.relation, Keys: []string{"key"}, Columns: []Column{{Name: "key", Kind: Text}}}
			a := Watermark{Relation: tc.relation, Count: 1, MaxKey: `["same-key"]`, Digest: d, SourceDigest: d}
			b := a
			b.Digest = Hash([]byte("change without a count or key change"))
			for _, form := range []DatabaseCut{
				{DB: tc.db, Pre: WatermarkSample{Relations: []Watermark{a}}, Cut: []Watermark{b}, Post: WatermarkSample{Relations: []Watermark{b}}},
				{DB: tc.db, Pre: WatermarkSample{Relations: []Watermark{a}}, Cut: []Watermark{a}, Post: WatermarkSample{Relations: []Watermark{b}}},
			} {
				if got := form.RaceReasons([]Relation{spec}); !slices.Equal(got, []string{"CUT_RACED:" + tc.db + ":" + tc.relation}) {
					t.Fatalf("watermark change escaped %s: %v", tc.name, got)
				}
			}
		})
	}
}

func TestK6CutRenewalAuditPrefix(t *testing.T) {
	spec := relation(DeadlineWrites, "id", "id:int branch:text")
	makeMark := func(kinds ...string) Watermark {
		w := NewWatermark(spec)
		for i, kind := range kinds {
			w.Add(Row{"id": json.Number([]string{"1", "2", "3"}[i]), "branch": kind}, "")
		}
		return w.Finish()
	}
	start := makeMark("claim")
	renewed := makeMark("claim", "renewal", "renewal")
	for _, tc := range []struct {
		name string
		post Watermark
		ok   bool
	}{
		{"renewals", renewed, true},
		{"new activity", makeMark("claim", "renewal", "lease-acquire"), false},
		{"rewritten prefix", makeMark("dispatch", "renewal", "renewal"), false},
		{"regressed", makeMark(), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pair := DatabaseCut{DB: "engine", Pre: WatermarkSample{Relations: []Watermark{start}},
				Cut: []Watermark{start}, Post: WatermarkSample{Relations: []Watermark{tc.post}}}
			if got := pair.RaceReasons([]Relation{spec}); (len(got) == 0) != tc.ok {
				t.Fatalf("unexpected renewal audit result: %v", got)
			}
		})
	}
}

func syntheticBarrier(t *testing.T) (Manifest, Plan, Arrivals, Barrier, map[string]Snapshot, map[string]File) {
	t.Helper()
	m := Manifest{Database: "engine", EngineInstance: "one", Sink: &DatabaseIdentity{"sink", "two"}, ResetID: "reset"}
	origin := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	at := func(n int64) Observer {
		return Observer{UTC(origin.Add(time.Duration(n) * time.Second)), n * int64(time.Second)}
	}
	bracket := func(n int64) Bracket { return Bracket{at(n), at(n + 1), at(n).UTC} }
	p := Plan{FaultMode: "control", SealedUTC: at(0).UTC, Windows: []PlanWindow{{Submissions: []Submission{{ScheduledUTC: at(1).UTC}}}}}
	a := Arrivals{SealedUTC: at(2).UTC}
	b := Barrier{DeadlineUTC: UTC(p.Deadline(a)), SelectedGeneration: 1}
	try := CutAttempt{Generation: 1, Started: at(3), Finished: at(20), Reasons: []string{}}
	snaps, files := map[string]Snapshot{}, map[string]File{}
	for j, db := range m.Databases() {
		d := DatabaseCut{DB: db, SnapshotSHA256: Hash(nil)}
		for _, spec := range m.Relations(db) {
			d.Cut = append(d.Cut, NewWatermark(spec).Finish())
		}
		identity := m.Identity(db)
		sample := func(offset int64) WatermarkSample {
			return WatermarkSample{Database: identity.Database, Instance: identity.Instance, DatabaseOID: 1, ResetID: m.ResetID,
				Visibility: "1:1:", Open: bracket(offset), CommitAfter: at(offset + 1), Relations: d.Cut}
		}
		start := 2 * int64(j)
		d.Pre, d.Post = sample(4+start), sample(14+start)
		try.Databases = append(try.Databases, d)
		snaps[db] = Snapshot{Generation: 1, DatabaseOID: 1, Open: bracket(8 + start), CommitAfter: at(9 + start)}
		files[db+"/snapshot.json"] = File{SHA256: Hash(nil)}
	}
	b.Attempts = []CutAttempt{try}
	if got := verifyBarrier(m, p, a, b, snaps, nil, files); len(got) != 0 {
		t.Fatalf("synthetic barrier: %v", got)
	}
	return m, p, a, b, snaps, files
}

func TestK6CutRetryGenerationBinding(t *testing.T) {
	m, p, a, b, snaps, files := syntheticBarrier(t)
	sink := snaps["sink"]
	sink.Generation++
	snaps["sink"] = sink
	// The wrong descriptor's hash was indexed and repeated by the pair.
	issues := verifyBarrier(m, p, a, b, snaps, nil, files)
	if !slices.ContainsFunc(issues, func(f Finding) bool { return f.Code == "CUT_GENERATION:sink" }) {
		t.Fatalf("mixed generation accepted: %+v", issues)
	}
}

func TestK6CutHistoricalDeadline(t *testing.T) {
	m, p, a, b, snaps, files := syntheticBarrier(t)
	b.Attempts[0].Finished = Observer{UTC(p.Deadline(a).Add(time.Nanosecond)), 1000 * int64(time.Second)}
	b.Attempts[0].Reasons = []string{"CUT_DEADLINE"}
	issues := verifyBarrier(m, p, a, b, snaps, nil, files)
	if !slices.ContainsFunc(issues, func(f Finding) bool { return f.Code == "CUT_DEADLINE" }) {
		t.Fatalf("late historical cut accepted: %+v", issues)
	}
}

func TestK6CensusSink(t *testing.T) {
	entry := CensusEntry{Name: "k6_sink.foreign_relation", Kind: "r", Classification: Unclassified, Reason: "outside_v1_census"}
	issue := verifyCensus([]CensusEntry{entry}, SinkRelations(), nil, false, "sink")
	if issue == nil || issue.Code != "RELATION_UNCLASSIFIED" {
		t.Fatalf("sink census omitted foreign relation: %+v", issue)
	}
}

func TestK6ColumnCensus(t *testing.T) {
	spec := relation("engine.test", "id", "id:int")
	n, hash := int64(0), Hash(nil)
	entry := CensusEntry{Name: spec.Name, Kind: "r", Classification: Exported, Count: &n, SHA256: &hash}
	entry.Columns = []string{"extra", "id"}
	issue := verifyCensus([]CensusEntry{entry}, []Relation{spec}, []Inventory{{Relation: spec.Name, SHA256: hash}}, false, "engine")
	if issue == nil || issue.Code != "COLUMN_UNCLASSIFIED" {
		t.Fatalf("column census admitted drift: %+v", issue)
	}
}

func TestK6SourcePureAbsence(t *testing.T) {
	hash := Hash(nil)
	m := Manifest{Schema: EvidenceVersion, Campaign: "test", Job: "test", RunID: "one", Episode: "hook-0-0",
		Ordinal: 0, Seed: 530001, Cell: "hook-0", Workload: "PURE", Namespace: "ns", ResetID: "reset", BarrierID: "barrier",
		Database: "db", EngineInstance: "instance", RegistrySHA256: RegistrySHA256, PlanSHA256: hash,
		SourceTreeSHA256: hash, ImageSetSHA256: hash, FreezeSHA256: hash, TopologySHA256: hash,
		Instrumentation: []string{}, RequiredSources: RequiredSources(false, false)}
	raw, err := Canonical(m)
	if err != nil {
		t.Fatal(err)
	}
	for _, indexed := range []bool{false, true} {
		index := Index{Schema: IndexVersion, Files: []File{{ManifestPath, int64(len(raw)), Hash(raw)}}}
		bundle := fstest.MapFS{ManifestPath: &fstest.MapFile{Data: raw}}
		if indexed {
			index.Files = append(index.Files, File{Path: SinkSnapshotPath, SHA256: hash})
		} else {
			bundle["sink/unchecked"] = &fstest.MapFile{Data: []byte("present")}
		}
		seal, err := Canonical(index)
		if err != nil {
			t.Fatal(err)
		}
		bundle[IndexPath] = &fstest.MapFile{Data: seal}
		issues := Verify(bundle).Findings
		if len(issues) == 0 || issues[0].Code != "PURE_SINK_PRESENT" {
			t.Fatalf("PURE sink entry accepted: %+v", issues)
		}
	}
}

func TestK6SourceOversizedRow(t *testing.T) {
	data := make([]byte, 1<<20)
	_, issue := verifyRows(fstest.MapFS{"row": &fstest.MapFile{Data: data}}, Relation{}, File{Path: "row"}, 1)
	if issue == nil || issue.Code != "RELATION_ROW_TOO_LARGE" {
		t.Fatalf("oversized row misclassified: %+v", issue)
	}
}

func TestK6SubmissionPartialReasons(t *testing.T) {
	m := Manifest{Cell: "hook-0", Workload: "PURE"}
	rows := map[string][]Row{executions: {}, history: {}, "engine.workflow_definitions": {}}
	r := evaluate(m, Snapshot{}, rows, Plan{}, Arrivals{}, true, true, "", nil)
	for _, i := range []int{0, 1, 10} {
		if label := r.Labels[i]; label.Reason != "predicate_partial:G2a" || label.Outcome != "NOT COVERED" {
			t.Fatalf("clean partial predicate claimed coverage: %+v", label)
		}
	}
}
