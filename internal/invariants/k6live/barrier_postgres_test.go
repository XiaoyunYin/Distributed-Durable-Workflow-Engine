package k6live

import (
	"context"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/invariants/k6v1"
	"durable-agent-execution-engine/migrations"
	"github.com/jackc/pgx/v5"
)

func pairDatabase(t *testing.T, raw, name string, migrations fs.FS, instance string) (*pgx.Conn, string) {
	t.Helper()
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	db := pgx.Identifier{name}.Sanitize()
	runSQL(t, admin, "CREATE DATABASE "+db+" TEMPLATE template0")
	dsn, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	dsn.Path, dsn.RawPath = "/"+name, ""
	conn, err := pgx.Connect(ctx, dsn.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close(ctx)
		runSQL(t, admin, "DROP DATABASE "+db+" WITH (FORCE)")
		_ = admin.Close(ctx)
	})
	entries, err := fs.ReadDir(migrations, ".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".up.sql") {
			continue
		}
		raw, err := fs.ReadFile(migrations, entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		runSQL(t, conn, string(raw))
	}
	runSQL(t, conn, "COMMENT ON DATABASE "+db+" IS 'dur053.k6.v1|"+instance+"|reset'")
	return conn, dsn.String()
}

func runSQL(t *testing.T, conn *pgx.Conn, sql string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := conn.Exec(ctx, sql); err != nil {
		t.Fatal(err)
	}
}

func newPair(t *testing.T) (*pgx.Conn, *pgx.Conn, Config) {
	t.Helper()
	rawEngine, rawSink := os.Getenv("DURABLE_DATABASE_URL"), os.Getenv("DURABLE_K6_SINK_TEST_URL")
	if rawEngine == "" || rawSink == "" {
		if os.Getenv("DURABLE_REQUIRE_K6_PAIR") == "1" {
			t.Fatal("separate engine and sink test URLs are required")
		}
		t.Skip("paired PostgreSQL URLs not set")
	}
	suffix := strings.ReplaceAll(time.Now().Format("150405.000000000"), ".", "_")
	engine, engDSN := pairDatabase(t, rawEngine, "pairengine_"+suffix, migrations.SQL, "engine-test")
	sink, sinkDSN := pairDatabase(t, rawSink, "pairsink_"+suffix, os.DirFS("../../../sink-migrations"), "sink-test")
	ctx := context.Background()
	var engineID, sinkID string
	for _, sample := range []struct {
		conn *pgx.Conn
		id   *string
	}{{engine, &engineID}, {sink, &sinkID}} {
		if err := sample.conn.QueryRow(ctx, "SELECT system_identifier::text FROM pg_control_system()").Scan(sample.id); err != nil {
			t.Fatal(err)
		}
	}
	if engineID == sinkID {
		t.Fatal("engine and sink fixtures share a PostgreSQL instance")
	}
	runSQL(t, sink, "ALTER ROLE k6_sink_exporter LOGIN PASSWORD 'dur053-k6-test'")
	exporter, err := url.Parse(sinkDSN)
	if err != nil {
		t.Fatal(err)
	}
	exporter.User = url.UserPassword("k6_sink_exporter", "dur053-k6-test")
	fill := k6v1.Hash(nil)
	m := k6v1.Manifest{
		Schema: k6v1.EvidenceVersion, Campaign: "test", Job: "paired", RunID: "1",
		Episode: "medium-12-0", Ordinal: 90, Seed: 530091, Cell: "medium-12", Workload: "COOPERATING_EFFECT",
		Namespace: "episode", ResetID: "reset", BarrierID: "barrier", Database: engine.Config().Database, EngineInstance: "engine-test",
		Sink: &k6v1.DatabaseIdentity{Database: sink.Config().Database, Instance: "sink-test"}, RegistrySHA256: k6v1.RegistrySHA256,
		SourceTreeSHA256: fill, ImageSetSHA256: fill, FreezeSHA256: fill, TopologySHA256: fill,
		Instrumentation: []string{}, RequiredSources: k6v1.RequiredSources(false, true),
	}
	return engine, sink, Config{Schema: ConfigVersion, Manifest: m, DatabaseURL: engDSN, SinkDatabaseURL: exporter.String()}
}

func pairSources(t *testing.T, c Config, deadline time.Time) Config {
	t.Helper()
	m := c.Manifest
	root := t.TempDir()
	c.EpisodeDir, c.PlanFile, c.ArrivalsFile = filepath.Join(root, "episode"), filepath.Join(root, "plan"), filepath.Join(root, "arrivals")
	last := deadline.Add(-600 * time.Second)
	p := k6v1.Plan{Schema: k6v1.PlanVersion, Episode: m.Episode, Namespace: m.Namespace, ResetID: m.ResetID,
		FaultMode: "control", SealedUTC: k6v1.UTC(last.Add(-2 * time.Minute))}
	a := k6v1.Arrivals{Schema: k6v1.ArrivalsVersion, Episode: m.Episode, ResetID: m.ResetID, SealedUTC: k6v1.UTC(last)}
	reason := "overload"
	for i, window := range []string{"warmup", "measured"} {
		key := "dur053-" + window + "-530091-000000"
		body := k6v1.SubmissionBody{DefinitionID: "definition", DefinitionVersion: 1, InitialNodeID: "pure.echo",
			InitialInput: []byte("{}"), Payload: []byte("{}")}
		p.Windows = append(p.Windows, k6v1.PlanWindow{Name: window, LoadRunID: window, Submissions: []k6v1.Submission{{
			Key: key, ScheduledUTC: k6v1.UTC(last.Add(time.Duration(i-1) * time.Minute)),
			BodySHA256: body.Digest(), Body: body, Target: "resource", Expected: []byte("{}"),
			Request: k6v1.WireDigest{Length: int64(len(key)), SHA256: k6v1.Hash([]byte(key))},
		}}})
		a.Rows = append(a.Rows, k6v1.Arrival{Window: window, Key: key, Disposition: "unsent", UnsentReason: &reason, Attempts: []k6v1.Send{}})
	}
	entry, err := writeJSON(root, "plan", p)
	if err != nil {
		t.Fatal(err)
	}
	c.Manifest.PlanSHA256, a.PlanSHA256 = entry.SHA256, entry.SHA256
	if _, err := writeJSON(root, "arrivals", a); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestK6CutPairedRetry(t *testing.T) {
	engine, sink, config := newPair(t)
	for _, tc := range []struct {
		name   string
		raceAt string
		code   string
		write  func()
	}{
		{"BetweenCuts", "engine", "CUT_RACED:sink:k6_sink.request_bindings", func() {
			runSQL(t, sink, "INSERT INTO k6_sink.request_bindings VALUES ('new-request', repeat('a',64))")
		}},
		{"WatermarkRegression", "sink", "CUT_RACED:sink:k6_sink.request_bindings", func() {
			runSQL(t, sink, "DELETE FROM k6_sink.request_bindings")
		}},
		{"MutableChange", "engine", "CUT_RACED:engine:engine.consumer_offsets", func() {
			runSQL(t, engine, "INSERT INTO engine.consumer_offsets(consumer_id,topic,kafka_partition,next_offset) VALUES ('test','topic',0,1)")
		}},
		{"IdleLeaseIdentity", "engine", "CUT_RACED:engine:engine.partition_leases", func() {
			runSQL(t, engine, "UPDATE engine.partition_leases SET epoch=epoch+1 WHERE partition_id=0")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := pairSources(t, config, time.Now().Add(10*time.Second))
			if err := acquire(context.Background(), cfg, func(db string, generation int) {
				if db == tc.raceAt && generation == 1 {
					tc.write()
				}
			}); err != nil {
				t.Fatal(err)
			}
			root := os.DirFS(cfg.EpisodeDir)
			barrier := readBarrier(t, root)
			if len(barrier.Attempts) != 2 || barrier.SelectedGeneration != 2 ||
				len(barrier.Attempts[0].Reasons) != 1 || barrier.Attempts[0].Reasons[0] != tc.code {
				t.Fatalf("wrong paired retry: generations=%d/%d first reasons=%v", len(barrier.Attempts), barrier.SelectedGeneration, barrier.Attempts[0].Reasons)
			}
			if result := k6v1.Verify(root); !onlyMissingExternal(result) {
				t.Fatalf("retaken pair failed offline: %+v", result.Findings)
			}
		})
	}
	t.Run("IdleLeaseRenewalAllowed", func(t *testing.T) {
		runSQL(t, engine, "UPDATE engine.partition_leases SET owner_id='00000000-0000-0000-0000-000000000001', lease_expires_at=clock_timestamp()+interval '1 minute' WHERE partition_id=0")
		cfg := pairSources(t, config, time.Now().Add(10*time.Second))
		if err := acquire(context.Background(), cfg, func(db string, generation int) {
			if db == "engine" && generation == 1 {
				runSQL(t, engine, "UPDATE engine.partition_leases SET updated_at=clock_timestamp(), lease_expires_at=clock_timestamp()+interval '2 minutes' WHERE partition_id=0")
			}
		}); err != nil {
			t.Fatal(err)
		}
		root := os.DirFS(cfg.EpisodeDir)
		if b := readBarrier(t, root); len(b.Attempts) != 1 || len(b.Attempts[0].Reasons) != 0 {
			t.Fatal("idle lease renewal caused a retake")
		}
		if r := k6v1.Verify(root); !onlyMissingExternal(r) {
			t.Fatalf("allowed idle churn: %+v", r.Findings)
		}
	})
	t.Run("ContinuousWritesTimeOut", func(t *testing.T) {
		cfg := pairSources(t, config, time.Now().Add(2*time.Second))
		if err := acquire(context.Background(), cfg, func(db string, _ int) {
			if db == "engine" {
				runSQL(t, engine, "UPDATE engine.consumer_offsets SET next_offset=next_offset+1")
			}
		}); err != nil {
			t.Fatal(err)
		}
		root := os.DirFS(cfg.EpisodeDir)
		barrier := readBarrier(t, root)
		if len(barrier.Attempts) < 2 || len(barrier.Attempts[0].Reasons) == 0 ||
			barrier.Attempts[0].Reasons[0] != "CUT_RACED:engine:engine.consumer_offsets" {
			t.Fatal("timed-out run lost first race")
		}
		requireCutCode(t, k6v1.Verify(root), "CUT_DEADLINE")
	})
}

func TestK6CutDeadlineSalvageLive(t *testing.T) {
	_, _, config := newPair(t)
	cfg := pairSources(t, config, time.Now().Add(5*time.Second))
	// Let the deadline elapse after the final DB sample and before recording Finished.
	if err := acquireObserved(context.Background(), cfg, nil, func(ctx context.Context) {
		<-ctx.Done()
	}, nil); err != nil {
		t.Fatal(err)
	}
	root := os.DirFS(cfg.EpisodeDir)
	barrier := readBarrier(t, root)
	if barrier.SelectedGeneration != 1 || len(barrier.Attempts) != 1 ||
		!slices.Equal(barrier.Attempts[0].Reasons, []string{"CUT_DEADLINE"}) {
		t.Fatalf("expected the completed deadline-only cut to be selected: %+v", barrier)
	}
	raw, err := fs.ReadFile(root, k6v1.IndexPath)
	if err != nil {
		t.Fatal(err)
	}
	var index k6v1.Index
	if err := k6v1.DecodeStrict(raw, &index); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{k6v1.SnapshotPath, k6v1.SinkSnapshotPath} {
		if !slices.ContainsFunc(index.Files, func(file k6v1.File) bool { return file.Path == path }) {
			t.Fatalf("salvage index missing %s", path)
		}
	}
	result := k6v1.Verify(root)
	requireCutCode(t, result, "CUT_DEADLINE")
	if result.ExitCode() != k6v1.Fail || slices.ContainsFunc(result.Findings, func(f k6v1.Finding) bool {
		return f.Code == "CUT_NO_GENERATION"
	}) {
		t.Fatalf("unexpected offline salvage result: %+v", result.Findings)
	}
}

func onlyMissingExternal(r k6v1.Result) bool {
	if len(r.Findings) == 0 {
		return false
	}
	for _, f := range r.Findings {
		if !strings.HasPrefix(f.Code, "required_proof_missing:") || !(k6v1.ExternalPath(f.Source) || f.Source == k6v1.SamplesPath) {
			return false
		}
	}
	return true
}

func readBarrier(t *testing.T, root fs.FS) k6v1.Barrier {
	t.Helper()
	data, err := fs.ReadFile(root, k6v1.BarrierPath)
	if err != nil {
		t.Fatal(err)
	}
	var barrier k6v1.Barrier
	if err := k6v1.DecodeStrict(data, &barrier); err != nil {
		t.Fatal(err)
	}
	return barrier
}

func requireCutCode(t *testing.T, r k6v1.Result, code string) {
	t.Helper()
	for _, finding := range r.Findings {
		if finding.Code == code {
			return
		}
	}
	t.Fatalf("expected %s: %+v", code, r.Findings)
}
