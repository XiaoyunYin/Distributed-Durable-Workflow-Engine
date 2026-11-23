package main

import (
	"context"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"durable-agent-execution-engine/internal/invariants/k6live"
	"durable-agent-execution-engine/internal/invariants/k6v1"
	"github.com/jackc/pgx/v5"
)

func k6SinkDatabase(t *testing.T, engine *pgx.Conn) (*pgx.Conn, string) {
	t.Helper()
	raw := os.Getenv("DURABLE_K6_SINK_TEST_URL")
	if raw == "" {
		if os.Getenv("DURABLE_REQUIRE_K6_PAIR") == "1" {
			t.Fatal("DURABLE_K6_SINK_TEST_URL is required")
		}
		t.Skip("a second PostgreSQL instance requires DURABLE_K6_SINK_TEST_URL")
	}
	admin, dsn := isolatedK6Database(t, raw, os.DirFS("../../sink-migrations"))
	ctx := context.Background()
	var engineID, sinkID string
	for _, sample := range []struct {
		conn *pgx.Conn
		id   *string
	}{{engine, &engineID}, {admin, &sinkID}} {
		if err := sample.conn.QueryRow(ctx, "SELECT system_identifier::text FROM pg_control_system()").Scan(sample.id); err != nil {
			t.Fatal(err)
		}
	}
	if engineID == sinkID {
		t.Fatal("paired fixtures must use separate PostgreSQL instances")
	}
	execK6SQL(t, admin, "ALTER ROLE k6_sink_exporter LOGIN PASSWORD 'dur053-k6-test'")
	bindK6Database(t, admin, "sink-instance", "reset-a")
	exporter, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	exporter.User = url.UserPassword("k6_sink_exporter", "dur053-k6-test")
	return admin, exporter.String()
}

func minimalExecution(t *testing.T, conn *pgx.Conn, entry k6v1.Submission) {
	t.Helper()
	execK6SQL(t, conn, `INSERT INTO engine.workflow_executions
		(workflow_id,namespace,submission_key,submission_payload_hash,definition_id,definition_version,partition_id,state,revision)
		VALUES ($1,'episode',$1,$2,'definition',1,0,'SUCCEEDED',2)`, entry.Key, "sub-v1:"+entry.BodySHA256)
	execK6SQL(t, conn, `INSERT INTO engine.transition_history(transition_id,workflow_id,revision,actor_kind,actor_id,old_state,new_state,reason,scheduler_epoch)
		VALUES (md5($1::text||'1')::uuid,$1,1,'client','actor',NULL,'RUNNABLE','WORKFLOW_CREATED',NULL),
			(md5($1::text||'2')::uuid,$1,2,'scheduler','actor','RUNNABLE','SUCCEEDED','WORKFLOW_ADVANCED',1)`, entry.Key)
}

func pairedK6Fixture(t *testing.T) (*pgx.Conn, *pgx.Conn, k6live.Config, k6v1.Plan, k6v1.Arrivals) {
	t.Helper()
	engine, engineDSN := k6TestDatabase(t)
	sink, sinkDSN := k6SinkDatabase(t, engine)
	bindK6Database(t, engine, "engine-instance", "reset-a")
	m := k6Manifest(engine.Config().Database, false)
	m.Ordinal, m.Seed, m.Cell, m.Episode, m.Workload = 90, 530091, "medium-12", "medium-12-0", "COOPERATING_EFFECT"
	m.Sink = &k6v1.DatabaseIdentity{Database: sink.Config().Database, Instance: "sink-instance"}
	m.RequiredSources = k6v1.RequiredSources(false, true)
	config := k6live.Config{Schema: k6live.ConfigVersion, DatabaseURL: engineDSN, SinkDatabaseURL: sinkDSN, Manifest: m}
	plan, arrivals := k6Sources(m, time.Now().Add(-time.Minute), "resource")
	execK6SQL(t, engine, `INSERT INTO engine.workflow_definitions(definition_id,version,definition_hash,graph,activity_versions,effect_classes)
		VALUES ('definition',1,'hash','{"nodes":["pure.echo"]}','{"pure.echo":"v1"}','{"pure.echo":"PURE_ACTIVITY"}')`)
	warmup := plan.Windows[0].Submissions[0]
	minimalExecution(t, engine, warmup)
	execK6SQL(t, engine, `INSERT INTO engine.node_instances(workflow_id,node_id,iteration,state,input)
		VALUES ($1,'pure.echo',0,'SUCCEEDED','{}')`, warmup.Key)
	execK6SQL(t, engine, `INSERT INTO engine.approval_action_intents(intent_id,workflow_id,node_id,iteration,proposal_hash,target,canonical_arguments,valid_until)
		VALUES ('00000000-0000-0000-0000-000000000001',$1,'pure.echo',0,'hash','resource','{"position":3,"value":"k6"}',clock_timestamp())`, warmup.Key)
	execK6SQL(t, sink, `INSERT INTO k6_sink.resources(resource_id) VALUES ('resource');
		UPDATE k6_sink.resources SET latest_fence=1 WHERE resource_id='resource';
		UPDATE k6_sink.resources SET latest_fence=2 WHERE resource_id='resource';
		INSERT INTO k6_sink.allocations VALUES
			('00000000-0000-0000-0000-000000000001',repeat('c',64),1,'resource',1),
			('00000000-0000-0000-0000-000000000001',repeat('c',64),2,'resource',2);
		INSERT INTO k6_sink.lookup_misses VALUES ('00000000-0000-0000-0000-000000000001',repeat('c',64),2);
		INSERT INTO k6_sink.request_bindings VALUES ('request',repeat('d',64));
		UPDATE k6_sink.resources SET state='{"value":"k6"}',revision=1,logical_key='key',intent_id='00000000-0000-0000-0000-000000000001'
		WHERE resource_id='resource'`)
	execK6SQL(t, sink, `INSERT INTO k6_sink.receipts(workflow_id,logical_key,resource_id,canonical_hash,scope_hash,intent_id,expected_revision,applied_revision)
		VALUES ($1,'key','resource',repeat('a',64),repeat('b',64),'00000000-0000-0000-0000-000000000001',0,1)`, warmup.Key)
	execK6SQL(t, sink, `INSERT INTO k6_sink.calls(operation,workflow_id,logical_key,intent_id,resource_id,message_id,attempt,request_id,body_hash,connection_hash,outcome,application_id,response_hash)
		SELECT 'apply',workflow_id,logical_key,intent_id,resource_id,'message',2,'request',repeat('e',64),repeat('f',64),'APPLIED',application_id,repeat('a',64)
		FROM k6_sink.receipts WHERE workflow_id=$1`, warmup.Key)
	return engine, sink, config, plan, arrivals
}

func acquireK6Fixture(t *testing.T, config k6live.Config, plan k6v1.Plan, arrivals k6v1.Arrivals, want int) fstest.MapFS {
	t.Helper()
	config.EpisodeDir = filepath.Join(t.TempDir(), "public")
	config = writeK6Sources(t, config, plan, arrivals)
	path, stdout, stderr, code := invokeLiveK6(t, config)
	expect := want
	if config.ExternalDir == "" {
		expect = k6v1.Fail
	}
	if code != expect || stderr != "" {
		t.Fatalf("live exit %d, want %d\n%s\n%s", code, want, stdout, stderr)
	}
	bundle := inspectK6Output(t, path, stdout, code)
	if want == k6v1.Incomplete && config.ExternalDir == "" {
		result := k6v1.Verify(bundle)
		requireMinimalHistoryProof(t, result)
	}
	return bundle
}

func resealK6File(t *testing.T, bundle fstest.MapFS, path string, value any) {
	t.Helper()
	bundle[path] = &fstest.MapFile{Data: canonical(t, value)}
	resealK6Index(t, bundle)
}

func TestK6SinkAndSources(t *testing.T) {
	_, sink, config, plan, arrivals := pairedK6Fixture(t)
	bundle := acquireK6Fixture(t, config, plan, arrivals, k6v1.Incomplete)
	result := k6v1.Verify(bundle)
	requireMinimalHistoryProof(t, result)
	if !slices.Equal(result.InvariantsCovered, []string{"I1", "I11"}) {
		t.Fatalf("clean paired cut: %+v", result.Findings)
	}
	for _, spec := range k6v1.SinkRelations() {
		if len(bundle[k6v1.RelationPath(spec.Name)].Data) == 0 {
			t.Fatalf("missing seeded %s rows", spec.Name)
		}
	}
	t.Run("MissingSources", func(t *testing.T) {
		paths := []string{k6v1.SinkSnapshotPath, k6v1.PlanPath, k6v1.ArrivalsPath, k6v1.BarrierPath}
		for _, spec := range k6v1.SinkRelations() {
			paths = append(paths, k6v1.RelationPath(spec.Name))
		}
		for _, path := range paths {
			t.Run(path, func(t *testing.T) {
				broken := cloneBundle(bundle)
				delete(broken, path)
				requireK6Finding(t, k6v1.Verify(broken), "required_proof_missing:"+path)
			})
		}
	})
	t.Run("ResealedRawRow", func(t *testing.T) {
		broken := cloneBundle(bundle)
		path := k6v1.RelationPath(k6v1.SinkResources)
		var row k6v1.Row
		if err := k6v1.DecodeStrict(broken[path].Data, &row); err != nil {
			t.Fatal(err)
		}
		row["final_state_hash"] = k6v1.Hash([]byte("corruption"))
		resealK6DatabaseRelation(t, broken, k6v1.SinkSnapshotPath, k6v1.SinkResources, canonical(t, row), 1)
		var barrier k6v1.Barrier
		if err := k6v1.DecodeStrict(broken[k6v1.BarrierPath].Data, &barrier); err != nil {
			t.Fatal(err)
		}
		cut := &barrier.Attempts[barrier.SelectedGeneration-1].Databases[1]
		cut.SnapshotSHA256 = k6v1.Hash(broken[k6v1.SinkSnapshotPath].Data)
		for i, spec := range k6v1.SinkRelations() {
			if spec.Name != k6v1.SinkResources {
				continue
			}
			mark := k6v1.NewWatermark(spec)
			mark.Add(row, k6v1.Hash(canonical(t, row)))
			watermark := mark.Finish()
			cut.Pre.Relations[i], cut.Cut[i], cut.Post.Relations[i] = watermark, watermark, watermark
		}
		resealK6File(t, broken, k6v1.BarrierPath, barrier)
		broken["checker.json"] = &fstest.MapFile{Data: []byte(`{"status":"PASS"}`)}
		result := k6v1.Verify(broken)
		requireK6Finding(t, result, "SINK_REVISION_ENDPOINT")
		if result.Labels[7].Outcome != "covered/fail" || result.Safety != "FAIL" {
			t.Fatal("sink corruption missed I8")
		}
	})
	t.Run("N12RevisionGap", func(t *testing.T) {
		setRevision := func(n string) {
			execK6SQL(t, sink, "ALTER TABLE k6_sink.resources DISABLE TRIGGER journal_resource_write; UPDATE k6_sink.resources SET revision="+n+" WHERE resource_id='resource'; ALTER TABLE k6_sink.resources ENABLE TRIGGER journal_resource_write")
		}
		setRevision("2")
		defer setRevision("1")
		broken := acquireK6Fixture(t, config, plan, arrivals, k6v1.Fail)
		requireK6Finding(t, k6v1.Verify(broken), "SINK_REVISION_ENDPOINT")
	})
	t.Run("N12ForeignResourceJournalReceipt", func(t *testing.T) {
		execK6SQL(t, sink, `INSERT INTO k6_sink.resources(resource_id) VALUES ('foreign');
			UPDATE k6_sink.resources SET state='{"value":"k6"}',revision=1,logical_key='foreign-key',intent_id='00000000-0000-0000-0000-000000000123'
			WHERE resource_id='foreign';
			INSERT INTO k6_sink.receipts(workflow_id,logical_key,resource_id,canonical_hash,scope_hash,intent_id,expected_revision,applied_revision)
			VALUES ('no-workflow','foreign-key','foreign',repeat('a',64),repeat('b',64),'00000000-0000-0000-0000-000000000123',0,1)`)
		defer execK6SQL(t, sink, `DELETE FROM k6_sink.receipts WHERE resource_id='foreign';
			DELETE FROM k6_sink.journal WHERE resource_id='foreign'; DELETE FROM k6_sink.resources WHERE resource_id='foreign'`)
		broken := acquireK6Fixture(t, config, plan, arrivals, k6v1.Fail)
		for _, code := range []string{"SINK_ORPHAN_RESOURCE", "SINK_ORPHAN_JOURNAL", "SINK_ORPHAN_RECEIPT"} {
			requireK6Finding(t, k6v1.Verify(broken), code)
		}
	})
	t.Run("SinkCensus", func(t *testing.T) {
		execK6SQL(t, sink, "CREATE TABLE k6_sink.unplanned (id int PRIMARY KEY); INSERT INTO k6_sink.unplanned VALUES (1)")
		defer execK6SQL(t, sink, "DROP TABLE k6_sink.unplanned")
		requireK6Finding(t, k6v1.Verify(acquireK6Fixture(t, config, plan, arrivals, k6v1.Fail)), "RELATION_UNCLASSIFIED")
	})
	t.Run("SinkColumn", func(t *testing.T) {
		execK6SQL(t, sink, "ALTER TABLE k6_sink.resources ADD COLUMN hidden_state text")
		defer execK6SQL(t, sink, "ALTER TABLE k6_sink.resources DROP COLUMN hidden_state")
		requireK6Finding(t, k6v1.Verify(acquireK6Fixture(t, config, plan, arrivals, k6v1.Fail)), "COLUMN_UNCLASSIFIED")
	})
	t.Run("SwappedReset", func(t *testing.T) {
		bindK6Database(t, sink, "sink-instance", "other-reset")
		defer bindK6Database(t, sink, "sink-instance", "reset-a")
		requireK6Finding(t, k6v1.Verify(acquireK6Fixture(t, config, plan, arrivals, k6v1.Fail)), "CUT_BINDING:sink")
	})
	t.Run("SwappedInstance", func(t *testing.T) {
		bindK6Database(t, sink, "other-instance", "reset-a")
		defer bindK6Database(t, sink, "sink-instance", "reset-a")
		requireK6Finding(t, k6v1.Verify(acquireK6Fixture(t, config, plan, arrivals, k6v1.Fail)), "CUT_BINDING:sink")
	})
}

func TestK6SourcePure(t *testing.T) {
	engine, dsn := k6TestDatabase(t)
	seedK6Postgres(t, engine)
	dir, out, diag, code := liveK6(t, engine, dsn, false)
	if code != k6v1.Fail || diag != "" {
		t.Fatalf("PURE exit %d: %s %s", code, out, diag)
	}
	bundle := inspectK6Output(t, dir, out, code)
	if _, err := fs.Stat(bundle, "sink"); !os.IsNotExist(err) {
		t.Fatalf("unexpected sink path: %v", err)
	}
	bundle["sink/unindexed.json"] = &fstest.MapFile{Data: []byte("foreign")}
	requireK6Finding(t, k6v1.Verify(bundle), "PURE_SINK_PRESENT")
	delete(bundle, "sink/unindexed.json")
	var index k6v1.Index
	if err := k6v1.DecodeStrict(bundle[k6v1.IndexPath].Data, &index); err != nil {
		t.Fatal(err)
	}
	index.Files = append(index.Files, k6v1.File{Path: k6v1.SinkSnapshotPath, SHA256: k6v1.Hash(nil)})
	slices.SortFunc(index.Files, func(a, b k6v1.File) int { return strings.Compare(a.Path, b.Path) })
	bundle[k6v1.IndexPath] = &fstest.MapFile{Data: canonical(t, index)}
	requireK6Finding(t, k6v1.Verify(bundle), "PURE_SINK_PRESENT")
}

func TestK6ColumnAndForeignTable(t *testing.T) {
	engine, dsn := k6TestDatabase(t)
	seedK6Postgres(t, engine)
	execK6SQL(t, engine, "ALTER TABLE engine.activity_attempts ADD COLUMN extra int")
	dir, out, diag, code := liveK6(t, engine, dsn, false)
	if code != k6v1.Fail || diag != "" {
		t.Fatalf("column drift exit %d: %s", code, diag)
	}
	requireK6Finding(t, k6v1.Verify(inspectK6Output(t, dir, out, code)), "COLUMN_UNCLASSIFIED")
	execK6SQL(t, engine, `ALTER TABLE engine.activity_attempts DROP COLUMN extra;
		CREATE FOREIGN DATA WRAPPER k6_test_wrapper; CREATE SERVER k6_test_server FOREIGN DATA WRAPPER k6_test_wrapper;
		CREATE FOREIGN TABLE public.k6_foreign (id bigint) SERVER k6_test_server`)
	dir, out, diag, code = liveK6(t, engine, dsn, false)
	if code != k6v1.Fail || diag != "" {
		t.Fatalf("foreign table exit %d: %s", code, diag)
	}
	requireK6Finding(t, k6v1.Verify(inspectK6Output(t, dir, out, code)), "RELATION_UNCLASSIFIED")
	execK6SQL(t, engine, "DROP FOREIGN TABLE public.k6_foreign; ALTER TABLE engine.activity_attempts DROP COLUMN k6_proof")
	dir, out, diag, code = liveK6(t, engine, dsn, false)
	if code != k6v1.Fail || diag != "" {
		t.Fatalf("removed column exit %d: %s", code, diag)
	}
	requireK6Finding(t, k6v1.Verify(inspectK6Output(t, dir, out, code)), "COLUMN_UNCLASSIFIED")
}
