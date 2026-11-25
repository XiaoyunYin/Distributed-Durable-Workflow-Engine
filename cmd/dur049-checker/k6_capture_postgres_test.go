package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/invariants/k6collect"
	"durable-agent-execution-engine/internal/invariants/k6live"
	"durable-agent-execution-engine/internal/invariants/k6v1"
	"github.com/jackc/pgx/v5"
)

const captureMigration = "../../migrations/k6_capture/000024_k6_capture."
const captureOwnerID = "00000000-0000-0000-0000-000000000001"
const takeoverOwnerID = "00000000-0000-0000-0000-000000000002"

func TestK6SourceCaptureParity(t *testing.T) {
	if os.Getenv("DURABLE_K6_VERDICT_ARTIFACT") == "" {
		t.Skip("requires hosted PostgreSQL and network-isolated checker")
	}
	engine, dsn := k6TestDatabase(t)
	bindK6Database(t, engine, "engine-instance", "reset-a")
	assertInstalled := func(want bool) {
		t.Helper()
		var installed, migrated bool
		err := engine.QueryRow(t.Context(), `SELECT to_regclass('engine.k6_write_capture') IS NOT NULL,
			EXISTS (SELECT 1 FROM engine.schema_migrations WHERE version=24)`).Scan(&installed, &migrated)
		if err != nil || installed != want || migrated != want {
			t.Fatalf("capture table/ledger %t/%t, expected %t: %v", installed, migrated, want, err)
		}
	}
	assertInstalled(false)
	up, down := string(getFile(t, captureMigration+"up.sql")), string(getFile(t, captureMigration+"down.sql"))
	execK6SQL(t, engine, up)
	assertInstalled(true)
	execK6SQL(t, engine, down)
	execK6SQL(t, engine, down)
	assertInstalled(false)
	execK6SQL(t, engine, up)
	execK6SQL(t, engine, up)
	assertInstalled(true)
	execK6SQL(t, engine, `UPDATE engine.partition_leases SET updated_at=updated_at WHERE partition_id=0`)
	assertCaptureCount(t, engine, 0)

	m := k6Manifest(engine.Config().Database, false)
	m.Cell, m.Episode, m.Ordinal, m.Seed = "hook-5", "hook-5-0", 25, 530026
	m.Instrumentation = []string{k6v1.WriteCapture}
	m.RequiredSources = m.SourcePaths()
	plan, arrivals := k6Sources(m, time.Now().Add(-time.Minute), "pure.echo")
	item := plan.Windows[0].Submissions[0]
	seedCapture(t, engine, item, "Clean")
	seedCapturedTransport(t, engine)
	var cleanCapture, cleanOwners []byte
	if err := engine.QueryRow(t.Context(), `SELECT
		(SELECT jsonb_agg(w) FROM engine.k6_write_capture w),
		(SELECT jsonb_agg(o) FROM engine.k6_owner_write_transactions o)`).Scan(&cleanCapture, &cleanOwners); err != nil {
		t.Fatal(err)
	}
	execK6SQL(t, engine, `ALTER TABLE engine.k6_write_capture DISABLE TRIGGER k6_write_immutable;
		ALTER TABLE engine.k6_owner_write_transactions DISABLE TRIGGER k6_owner_immutable`)
	restore := func(t *testing.T) {
		t.Helper()
		execK6SQL(t, engine, `INSERT INTO engine.k6_write_capture
			SELECT * FROM jsonb_populate_recordset(NULL::engine.k6_write_capture, $1)`, cleanCapture)
		execK6SQL(t, engine, `INSERT INTO engine.k6_owner_write_transactions
			SELECT * FROM jsonb_populate_recordset(NULL::engine.k6_owner_write_transactions, $1)`, cleanOwners)
	}

	for _, tc := range []struct {
		name, change, predicate, code, relation, keyQuery string
	}{
		{name: "Clean"},
		{"MissingWrite", "DELETE FROM engine.k6_write_capture WHERE subject='engine.node_instances' AND operation='insert'",
			"EngineCaptureClosure", "CAPTURE_IMAGE_CHAIN", k6v1.WriteCapture,
			"SELECT event_order FROM engine.k6_write_capture WHERE subject='engine.node_instances' AND operation='update' ORDER BY event_order LIMIT 1"},
		{"WrongTransaction", "UPDATE engine.k6_write_capture SET transaction_id='999999999' WHERE subject='engine.node_instances' AND operation='insert'",
			"EngineCaptureClosure", "CAPTURE_NODE_TRANSACTION", k6v1.WriteCapture,
			"SELECT event_order FROM engine.k6_write_capture WHERE subject='engine.node_instances' AND operation='insert'"},
		{"OmittedOwnerClass", "DELETE FROM engine.k6_owner_write_transactions WHERE subject='engine.timers' AND operation='insert'",
			"OwnerAtCommit", "OWNER_INVENTORY_WRITE_MISSING", k6v1.WriteCapture,
			"SELECT event_order FROM engine.k6_write_capture WHERE subject='engine.timers' AND operation='insert'"},
		{"StaleEpoch", "UPDATE engine.k6_owner_write_transactions SET epoch=1,owner_id='" + captureOwnerID + "' WHERE subject='engine.timers' AND operation='update'",
			"OwnerAtCommit", "OWNER_INVENTORY_STALE_EPOCH", k6v1.OwnerWrites,
			"SELECT event_order FROM engine.k6_owner_write_transactions WHERE subject='engine.timers' AND operation='update'"},
		{"ResultMismatch", "UPDATE engine.k6_write_capture SET after_image=jsonb_set(after_image,'{token_hash}',to_jsonb(repeat('a',64))) WHERE subject='result_request'",
			"CurrentClaimWinner", "CAPTURE_RESULT_IDENTITY", k6v1.WriteCapture,
			"SELECT event_order FROM engine.k6_write_capture WHERE subject='result_request'"},
		{"RetryIdentity", "UPDATE engine.k6_write_capture SET after_image=jsonb_set(after_image,'{payload_hash}',to_jsonb(repeat('b',64))) WHERE subject='publication_start' AND row_key->>1='2'",
			"DispatchDisposition", "CAPTURE_PUBLICATION_IDENTITY", k6v1.WriteCapture,
			"SELECT event_order FROM engine.k6_write_capture WHERE subject='publication_start' AND row_key->>1='2'"},
		{"MarkWithoutStart", "DELETE FROM engine.k6_write_capture WHERE subject='publication_start' AND row_key->>1='2'",
			"DispatchDisposition", "CAPTURE_PUBLICATION_MARK", k6v1.WriteCapture,
			"SELECT event_order FROM engine.k6_write_capture WHERE subject='engine.outbox_publications'"},
		{"ClaimOtherTransaction", "UPDATE engine.k6_write_capture SET transaction_id='999999999' WHERE subject='engine.activity_attempts' AND after_image->>'state'='CLAIMED'",
			"CurrentClaimWinner", "CAPTURE_CLAIM_TRANSACTION", "engine.k4_audit",
			"SELECT event_order FROM engine.k4_audit WHERE kind='claim'"},
		{"OmittedDisposition", "DELETE FROM engine.k6_write_capture WHERE subject='engine.event_inbox'",
			"DispatchDisposition", "CAPTURE_DELIVERY_BINDING", k6v1.WriteCapture,
			"SELECT event_order FROM engine.k6_write_capture WHERE subject='physical_delivery' AND operation='ACCEPTED'"},
		{"ReconciliationBinding", "UPDATE engine.k6_write_capture SET after_image=jsonb_set(after_image,'{reference}',to_jsonb('outbox/unmatched'::text)) WHERE subject='engine.reconciliation_items'",
			"DispatchDisposition", "CAPTURE_RECONCILIATION_BINDING", k6v1.WriteCapture,
			"SELECT event_order FROM engine.k6_write_capture WHERE subject='engine.reconciliation_items'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var order int64
			if tc.change != "" {
				if err := engine.QueryRow(t.Context(), tc.keyQuery).Scan(&order); err != nil {
					t.Fatal(err)
				}
				execK6SQL(t, engine, tc.change)
				t.Cleanup(func() {
					execK6SQL(t, engine, "TRUNCATE engine.k6_owner_write_transactions, engine.k6_write_capture")
					restore(t)
				})
			}
			cfg := captureFixtureConfig(t, dsn, m)
			if tc.name == "Clean" {
				cfg.ExternalDir = t.TempDir()
				sources := parityExternal(t, &cfg.Manifest, false, item.Key)
				publishFixtureFrames(t, cfg, sources)
			}
			r := captureFixtureVerdict(t, cfg, plan, arrivals)
			if tc.change == "" {
				for _, i := range []int{1, 3} {
					if label := r.Labels[i]; label.Outcome != "covered/pass" || label.PopulationCount == 0 {
						t.Fatalf("missing capture award: %+v, findings %+v", label, r.Findings)
					}
				}
				if label := r.Labels[2]; label.Outcome != "NOT COVERED" ||
					label.Reason != "required_proof_missing:DeployedStaleChallenge" || label.PopulationCount == 0 {
					t.Fatalf("I3 must retain the owner population and withhold the challenge award: %+v", label)
				}
				if len(r.Findings) != 2 {
					t.Fatalf("clean capture remainder: %+v", r.Findings)
				}
				requirePredicateFinding(t, r, k6v1.MissingProof, "RelationIntegrity", "required_proof_missing:"+k6v1.StalePath)
				requirePredicateFinding(t, r, k6v1.MissingProof, "DispatchDisposition", "required_proof_missing:broker_event_copies")
				return
			}
			key, _ := json.Marshal(order)
			requireOrderingFinding(t, r, tc.predicate, tc.code, k6v1.Violation, tc.relation, []any{json.Number(key)})
			for _, label := range r.Labels {
				if label.Registered && slices.Contains(label.Predicates, tc.predicate) && label.Outcome != "covered/fail" {
					t.Fatalf("registered predicate lost its violation: %+v", label)
				}
			}
		})
	}
	for _, name := range []string{"CaptureTablesEmpty", "OwnerInventoryEmpty", "CaptureObjectsAbsent"} {
		t.Run(name, func(t *testing.T) {
			switch name {
			case "CaptureObjectsAbsent":
				execK6SQL(t, engine, down)
				assertInstalled(false)
			case "OwnerInventoryEmpty":
				execK6SQL(t, engine, "TRUNCATE engine.k6_owner_write_transactions")
			default:
				execK6SQL(t, engine, "TRUNCATE engine.k6_owner_write_transactions, engine.k6_write_capture")
			}
			t.Cleanup(func() {
				if name == "CaptureObjectsAbsent" {
					execK6SQL(t, engine, up)
				} else if name == "OwnerInventoryEmpty" {
					execK6SQL(t, engine, "TRUNCATE engine.k6_owner_write_transactions, engine.k6_write_capture")
				}
				restore(t)
			})
			r := captureFixtureVerdict(t, captureFixtureConfig(t, dsn, m), plan, arrivals)
			for _, relation := range []string{k6v1.WriteCapture, k6v1.OwnerWrites} {
				if name == "OwnerInventoryEmpty" && relation == k6v1.WriteCapture {
					continue
				}
				requirePredicateFinding(t, r, k6v1.MissingProof, "EngineCaptureClosure", "required_proof_missing:"+k6v1.RelationPath(relation))
			}
			for _, label := range r.Labels[1:4] {
				if label.Outcome != "NOT COVERED" {
					t.Fatalf("missing capture awarded coverage: %+v", label)
				}
			}
			if slices.ContainsFunc(r.Findings, func(f k6v1.Finding) bool { return f.Kind != k6v1.MissingProof }) {
				t.Fatalf("absent capture produced something other than missing proof: %+v", r.Findings)
			}
		})
	}
	var count int
	if err := engine.QueryRow(t.Context(), "SELECT count(*) FROM engine.k6_write_capture").Scan(&count); err != nil {
		t.Fatal(err)
	}
	execK6SQL(t, engine, `BEGIN;
		UPDATE engine.activity_attempts SET updated_at=clock_timestamp();
		SET CONSTRAINTS ALL IMMEDIATE;
		ROLLBACK`)
	assertCaptureCount(t, engine, count)
	measureCapture(t, engine)
}

func assertCaptureCount(t *testing.T, conn *pgx.Conn, want int) {
	t.Helper()
	var got int
	err := conn.QueryRow(t.Context(), "SELECT count(*) FROM engine.k6_write_capture").Scan(&got)
	if err != nil || got != want {
		t.Fatalf("committed capture rows %d, expected %d: %v", got, want, err)
	}
}

func captureFixtureConfig(t *testing.T, dsn string, m k6v1.Manifest) k6live.Config {
	t.Helper()
	return k6live.Config{Schema: k6live.ConfigVersion, DatabaseURL: dsn, Manifest: m, EpisodeDir: filepath.Join(t.TempDir(), "public")}
}

func captureFixtureVerdict(t *testing.T, cfg k6live.Config, plan k6v1.Plan, arrivals k6v1.Arrivals, want ...int) k6v1.Result {
	t.Helper()
	path, stdout, diag, code := invokeLiveK6(t, writeK6Sources(t, cfg, plan, arrivals))
	expected := k6v1.Fail
	if len(want) != 0 {
		expected = want[0]
	}
	if code != expected || diag != "" {
		t.Fatalf("capture fixture exit %d, diagnostics %s: %s", code, diag, stdout)
	}
	return verifyVerdictParity(t, path, stdout, code)
}

func capturedBrokerCopies(t *testing.T, m k6v1.Manifest, db *pgx.Conn, sources *k6v1.External, variant string) {
	t.Helper()
	var id, payload string
	if err := db.QueryRow(t.Context(), `SELECT event_id::text,payload::text FROM engine.outbox WHERE event_id=md5('o2')::uuid`).Scan(&id, &payload); err != nil {
		t.Fatal(err)
	}
	at := time.Now()
	stream := k6collect.NewStream[k6v1.BrokerCopyRow](m, "broker-copies", "kafka-fixture", at)
	positions := sources.Broker
	events := k6v1.BrokerPartition{Topic: "durable-agent.events.v1", TopicID: "events-id"}
	for i := range positions.Rows {
		p := positions.Rows[i].Data.Partitions[0]
		positions.Rows[i].Data.Partitions = []k6v1.BrokerPartition{events, p}
	}
	last := &positions.Rows[len(positions.Rows)-1].Data.Partitions[1]
	var unattempted k6v1.BrokerCopy
	if variant == "CopyWithNoRelayStart" {
		var body string
		if err := db.QueryRow(t.Context(), `SELECT event_id::text,payload::text FROM engine.outbox WHERE event_id=md5('o1')::uuid`).Scan(&unattempted.EventID, &body); err != nil {
			t.Fatal(err)
		}
		unattempted.Key, unattempted.PayloadSHA256 = []byte(unattempted.EventID), k6v1.Hash([]byte(body))
		events.End = 1
		positions.Rows[len(positions.Rows)-1].Data.Partitions[0] = events
	}
	tasks := *last
	end := int64(2)
	switch variant {
	case "MissingCopy":
		end = 0
	case "DeliveryWithoutCopy":
		end = 1
	case "CopiesExceedRelayStarts":
		end = 3
	}
	for i := range positions.Rows {
		p := &positions.Rows[i].Data.Partitions[1]
		p.End = min(p.End, end)
		if p.Committed != nil && *p.Committed > p.End {
			p.Committed = &p.End
		}
	}
	if variant == "CopiesExceedRelayStarts" {
		last.End = end
	}
	var captureEnd int64
	if err := db.QueryRow(t.Context(), `SELECT max(event_order) FROM engine.k6_write_capture`).Scan(&captureEnd); err != nil {
		t.Fatal(err)
	}
	for i, name := range []string{"reset", "hold", "restart"} {
		row := &positions.Rows[i].Data
		before, err := time.Parse(k6v1.TimeLayout, row.BeforeUTC)
		if err != nil {
			t.Fatal(err)
		}
		through := captureEnd
		if i == 0 {
			through = 0
		}
		row.OffsetPhase = &k6v1.OffsetPhase{Name: name, Binding: k6v1.Binding(m),
			RequestedUTC: k6v1.UTC(before.Add(-time.Microsecond)), CaptureThrough: through}
	}
	if variant == "BaselineMismatch" {
		p := &positions.Rows[0].Data.Partitions[1]
		p.End = 1
		p.Committed = &p.End
	}
	if variant == "TopicIDChange" {
		positions.Rows[1].Data.TopicIDsAfter = map[string]string{testTaskTopic: "recreated-task-id"}
	}
	tasks.Committed, tasks.End = nil, end
	for _, part := range []k6v1.BrokerPartition{events, tasks} {
		key := k6v1.PartitionIdentity{TopicID: part.TopicID, Partition: part.Partition}
		for offset := int64(0); offset < part.End; offset++ {
			copy := k6v1.BrokerCopy{Offset: offset, EventID: id, Key: []byte(id), PayloadSHA256: k6v1.Hash([]byte(payload))}
			if part.Topic == events.Topic {
				copy = unattempted
			}
			if offset == 0 && variant == "KeyMismatch" {
				copy.Key = []byte("other")
			}
			if offset == 0 && variant == "PayloadMismatch" {
				copy.PayloadSHA256 = k6v1.Hash([]byte("other"))
			}
			k6collect.Append(&stream, at, k6v1.BrokerCopyRow{PartitionIdentity: key, Copy: &copy})
		}
		k6collect.Append(&stream, at, k6v1.BrokerCopyRow{PartitionIdentity: key, Range: &part,
			Complete: variant != "CollectionGap" || part.Topic != testTaskTopic})
	}
	sealFixtureStream(t, positions, at)
	sealFixtureStream(t, &stream, at)
	sources.BrokerCopies = &stream
}

func capturedShutdown(t *testing.T, m k6v1.Manifest, sources *k6v1.External, withSignal bool) {
	t.Helper()
	opened := sources.Roster.Rows[0].Position.UTC
	at, err := time.Parse(k6v1.TimeLayout, opened)
	if err != nil {
		t.Fatal(err)
	}
	watch := k6collect.NewPodWatch(m, sources.Roster.Header.Source, at)
	if err := watch.Baseline([]byte(`{"metadata":{"resourceVersion":"1"},"items":[]}`), "worker", at); err != nil {
		t.Fatal(err)
	}
	for id, ledger := range sources.Sessions {
		session := ledger.Rows[0].Data.Session
		read := func(cursor, metadata, state string, at time.Time) {
			t.Helper()
			raw := fmt.Sprintf(`{"type":"MODIFIED","object":{"metadata":{"uid":%q,"namespace":%q,"resourceVersion":%q%s},"status":{"containerStatuses":[{"name":"worker","containerID":%q,"restartCount":%d,"state":%s}]}}}`,
				session.PodUID, m.Namespace, cursor, metadata, "containerd://"+session.ContainerID, session.Restart, state)
			if err := watch.Read(strings.NewReader(raw), "worker", func() time.Time { return at }); err != nil {
				t.Fatal(err)
			}
		}
		running := fmt.Sprintf(`{"running":{"startedAt":%q}}`, session.ContainerStartedUTC)
		read("2", "", running, at)
		reg := sources.Roster.Rows[2].Data.Registration
		if err := watch.Register(at, *session, *reg); err != nil {
			t.Fatal(err)
		}
		if withSignal {
			read("3", fmt.Sprintf(`,"deletionTimestamp":%q`, k6v1.UTC(at.Add(30*time.Second))), running, at.Add(time.Second))
		}
		last := &ledger.Rows[len(ledger.Rows)-1]
		last.Data.Kind = "abort"
		last.Position.UTC = k6v1.UTC(at.Add(2 * time.Second))
		finished := at.Add(3 * time.Second)
		observed := finished.Add(time.Second)
		terminated := fmt.Sprintf(`{"terminated":{"containerID":%q,"startedAt":%q,"finishedAt":%q,"exitCode":143,"signal":15}}`,
			"containerd://"+session.ContainerID, session.ContainerStartedUTC, finished.UTC().Format(time.RFC3339))
		read("4", "", terminated, observed)
		if err := k6v1.SealStream(&ledger, "checkpoint", k6v1.UTC(observed), int64(len(ledger.Rows)), 0); err != nil {
			t.Fatal(err)
		}
		sources.Sessions[id] = ledger
		watch.BindShutdown(observed, *session, sources.Sessions)
		if err := watch.ConfirmDeath(observed, *session, &ledger); err != nil {
			t.Fatal(err)
		}
		sources.Sessions[id] = ledger
		sources.Roster, err = watch.Snapshot(observed, true)
		if err != nil {
			t.Fatal(err)
		}
	}
	sealFixtureStream(t, &sources.Roster, time.Now())
}

func TestK6SourceCapturedJoins(t *testing.T) {
	if os.Getenv("DURABLE_K6_VERDICT_ARTIFACT") == "" {
		t.Skip("requires hosted PostgreSQL and network-isolated checker")
	}
	for _, name := range []string{"CleanOffsets", "LowerDispositionGap", "ResultReplay", "UnresolvedEffect", "ShutdownFailure",
		"MissingCopy", "CopiesExceedRelayStarts", "CopyWithNoRelayStart", "KeyMismatch", "PayloadMismatch", "DeliveryWithoutCopy", "CollectionGap",
		"BaselineMismatch", "TopicIDChange", "ShutdownNoObservation",
		"MissingCancellationChanges", "MissingGraphChanges"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			engine, dsn := k6TestDatabase(t)
			bindK6Database(t, engine, "engine-instance", "reset-a")
			execK6SQL(t, engine, string(getFile(t, captureMigration+"up.sql")))
			m := k6Manifest(engine.Config().Database, false)
			m.Cell, m.Episode, m.Ordinal, m.Seed = "hook-3", "hook-3-0", 15, 530016
			m.Instrumentation = []string{k6v1.WriteCapture}
			m.RequiredSources = k6v1.RequiredSources(false, false, true)
			plan, arrivals := k6Sources(m, time.Now().Add(-time.Minute), "pure.echo")
			item := plan.Windows[0].Submissions[0]
			seed := name
			if name == "ShutdownNoObservation" {
				seed = "ShutdownFailure"
			}
			seedCapture(t, engine, item, seed)
			seedCapturedTransport(t, engine)
			if name == "LowerDispositionGap" {
				execK6SQL(t, engine, `ALTER TABLE engine.k6_write_capture DISABLE TRIGGER k6_write_immutable;
					DELETE FROM engine.k6_write_capture WHERE subject='physical_delivery' AND row_key->>3='1'`)
			}
			cfg := captureFixtureConfig(t, dsn, m)
			if name != "ResultReplay" && name != "UnresolvedEffect" {
				cfg.ExternalDir = t.TempDir()
				sources := parityExternal(t, &cfg.Manifest, false, item.Key)
				transportParityObservations(t, &sources, "N8LowerGap")
				capturedBrokerCopies(t, cfg.Manifest, engine, &sources, name)
				if seed == "ShutdownFailure" {
					capturedShutdown(t, cfg.Manifest, &sources, name == "ShutdownFailure")
				}
				publishFixtureFrames(t, cfg, sources)
			}
			exit := k6v1.Fail
			if name == "CleanOffsets" {
				exit = k6v1.Incomplete
			}
			r := captureFixtureVerdict(t, cfg, plan, arrivals, exit)
			switch name {
			case "CleanOffsets":
				if len(r.Findings) != 0 || r.Labels[5].Outcome != "covered/pass" || r.Labels[6].Outcome != "covered/pass" {
					t.Fatalf("hook-3 remainder: %+v", r.Findings)
				}
			case "MissingCopy", "CopiesExceedRelayStarts", "CopyWithNoRelayStart", "KeyMismatch", "PayloadMismatch", "DeliveryWithoutCopy":
				codes := map[string]string{"MissingCopy": "BROKER_COPY_MISSING", "CopiesExceedRelayStarts": "BROKER_COPY_WITHOUT_ATTEMPT",
					"CopyWithNoRelayStart": "BROKER_COPY_WITHOUT_ATTEMPT", "KeyMismatch": "BROKER_COPY_KEY_MISMATCH", "PayloadMismatch": "BROKER_COPY_PAYLOAD_MISMATCH",
					"DeliveryWithoutCopy": "BROKER_DELIVERY_WITHOUT_COPY"}
				requirePredicateFinding(t, r, k6v1.Violation, "DispatchDisposition", codes[name])
			case "CollectionGap":
				requirePredicateFinding(t, r, k6v1.MissingProof, "DispatchDisposition", "required_proof_missing:broker_event_copy_range")
			case "BaselineMismatch":
				requirePredicateFinding(t, r, k6v1.Violation, "LowerOffsetsDisposed", "OFFSET_ENGINE_BASELINE_MISMATCH")
			case "TopicIDChange":
				requirePredicateFinding(t, r, k6v1.Violation, "LowerOffsetsDisposed", "OFFSET_BRACKET_TOPIC_CHANGED")
			case "LowerDispositionGap":
				requirePredicateFinding(t, r, k6v1.Violation, "LowerOffsetsDisposed", "CAPTURE_OFFSET_DISPOSITION_GAP")
				requirePredicateFinding(t, r, k6v1.Violation, "LowerOffsetsDisposed", "OFFSET_BRACKET_UNDISPOSED_COPY")
				if r.Labels[6].Outcome != "covered/fail" {
					t.Fatalf("hook-3 offset violation lost: %+v", r.Labels[6])
				}
			case "ResultReplay":
				requirePredicateFinding(t, r, k6v1.Violation, "LostResultReplay", "RESULT_REAPPLIED")
				requireTransportCeiling(t, r.Labels[4])
				if slices.ContainsFunc(r.Findings, func(f k6v1.Finding) bool { return f.Predicate == "LostResultReplay" && f.Kind == k6v1.MissingProof }) {
					t.Fatalf("replay lacked a captured identity: %+v", r.Findings)
				}
			case "UnresolvedEffect":
				requirePredicateFinding(t, r, k6v1.Violation, "UnknownEffectVisible", "UNRESOLVED_EFFECT_REAPPLIED")
				requireTransportCeiling(t, r.Labels[8])
			case "ShutdownFailure":
				requirePredicateFinding(t, r, k6v1.Violation, "ShutdownDisposition", "SHUTDOWN_RECORDED_AS_FAILURE")
				requireTransportCeiling(t, r.Labels[12])
			case "ShutdownNoObservation":
				requirePredicateFinding(t, r, k6v1.MissingProof, "ShutdownDisposition", "required_proof_missing:shutdown_signal_or_exit")
				requireTransportCeiling(t, r.Labels[12])
			case "MissingCancellationChanges", "MissingGraphChanges":
				revision := 4
				if name == "MissingGraphChanges" {
					revision = 6
				}
				var transition string
				if err := engine.QueryRow(t.Context(), "SELECT transition_id::text FROM engine.transition_history WHERE revision=$1",
					revision).Scan(&transition); err != nil {
					t.Fatal(err)
				}
				requireOrderingFinding(t, r, "AtomicHistory", "CAPTURE_HISTORY_CHANGES", k6v1.Violation, "engine.transition_history",
					[]any{item.Key, json.Number(fmt.Sprint(revision)), transition})
				if r.Labels[1].Outcome != "covered/fail" {
					t.Fatalf("history violation lost: %+v", r.Labels[1])
				}
			}
		})
	}
}

func seedCapturedTransport(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	execK6SQL(t, conn, `INSERT INTO engine.reconciliation_items(item_id,workflow_id,partition_id,kind,reference)
		SELECT md5('pending')::uuid,workflow_id,0,'PENDING_OUTBOX','outbox/'||event_id FROM engine.outbox WHERE event_id=md5('o1')::uuid`)
	// The first broker call loses its acknowledgement or its mark. The
	// retry uses the same event and bytes under a second relay claim.
	for _, attempt := range []string{"1", "2"} {
		execK6SQL(t, conn, `UPDATE engine.outbox SET publish_state='CLAIMED',claim_owner='`+captureOwnerID+`',
			claim_expires_at=now(),relay_attempts=`+attempt+` WHERE event_id=md5('o2')::uuid`)
		execK6SQL(t, conn, `INSERT INTO engine.k6_capture_queue(subject,operation,row_key,workflow_id,partition_id,after_image)
			SELECT 'publication_start','start',jsonb_build_array(event_id,relay_attempts),workflow_id,0,
				jsonb_build_object('topic',topic,'relay_owner',claim_owner,'key',event_id,
				'payload_hash',encode(sha256(convert_to(payload::text,'UTF8')),'hex'))
			FROM engine.outbox WHERE event_id=md5('o2')::uuid`)
	}
	execK6SQL(t, conn, `BEGIN;
		INSERT INTO engine.outbox_publications(publication_id,event_id,relay_owner,relay_attempt,broker_acknowledged)
		VALUES (md5('publication-task')::uuid,md5('o2')::uuid,'`+captureOwnerID+`',2,true);
		UPDATE engine.outbox SET publish_state='PUBLISHED',claim_owner=NULL,claim_expires_at=NULL,published_at=now()
		WHERE event_id=md5('o2')::uuid;
		COMMIT;
		BEGIN;
		INSERT INTO engine.event_inbox(consumer_id,event_id,disposition,topic,kafka_partition,kafka_offset)
		VALUES ('runtime-workers-v1',md5('o2')::uuid,'ACCEPTED','durable-agent.tasks.v1',0,0);
		INSERT INTO engine.consumer_offsets(consumer_id,topic,kafka_partition,next_offset)
		VALUES ('runtime-workers-v1','durable-agent.tasks.v1',0,0);
		UPDATE engine.consumer_offsets SET next_offset=1 WHERE consumer_id='runtime-workers-v1';
		INSERT INTO engine.k6_capture_queue(subject,operation,row_key,after_image)
		VALUES ('physical_delivery','ACCEPTED','["runtime-workers-v1","durable-agent.tasks.v1",0,0]',jsonb_build_object('event_id',md5('o2')::uuid));
		COMMIT;
		BEGIN;
		UPDATE engine.consumer_offsets SET next_offset=2 WHERE consumer_id='runtime-workers-v1';
		INSERT INTO engine.k6_capture_queue(subject,operation,row_key,after_image)
		VALUES ('physical_delivery','ALREADY_HANDLED','["runtime-workers-v1","durable-agent.tasks.v1",0,1]',jsonb_build_object('event_id',md5('o2')::uuid));
		COMMIT;`)
}

func seedCapture(t *testing.T, conn *pgx.Conn, item k6v1.Submission, variant string) {
	t.Helper()
	execK6SQL(t, conn, `
		INSERT INTO engine.workflow_definitions(definition_id,version,definition_hash,graph,activity_versions,effect_classes)
			VALUES ('definition',1,'hash','{"nodes":["pure.echo"]}','{"pure.echo":"v1"}','{"pure.echo":"PURE_ACTIVITY"}');
		SET durable.k4_campaign = 'on';
		SET durable.k4_session = 'session';
		SET durable.k4_pod_uid = 'uid';
		SET durable.k4_container_id = 'container';
		SET durable.k6_capture = 'on';
		BEGIN;
		UPDATE engine.partition_leases SET owner_id='`+captureOwnerID+`', epoch=1, lease_expires_at=now()+interval '1 hour' WHERE partition_id=0;
		INSERT INTO engine.k4_audit_queue(kind,partition_id,owner_id,epoch,session_id,pod_uid,container_id)
			VALUES ('acquire',0,'`+captureOwnerID+`',1,'session','uid','container');
		COMMIT;`)
	inTx := func(stmt string, args ...any) {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, stmt, append([]any{pgx.QueryExecModeSimpleProtocol}, args...)...); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	inTx(`INSERT INTO engine.workflow_executions
		(workflow_id,namespace,submission_key,submission_payload_hash,definition_id,definition_version,partition_id,state,revision)
		VALUES ($1,'episode',$1,$2,'definition',1,0,'RUNNABLE',1);
		INSERT INTO engine.node_instances(workflow_id,node_id,iteration,state,input)
		VALUES ($1,'pure.echo',0,'RUNNABLE','{}');
		INSERT INTO engine.retry_policies(workflow_id,node_id,iteration,max_retries,retries_used,checkpoint_schema_version)
		VALUES ($1,'pure.echo',0,3,0,1);
		INSERT INTO engine.transition_history(transition_id,workflow_id,revision,actor_kind,actor_id,node_id,new_state,reason)
		VALUES (md5('h1')::uuid,$1,1,'client','client','pure.echo','RUNNABLE','WORKFLOW_CREATED');
		INSERT INTO engine.outbox(event_id,workflow_id,aggregate_revision,event_type,payload)
		VALUES (md5('o1')::uuid,$1,1,'workflow.created','{}')`, item.Key, "sub-v1:"+item.BodySHA256)
	const fence = `SET LOCAL durable.k4_owner = '` + captureOwnerID + `';
		SET LOCAL durable.k6_owner_partition = '0';
		SET LOCAL durable.k6_owner_epoch = '1';`
	effectClass := "PURE_ACTIVITY"
	if variant == "UnresolvedEffect" {
		effectClass = "NON_COOPERATING_EFFECT"
	}
	inTx(fence+`
		UPDATE engine.workflow_executions SET state='WAITING_ACTIVITY',revision=2;
		UPDATE engine.node_instances SET state='WAITING_ACTIVITY',revision=1;
		INSERT INTO engine.transition_history(transition_id,workflow_id,revision,actor_kind,actor_id,scheduler_epoch,node_id,iteration,old_state,new_state,reason)
		VALUES (md5('h2')::uuid,$1,2,'scheduler','actor',1,'pure.echo',0,'RUNNABLE','WAITING_ACTIVITY','ACTIVITY_SCHEDULED')`, item.Key)
	inTx(fence+`
		UPDATE engine.workflow_executions SET revision=3;
		UPDATE engine.node_instances SET current_attempt_number=1;
		INSERT INTO engine.activity_attempts(workflow_id,node_id,iteration,attempt_number,state,effect_class)
		VALUES ($1,'pure.echo',0,1,'DISPATCHABLE',$2);
		INSERT INTO engine.timers(timer_id,workflow_id,node_id,iteration,due_at,purpose,owning_revision)
		VALUES (md5('timer')::uuid,$1,'pure.echo',0,now(),'RETRY_BACKOFF',2);
		INSERT INTO engine.transition_history(transition_id,workflow_id,revision,actor_kind,actor_id,scheduler_epoch,node_id,iteration,attempt_number,old_state,new_state,reason)
		VALUES (md5('h3')::uuid,$1,3,'scheduler','actor',1,'pure.echo',0,1,'WAITING_ACTIVITY','WAITING_ACTIVITY','ATTEMPT_CREATED');
		INSERT INTO engine.outbox(event_id,workflow_id,aggregate_revision,event_type,topic,payload)
		VALUES (md5('o2')::uuid,$1,3,'attempt.dispatch','durable-agent.tasks.v1','{"node_id":"pure.echo","iteration":0,"attempt_number":1}')`, item.Key, effectClass)
	inTx(`UPDATE engine.activity_attempts SET state='CLAIMED',claim_token=$1,worker_id='worker',worker_request_id='request'`, claimSecret)
	if variant == "MissingCancellationChanges" {
		// The workflow and history commit while the active node and claim remain unchanged.
		inTx(fence+`
			UPDATE engine.workflow_executions SET state='CANCELED',revision=4;
			INSERT INTO engine.transition_history(transition_id,workflow_id,revision,actor_kind,actor_id,scheduler_epoch,old_state,new_state,reason)
			VALUES (md5('h4')::uuid,$1,4,'scheduler','actor',1,'WAITING_ACTIVITY','CANCELED','CANCELED_ALL_ACTIVE_NODES');
			INSERT INTO engine.outbox(event_id,workflow_id,aggregate_revision,event_type,payload)
			VALUES (md5('o4')::uuid,$1,4,'workflow.canceled',jsonb_build_object('workflow_id',$1::text))`, item.Key)
		return
	}
	const result = `UPDATE engine.activity_attempts SET state=$5,is_current=false,result='5' WHERE attempt_number=$4;
		UPDATE engine.workflow_executions SET revision=$3;
		INSERT INTO engine.transition_history(transition_id,workflow_id,revision,actor_kind,actor_id,node_id,iteration,attempt_number,old_state,new_state,reason)
		VALUES (md5('h'||$3::int::text)::uuid,$1,$3,'worker','worker','pure.echo',0,$4,'WAITING_ACTIVITY','WAITING_ACTIVITY','ATTEMPT_RESULT_RECORDED');
		INSERT INTO engine.outbox(event_id,workflow_id,aggregate_revision,event_type,payload)
		VALUES (md5('o'||$3::int::text)::uuid,$1,$3,'activity.result','5');
		INSERT INTO engine.k6_capture_queue(subject,operation,row_key,workflow_id,partition_id,after_image)
		VALUES ('result_request','ACCEPTED',jsonb_build_array($1::text,'pure.echo',0,$4::int),$1,0,
		        jsonb_build_object('token_hash',$2::text,'worker_request_id',$6::text,'receipt_id',NULL,'attempt_state',$5::text))`
	attempt, resultRevision, resultState, token := 1, 4, "SUCCEEDED", claimSecret
	request := "request"
	if variant == "ShutdownFailure" {
		resultState = "FAILED_FINAL"
	}
	if variant == "UnresolvedEffect" {
		inTx(fence+`UPDATE engine.workflow_executions SET state='RECONCILIATION_REQUIRED',revision=4;
			UPDATE engine.activity_attempts SET state='TIMED_OUT',outcome_disposition='OUTCOME_UNKNOWN',is_current=false;
			INSERT INTO engine.reconciliation_items(item_id,workflow_id,partition_id,node_id,iteration,attempt_number,kind,reference)
			VALUES (md5('uncertain')::uuid,$1,0,'pure.echo',0,1,'EXPIRED_ATTEMPT','attempt/uncertain');
			INSERT INTO engine.transition_history(transition_id,workflow_id,revision,actor_kind,actor_id,scheduler_epoch,node_id,iteration,attempt_number,old_state,new_state,reason)
			VALUES (md5('h4')::uuid,$1,4,'scheduler','actor',1,'pure.echo',0,1,'WAITING_ACTIVITY','RECONCILIATION_REQUIRED','TIMEOUT_RECONCILIATION_REQUIRED');
			INSERT INTO engine.outbox(event_id,workflow_id,aggregate_revision,event_type,payload)
			VALUES (md5('o4')::uuid,$1,4,'reconciliation.required','{}')`, item.Key)
		inTx(fence+`UPDATE engine.workflow_executions SET state='WAITING_ACTIVITY',revision=5;
			UPDATE engine.node_instances SET current_attempt_number=2;
			INSERT INTO engine.activity_attempts(workflow_id,node_id,iteration,attempt_number,state,effect_class)
			VALUES ($1,'pure.echo',0,2,'DISPATCHABLE','NON_COOPERATING_EFFECT');
			INSERT INTO engine.transition_history(transition_id,workflow_id,revision,actor_kind,actor_id,scheduler_epoch,node_id,iteration,attempt_number,old_state,new_state,reason)
			VALUES (md5('h5')::uuid,$1,5,'scheduler','actor',1,'pure.echo',0,2,'RECONCILIATION_REQUIRED','WAITING_ACTIVITY','ATTEMPT_CREATED');
			INSERT INTO engine.outbox(event_id,workflow_id,aggregate_revision,event_type,topic,payload)
			VALUES (md5('o5')::uuid,$1,5,'attempt.dispatch','durable-agent.tasks.v1','{"node_id":"pure.echo","iteration":0,"attempt_number":2}')`, item.Key)
		attempt, resultRevision, token = 2, 6, "00000000-0000-0000-0000-000000000066"
		request = "replacement-request"
		inTx(`UPDATE engine.activity_attempts SET state='CLAIMED',claim_token=$1,worker_id='worker',worker_request_id=$2 WHERE attempt_number=2`, token, request)
	}
	inTx(result, item.Key, k6v1.Hash([]byte(token)), resultRevision, attempt, resultState, request)
	if variant == "ResultReplay" {
		resultRevision++
		inTx(`UPDATE engine.activity_attempts SET state='CLAIMED',is_current=true;`+result, item.Key, k6v1.Hash([]byte(token)), resultRevision, attempt, resultState, request)
	}
	nodeState, workflowState := "SUCCEEDED", "RUNNABLE"
	if variant == "ShutdownFailure" {
		nodeState, workflowState = "FAILED", "FAILED"
	}
	inTx(fence+`
		UPDATE engine.workflow_executions SET state=$3,revision=$2;
		UPDATE engine.node_instances SET state=$4,accepted_result='5',current_attempt_number=NULL,revision=2;
		INSERT INTO engine.transition_history(transition_id,workflow_id,revision,actor_kind,actor_id,scheduler_epoch,node_id,iteration,attempt_number,old_state,new_state,reason)
		VALUES (md5('h'||$2::int::text)::uuid,$1,$2,'scheduler','actor',1,'pure.echo',0,$5,'WAITING_ACTIVITY',$3,'RESULT_CONSUMED');
		INSERT INTO engine.outbox(event_id,workflow_id,aggregate_revision,event_type,payload)
		VALUES (md5('o'||$2::int::text)::uuid,$1,$2,'workflow.result_consumed','{}')`, item.Key, resultRevision+1, workflowState, nodeState, attempt)
	if workflowState == "RUNNABLE" {
		sourceWrite := `UPDATE engine.node_instances SET revision=revision+1;`
		if variant == "MissingGraphChanges" {
			sourceWrite = ""
		}
		inTx(fence+sourceWrite+`UPDATE engine.workflow_executions SET state='SUCCEEDED',revision=$2;
			INSERT INTO engine.transition_history(transition_id,workflow_id,revision,actor_kind,actor_id,scheduler_epoch,node_id,iteration,old_state,new_state,reason)
			VALUES (md5('h'||$2::int::text)::uuid,$1,$2,'scheduler','actor',1,'pure.echo',0,'RUNNABLE','SUCCEEDED','GRAPH_ADVANCED');
			INSERT INTO engine.outbox(event_id,workflow_id,aggregate_revision,event_type,payload)
			VALUES (md5('o'||$2::int::text)::uuid,$1,$2,'graph.advanced',jsonb_build_object('workflow_id',$1::text,'from_node_id','pure.echo','next',NULL))`, item.Key, resultRevision+2)
	}
	inTx(`UPDATE engine.partition_leases SET owner_id='` + takeoverOwnerID + `', epoch=2, lease_expires_at=now()+interval '1 hour' WHERE partition_id=0;
		INSERT INTO engine.k4_audit_queue(kind,partition_id,owner_id,epoch,session_id,pod_uid,container_id)
		VALUES ('acquire',0,'` + takeoverOwnerID + `',2,'session','uid','container')`)
	inTx(`SET LOCAL durable.k4_owner = '` + takeoverOwnerID + `';
		SET LOCAL durable.k6_owner_partition = '0'; SET LOCAL durable.k6_owner_epoch = '2';
		UPDATE engine.timers SET consumed_at=clock_timestamp()`)
}
