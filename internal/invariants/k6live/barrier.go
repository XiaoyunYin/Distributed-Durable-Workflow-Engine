package k6live

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/invariants/k6v1"
	"github.com/jackc/pgx/v5"
)

type observer func() k6v1.Observer
type observationGate func(context.Context, string, k6v1.QuietSample) error

type database struct {
	name  string
	conn  *pgx.Conn
	specs []k6v1.Relation
}

func Acquire(ctx context.Context, config Config) error {
	return acquireObserved(ctx, config, nil, smokeGate())
}

func acquire(parent context.Context, config Config, afterCut func(string, int)) error {
	return acquireObserved(parent, config, afterCut, nil)
}

func acquireObserved(parent context.Context, config Config, afterCut func(string, int), gate observationGate) error {
	if !config.Valid() {
		return errors.New("K6_LIVE_CONFIG")
	}
	m := config.Manifest
	var plan k6v1.Plan
	planRaw, err := loadSource(config.PlanFile, &plan)
	if err != nil || k6v1.Hash(planRaw) != m.PlanSHA256 || !plan.Valid(m) {
		return errors.New("K6_PLAN_SOURCE")
	}
	var arrivals k6v1.Arrivals
	if _, err := loadSource(config.ArrivalsFile, &arrivals); err != nil || !arrivals.Valid(m) {
		return errors.New("K6_ARRIVALS_SOURCE")
	}
	deadline := plan.Deadline(arrivals)
	ctx, cancel := context.WithDeadline(parent, deadline)
	defer cancel()
	start := time.Now()
	observe := func() k6v1.Observer {
		now := time.Now()
		return k6v1.Observer{UTC: k6v1.UTC(now), MonotonicNS: now.Sub(start).Nanoseconds()}
	}

	dir := config.EpisodeDir
	if os.Mkdir(dir, 0o755) != nil {
		return errors.New("K6_CREATE_BUNDLE")
	}
	index := k6v1.Index{Schema: k6v1.IndexVersion}
	for _, source := range []struct {
		path  string
		value any
	}{
		{k6v1.ManifestPath, m}, {k6v1.PlanPath, plan}, {k6v1.ArrivalsPath, arrivals},
	} {
		entry, err := writeJSON(dir, source.path, source.value)
		if err != nil {
			return err
		}
		index.Files = append(index.Files, entry)
	}
	barrier := k6v1.Barrier{
		Schema: k6v1.BarrierVersion, ManifestSHA256: index.Files[0].SHA256,
		BarrierID: m.BarrierID, ResetID: m.ResetID, DeadlineUTC: k6v1.UTC(deadline),
	}
	external := newExternal(config.ExternalDir, m, observe)
	quietFloor := max(plan.LastScheduledUTC(), arrivals.SealedUTC)
	if arrivals.RemovalUTC != nil {
		quietFloor = max(quietFloor, *arrivals.RemovalUTC)
	}
	var databases []database
	openDatabases := func() error {
		for _, db := range m.Databases() {
			dsn := config.DatabaseURL
			if db == "sink" {
				dsn = config.SinkDatabaseURL
			}
			conn, err := connect(ctx, db, dsn)
			if err != nil {
				return err
			}
			databases = append(databases, database{db, conn, m.Relations(db)})
		}
		return nil
	}
	defer func() {
		for _, db := range databases {
			closeCtx, stop := context.WithTimeout(context.Background(), 2*time.Second)
			_ = db.conn.Close(closeCtx)
			stop()
		}
	}()
	backoff := 25 * time.Millisecond
	var acquisitionErr error
	for generation := 1; ; generation++ {
		if generation > 1 {
			gate = nil
		}
		attempt := k6v1.CutAttempt{Generation: generation, Started: observe(), Reasons: []string{}, Databases: []k6v1.DatabaseCut{}}
		if ctx.Err() == nil && len(databases) == 0 {
			acquisitionErr = openDatabases()
		}
		var externalPre, externalPost k6v1.QuietSample
		quiet, externalRaced, externalContinuous := false, false, true
		if acquisitionErr == nil && config.ExternalDir != "" {
			externalPre, quiet, acquisitionErr = external.waitQuiet(ctx, generation, quietFloor, databases, gate)
			attempt.Started = observe()
		}
		var files []k6v1.File
		var stage string
		if ctx.Err() == nil && acquisitionErr == nil {
			stage, acquisitionErr = os.MkdirTemp(dir, ".pair-")
			if acquisitionErr == nil {
				attempt.Databases, files, acquisitionErr = capturePair(ctx, m, databases, stage, barrier.ManifestSHA256, generation, observe, afterCut, func() error {
					if gate != nil && quiet {
						return gate(ctx, "mid-cut", externalPre)
					}
					return nil
				})
			}
		}
		attempt.Finished = observe()
		if quiet && len(attempt.Databases) > 0 {
			externalPost, externalRaced, externalContinuous = external.cutSample(attempt.Finished, externalPre, attempt.Databases)
		}
		for i, cut := range attempt.Databases {
			attempt.Reasons = append(attempt.Reasons, cut.RaceReasons(databases[i].specs)...)
		}
		expired := attempt.Finished.UTC > barrier.DeadlineUTC
		if ctx.Err() != nil && acquisitionErr == nil && !expired {
			acquisitionErr = errors.New("K6_CUT_CONTEXT_DONE")
		}
		if acquisitionErr != nil && !expired {
			reason := acquisitionErr.Error()
			if reason != "CUT_BINDING:engine" && reason != "CUT_BINDING:sink" && !strings.HasPrefix(reason, "COLUMN_UNCLASSIFIED:") {
				reason = "CUT_INTERRUPTED"
			}
			attempt.Reasons = append(attempt.Reasons, reason)
		}
		if expired {
			attempt.Reasons = append(attempt.Reasons, "CUT_DEADLINE")
		}
		if externalRaced {
			attempt.Reasons = append(attempt.Reasons, "CUT_RACED:external")
		}
		barrier.Attempts = append(barrier.Attempts, attempt)
		retry := acquisitionErr == nil && ctx.Err() == nil && len(attempt.Reasons) != 0 && !expired && externalContinuous
		if canSelect(attempt, stage, files, config.ExternalDir == "" || quiet && externalContinuous) {
			for _, db := range databases {
				if err := os.Rename(filepath.Join(stage, db.name), filepath.Join(dir, db.name)); err != nil {
					return errors.New("K6_BUNDLE_WRITE")
				}
			}
			barrier.SelectedGeneration = generation
			index.Files = append(index.Files, files...)
			if quiet && externalContinuous && externalPost.At.UTC != "" {
				external.finish(ctx, externalPost, databases)
			}
		}
		if stage != "" {
			if err := os.RemoveAll(stage); err != nil {
				return errors.New("K6_BUNDLE_WRITE")
			}
		}
		if !retry {
			break
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
		case <-timer.C:
		}
		timer.Stop()
		backoff = min(200*time.Millisecond, backoff*2)
	}
	entry, err := writeJSON(dir, k6v1.BarrierPath, barrier)
	if err != nil {
		return err
	}
	if acquisitionErr != nil && slices.Contains(barrier.Attempts[len(barrier.Attempts)-1].Reasons, "CUT_INTERRUPTED") {
		return acquisitionErr
	}
	index.Files = append(index.Files, entry)
	if err := external.publish(dir, &index); err != nil {
		return err
	}
	slices.SortFunc(index.Files, func(a, b k6v1.File) int { return strings.Compare(a.Path, b.Path) })
	_, err = writeJSON(dir, k6v1.IndexPath, index)
	return err
}

func canSelect(attempt k6v1.CutAttempt, stage string, files []k6v1.File, continuous bool) bool {
	return len(attempt.Reasons) == 0 && stage != "" && len(files) != 0 && continuous
}

func loadSource(path string, target any) ([]byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil || k6v1.DecodeStrict(raw, target) != nil {
		return nil, errors.New("K6_SOURCE_READ")
	}
	canonical, err := k6v1.Canonical(target)
	if err != nil || !bytes.Equal(raw, canonical) {
		return nil, errors.New("K6_SOURCE_SCHEMA")
	}
	return raw, nil
}

func connect(ctx context.Context, db, dsn string) (*pgx.Conn, error) {
	settings, err := pgx.ParseConfig(dsn)
	if err != nil || (db == "sink" && settings.User != "k6_sink_exporter") {
		return nil, fmt.Errorf("K6_DATABASE_CONFIG:%s", db)
	}
	settings.ConnectTimeout = 10 * time.Second
	settings.RuntimeParams = map[string]string{
		"application_name":                    "durable-checker-k6-v1-" + db,
		"statement_timeout":                   "30s",
		"idle_in_transaction_session_timeout": "30s",
		"lock_timeout":                        "2s",
		"search_path":                         "pg_catalog",
		"client_encoding":                     "UTF8",
		"row_security":                        "off",
	}
	conn, err := pgx.ConnectConfig(ctx, settings)
	if err != nil {
		return nil, fmt.Errorf("K6_DATABASE_CONNECT:%s", db)
	}
	return conn, nil
}

func capturePair(ctx context.Context, m k6v1.Manifest, databases []database, stage, manifestHash string, generation int, observe observer, afterCut func(string, int), beforePost func() error) ([]k6v1.DatabaseCut, []k6v1.File, error) {
	cuts := make([]k6v1.DatabaseCut, len(databases))
	var files []k6v1.File
	for i, db := range databases {
		pre, err := sample(ctx, db, m, observe)
		if err != nil {
			return []k6v1.DatabaseCut{}, nil, err
		}
		cuts[i] = k6v1.DatabaseCut{DB: db.name, Pre: pre}
	}
	for i, db := range databases {
		entry, mark, err := capture(ctx, db, m, stage, manifestHash, generation, observe)
		if err != nil {
			return []k6v1.DatabaseCut{}, nil, err
		}
		files = append(files, entry...)
		cuts[i].Cut, cuts[i].SnapshotSHA256 = mark, entry[len(entry)-1].SHA256
		if afterCut != nil {
			afterCut(db.name, generation)
		}
	}
	if err := beforePost(); err != nil {
		return []k6v1.DatabaseCut{}, nil, err
	}
	for i, db := range databases {
		post, err := sample(ctx, db, m, observe)
		if err != nil {
			return []k6v1.DatabaseCut{}, nil, err
		}
		cuts[i].Post = post
	}
	return cuts, files, nil
}

var cutOptions = pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}

func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func openSnapshot(ctx context.Context, tx pgx.Tx, db database, m k6v1.Manifest, observe observer) (k6v1.WatermarkSample, string, error) {
	var sample k6v1.WatermarkSample
	var comment, role string
	var transaction, clock time.Time
	sample.Open.Before = observe()
	err := tx.QueryRow(ctx, `SELECT pg_current_snapshot()::text, current_database()::text, oid,
		COALESCE(shobj_description(oid,'pg_database'), ''), current_user::text,
		transaction_timestamp(), clock_timestamp() FROM pg_database WHERE datname = current_database()`).
		Scan(&sample.Visibility, &sample.Database, &sample.DatabaseOID, &comment, &role, &transaction, &clock)
	sample.Open.After = observe()
	expected := m.Identity(db.name)
	if err != nil || sample.Database != expected.Database || comment != "dur053.k6.v1|"+expected.Instance+"|"+m.ResetID ||
		(db.name == "sink" && role != "k6_sink_exporter") {
		return sample, "", fmt.Errorf("CUT_BINDING:%s", db.name)
	}
	sample.Instance, sample.ResetID = expected.Instance, m.ResetID
	sample.Open.DatabaseUTC = k6v1.UTC(clock)
	return sample, k6v1.UTC(transaction), nil
}

func sample(ctx context.Context, db database, m k6v1.Manifest, observe observer) (k6v1.WatermarkSample, error) {
	tx, err := db.conn.BeginTx(ctx, cutOptions)
	if err != nil {
		return k6v1.WatermarkSample{}, errors.New("K6_DATABASE_BEGIN")
	}
	defer rollback(tx)
	mark, _, err := openSnapshot(ctx, tx, db, m, observe)
	if err != nil {
		return mark, err
	}
	mark.Terminal = true
	if db.name == "engine" {
		if err := tx.QueryRow(ctx, `SELECT NOT EXISTS (SELECT 1 FROM engine.workflow_executions
			WHERE state NOT IN ('SUCCEEDED','FAILED','REJECTED','CANCELED','ABANDONED'))`).Scan(&mark.Terminal); err != nil {
			return mark, errors.New("K6_TERMINAL_SAMPLE")
		}
	}
	for _, spec := range db.specs {
		value, err := readRelation(ctx, tx, spec, nil)
		if err != nil {
			return mark, err
		}
		mark.Relations = append(mark.Relations, value)
	}
	if err := tx.Commit(ctx); err != nil {
		return mark, errors.New("K6_WATERMARK_COMMIT")
	}
	mark.CommitAfter = observe()
	return mark, nil
}

func capture(ctx context.Context, db database, m k6v1.Manifest, stage, manifestHash string, generation int, observe observer) ([]k6v1.File, []k6v1.Watermark, error) {
	if os.Mkdir(filepath.Join(stage, db.name), 0o755) != nil {
		return nil, nil, errors.New("K6_CREATE_BUNDLE")
	}
	tx, err := db.conn.BeginTx(ctx, cutOptions)
	if err != nil {
		return nil, nil, errors.New("K6_DATABASE_BEGIN")
	}
	defer rollback(tx)
	open, transaction, err := openSnapshot(ctx, tx, db, m, observe)
	if err != nil {
		return nil, nil, err
	}
	version := k6v1.SnapshotVersion
	if db.name == "sink" {
		version = k6v1.SinkSnapshotVersion
	}
	snap := k6v1.Snapshot{
		Schema: version, ManifestSHA256: manifestHash, BarrierID: m.BarrierID, ResetID: open.ResetID,
		Database: open.Database, Instance: open.Instance, DatabaseOID: open.DatabaseOID, Generation: generation,
		Visibility: open.Visibility, TransactionUTC: transaction, Open: open.Open,
	}
	snap.Census, err = census(ctx, tx, db.specs, m.Instrumented())
	if err != nil {
		return nil, nil, err
	}
	catalog := make(map[string]*k6v1.CensusEntry, len(snap.Census))
	for i := range snap.Census {
		entry := &snap.Census[i]
		catalog[entry.Name] = entry
	}
	if db.name == "engine" {
		err = tx.QueryRow(ctx, "SELECT count(*) FROM engine.workflow_executions WHERE namespace=$1", m.Namespace).Scan(&snap.NamespaceCount)
		if err != nil {
			return nil, nil, errors.New("K6_ENGINE_NAMESPACE_COUNT")
		}
	}
	var files []k6v1.File
	var watermarks []k6v1.Watermark
	for _, spec := range db.specs {
		item := catalog[spec.Name]
		if item == nil || item.Classification != k6v1.Exported {
			return nil, nil, fmt.Errorf("K6_RELATION_MISSING:%s:%s", db.name, spec.Name)
		}
		entry, inventory, mark, err := exportRelation(ctx, tx, stage, spec)
		if err != nil {
			return nil, nil, err
		}
		files = append(files, entry)
		snap.Relations = append(snap.Relations, inventory)
		watermarks = append(watermarks, mark)
		item.Count, item.SHA256 = &inventory.Count, &inventory.SHA256
	}
	var sampled time.Time
	snap.Close.Before = observe()
	err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&sampled)
	snap.Close.After = observe()
	if err != nil {
		return nil, nil, errors.New("K6_DATABASE_CLOSE_CLOCK")
	}
	snap.Close.DatabaseUTC = k6v1.UTC(sampled)
	snap.Commit = "committed"
	if err := tx.Commit(ctx); err != nil {
		snap.Commit = "failed_or_unconfirmed"
	}
	snap.CommitAfter = observe()
	entry, err := writeJSON(stage, db.name+"/snapshot.json", snap)
	if err != nil {
		return nil, nil, err
	}
	return append(files, entry), watermarks, nil
}
