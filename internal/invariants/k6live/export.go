// Package k6live acquires the engine cut and writes only public v1 evidence.
package k6live

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/invariants/k6v1"
	"github.com/jackc/pgx/v5"
)

const ConfigVersion = "dur053.k6-live-config.v1"

type Config struct {
	Schema      string        `json:"schema"`
	DatabaseURL string        `json:"database_url"`
	EpisodeDir  string        `json:"episode_dir"`
	Manifest    k6v1.Manifest `json:"manifest"`
}

func (c Config) Valid() bool {
	return c.Schema == ConfigVersion && c.DatabaseURL != "" && c.EpisodeDir != "" && c.Manifest.Valid()
}

func Acquire(ctx context.Context, config Config) error {
	if !config.Valid() {
		return errors.New("K6_LIVE_CONFIG")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	settings, err := pgx.ParseConfig(config.DatabaseURL)
	if err != nil {
		return errors.New("K6_DATABASE_CONFIG")
	}
	settings.ConnectTimeout = 10 * time.Second
	settings.RuntimeParams = map[string]string{
		"application_name":                    "durable-checker-k6-v1",
		"statement_timeout":                   "30s",
		"idle_in_transaction_session_timeout": "30s",
		"lock_timeout":                        "2s",
		"search_path":                         "pg_catalog",
		"client_encoding":                     "UTF8",
		"row_security":                        "off",
	}
	conn, err := pgx.ConnectConfig(ctx, settings)
	if err != nil {
		return errors.New("K6_DATABASE_CONNECT")
	}
	defer func() {
		closeCtx, stop := context.WithTimeout(context.Background(), 2*time.Second)
		defer stop()
		_ = conn.Close(closeCtx)
	}()
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return errors.New("K6_DATABASE_BEGIN")
	}
	defer func() { _ = tx.Rollback(ctx) }()

	start := time.Now()
	observe := func() k6v1.Observer {
		now := time.Now()
		return k6v1.Observer{UTC: k6v1.UTC(now), MonotonicNS: now.Sub(start).Nanoseconds()}
	}
	m := config.Manifest
	snap := k6v1.Snapshot{Schema: k6v1.SnapshotVersion, BarrierID: m.BarrierID, ResetID: m.ResetID}
	var transaction, sampled time.Time
	snap.Open.Before = observe()
	err = tx.QueryRow(ctx, `SELECT pg_current_snapshot()::text, current_database()::text,
		oid, transaction_timestamp(), clock_timestamp() FROM pg_database WHERE datname = current_database()`).
		Scan(&snap.Visibility, &snap.Database, &snap.DatabaseOID, &transaction, &sampled)
	snap.Open.After = observe()
	if err != nil || snap.Database != m.Database {
		return errors.New("K6_DATABASE_IDENTITY")
	}
	snap.Open.DatabaseUTC, snap.TransactionUTC = k6v1.UTC(sampled), k6v1.UTC(transaction)
	specs := k6v1.EngineRelations(m.Instrumented())
	snap.Census, err = census(ctx, tx, specs, m.Instrumented())
	if err != nil {
		return err
	}
	catalog := make(map[string]*k6v1.CensusEntry, len(snap.Census))
	for i := range snap.Census {
		entry := &snap.Census[i]
		catalog[entry.Name] = entry
	}
	for _, spec := range specs {
		if entry := catalog[spec.Name]; entry == nil || entry.Classification != k6v1.Exported {
			return fmt.Errorf("K6_ENGINE_RELATION_MISSING %s", spec.Name)
		}
	}
	if err := tx.QueryRow(ctx, "SELECT count(*) FROM engine.workflow_executions WHERE namespace=$1", m.Namespace).
		Scan(&snap.NamespaceCount); err != nil {
		return errors.New("K6_ENGINE_NAMESPACE_COUNT")
	}

	if os.Mkdir(config.EpisodeDir, 0o755) != nil || os.Mkdir(filepath.Join(config.EpisodeDir, "engine"), 0o755) != nil {
		return errors.New("K6_CREATE_BUNDLE")
	}
	entry, err := writeJSON(config.EpisodeDir, k6v1.ManifestPath, m)
	if err != nil {
		return err
	}
	index := k6v1.Index{Schema: k6v1.IndexVersion, Files: []k6v1.File{entry}}
	snap.ManifestSHA256 = entry.SHA256
	for _, spec := range specs {
		entry, inventory, err := exportRelation(ctx, tx, config.EpisodeDir, spec)
		if err != nil {
			return err
		}
		index.Files = append(index.Files, entry)
		snap.Relations = append(snap.Relations, inventory)
		exported := catalog[spec.Name]
		exported.Count, exported.SHA256 = &inventory.Count, &inventory.SHA256
	}

	snap.Close.Before = observe()
	err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&sampled)
	snap.Close.After = observe()
	if err != nil {
		return errors.New("K6_DATABASE_CLOSE_CLOCK")
	}
	snap.Close.DatabaseUTC = k6v1.UTC(sampled)
	snap.Commit = "committed"
	if err := tx.Commit(ctx); err != nil {
		snap.Commit = "failed_or_unconfirmed"
	}
	snap.CommitAfter = observe()
	entry, err = writeJSON(config.EpisodeDir, k6v1.SnapshotPath, snap)
	if err != nil {
		return err
	}
	index.Files = append(index.Files, entry)
	slices.SortFunc(index.Files, func(a, b k6v1.File) int { return strings.Compare(a.Path, b.Path) })
	_, err = writeJSON(config.EpisodeDir, k6v1.IndexPath, index)
	return err
}

func identifier(name string) string { return pgx.Identifier(strings.Split(name, ".")).Sanitize() }

func sqlProjection(col k6v1.Column) string {
	source := identifier(col.Source)
	switch col.Kind {
	case k6v1.Presence:
		return source + " IS NOT NULL"
	case k6v1.WireHash:
		return "encode(sha256(" + source + "), 'hex')"
	case k6v1.WireLength:
		return "octet_length(" + source + ")::bigint"
	case k6v1.Integer:
		return source + "::bigint"
	case k6v1.Token:
		return "encode(sha256(convert_to(" + source + "::text, 'UTF8')), 'hex')"
	case k6v1.UUID:
		return source + "::text"
	default:
		return source
	}
}

func exportRelation(ctx context.Context, tx pgx.Tx, directory string, spec k6v1.Relation) (k6v1.File, k6v1.Inventory, error) {
	var entry k6v1.File
	inventory := k6v1.Inventory{Relation: spec.Name, Keys: spec.Keys}
	table := identifier(spec.Name)
	if err := tx.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&inventory.Count); err != nil {
		return entry, inventory, fmt.Errorf("K6_ENGINE_RELATION_COUNT %s", spec.Name)
	}
	projection := make([]string, 0, len(spec.Columns))
	order := make([]string, len(spec.Keys))
	for _, col := range spec.Columns {
		projection = append(projection, sqlProjection(col))
		if i := slices.Index(spec.Keys, col.Name); i >= 0 {
			key := identifier(col.Source)
			if col.Kind == k6v1.Text {
				key += ` COLLATE "C"`
			}
			order[i] = key
		}
	}
	rows, err := tx.Query(ctx, "SELECT "+strings.Join(projection, ", ")+" FROM "+table+" ORDER BY "+strings.Join(order, ", "))
	if err != nil {
		return entry, inventory, fmt.Errorf("K6_ENGINE_RELATION_READ %s", spec.Name)
	}
	defer rows.Close()

	rowPath := k6v1.RelationPath(spec.Name)
	file, err := os.OpenFile(filepath.Join(directory, filepath.FromSlash(rowPath)), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return entry, inventory, errors.New("K6_BUNDLE_WRITE")
	}
	defer file.Close()
	hash := sha256.New()
	buffer := bufio.NewWriter(io.MultiWriter(file, hash))
	for rows.Next() {
		row, err := scanRow(rows, spec)
		if err != nil {
			return entry, inventory, fmt.Errorf("K6_ENGINE_PUBLIC_ROW_SCHEMA %s", spec.Name)
		}
		raw, err := k6v1.Canonical(row)
		if err != nil {
			return entry, inventory, fmt.Errorf("K6_ENGINE_PUBLIC_ROW_SCHEMA %s", spec.Name)
		}
		n, err := buffer.Write(raw)
		if err != nil {
			return entry, inventory, errors.New("K6_BUNDLE_WRITE")
		}
		inventory.Length += int64(n)
	}
	if rows.Err() != nil {
		return entry, inventory, fmt.Errorf("K6_ENGINE_RELATION_READ %s", spec.Name)
	}
	if buffer.Flush() != nil || file.Sync() != nil || file.Close() != nil {
		return entry, inventory, errors.New("K6_BUNDLE_WRITE")
	}
	inventory.SHA256 = hex.EncodeToString(hash.Sum(nil))
	return k6v1.File{Path: rowPath, Length: inventory.Length, SHA256: inventory.SHA256}, inventory, nil
}

func writeJSON(dir, path string, v any) (k6v1.File, error) {
	var entry k6v1.File
	raw, err := k6v1.Canonical(v)
	if err != nil {
		return entry, errors.New("K6_BUNDLE_ENCODE")
	}
	file, err := os.OpenFile(filepath.Join(dir, filepath.FromSlash(path)), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return entry, errors.New("K6_BUNDLE_WRITE")
	}
	defer file.Close()
	if _, err := file.Write(raw); err != nil || file.Sync() != nil || file.Close() != nil {
		return entry, errors.New("K6_BUNDLE_WRITE")
	}
	return k6v1.File{Path: path, Length: int64(len(raw)), SHA256: k6v1.Hash(raw)}, nil
}

func scanRow(rows pgx.Rows, spec k6v1.Relation) (k6v1.Row, error) {
	dest := make([]any, len(spec.Columns))
	for i, col := range spec.Columns {
		switch col.Kind {
		case k6v1.Text, k6v1.UUID, k6v1.Digest, k6v1.Token, k6v1.WireHash, "traceparent", "container_id":
			dest[i] = new(*string)
		case k6v1.Integer, k6v1.WireLength:
			dest[i] = new(*int64)
		case k6v1.Timestamp:
			dest[i] = new(*time.Time)
		case k6v1.Boolean, k6v1.Presence:
			dest[i] = new(*bool)
		default:
			dest[i] = new([]byte)
		}
	}
	if err := rows.Scan(dest...); err != nil {
		return nil, err
	}
	values := make([]any, len(dest))
	for i, col := range spec.Columns {
		value, err := publicValue(col.Kind, dest[i])
		if err != nil {
			return nil, err
		}
		values[i] = value
	}
	row := make(k6v1.Row, len(values))
	for i, col := range spec.Columns {
		row[col.Name] = values[i]
	}
	return row, k6v1.ValidateRow(spec, row)
}

func publicValue(kind k6v1.Kind, dest any) (any, error) {
	switch value := dest.(type) {
	case **string:
		if *value != nil {
			return **value, nil
		}
	case **int64:
		if *value != nil {
			return json.Number(strconv.FormatInt(**value, 10)), nil
		}
	case **bool:
		if *value != nil {
			return **value, nil
		}
	case **time.Time:
		if *value != nil {
			return k6v1.UTC(**value), nil
		}
	case *[]byte:
		if *value != nil {
			if string(*value) == "null" {
				return nil, nil
			}
			return k6v1.DecodeDatabaseStructure(kind, *value)
		}
	}
	return nil, nil
}
