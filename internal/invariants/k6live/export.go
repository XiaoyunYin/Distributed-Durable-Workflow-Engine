// Package k6live acquires the local cuts and writes only public v1 evidence.
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
	"github.com/jackc/pgx/v5/pgconn"
)

const ConfigVersion = "dur053.k6-live-config.v1"

type Config struct {
	Schema          string        `json:"schema"`
	DatabaseURL     string        `json:"database_url"`
	SinkDatabaseURL string        `json:"sink_database_url"`
	EpisodeDir      string        `json:"episode_dir"`
	Manifest        k6v1.Manifest `json:"manifest"`
	PlanFile        string        `json:"plan_file"`
	ArrivalsFile    string        `json:"arrivals_file"`
	ExternalDir     string        `json:"external_dir"`
}

func (c Config) Valid() bool {
	return c.Schema == ConfigVersion && c.DatabaseURL != "" && c.EpisodeDir != "" &&
		c.PlanFile != "" && c.ArrivalsFile != "" && c.Manifest.Valid() &&
		(c.SinkDatabaseURL != "") == (c.Manifest.Sink != nil)
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
	case k6v1.AttemptScope:
		// The ordinary scheduler stores the node name as PURE scope.
		// A cooperating scope is already a digest and must compare as written.
		return `CASE WHEN "effect_class" = 'PURE_ACTIVITY' AND ` + source + ` = "node_id" THEN ` +
			"encode(sha256(convert_to(" + source + ", 'UTF8')), 'hex') ELSE " + source + " END"
	case k6v1.UUID:
		return source + "::text"
	default:
		return source
	}
}

func readRelation(ctx context.Context, tx pgx.Tx, spec k6v1.Relation, write io.Writer) (k6v1.Watermark, error) {
	projection := make([]string, 0, len(spec.Columns)+1)
	stable := make([]string, 0, len(spec.Columns))
	seen := map[string]bool{}
	order := make([]string, len(spec.Keys))
	for _, col := range spec.Columns {
		projected := sqlProjection(col)
		projection = append(projection, projected)
		if !seen[col.Source] && !k6v1.IdleColumn(spec.Name, col.Source) {
			source := identifier(col.Source)
			if col.Kind == k6v1.Token {
				source = projected
			}
			stable = append(stable, source)
			seen[col.Source] = true
		}
		if i := slices.Index(spec.Keys, col.Name); i >= 0 {
			key := identifier(col.Source)
			if col.Kind == k6v1.Text || col.Kind == k6v1.Digest {
				key += ` COLLATE "C"`
			}
			order[i] = key
		}
	}
	// The additional SQL fingerprint detects changes hidden by public
	// redaction. Only the rolling digest leaves the live adapter. Public
	// journals with idle churn use their verifiable prefix instead.
	sourceHash := "''::text"
	if !k6v1.PrefixWatermark(spec.Name) {
		sourceHash = "encode(sha256(record_send(ROW(" + strings.Join(stable, ", ") + "))), 'hex')"
	}
	projection = append(projection, sourceHash)
	rows, err := tx.Query(ctx, "SELECT "+strings.Join(projection, ", ")+" FROM "+identifier(spec.Name)+" ORDER BY "+strings.Join(order, ", "))
	if err != nil {
		return k6v1.Watermark{}, relationReadError(err, spec)
	}
	defer rows.Close()
	builder := k6v1.NewWatermark(spec)
	for rows.Next() {
		row, sourceDigest, err := scanRow(rows, spec)
		if err != nil {
			return k6v1.Watermark{}, fmt.Errorf("K6_%s_PUBLIC_ROW_SCHEMA %s: %w", databaseLabel(spec), spec.Name, err)
		}
		builder.Add(row, sourceDigest)
		if write != nil {
			raw, err := k6v1.Canonical(row)
			if err != nil {
				return k6v1.Watermark{}, fmt.Errorf("K6_%s_PUBLIC_ROW_SCHEMA %s", databaseLabel(spec), spec.Name)
			}
			if _, err := write.Write(raw); err != nil {
				return k6v1.Watermark{}, errors.New("K6_BUNDLE_WRITE")
			}
		}
	}
	if err := rows.Err(); err != nil {
		return k6v1.Watermark{}, relationReadError(err, spec)
	}
	return builder.Finish(), nil
}

func relationReadError(err error, spec k6v1.Relation) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42703" {
		return fmt.Errorf("COLUMN_UNCLASSIFIED:%s", spec.Name)
	}
	return fmt.Errorf("K6_%s_RELATION_READ %s", databaseLabel(spec), spec.Name)
}

func databaseLabel(spec k6v1.Relation) string {
	if strings.HasPrefix(spec.Name, "k6_sink.") {
		return "SINK"
	}
	return "ENGINE"
}

func exportRelation(ctx context.Context, tx pgx.Tx, directory string, spec k6v1.Relation) (k6v1.File, k6v1.Inventory, k6v1.Watermark, error) {
	var entry k6v1.File
	var mark k6v1.Watermark
	inventory := k6v1.Inventory{Relation: spec.Name, Keys: spec.Keys}
	if err := tx.QueryRow(ctx, "SELECT count(*) FROM "+identifier(spec.Name)).Scan(&inventory.Count); err != nil {
		return entry, inventory, mark, fmt.Errorf("K6_%s_RELATION_COUNT %s", databaseLabel(spec), spec.Name)
	}
	rowPath := k6v1.RelationPath(spec.Name)
	file, err := os.OpenFile(filepath.Join(directory, filepath.FromSlash(rowPath)), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return entry, inventory, mark, errors.New("K6_BUNDLE_WRITE")
	}
	defer file.Close()
	hash := sha256.New()
	buffer := bufio.NewWriter(io.MultiWriter(file, hash))
	mark, err = readRelation(ctx, tx, spec, buffer)
	if err != nil {
		return entry, inventory, mark, err
	}
	if buffer.Flush() != nil || file.Sync() != nil {
		return entry, inventory, mark, errors.New("K6_BUNDLE_WRITE")
	}
	info, err := file.Stat()
	if err != nil || file.Close() != nil {
		return entry, inventory, mark, errors.New("K6_BUNDLE_WRITE")
	}
	inventory.Length = info.Size()
	inventory.SHA256 = hex.EncodeToString(hash.Sum(nil))
	return k6v1.File{Path: rowPath, Length: inventory.Length, SHA256: inventory.SHA256}, inventory, mark, nil
}

func writeJSON(dir, path string, v any) (k6v1.File, error) {
	raw, err := k6v1.Canonical(v)
	if err != nil {
		return k6v1.File{}, errors.New("K6_BUNDLE_ENCODE")
	}
	return writePublic(dir, path, raw)
}

func writePublic(dir, path string, raw []byte) (k6v1.File, error) {
	var entry k6v1.File
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

func scanRow(rows pgx.Rows, spec k6v1.Relation) (k6v1.Row, string, error) {
	dest := make([]any, len(spec.Columns)+1)
	var sourceDigest string
	dest[len(spec.Columns)] = &sourceDigest
	for i, col := range spec.Columns {
		switch col.Kind {
		case k6v1.Text, k6v1.UUID, k6v1.Digest, k6v1.Token, k6v1.WireHash, k6v1.AttemptScope, "traceparent", "container_id":
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
		return nil, "", errors.New("scan projected columns")
	}
	values := make([]any, len(dest))
	for i, col := range spec.Columns {
		value, err := publicValue(col.Kind, dest[i])
		if err != nil {
			return nil, "", fmt.Errorf("decode projected column %s", col.Name)
		}
		values[i] = value
	}
	row := make(k6v1.Row, len(values))
	for i, col := range spec.Columns {
		row[col.Name] = values[i]
	}
	return row, sourceDigest, k6v1.ValidateRow(spec, row)
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
