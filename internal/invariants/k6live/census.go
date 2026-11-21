package k6live

import (
	"context"
	"errors"
	"slices"
	"strings"

	"durable-agent-execution-engine/internal/invariants/k6v1"
	"github.com/jackc/pgx/v5"
)

func census(ctx context.Context, tx pgx.Tx, specs []k6v1.Relation, instrumented bool) ([]k6v1.CensusEntry, error) {
	rows, err := tx.Query(ctx, `
		SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname),
		       c.relkind::text, c.relispartition, x.extname::text,
		       ARRAY(SELECT attname::text FROM pg_attribute WHERE attrelid=c.oid AND attnum > 0 AND NOT attisdropped ORDER BY attname::text COLLATE "C")
		FROM pg_class AS c
		JOIN pg_namespace AS n ON n.oid = c.relnamespace
		LEFT JOIN pg_depend AS d ON d.classid = 'pg_class'::regclass AND d.objid = c.oid
			AND d.objsubid = 0 AND d.refclassid = 'pg_extension'::regclass AND d.deptype = 'e'
		LEFT JOIN pg_extension AS x ON x.oid = d.refobjid
		WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
		  AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f')`)
	if err != nil {
		return nil, errors.New("K6_ENGINE_CENSUS")
	}
	defer rows.Close()
	entries := []k6v1.CensusEntry{}
	for rows.Next() {
		var entry k6v1.CensusEntry
		var columns []string
		if err := rows.Scan(&entry.Name, &entry.Kind, &entry.Partition, &entry.Extension, &columns); err != nil {
			return nil, errors.New("K6_ENGINE_CENSUS")
		}
		entry.Name = k6v1.CatalogName(entry.Name)
		if entry.Extension != nil && *entry.Extension != "pg_stat_statements" {
			// Known exclusion needs the extension identity. For an unexpected
			// extension retain a stable fingerprint of its catalog name.
			safe := "extension_" + k6v1.Hash([]byte(*entry.Extension))[:48]
			entry.Extension = &safe
		}
		entry.Classification, entry.Reason = k6v1.Classify(entry, specs, instrumented)
		if entry.Classification == k6v1.Exported || entry.Reason == "safe_resource_projection" {
			// Unexpected column names need only be identifiable by a safe fingerprint.
			var pinned []string
			for _, spec := range specs {
				if spec.Name == entry.Name {
					pinned = spec.SourceColumns()
				}
			}
			if entry.Reason == "safe_resource_projection" {
				pinned = k6v1.ResourceSourceColumns()
			}
			for i, column := range columns {
				if !slices.Contains(pinned, column) {
					columns[i] = "unclassified_" + k6v1.Hash([]byte(column))
				}
			}
			slices.Sort(columns)
			entry.Columns = columns
		}
		entries = append(entries, entry)
	}
	if rows.Err() != nil {
		return nil, errors.New("K6_ENGINE_CENSUS")
	}
	slices.SortFunc(entries, func(a, b k6v1.CensusEntry) int { return strings.Compare(a.Name, b.Name) })
	for i := range entries {
		entry := &entries[i]
		if entry.Reason == "safe_resource_projection" {
			if err := tx.QueryRow(ctx, "SELECT count(resource_id) FROM k6_sink.resources").Scan(&entry.Count); err != nil {
				return nil, errors.New("K6_SINK_RESOURCE_COUNT")
			}
		}
		if entry.Reason != k6v1.FixtureCorpus {
			continue
		}
		// Excluded fixture bodies and embeddings never cross the DB boundary.
		// Hash sorted fixed-width row digests, including multiplicity, in SQL.
		table := identifier(entry.Name)
		query := `SELECT count(*), encode(sha256(convert_to(COALESCE(string_agg(h, '' ORDER BY h COLLATE "C"),''), 'UTF8')), 'hex')
		           FROM (SELECT encode(sha256(record_send(r)), 'hex') AS h FROM ` + table + ` AS r) AS hashes`
		if err := tx.QueryRow(ctx, query).Scan(&entry.Count, &entry.SHA256); err != nil {
			return nil, errors.New("K6_ENGINE_CORPUS_DIGEST")
		}
	}
	return entries, nil
}
