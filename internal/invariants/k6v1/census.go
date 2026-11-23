package k6v1

import (
	"regexp"
	"slices"
	"strings"
)

const (
	Exported     = "exported"
	Excluded     = "excluded"
	Unclassified = "unclassified"
)

type CensusEntry struct {
	Name           string   `json:"name"`
	Kind           string   `json:"kind"`
	Partition      bool     `json:"partition"`
	Extension      *string  `json:"extension"`
	Classification string   `json:"classification"`
	Reason         string   `json:"reason"`
	Count          *int64   `json:"count"`
	SHA256         *string  `json:"sha256"`
	Columns        []string `json:"columns"`
}

const FixtureCorpus = "fixture_corpus"

var qualified = regexp.MustCompile(`^[a-z_][a-z_0-9]*\.[a-z_][a-z_0-9]*$`)

// CatalogName safely identifies even an unexpected relation with an opaque
// identifier. The digest covers PostgreSQL's quoted schema-qualified name.
func CatalogName(name string) string {
	if qualified.MatchString(name) && !strings.HasPrefix(name, "unpublished.") {
		return name
	}
	return "unpublished.n" + Hash([]byte(name))
}

func Classify(entry CensusEntry, relations []Relation, instrumented bool) (string, string) {
	if !entry.Partition && entry.Extension == nil && slices.ContainsFunc(relations, func(spec Relation) bool { return spec.Name == SinkResources }) {
		switch {
		case entry.Kind == "v" && entry.Name == SinkResources:
			return Exported, ""
		case entry.Kind == "r" && entry.Name == "k6_sink.resources":
			return Excluded, "safe_resource_projection"
		case entry.Kind == "S" && entry.Name == "k6_sink.calls_call_id_seq":
			return Excluded, "allocator_for_exported_calls"
		case entry.Kind == "r" && slices.ContainsFunc(relations, func(spec Relation) bool { return spec.Name == entry.Name && spec.Name != SinkResources }):
			return Exported, ""
		default:
			return Unclassified, "outside_v1_census"
		}
	}
	if slices.ContainsFunc(relations, func(spec Relation) bool { return spec.Name == SinkResources }) {
		return Unclassified, "outside_v1_census"
	}
	if entry.Kind == "r" && !entry.Partition && entry.Extension == nil {
		for _, spec := range relations {
			if spec.Name == entry.Name {
				return Exported, ""
			}
		}
		switch entry.Name {
		case "source_corpus.documents", "source_corpus.chunks", "source_corpus.fixture_cases":
			return Excluded, FixtureCorpus
		}
	}
	if !entry.Partition && entry.Extension == nil {
		switch entry.Kind {
		case "v":
			switch entry.Name {
			case "engine.k4_claim_audit", "engine.k4_history_order", "engine.k4_lease_audit":
				return Excluded, "view_of_exported_audit"
			case "k6_auth.episode_grants":
				return Excluded, "credential_view_of_exported_tables"
			}
		case "S":
			switch entry.Name {
			case "engine.k4_audit_queue_id_seq", "engine.k4_commit_order":
				return Excluded, "allocator_for_exported_audit"
			case "engine.k6_capture_queue_id_seq":
				if slices.ContainsFunc(relations, func(spec Relation) bool { return spec.Name == CaptureQueue }) {
					return Excluded, "allocator_for_exported_audit"
				}
			case "k6_f3.deadline_writes_id_seq":
				if instrumented {
					return Excluded, "allocator_for_exported_instrumentation"
				}
			}
		}
	}
	if entry.Kind == "v" && !entry.Partition && entry.Extension != nil && *entry.Extension == "pg_stat_statements" {
		switch entry.Name {
		case "public.pg_stat_statements", "public.pg_stat_statements_info":
			return Excluded, "extension_statistics"
		}
	}
	return Unclassified, "outside_v1_census"
}

func (spec Relation) SourceColumns() []string {
	columns := slices.Clone(spec.ExcludedColumns)
	for _, col := range spec.Columns {
		columns = append(columns, col.Source)
	}
	slices.Sort(columns)
	return slices.Compact(columns)
}

func ResourceSourceColumns() []string {
	return []string{"intent_id", "latest_fence", "logical_key", "resource_id", "revision", "state"}
}

func verifyCensus(entries []CensusEntry, relations []Relation, inventories []Inventory, instrumented bool, db string) *Finding {
	exported := 0
	prior := ""
	source := db + "/snapshot.json"
	for _, entry := range entries {
		if !qualified.MatchString(entry.Name) || entry.Name <= prior ||
			strings.HasPrefix(entry.Name, "pg_catalog.") || strings.HasPrefix(entry.Name, "information_schema.") ||
			!slices.Contains([]string{"r", "p", "v", "m", "S", "f"}, entry.Kind) {
			return integrity("CENSUS_SCHEMA", source)
		}
		prior = entry.Name
		classification, reason := Classify(entry, relations, instrumented)
		if classification == Unclassified {
			return integrity("RELATION_UNCLASSIFIED", entry.Name)
		}
		if entry.Classification != classification || entry.Reason != reason ||
			(entry.Extension != nil && !qualifiedExtension(*entry.Extension)) {
			return integrity("CENSUS_CLASSIFICATION", entry.Name)
		}
		switch {
		case classification == Exported:
			exported++
			i, found := slices.BinarySearchFunc(inventories, entry.Name, func(a Inventory, b string) int {
				return strings.Compare(a.Relation, b)
			})
			if !found || entry.Count == nil || entry.SHA256 == nil ||
				*entry.Count != inventories[i].Count || *entry.SHA256 != inventories[i].SHA256 {
				return integrity("CENSUS_BINDING", entry.Name)
			}
			if !slices.Equal(entry.Columns, relations[i].SourceColumns()) {
				return integrity("COLUMN_UNCLASSIFIED", entry.Name)
			}
		case reason == "safe_resource_projection":
			if !slices.Equal(entry.Columns, ResourceSourceColumns()) {
				return integrity("COLUMN_UNCLASSIFIED", entry.Name)
			}
			if entry.Count == nil || entry.SHA256 != nil {
				return integrity("CENSUS_BINDING", entry.Name)
			}
			i := slices.IndexFunc(inventories, func(item Inventory) bool { return item.Relation == SinkResources })
			if i < 0 || *entry.Count != inventories[i].Count {
				return integrity("CENSUS_BINDING", entry.Name)
			}
		case reason == FixtureCorpus:
			if entry.Count == nil || *entry.Count < 0 || entry.SHA256 == nil || !digest.MatchString(*entry.SHA256) ||
				(*entry.Count == 0 && *entry.SHA256 != Hash(nil)) {
				return integrity("CENSUS_EXCLUSION_PROOF", entry.Name)
			}
		case entry.Count != nil || entry.SHA256 != nil || entry.Columns != nil:
			return integrity("CENSUS_SCHEMA", source)
		}
	}
	if exported != len(relations) {
		return integrity("CENSUS_EXPORT_MISSING", source)
	}
	return nil
}

var extensionName = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,63}$`)

func qualifiedExtension(name string) bool { return extensionName.MatchString(name) }
