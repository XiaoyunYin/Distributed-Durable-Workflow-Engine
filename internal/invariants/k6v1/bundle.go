package k6v1

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"slices"
	"strconv"
	"strings"
)

const (
	EvidenceVersion = "dur053.k6-evidence.v1"
	SnapshotVersion = "dur053.k6-engine-snapshot.v1"
	IndexVersion    = "dur053.k6-index.v1"
	RegistrySHA256  = "26477acd794a6be9ea3cd1a71741c2b62a9fa2f2a4ab0b53decb60ffb334c7e2"
	ManifestPath    = "manifest.json"
	SnapshotPath    = "engine/snapshot.json"
	IndexPath       = "index.json"
)

type Manifest struct {
	Schema           string   `json:"schema"`
	Campaign         string   `json:"campaign"`
	Job              string   `json:"job"`
	RunID            string   `json:"run_id"`
	Episode          string   `json:"episode"`
	Ordinal          int      `json:"ordinal"`
	Seed             int      `json:"seed"`
	Cell             string   `json:"cell"`
	Workload         string   `json:"workload"`
	Namespace        string   `json:"namespace"`
	ResetID          string   `json:"reset_id"`
	BarrierID        string   `json:"barrier_id"`
	Database         string   `json:"database"`
	RegistrySHA256   string   `json:"registry_sha256"`
	SourceTreeSHA256 string   `json:"source_tree_sha256"`
	ImageSetSHA256   string   `json:"image_set_sha256"`
	FreezeSHA256     string   `json:"freeze_sha256"`
	TopologySHA256   string   `json:"topology_sha256"`
	Instrumentation  []string `json:"instrumentation"`
	RequiredSources  []string `json:"required_sources"`
}

func (m Manifest) Instrumented() bool {
	return slices.Contains(m.Instrumentation, DeadlineWrites)
}

func (m Manifest) Valid() bool {
	if m.Schema != EvidenceVersion || m.RegistrySHA256 != RegistrySHA256 ||
		m.Ordinal < 0 || m.Ordinal >= 115 || m.Seed != 530001+m.Ordinal {
		return false
	}
	for _, id := range []string{m.Campaign, m.Job, m.RunID, m.Namespace, m.ResetID, m.BarrierID, m.Database} {
		if id == "" || !publicIdentity(id) {
			return false
		}
	}
	for _, hash := range []string{m.SourceTreeSHA256, m.ImageSetSHA256, m.FreezeSHA256, m.TopologySHA256} {
		if !digest.MatchString(hash) {
			return false
		}
	}
	cell, workload := "hook-"+strconv.Itoa(m.Ordinal/5), "PURE"
	if m.Ordinal >= 30 {
		cell = "medium-" + strconv.Itoa((m.Ordinal-30)/5)
	}
	if m.Ordinal >= 90 && (m.Ordinal < 110 || m.Ordinal >= 113) {
		workload = "COOPERATING_EFFECT"
	}
	return m.Cell == cell && m.Episode == cell+"-"+strconv.Itoa(m.Ordinal%5) && m.Workload == workload &&
		m.Instrumentation != nil && (len(m.Instrumentation) == 0 || slices.Equal(m.Instrumentation, []string{DeadlineWrites})) &&
		slices.Equal(m.RequiredSources, RequiredSources(m.Instrumented()))
}

type Observer struct {
	UTC         string `json:"utc"`
	MonotonicNS int64  `json:"monotonic_ns"`
}

type Bracket struct {
	Before      Observer `json:"before"`
	After       Observer `json:"after"`
	DatabaseUTC string   `json:"database_utc"`
}

type Inventory struct {
	Relation string   `json:"relation"`
	Keys     []string `json:"keys"`
	Count    int64    `json:"count"`
	Length   int64    `json:"length"`
	SHA256   string   `json:"sha256"`
}

type Snapshot struct {
	Schema         string        `json:"schema"`
	ManifestSHA256 string        `json:"manifest_sha256"`
	BarrierID      string        `json:"barrier_id"`
	ResetID        string        `json:"reset_id"`
	Database       string        `json:"database"`
	DatabaseOID    uint32        `json:"database_oid"`
	Visibility     string        `json:"visibility"`
	TransactionUTC string        `json:"transaction_utc"`
	Open           Bracket       `json:"open"`
	Close          Bracket       `json:"close"`
	Commit         string        `json:"commit"`
	CommitAfter    Observer      `json:"commit_after"`
	NamespaceCount int64         `json:"namespace_count"`
	Census         []CensusEntry `json:"census"`
	Relations      []Inventory   `json:"relations"`
}

type File struct {
	Path   string `json:"path"`
	Length int64  `json:"length"`
	SHA256 string `json:"sha256"`
}

type Index struct {
	Schema string `json:"schema"`
	Files  []File `json:"files"`
}

func RelationPath(name string) string { return "engine/" + name + ".jsonl" }

func RequiredSources(instrumented bool) []string {
	paths := []string{ManifestPath, SnapshotPath}
	for _, spec := range EngineRelations(instrumented) {
		paths = append(paths, RelationPath(spec.Name))
	}
	slices.Sort(paths)
	return paths
}

func verifySnapshot(m Manifest, s Snapshot, manifestHash string) bool {
	return s.Schema == SnapshotVersion && s.ManifestSHA256 == manifestHash &&
		s.BarrierID == m.BarrierID && s.ResetID == m.ResetID && s.Database == m.Database &&
		s.DatabaseOID > 0 && validVisibility(s.Visibility) && validTime(s.TransactionUTC) &&
		validBracket(s.Open) && validBracket(s.Close) && validTime(s.CommitAfter.UTC) &&
		s.Open.After.MonotonicNS <= s.Close.Before.MonotonicNS && s.Close.After.MonotonicNS <= s.CommitAfter.MonotonicNS &&
		s.NamespaceCount >= 0
}

func validVisibility(s string) bool {
	parts := strings.Split(s, ":")
	if len(parts) != 3 {
		return false
	}
	minimum, err1 := strconv.ParseUint(parts[0], 10, 64)
	maximum, err2 := strconv.ParseUint(parts[1], 10, 64)
	if err1 != nil || err2 != nil || minimum > maximum {
		return false
	}
	var prior uint64
	for xid := range strings.SplitSeq(parts[2], ",") {
		if parts[2] == "" {
			break
		}
		current, err := strconv.ParseUint(xid, 10, 64)
		if err != nil || current < minimum || current >= maximum || current <= prior {
			return false
		}
		prior = current
	}
	return true
}

func validBracket(b Bracket) bool {
	return b.Before.MonotonicNS >= 0 && b.Before.MonotonicNS <= b.After.MonotonicNS &&
		validTime(b.Before.UTC) && validTime(b.After.UTC) && b.Before.UTC <= b.After.UTC &&
		validTime(b.DatabaseUTC)
}

// Verify derives the result from the indexed public bytes. Derived summaries
// and filenames outside the pinned input inventory are never evidence.
func Verify(bundle fs.FS) Result {
	indexBytes, err := fs.ReadFile(bundle, IndexPath)
	if err != nil {
		return Failed(missing(IndexPath))
	}
	var index Index
	if decodeCanonical(indexBytes, &index) != nil || index.Schema != IndexVersion {
		return Failed(integrity("INDEX_SCHEMA", IndexPath))
	}
	files := map[string]File{}
	previous := ""
	allowed := RequiredSources(true)
	for _, f := range index.Files {
		if f.Path <= previous || !slices.Contains(allowed, f.Path) || f.Length < 0 || !digest.MatchString(f.SHA256) {
			return Failed(integrity("INDEX_ENTRY_SCHEMA", IndexPath))
		}
		previous = f.Path
		files[f.Path] = f
	}
	manifestBytes, issue := readIndexed(bundle, files, ManifestPath)
	if issue != nil {
		return Failed(issue)
	}
	var manifest Manifest
	if decodeCanonical(manifestBytes, &manifest) != nil || !manifest.Valid() {
		return Failed(integrity("MANIFEST_SCHEMA", ManifestPath))
	}
	for _, f := range index.Files {
		if _, found := slices.BinarySearch(manifest.RequiredSources, f.Path); !found {
			return Failed(integrity("INDEX_UNDECLARED_SOURCE", IndexPath))
		}
	}
	snapshotBytes, issue := readIndexed(bundle, files, SnapshotPath)
	if issue != nil {
		return Failed(issue)
	}
	var snap Snapshot
	if decodeCanonical(snapshotBytes, &snap) != nil || !verifySnapshot(manifest, snap, Hash(manifestBytes)) {
		return Failed(integrity("SNAPSHOT_SCHEMA", SnapshotPath))
	}
	if snap.Commit != "committed" {
		return Failed(integrity("SNAPSHOT_COMMIT", SnapshotPath))
	}
	specs := EngineRelations(manifest.Instrumented())
	if len(snap.Relations) != len(specs) {
		return Failed(integrity("RELATION_INVENTORY", SnapshotPath))
	}
	rows := make(map[string][]Row, len(specs))
	var issues []Finding
	for i, spec := range specs {
		path := RelationPath(spec.Name)
		file, found := files[path]
		if !found {
			issues = append(issues, *missing(path))
			continue
		}
		meta := snap.Relations[i]
		if meta.Relation != spec.Name || !slices.Equal(meta.Keys, spec.Keys) || meta.Count < 0 {
			return Failed(integrity("RELATION_INVENTORY", path))
		}
		if meta.SHA256 != file.SHA256 || meta.Length != file.Length {
			return Failed(integrity("RELATION_INDEX_BINDING", path))
		}
		result, issue := verifyRows(bundle, spec, file, meta.Count)
		if issue != nil {
			issues = append(issues, *issue)
			continue
		}
		rows[spec.Name] = result
	}
	if issue := verifyCensus(snap.Census, specs, snap.Relations, manifest.Instrumented()); issue != nil {
		issues = append(issues, *issue)
	}
	return evaluate(manifest, snap, rows, Hash(indexBytes), issues)
}

func readIndexed(bundle fs.FS, files map[string]File, path string) ([]byte, *Finding) {
	file, ok := files[path]
	if !ok {
		return nil, missing(path)
	}
	raw, err := fs.ReadFile(bundle, path)
	if err != nil {
		return nil, missing(path)
	}
	if int64(len(raw)) != file.Length {
		return nil, integrity("INDEX_LENGTH", path)
	}
	if Hash(raw) != file.SHA256 {
		return nil, integrity("INDEX_DIGEST", path)
	}
	return raw, nil
}

func verifyRows(bundle fs.FS, spec Relation, entry File, count int64) ([]Row, *Finding) {
	file, err := bundle.Open(entry.Path)
	if err != nil {
		return nil, missing(entry.Path)
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil || !stat.Mode().IsRegular() {
		return nil, integrity("INDEX_FILE_TYPE", entry.Path)
	}
	reader := bufio.NewReaderSize(file, 1<<20)
	hash := sha256.New()
	var rows []Row
	var length int64
	var issue *Finding
	for {
		line, readErr := reader.ReadSlice('\n')
		length += int64(len(line))
		_, _ = hash.Write(line)
		if readErr != nil {
			if len(line) != 0 || !errors.Is(readErr, io.EOF) {
				issue = integrity("RELATION_FRAMING", entry.Path)
			}
			break
		}
		row, err := decodeRow(spec, line)
		if err != nil {
			issue = integrity("RELATION_ROW_SCHEMA", entry.Path)
			break
		}
		if len(rows) > 0 && compareKeys(spec, rows[len(rows)-1], row) >= 0 {
			issue = integrity("RELATION_ORDER_OR_DUPLICATE", entry.Path)
			break
		}
		rows = append(rows, row)
	}
	if issue != nil {
		return nil, issue
	}
	if length != entry.Length {
		return nil, integrity("INDEX_LENGTH", entry.Path)
	}
	if hex.EncodeToString(hash.Sum(nil)) != entry.SHA256 {
		return nil, integrity("INDEX_DIGEST", entry.Path)
	}
	if int64(len(rows)) != count {
		return nil, integrity("RELATION_COUNT", entry.Path)
	}
	return rows, nil
}
