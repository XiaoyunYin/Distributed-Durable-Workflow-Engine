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
	EvidenceVersion     = "dur053.k6-evidence.v1"
	SnapshotVersion     = "dur053.k6-engine-snapshot.v1"
	SinkSnapshotVersion = "dur053.k6-sink-snapshot.v1"
	IndexVersion        = "dur053.k6-index.v1"
	RegistrySHA256      = "26477acd794a6be9ea3cd1a71741c2b62a9fa2f2a4ab0b53decb60ffb334c7e2"
	ManifestPath        = "manifest.json"
	SnapshotPath        = "engine/snapshot.json"
	SinkSnapshotPath    = "sink/snapshot.json"
	IndexPath           = "index.json"
)

type Manifest struct {
	Schema           string            `json:"schema"`
	Campaign         string            `json:"campaign"`
	Job              string            `json:"job"`
	RunID            string            `json:"run_id"`
	Episode          string            `json:"episode"`
	Ordinal          int               `json:"ordinal"`
	Seed             int               `json:"seed"`
	Cell             string            `json:"cell"`
	Workload         string            `json:"workload"`
	Namespace        string            `json:"namespace"`
	ResetID          string            `json:"reset_id"`
	BarrierID        string            `json:"barrier_id"`
	Database         string            `json:"database"`
	EngineInstance   string            `json:"engine_instance"`
	Sink             *DatabaseIdentity `json:"sink"`
	PlanSHA256       string            `json:"plan_sha256"`
	RegistrySHA256   string            `json:"registry_sha256"`
	SourceTreeSHA256 string            `json:"source_tree_sha256"`
	ImageSetSHA256   string            `json:"image_set_sha256"`
	FreezeSHA256     string            `json:"freeze_sha256"`
	TopologySHA256   string            `json:"topology_sha256"`
	Instrumentation  []string          `json:"instrumentation"`
	RequiredSources  []string          `json:"required_sources"`
}

type DatabaseIdentity struct {
	Database string `json:"database"`
	Instance string `json:"instance"`
}

func (m Manifest) Identity(db string) DatabaseIdentity {
	if db == "sink" {
		return *m.Sink
	}
	return DatabaseIdentity{Database: m.Database, Instance: m.EngineInstance}
}

func (m Manifest) Databases() []string {
	if m.Sink != nil {
		return []string{"engine", "sink"}
	}
	return []string{"engine"}
}

func (m Manifest) Relations(db string) []Relation {
	if db == "sink" {
		return SinkRelations()
	}
	return EngineRelations(m.Instrumented())
}

func (m Manifest) Instrumented() bool {
	return slices.Contains(m.Instrumentation, DeadlineWrites)
}

func (m Manifest) Valid() bool {
	if m.Schema != EvidenceVersion || m.RegistrySHA256 != RegistrySHA256 ||
		m.Ordinal < 0 || m.Ordinal >= 115 || m.Seed != 530001+m.Ordinal {
		return false
	}
	for _, id := range []string{m.Campaign, m.Job, m.RunID, m.Namespace, m.ResetID, m.BarrierID, m.Database, m.EngineInstance} {
		if id == "" || !publicIdentity(id) {
			return false
		}
	}
	for _, hash := range []string{m.SourceTreeSHA256, m.ImageSetSHA256, m.FreezeSHA256, m.TopologySHA256, m.PlanSHA256} {
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
	if (workload == "PURE") != (m.Sink == nil) {
		return false
	}
	if m.Sink != nil && (m.Sink.Database == "" || !publicIdentity(m.Sink.Database) ||
		m.Sink.Instance == "" || !publicIdentity(m.Sink.Instance) || m.Sink.Instance == m.EngineInstance) {
		return false
	}
	return m.Cell == cell && m.Episode == cell+"-"+strconv.Itoa(m.Ordinal%5) && m.Workload == workload &&
		m.Instrumentation != nil && (len(m.Instrumentation) == 0 || slices.Equal(m.Instrumentation, []string{DeadlineWrites})) &&
		slices.Equal(m.RequiredSources, RequiredSources(m.Instrumented(), m.Sink != nil))
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
	Instance       string        `json:"instance"`
	Generation     int           `json:"generation"`
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

func RelationPath(name string) string {
	if table, ok := strings.CutPrefix(name, "k6_sink."); ok {
		return "sink/" + table + ".jsonl"
	}
	return "engine/" + name + ".jsonl"
}

func RequiredSources(instrumented, sink bool) []string {
	paths := []string{ManifestPath, SnapshotPath, PlanPath, ArrivalsPath, BarrierPath}
	m := Manifest{}
	if sink {
		m.Sink = &DatabaseIdentity{}
	}
	paths = append(paths, requiredExternal(m)...)
	for _, spec := range EngineRelations(instrumented) {
		paths = append(paths, RelationPath(spec.Name))
	}
	if sink {
		paths = append(paths, SinkSnapshotPath)
		for _, spec := range SinkRelations() {
			paths = append(paths, RelationPath(spec.Name))
		}
	}
	slices.Sort(paths)
	return paths
}

func verifySnapshot(m Manifest, db string, s Snapshot, manifestHash string) bool {
	version := SnapshotVersion
	if db == "sink" {
		version = SinkSnapshotVersion
	}
	identity := m.Identity(db)
	return s.Schema == version && s.ManifestSHA256 == manifestHash &&
		s.BarrierID == m.BarrierID && s.ResetID == m.ResetID && s.Database == identity.Database && s.Instance == identity.Instance &&
		s.Generation > 0 && s.DatabaseOID > 0 && validVisibility(s.Visibility) && validTime(s.TransactionUTC) &&
		validBracket(s.Open) && validBracket(s.Close) &&
		orderedObservers(s.Open.After, s.Close.Before) && orderedObservers(s.Close.After, s.CommitAfter) &&
		s.NamespaceCount >= 0 && (db == "engine" || s.NamespaceCount == 0)
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
	allowed := append(RequiredSources(true, true), TopologyPath)
	for _, f := range index.Files {
		if f.Path <= previous || !(slices.Contains(allowed, f.Path) || isSessionPath(f.Path)) || f.Length < 0 || !digest.MatchString(f.SHA256) {
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
	if manifest.Sink == nil && hasSinkSource(bundle, index) {
		result := newResult(manifest, Hash(indexBytes))
		result.failIntegrity(integrity("PURE_SINK_PRESENT", "sink"))
		result.Safety = "FAIL"
		result.finish()
		return result
	}
	for _, f := range index.Files {
		if _, found := slices.BinarySearch(manifest.RequiredSources, f.Path); !found && !isSessionPath(f.Path) {
			return Failed(integrity("INDEX_UNDECLARED_SOURCE", IndexPath))
		}
	}
	var issues []Finding
	var plan Plan
	var arrivals Arrivals
	planOK := readSource(bundle, files, PlanPath, &plan, &issues, func(raw []byte) bool {
		return Hash(raw) == manifest.PlanSHA256 && plan.Valid(manifest)
	})
	arrivalsOK := readSource(bundle, files, ArrivalsPath, &arrivals, &issues, func(_ []byte) bool {
		return arrivals.Valid(manifest)
	})
	if !arrivalsOK {
		arrivals = Arrivals{}
	}
	var barrier Barrier
	barrierOK := readSource(bundle, files, BarrierPath, &barrier, &issues, func(_ []byte) bool {
		return barrier.ValidBinding(manifest, Hash(manifestBytes))
	})
	rows := map[string][]Row{}
	snaps := map[string]Snapshot{}
	for _, db := range manifest.Databases() {
		snaps[db] = verifyDatabase(bundle, files, manifest, db, Hash(manifestBytes), rows, &issues)
	}
	if barrierOK && planOK && arrivalsOK {
		issues = append(issues, verifyBarrier(manifest, plan, arrivals, barrier, snaps, rows, files)...)
	}
	external, externalIssues := readExternal(func(path string) ([]byte, *Finding) {
		return readIndexed(bundle, files, path)
	}, manifest, index.Files)
	issues = append(issues, externalIssues...)
	var samples ExternalSamples
	if readSource(bundle, files, SamplesPath, &samples, &issues, func([]byte) bool { return true }) &&
		barrierOK && planOK && arrivalsOK && !slices.ContainsFunc(externalIssues, func(f Finding) bool {
		return f.Code == "required_proof_missing:"+f.Source
	}) {
		issues = append(issues, verifyExternalQuiet(manifest, plan, arrivals, barrier, external, samples, rows)...)
	}
	return evaluate(manifest, snaps["engine"], rows, plan, arrivals, planOK, arrivalsOK, Hash(indexBytes), issues, external)
}

func hasSinkSource(bundle fs.FS, index Index) bool {
	_, err := fs.Stat(bundle, "sink")
	_, ingress := fs.Stat(bundle, IngressPath)
	_, decisions := fs.Stat(bundle, "auth")
	return !errors.Is(err, fs.ErrNotExist) || !errors.Is(ingress, fs.ErrNotExist) || !errors.Is(decisions, fs.ErrNotExist) ||
		slices.ContainsFunc(index.Files, func(file File) bool {
			return strings.HasPrefix(file.Path, "sink/") || file.Path == IngressPath || file.Path == DecisionsPath
		})
}

func readSource(bundle fs.FS, files map[string]File, path string, dst any, issues *[]Finding, check func([]byte) bool) bool {
	raw, issue := readIndexed(bundle, files, path)
	if issue != nil {
		*issues = append(*issues, *issue)
		return false
	}
	if decodeCanonical(raw, dst) != nil || !check(raw) {
		*issues = append(*issues, *integrity("SOURCE_SCHEMA", path))
		return false
	}
	return true
}

func verifyDatabase(bundle fs.FS, files map[string]File, manifest Manifest, db, manifestHash string, rows map[string][]Row, issues *[]Finding) Snapshot {
	snapshotPath := db + "/snapshot.json"
	snapshotBytes, issue := readIndexed(bundle, files, snapshotPath)
	var snap Snapshot
	if issue != nil {
		*issues = append(*issues, *issue)
		return snap
	}
	if decodeCanonical(snapshotBytes, &snap) != nil || !verifySnapshot(manifest, db, snap, manifestHash) {
		*issues = append(*issues, *integrity("CUT_BINDING:"+db, snapshotPath))
		return Snapshot{}
	}
	if snap.Commit != "committed" {
		*issues = append(*issues, *integrity("SNAPSHOT_COMMIT", snapshotPath))
	}
	specs := manifest.Relations(db)
	if len(snap.Relations) != len(specs) {
		*issues = append(*issues, *integrity("RELATION_INVENTORY", snapshotPath))
		return snap
	}
	for i, spec := range specs {
		path := RelationPath(spec.Name)
		file, found := files[path]
		if !found {
			*issues = append(*issues, *missing(path))
			continue
		}
		meta := snap.Relations[i]
		if meta.Relation != spec.Name || !slices.Equal(meta.Keys, spec.Keys) || meta.Count < 0 {
			*issues = append(*issues, *integrity("RELATION_INVENTORY", path))
			continue
		}
		if meta.SHA256 != file.SHA256 || meta.Length != file.Length {
			*issues = append(*issues, *integrity("RELATION_INDEX_BINDING", path))
			continue
		}
		result, issue := verifyRows(bundle, spec, file, meta.Count)
		if issue != nil {
			*issues = append(*issues, *issue)
			continue
		}
		rows[spec.Name] = result
	}
	if issue := verifyCensus(snap.Census, specs, snap.Relations, manifest.Instrumented(), db); issue != nil {
		*issues = append(*issues, *issue)
	}
	return snap
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
			if errors.Is(readErr, bufio.ErrBufferFull) {
				issue = integrity("RELATION_ROW_TOO_LARGE", entry.Path)
			} else if len(line) != 0 || !errors.Is(readErr, io.EOF) {
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
