package k6v1

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"slices"
	"strings"
)

const (
	BarrierPath    = "barrier.json"
	BarrierVersion = "dur053.k6-barrier.v1"
	AppendOnly     = "append_only"
	Quiescent      = "quiescent"
	IdleChurn      = "idle_churn"
)

type Barrier struct {
	Schema             string       `json:"schema"`
	ManifestSHA256     string       `json:"manifest_sha256"`
	BarrierID          string       `json:"barrier_id"`
	ResetID            string       `json:"reset_id"`
	DeadlineUTC        string       `json:"deadline_utc"`
	SelectedGeneration int          `json:"selected_generation"`
	Attempts           []CutAttempt `json:"attempts"`
}

type CutAttempt struct {
	Generation int           `json:"generation"`
	Started    Observer      `json:"started"`
	Finished   Observer      `json:"finished"`
	Databases  []DatabaseCut `json:"databases"`
	Reasons    []string      `json:"reasons"`
}

type DatabaseCut struct {
	DB             string          `json:"db"`
	Pre            WatermarkSample `json:"pre"`
	Cut            []Watermark     `json:"cut"`
	Post           WatermarkSample `json:"post"`
	SnapshotSHA256 string          `json:"snapshot_sha256"`
}

type WatermarkSample struct {
	Terminal    bool        `json:"terminal"`
	Database    string      `json:"database"`
	Instance    string      `json:"instance"`
	DatabaseOID uint32      `json:"database_oid"`
	ResetID     string      `json:"reset_id"`
	Visibility  string      `json:"visibility"`
	Open        Bracket     `json:"open"`
	CommitAfter Observer    `json:"commit_after"`
	Relations   []Watermark `json:"relations"`
}

type Watermark struct {
	Relation     string     `json:"relation"`
	Count        int64      `json:"count"`
	MaxKey       string     `json:"max_key"`
	Digest       string     `json:"digest"`
	SourceDigest string     `json:"source_digest"`
	Audit        []AuditRow `json:"audit"`
}

type AuditRow struct {
	ID     int64  `json:"id"`
	Kind   string `json:"kind"`
	Digest string `json:"digest"`
}

func WatermarkClass(relation string) string {
	switch relation {
	case "engine.partition_leases", DeadlineWrites:
		return IdleChurn
	case "engine.schema_migrations", "engine.attempt_result_evidence",
		"engine.transition_history", "engine.lease_acquisitions", "engine.k4_audit", "engine.checkpoints",
		"engine.outbox_publications", "engine.event_inbox", "engine.transport_quarantine",
		"engine.effect_call_attempts", "effects.effect_call_attempts",
		"engine.effect_resolution_audit", "effects.effect_resolution_audit",
		SinkAllocations, SinkCalls, SinkJournal, SinkLookupMisses, SinkReceipts, SinkBindings:
		return AppendOnly
	default:
		return Quiescent
	}
}

func IdleColumn(relation, column string) bool {
	return relation == "engine.partition_leases" && (column == "lease_expires_at" || column == "updated_at")
}

type WatermarkBuilder struct {
	spec   Relation
	value  Watermark
	public hash.Hash
	source hash.Hash
}

func NewWatermark(spec Relation) *WatermarkBuilder {
	return &WatermarkBuilder{spec: spec, value: Watermark{Relation: spec.Name}, public: sha256.New(), source: sha256.New()}
}

func (b *WatermarkBuilder) Add(row Row, sourceDigest string) {
	stable := row
	if b.spec.Name == "engine.partition_leases" {
		stable = Row{}
		for name, value := range row {
			if !IdleColumn(b.spec.Name, name) {
				stable[name] = value
			}
		}
	}
	raw, _ := encode(stable)
	rowHash := Hash(raw)
	_, _ = b.public.Write([]byte(rowHash))
	if sourceDigest != "" {
		_, _ = b.source.Write([]byte(sourceDigest))
	}
	if b.spec.Name == DeadlineWrites {
		b.value.Audit = append(b.value.Audit, AuditRow{number(row["id"]), row["branch"].(string), rowHash})
	}
	key := make([]any, 0, len(b.spec.Keys))
	for _, k := range b.spec.Keys {
		key = append(key, row[k])
	}
	encoded, _ := encode(key)
	b.value.MaxKey = string(encoded[:len(encoded)-1])
	b.value.Count++
}

func (b *WatermarkBuilder) Finish() Watermark {
	b.value.Digest = hex.EncodeToString(b.public.Sum(nil))
	b.value.SourceDigest = hex.EncodeToString(b.source.Sum(nil))
	return b.value
}

func (w Watermark) valid(spec Relation) bool {
	if w.Relation != spec.Name || w.Count < 0 || !digest.MatchString(w.Digest) || !digest.MatchString(w.SourceDigest) {
		return false
	}
	if w.Count == 0 {
		return w.MaxKey == "" && w.Digest == Hash(nil) && w.SourceDigest == Hash(nil) && w.Audit == nil
	}
	value, err := parse([]byte(w.MaxKey))
	key, ok := value.([]any)
	if err != nil || !ok || len(key) != len(spec.Keys) {
		return false
	}
	for i, name := range spec.Keys {
		for _, col := range spec.Columns {
			if col.Name == name && (key[i] == "" || !validValue(col.Kind, key[i])) {
				return false
			}
		}
	}
	if spec.Name != DeadlineWrites {
		return w.Audit == nil
	}
	if int64(len(w.Audit)) != w.Count || number(key[0]) != w.Audit[len(w.Audit)-1].ID || w.SourceDigest != Hash(nil) {
		return false
	}
	h := sha256.New()
	var prior int64
	for _, row := range w.Audit {
		if row.ID <= prior || !nonempty(row.Kind) || !digest.MatchString(row.Digest) {
			return false
		}
		prior = row.ID
		_, _ = h.Write([]byte(row.Digest))
	}
	return w.Digest == hex.EncodeToString(h.Sum(nil))
}

func equalWatermark(before, after Watermark) bool {
	publicEqual := before.Digest == after.Digest
	sourceEqual := before.SourceDigest == after.SourceDigest
	switch WatermarkClass(before.Relation) {
	case AppendOnly:
		return before.Count == after.Count && before.MaxKey == after.MaxKey && publicEqual && sourceEqual
	case Quiescent:
		return before.Count == after.Count && publicEqual && sourceEqual
	case IdleChurn:
		if before.Relation != DeadlineWrites {
			return before.Count == after.Count && publicEqual && sourceEqual
		}
		if len(before.Audit) > len(after.Audit) || !slices.Equal(before.Audit, after.Audit[:len(before.Audit)]) {
			return false
		}
		for _, row := range after.Audit[len(before.Audit):] {
			if row.Kind != "renewal" {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// RaceReasons compares each DB on its own terms. XIDs and clocks from
// different servers are never compared with one another.
func (d DatabaseCut) RaceReasons(specs []Relation) []string {
	reasons := []string{}
	if len(d.Pre.Relations) != len(specs) || len(d.Cut) != len(specs) || len(d.Post.Relations) != len(specs) {
		return []string{"CUT_WATERMARK_SCHEMA:" + d.DB}
	}
	for i, spec := range specs {
		pre, cut, post := d.Pre.Relations[i], d.Cut[i], d.Post.Relations[i]
		if !pre.valid(spec) || !cut.valid(spec) || !post.valid(spec) {
			reasons = append(reasons, "CUT_WATERMARK_SCHEMA:"+d.DB+":"+spec.Name)
			continue
		}
		if !equalWatermark(pre, cut) || !equalWatermark(cut, post) {
			reasons = append(reasons, "CUT_RACED:"+d.DB+":"+spec.Name)
		}
	}
	return reasons
}

func orderedObservers(a, b Observer) bool {
	return a.MonotonicNS >= 0 && a.MonotonicNS <= b.MonotonicNS && validTime(a.UTC) && validTime(b.UTC) && a.UTC <= b.UTC
}

func (s WatermarkSample) valid(m Manifest, db string) bool {
	id := m.Identity(db)
	return s.Database == id.Database && s.Instance == id.Instance && s.ResetID == m.ResetID &&
		s.DatabaseOID > 0 && validVisibility(s.Visibility) && validBracket(s.Open) && orderedObservers(s.Open.After, s.CommitAfter)
}

func (b Barrier) ValidBinding(m Manifest, manifestHash string) bool {
	return b.Schema == BarrierVersion && b.ManifestSHA256 == manifestHash && b.BarrierID == m.BarrierID &&
		b.ResetID == m.ResetID && validTime(b.DeadlineUTC) && b.SelectedGeneration >= 0 && len(b.Attempts) > 0
}

func verifyBarrier(m Manifest, plan Plan, arrivals Arrivals, b Barrier, snaps map[string]Snapshot, rows map[string][]Row, files map[string]File) []Finding {
	issues := []Finding{}
	add := func(code string) { issues = append(issues, *integrity(code, BarrierPath)) }
	deadline := UTC(plan.Deadline(arrivals))
	if b.DeadlineUTC != deadline {
		add("CUT_DEADLINE_BINDING")
	}
	if plan.FaultMode == "control" && arrivals.RemovalUTC != nil {
		add("CUT_CONTROL_REMOVAL")
	}
	if plan.FaultMode == "injected" && arrivals.RemovalUTC == nil {
		add("CUT_REMOVAL_UNCONFIRMED")
	}
	if arrivals.SealedUTC < plan.LastScheduledUTC() {
		add("CUT_ARRIVALS_SEALED_EARLY")
	}
	previous := Observer{UTC: plan.SealedUTC}
	databases := m.Databases()
	for i, attempt := range b.Attempts {
		selected := b.SelectedGeneration == attempt.Generation
		if attempt.Generation != i+1 || !orderedObservers(previous, attempt.Started) || !orderedObservers(attempt.Started, attempt.Finished) ||
			(selected && i != len(b.Attempts)-1) || attempt.Reasons == nil || attempt.Databases == nil {
			add("CUT_GENERATION")
			break
		}
		previous = attempt.Finished
		late := attempt.Finished.UTC > deadline
		if late {
			add("CUT_DEADLINE")
		}
		if len(attempt.Databases) == 0 && !selected && i == len(b.Attempts)-1 {
			if late && slices.Equal(attempt.Reasons, []string{"CUT_DEADLINE"}) {
				break
			}
			if len(attempt.Reasons) == 1 && slices.Contains([]string{"CUT_BINDING:engine", "CUT_BINDING:sink", "CUT_INTERRUPTED"}, attempt.Reasons[0]) {
				add(attempt.Reasons[0])
			} else if len(attempt.Reasons) == 1 && isColumnIssue(m, attempt.Reasons[0]) {
				relation, _ := strings.CutPrefix(attempt.Reasons[0], "COLUMN_UNCLASSIFIED:")
				issues = append(issues, *integrity("COLUMN_UNCLASSIFIED", relation))
			} else {
				add("CUT_ATTEMPT_REASONS")
			}
			break
		}
		if len(attempt.Databases) != len(databases) {
			add("CUT_GENERATION")
			break
		}
		reasons := []string{}
		lastPre, lastClose, lastPost := attempt.Started, attempt.Started, attempt.Started
		for j, db := range databases {
			d := attempt.Databases[j]
			if d.DB != db || !d.Pre.valid(m, db) || !d.Post.valid(m, db) ||
				d.Pre.DatabaseOID != d.Post.DatabaseOID || !digest.MatchString(d.SnapshotSHA256) {
				add("CUT_BINDING:" + db)
				continue
			}
			if !orderedObservers(lastPre, d.Pre.Open.Before) || !orderedObservers(lastPost, d.Post.Open.Before) ||
				!orderedObservers(d.Post.CommitAfter, attempt.Finished) {
				add("CUT_BRACKETS:" + db)
			}
			lastPre, lastPost = d.Pre.CommitAfter, d.Post.CommitAfter
			specs := m.Relations(db)
			reasons = append(reasons, d.RaceReasons(specs)...)
			if !selected {
				continue
			}
			snap := snaps[db]
			if snap.Generation == 0 {
				continue
			} // The snapshot failure is already recorded.
			if snap.Generation != attempt.Generation || files[db+"/snapshot.json"].SHA256 != d.SnapshotSHA256 || snap.DatabaseOID != d.Pre.DatabaseOID {
				add("CUT_GENERATION:" + db)
			}
			allPre := attempt.Databases[len(databases)-1].Pre.CommitAfter
			firstPost := attempt.Databases[0].Post.Open.Before
			if !orderedObservers(allPre, snap.Open.Before) || !orderedObservers(lastClose, snap.Open.Before) ||
				!orderedObservers(snap.CommitAfter, firstPost) {
				add("CUT_BRACKETS:" + db)
			}
			lastClose = snap.CommitAfter
			for k, spec := range specs {
				raw, ok := rows[spec.Name]
				if !ok || k >= len(d.Cut) {
					continue
				}
				builder := NewWatermark(spec)
				for _, row := range raw {
					builder.Add(row, "")
				}
				got, cut := builder.Finish(), d.Cut[k]
				if got.Count != cut.Count || got.Digest != cut.Digest || got.MaxKey != cut.MaxKey || !slices.Equal(got.Audit, cut.Audit) {
					add("CUT_WATERMARK_BINDING:" + db + ":" + spec.Name)
				}
			}
		}
		if late {
			reasons = append(reasons, "CUT_DEADLINE")
		}
		if slices.Contains(attempt.Reasons, "CUT_RACED:external") {
			reasons = append(reasons, "CUT_RACED:external")
		}
		if !slices.Equal(reasons, attempt.Reasons) || (!selected && len(reasons) == 0) {
			add("CUT_ATTEMPT_REASONS")
		}
		if selected || i == len(b.Attempts)-1 {
			for _, reason := range reasons {
				if reason != "CUT_DEADLINE" {
					add(reason)
				}
			}
		}
	}
	final := b.Attempts[len(b.Attempts)-1]
	if b.SelectedGeneration == 0 || final.Generation != b.SelectedGeneration {
		add("CUT_NO_GENERATION")
	}
	if b.SelectedGeneration != 0 &&
		(final.Started.UTC < arrivals.SealedUTC || final.Started.UTC < plan.LastScheduledUTC()) {
		add("CUT_BEFORE_ARRIVALS_CLOSE")
	}
	return issues
}

func isColumnIssue(m Manifest, reason string) bool {
	relation, ok := strings.CutPrefix(reason, "COLUMN_UNCLASSIFIED:")
	if !ok {
		return false
	}
	for _, db := range m.Databases() {
		if slices.ContainsFunc(m.Relations(db), func(spec Relation) bool { return spec.Name == relation }) {
			return true
		}
	}
	return false
}
