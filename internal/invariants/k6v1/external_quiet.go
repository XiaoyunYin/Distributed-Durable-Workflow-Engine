package k6v1

import (
	"slices"
	"strings"
	"time"
)

const (
	QuietDuration  = 30 * time.Second
	MaxSampleGap   = 5 * time.Second
	SamplesVersion = "dur053.k6-external-samples.v1"
)

type Cursor struct {
	Count  int64  `json:"count"`
	SHA256 string `json:"sha256"`
}

type ExternalMark struct {
	Path          string `json:"path"`
	Identity      string `json:"identity"`
	Sequence      int64  `json:"sequence"`
	Activity      Cursor `json:"activity"`
	IdleProbes    Cursor `json:"idle_probes"`
	LeaseRenewals Cursor `json:"lease_renewals"`
	WatchProgress Cursor `json:"watch_progress"`
	Digest        string `json:"digest"`
	ThroughUTC    string `json:"through_utc"`
	SealMode      string `json:"seal_mode"`
}

type prefix struct {
	marks []ExternalMark
	utc   []string
	seal  StreamSeal
}

func advance(c *Cursor, hash string) {
	c.Count++
	c.SHA256 = Hash([]byte(c.SHA256 + hash))
}

func makePrefix[T any](path string, stream *Stream[T], kind func(T) string) prefix {
	raw, _ := Canonical(stream.Header)
	empty := Cursor{SHA256: Hash(nil)}
	mark := ExternalMark{Path: path, Identity: Hash(raw), Activity: empty, IdleProbes: empty,
		LeaseRenewals: empty, WatchProgress: empty, Digest: Hash(nil)}
	p := prefix{marks: []ExternalMark{mark}, utc: []string{stream.Header.OpenedUTC}, seal: stream.Seal}
	for _, row := range stream.Rows {
		raw, _ := Canonical(row)
		rowHash := Hash(raw)
		mark.Sequence = row.Position.Sequence
		mark.Digest = Hash([]byte(mark.Digest + rowHash))
		cursor := &mark.Activity
		switch kind(row.Data) {
		case "idle_probe":
			cursor = &mark.IdleProbes
		case "lease_renewal":
			cursor = &mark.LeaseRenewals
		case "bookmark":
			cursor = &mark.WatchProgress
		}
		advance(cursor, rowHash)
		p.marks = append(p.marks, mark)
		p.utc = append(p.utc, row.Position.UTC)
	}
	return p
}

func (e External) prefixes() map[string]prefix {
	out := map[string]prefix{}
	if e.Roster.Header.Source != "" {
		out[RosterPath] = makePrefix(RosterPath, &e.Roster, func(w WatchRow) string { return w.Kind })
	}
	for id, stream := range e.Sessions {
		path := SessionPath(id)
		out[path] = makePrefix(path, &stream, func(w WorkerRow) string { return w.Kind })
	}
	if e.Broker != nil {
		out[BrokerPath] = makePrefix(BrokerPath, e.Broker, func(BrokerRow) string { return "" })
	}
	if e.Topology != nil {
		out[TopologyPath] = makePrefix(TopologyPath, e.Topology, func(TopologyRow) string { return "" })
	}
	if e.Ingress != nil {
		out[IngressPath] = makePrefix(IngressPath, e.Ingress, func(IngressRow) string { return "" })
	}
	if e.Decisions != nil {
		out[DecisionsPath] = makePrefix(DecisionsPath, e.Decisions, func(DecisionRow) string { return "" })
	}
	return out
}

func (e External) Marks() []ExternalMark {
	ps := e.prefixes()
	out := make([]ExternalMark, 0, len(ps))
	for _, path := range sortedKeys(ps) {
		marks := ps[path].marks
		mark := marks[len(marks)-1]
		mark.ThroughUTC = ps[path].seal.ThroughUTC
		mark.SealMode = ps[path].seal.Mode
		out = append(out, mark)
	}
	return out
}

func (e External) Terminal() bool {
	marks := e.Marks()
	return mutableFenced(marks) && e.terminalAt(marks)
}

func mutableFenced(marks []ExternalMark) bool {
	for _, mark := range marks {
		if (mark.Path == BrokerPath || mark.Path == TopologyPath) && mark.SealMode != "terminal" {
			return false
		}
	}
	return true
}

func (e External) SealedThrough(at string) bool {
	for path, p := range e.prefixes() {
		if p.seal.Mode == "checkpoint" || !isSessionPath(path) && p.seal.ThroughUTC < at {
			return false
		}
	}
	return true
}

func (e External) terminalAt(marks []ExternalMark) bool {
	deaths := map[string]Death{}
	for _, mark := range marks {
		if mark.Path != RosterPath || mark.Sequence < 0 || mark.Sequence > int64(len(e.Roster.Rows)) {
			continue
		}
		for _, row := range e.Roster.Rows[:mark.Sequence] {
			if row.Data.Kind == "death" {
				deaths[row.Data.Session.ID] = *row.Data.Death
			}
		}
	}
	for _, mark := range marks {
		if !isSessionPath(mark.Path) {
			continue
		}
		id := strings.TrimSuffix(strings.TrimPrefix(mark.Path, "witness/"), ".jsonl")
		ledger := e.Sessions[id]
		if mark.Sequence < 1 || mark.Sequence > int64(len(ledger.Rows)) {
			return false
		}
		active, sent := map[string]bool{}, map[string]bool{}
		for _, row := range ledger.Rows[:mark.Sequence] {
			w := row.Data
			switch w.Kind {
			case "start":
				active[w.Invocation] = true
			case "finish", "abort":
				delete(active, w.Invocation)
			case "send":
				sent[w.Traffic.exchange()] = true
			case "receive", "close", "timeout":
				delete(sent, w.Traffic.exchange())
			}
		}
		if len(sent) > 0 || len(active) > 0 && (mark.Sequence != ledger.Seal.Last || !corroboratedDeath(ledger, deaths[id])) {
			return false
		}
	}
	return true
}

type QuietDatabase struct {
	DB             string `json:"db"`
	ActivitySHA256 string `json:"activity_sha256"`
}

// QuietDB projects the same activity-sensitive relations used by the paired
// cuts. Renewal-only audit rows are compared by the paired watermark check.
func QuietDB(db string, marks []Watermark) QuietDatabase {
	stable := make([]Watermark, 0, len(marks))
	for _, mark := range marks {
		if mark.Relation == DeadlineWrites {
			for _, a := range mark.Audit {
				if a.Kind != "renewal" {
					stable = append(stable, Watermark{Relation: mark.Relation, Count: a.ID, Digest: a.Digest})
				}
			}
			continue
		}
		stable = append(stable, mark)
	}
	raw, _ := Canonical(stable)
	return QuietDatabase{db, Hash(raw)}
}

type QuietSample struct {
	Sequence   int64           `json:"sequence"`
	Generation int             `json:"generation"`
	Phase      string          `json:"phase"`
	At         Observer        `json:"at"`
	Terminal   bool            `json:"terminal"`
	Marks      []ExternalMark  `json:"marks"`
	Databases  []QuietDatabase `json:"databases"`
}

type ExternalSamples struct {
	Schema   string        `json:"schema"`
	Binding  SourceBinding `json:"binding"`
	Observer string        `json:"observer"`
	Rows     []QuietSample `json:"rows"`
	Seal     StreamSeal    `json:"seal"`
}

func (s *ExternalSamples) SealAt(at string) error {
	var stream Stream[QuietSample]
	stream.Header.Source = s.Observer
	stream.Rows = make([]Record[QuietSample], 0, len(s.Rows))
	for _, row := range s.Rows {
		stream.Rows = append(stream.Rows, Record[QuietSample]{Position{row.Sequence, row.At.UTC}, row})
	}
	if err := SealStream(&stream, "terminal", at, int64(len(s.Rows)), 0); err != nil {
		return err
	}
	s.Seal = stream.Seal
	return nil
}

// ExternalStep compares consecutive observations. Recomputed history and
// payload positions are checked separately when reading a completed bundle.
func ExternalStep(old, next []ExternalMark) (activity bool, regression string) {
	byPath := map[string]ExternalMark{}
	for _, mark := range next {
		byPath[mark.Path] = mark
	}
	for _, a := range old {
		b, ok := byPath[a.Path]
		if !ok || a.Identity != b.Identity || b.Sequence < a.Sequence || b.ThroughUTC < a.ThroughUTC ||
			(a.SealMode != "checkpoint" && b.SealMode != a.SealMode) ||
			(isSessionPath(a.Path) && a.SealMode != "checkpoint" && b.Sequence != a.Sequence) ||
			b.Activity.Count < a.Activity.Count || b.IdleProbes.Count < a.IdleProbes.Count ||
			b.LeaseRenewals.Count < a.LeaseRenewals.Count || b.WatchProgress.Count < a.WatchProgress.Count ||
			b.Sequence == a.Sequence && b.Digest != a.Digest {
			return false, a.Path
		}
		activity = activity || a.Activity != b.Activity
	}
	return activity || len(old) != len(next), ""
}

func elapsed(a, b Observer) time.Duration {
	x, errX := time.Parse(TimeLayout, a.UTC)
	y, errY := time.Parse(TimeLayout, b.UTC)
	if errX != nil || errY != nil {
		return -1
	}
	return min(y.Sub(x), time.Duration(b.MonotonicNS-a.MonotonicNS))
}

func SampleGap(a, b Observer) bool {
	x, _ := time.Parse(TimeLayout, a.UTC)
	y, _ := time.Parse(TimeLayout, b.UTC)
	return !orderedObservers(a, b) || y.Sub(x) > MaxSampleGap ||
		time.Duration(b.MonotonicNS-a.MonotonicNS) > MaxSampleGap
}

func QuietElapsed(a, b Observer) bool { return elapsed(a, b) >= QuietDuration }

func verifyExternalQuiet(m Manifest, plan Plan, arrivals Arrivals, cut Barrier, e External, input ExternalSamples, rows map[string][]Row) []Finding {
	var issues []Finding
	add := func(code, path string) { issues = append(issues, *integrity(code, path)) }
	expected := input.Seal
	if input.Schema != SamplesVersion || input.Binding != Binding(m) || !nonempty(input.Observer) || len(input.Rows) < 2 ||
		input.SealAt(expected.ThroughUTC) != nil || input.Seal != expected ||
		!validTime(expected.ThroughUTC) || expected.ThroughUTC != input.Rows[len(input.Rows)-1].At.UTC {
		add("EXTERNAL_SAMPLES_SEAL", SamplesPath)
		return issues
	}
	prefixes := e.prefixes()
	phase := ""
	var previous QuietSample
	var quietStart Observer
	ready := false
	floor := max(plan.LastScheduledUTC(), arrivals.SealedUTC)
	if arrivals.RemovalUTC != nil {
		floor = max(floor, *arrivals.RemovalUTC)
	}
	var generation int
	dbs := m.Databases()
	sealed := false
	for index, sample := range input.Rows {
		if sealed || sample.Sequence != int64(index+1) || !orderedObservers(sample.At, sample.At) ||
			sample.Generation < 1 || sample.Generation > len(cut.Attempts) || sample.Generation < generation ||
			len(sample.Databases) != len(dbs) || len(sample.Marks) == 0 {
			add("EXTERNAL_SAMPLE_SHAPE", SamplesPath)
			return issues
		}
		for i, db := range sample.Databases {
			if db.DB != dbs[i] || !hashes(db.ActivitySHA256) {
				add("EXTERNAL_DATABASE_SAMPLE", SamplesPath)
				return issues
			}
		}
		last := ""
		for _, mark := range sample.Marks {
			p, ok := prefixes[mark.Path]
			through := mark.ThroughUTC
			mode := mark.SealMode
			mark.ThroughUTC, mark.SealMode = "", ""
			if !ok || mark.Path <= last || mark.Sequence < 0 || mark.Sequence >= int64(len(p.marks)) ||
				p.marks[mark.Sequence] != mark || !validTime(through) || p.utc[mark.Sequence] > through ||
				through > sample.At.UTC || through > p.seal.ThroughUTC ||
				mode != "checkpoint" && mode != p.seal.Mode ||
				mark.Sequence+1 < int64(len(p.utc)) && p.utc[mark.Sequence+1] < through {
				add("EXTERNAL_PREFIX_BINDING", mark.Path)
			} else if (!isSessionPath(mark.Path) || p.seal.Mode == "checkpoint" || mark.Sequence < p.seal.Last) &&
				SampleGap(Observer{UTC: through}, Observer{UTC: sample.At.UTC}) {
				add("EXTERNAL_SOURCE_STALE", mark.Path)
			}
			last = mark.Path
		}
		for _, path := range requiredExternal(m) {
			if path != SamplesPath && !slices.ContainsFunc(sample.Marks, func(mark ExternalMark) bool { return mark.Path == path }) {
				add("EXTERNAL_SAMPLE_MISSING", path)
			}
		}
		for _, mark := range sample.Marks {
			if mark.Path != RosterPath || mark.Sequence < 0 || mark.Sequence > int64(len(e.Roster.Rows)) {
				continue
			}
			for _, entry := range e.Roster.Rows[:mark.Sequence] {
				if entry.Data.Session == nil {
					continue
				}
				path := SessionPath(entry.Data.Session.ID)
				if !slices.ContainsFunc(sample.Marks, func(seen ExternalMark) bool { return seen.Path == path && seen.Sequence >= 1 }) {
					add("EXTERNAL_SAMPLE_MISSING", path)
				}
			}
		}
		activity, regression := ExternalStep(previous.Marks, sample.Marks)
		if regression != "" {
			add("EXTERNAL_REGRESSION", regression)
		}
		if index > 0 && SampleGap(previous.At, sample.At) {
			add("EXTERNAL_SAMPLE_GAP", SamplesPath)
		}
		if sample.Terminal && !e.terminalAt(sample.Marks) {
			add("EXTERNAL_OPEN_EXCHANGE", SamplesPath)
		}
		if sample.Terminal && !mutableFenced(sample.Marks) {
			add("EXTERNAL_MUTABLE_WRITER_UNFENCED", SamplesPath)
		}
		changed := activity || !slices.Equal(previous.Databases, sample.Databases)
		if generation != sample.Generation {
			if sample.Generation != generation+1 {
				add("EXTERNAL_GENERATION_GAP", SamplesPath)
			}
			if phase != "" && phase != "post" {
				add("EXTERNAL_PHASE_ORDER", SamplesPath)
			}
			phase, quietStart, ready = "", Observer{}, false
		}
		gen := cut.Attempts[sample.Generation-1]
		switch sample.Phase {
		case "quiet", "pre":
			if phase != "" && phase != "quiet" {
				add("EXTERNAL_PHASE_ORDER", SamplesPath)
			}
			if !sample.Terminal || sample.At.UTC < floor {
				quietStart = Observer{}
			} else if quietStart.UTC == "" || changed {
				quietStart = sample.At
			}
			if sample.Phase == "pre" {
				ready = quietStart.UTC != "" && QuietElapsed(quietStart, sample.At) && orderedObservers(sample.At, gen.Started)
				if !ready {
					add("EXTERNAL_QUIET_WINDOW", SamplesPath)
				}
			}
		case "post":
			if phase != "pre" || !orderedObservers(gen.Finished, sample.At) {
				add("EXTERNAL_PHASE_ORDER", SamplesPath)
			}
			raced := changed || !sample.Terminal
			if raced && sample.Generation == cut.SelectedGeneration {
				add("EXTERNAL_CUT_ACTIVITY", SamplesPath)
			}
			for i, d := range gen.Databases {
				if i >= len(previous.Databases) || i >= len(sample.Databases) {
					add("EXTERNAL_DATABASE_BINDING", SamplesPath)
					break
				}
				raced = raced || QuietDB(d.DB, d.Pre.Relations) != previous.Databases[i]
				if QuietDB(d.DB, d.Post.Relations) != sample.Databases[i] {
					add("EXTERNAL_DATABASE_BINDING", SamplesPath)
				}
			}
			if raced != slices.Contains(gen.Reasons, "CUT_RACED:external") {
				add("EXTERNAL_CUT_REASON", SamplesPath)
			}
		case "export":
			if phase != "post" || !ready || changed || !sample.Terminal ||
				sample.Generation != cut.SelectedGeneration || sample.At.UTC > cut.DeadlineUTC {
				add("EXTERNAL_EXPORT_ACTIVITY", SamplesPath)
			}
			sealed = true
		default:
			add("EXTERNAL_PHASE_ORDER", SamplesPath)
		}
		generation, phase, previous = sample.Generation, sample.Phase, sample
	}
	if !sealed || !slices.Equal(previous.Marks, e.Marks()) {
		add("EXTERNAL_FINAL_BINDING", SamplesPath)
	}
	for _, path := range sortedKeys(prefixes) {
		p := prefixes[path]
		if p.seal.Mode == "checkpoint" || p.seal.ThroughUTC > previous.At.UTC ||
			(!isSessionPath(path) && p.seal.ThroughUTC < input.Rows[max(0, len(input.Rows)-2)].At.UTC) {
			add("EXTERNAL_FINAL_SEAL", path)
		}
	}
	for _, w := range rows[executions] {
		if !isTerminal(w["state"]) {
			add("EXTERNAL_TERMINAL_WORK", RelationPath(executions))
			break
		}
	}
	return issues
}

func requiredExternal(m Manifest) []string {
	paths := []string{RosterPath, BrokerPath, SamplesPath}
	if m.Sink == nil {
		return append(paths, TopologyPath)
	}
	return append(paths, DecisionsPath, IngressPath)
}

func ExternalPath(path string) bool {
	return isSessionPath(path) || strings.HasPrefix(path, "witness/") ||
		path == DecisionsPath || path == BrokerPath
}
