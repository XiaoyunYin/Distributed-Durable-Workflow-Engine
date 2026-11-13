package invariants

// K4 is an explicit, independent export contract. It never relaxes CheckCampaign.
import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"reflect"
	"sort"
	"strings"
)

type LifecycleIdentity struct {
	Namespace string `json:"namespace"`
	Workflow  string `json:"workflow"`
	Node      string `json:"node"`
	Iteration int    `json:"iteration"`
	Attempt   int64  `json:"attempt"`
	TokenHash string `json:"token_hash"`
}
type LifecycleSession struct {
	Session       string                `json:"session"`
	PodUID        string                `json:"pod_uid"`
	ContainerID   string                `json:"container_id"`
	Role          string                `json:"role"`
	Start         float64               `json:"start"`
	LastAlive     float64               `json:"last_alive"`
	Death         *float64              `json:"death"`
	DeathEvidence string                `json:"death_evidence"`
	ExitCode      *int                  `json:"exit_code"`
	ExitSignal    *int                  `json:"exit_signal"`
	Signals       []LifecycleSignal     `json:"signals"`
	Readiness     []LifecycleMembership `json:"readiness"`
	Endpoints     []LifecycleMembership `json:"endpoints"`
}
type LifecycleSignal struct {
	At       float64 `json:"at"`
	Signal   int     `json:"signal"`
	Evidence string  `json:"evidence"`
}
type LifecycleMembership struct {
	At      float64 `json:"at"`
	Present bool    `json:"present"`
	Width   float64 `json:"width"`
}
type LifecycleInvocation struct {
	ID            string            `json:"id"`
	Identity      LifecycleIdentity `json:"identity"`
	Session       string            `json:"session"`
	Start         float64           `json:"start"`
	Finish        *float64          `json:"finish"`
	LastAlive     float64           `json:"last_alive"`
	Abort         *float64          `json:"abort"`
	AbortEvidence string            `json:"abort_evidence"`
	Outcome       string            `json:"outcome"`
}
type LifecycleCommitBracket struct {
	Statement   float64 `json:"statement"`
	Finalized   float64 `json:"finalized"`
	Transaction string  `json:"transaction"`
	Session     string  `json:"session"`
	PodUID      string  `json:"pod_uid"`
	ContainerID string  `json:"container_id"`
}

type LifecycleClaim struct {
	CommitBracket *LifecycleCommitBracket `json:"commit_bracket"`
	Identity      LifecycleIdentity       `json:"identity"`
	Session       string                  `json:"session"`
	Order         int64                   `json:"order"`
	At            float64                 `json:"at"`
}
type LifecycleHistory struct {
	SchedulerEpoch *int64            `json:"scheduler_epoch"`
	ActorKind      string            `json:"actor_kind"`
	Identity       LifecycleIdentity `json:"identity"`
	Order          int64             `json:"order"`
	At             float64           `json:"at"`
	Kind           string            `json:"kind"`
	Receipt        string            `json:"receipt"`
	Partition      int               `json:"partition"`
	Owner          string            `json:"owner"`
	Epoch          int64             `json:"epoch"`
	Revision       int64             `json:"revision"`
}
type LifecycleReceipt struct {
	ID        string            `json:"id"`
	Identity  LifecycleIdentity `json:"identity"`
	Session   string            `json:"session"`
	Order     int64             `json:"order"`
	At        float64           `json:"at"`
	Category  string            `json:"category"`
	Outcome   string            `json:"outcome"`
	Rejection string            `json:"rejection"`
}
type LifecycleWorkflow struct {
	Workflow      string   `json:"workflow"`
	Key           string   `json:"key"`
	State         string   `json:"state"`
	Terminal      *float64 `json:"terminal"`
	ClockWidth    float64  `json:"clock_width"`
	FirstObserved *float64 `json:"first_observed"`
}
type LifecycleHTTP struct {
	Role      string  `json:"role"`
	Request   string  `json:"request"`
	BodyHash  string  `json:"body_hash"`
	Attempt   int     `json:"attempt"`
	Operation string  `json:"operation"`
	At        float64 `json:"at"`
	Status    int     `json:"status"`
	Error     string  `json:"error"`
}
type LifecycleWitness struct {
	Session      string   `json:"session"`
	PodUID       string   `json:"pod_uid"`
	ContainerID  string   `json:"container_id"`
	SHA256       string   `json:"sha256"`
	Data         string   `json:"data"` // Exact UTF-8 JSONL bytes; no raw tokens.
	Acknowledged []string `json:"acknowledged"`
	Complete     bool     `json:"complete"`
	Errors       int      `json:"errors"`
}
type LifecycleTimeline struct {
	Schema      string                `json:"schema"`
	Episode     string                `json:"episode"`
	Namespace   string                `json:"namespace"`
	Sessions    []LifecycleSession    `json:"sessions"`
	Claims      []LifecycleClaim      `json:"claims"`
	Invocations []LifecycleInvocation `json:"invocations"`
	History     []LifecycleHistory    `json:"history"`
	Receipts    []LifecycleReceipt    `json:"receipts"`
	Workflows   []LifecycleWorkflow   `json:"workflows"`
	HTTP        []LifecycleHTTP       `json:"http"`
	Witness     []LifecycleWitness    `json:"witness"`
}
type LifecycleLease struct {
	Order       int64   `json:"order"`
	At          float64 `json:"at"`
	Kind        string  `json:"kind"`
	Partition   int     `json:"partition"`
	Owner       string  `json:"owner"`
	Epoch       int64   `json:"epoch"`
	Session     string  `json:"session"`
	Transaction string  `json:"transaction"`
}
type LifecycleLeaseAudit struct {
	Schema  string           `json:"schema"`
	Episode string           `json:"episode"`
	Rows    []LifecycleLease `json:"rows"`
}
type LifecycleInterval struct {
	ID          string   `json:"id"`
	Lower       float64  `json:"lower"`
	Upper       *float64 `json:"upper"`
	Abandoned   bool     `json:"abandoned"`
	Canceled    bool     `json:"canceled"`
	Reexecution bool     `json:"reexecution"`
}
type LifecycleReport struct {
	Schema                string              `json:"schema"`
	Valid                 bool                `json:"valid"`
	Complete              bool                `json:"complete"`
	Violations            []string            `json:"violations"`
	Intervals             []LifecycleInterval `json:"intervals"`
	ClaimedUnexecuted     int                 `json:"claimed_unexecuted"`
	DuplicateExecutions   int                 `json:"duplicate_executions"`
	ExecutedIterations    int                 `json:"executed_iterations"`
	CanceledIterations    int                 `json:"canceled_iterations"`
	Winners               int                 `json:"winners"`
	Retryable             int                 `json:"retryable"`
	FinalFailures         int                 `json:"final_failures"`
	StaleCommitted        int                 `json:"stale_committed"`
	StaleRejected         int                 `json:"stale_rejected"`
	EvidenceOnly          int                 `json:"evidence_only"`
	SupersededTransitions int                 `json:"superseded_transitions"`
}

// DecodeLifecycle rejects unknown, missing and null non-nullable fields recursively.
// A trailing JSON value is also an error. Shared legacy decoding is unchanged.
func DecodeLifecycle(data []byte, target any) error {
	var raw any
	d := json.NewDecoder(bytes.NewReader(data))
	if err := d.Decode(&raw); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("lifecycle trailing JSON")
	}
	if err := lifecycleShape(raw, reflect.TypeOf(target).Elem()); err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	return dec.Decode(target)
}
func lifecycleShape(raw any, typ reflect.Type) error {
	if typ.Kind() == reflect.Pointer {
		if raw == nil {
			return nil
		}
		return lifecycleShape(raw, typ.Elem())
	}
	if raw == nil {
		return fmt.Errorf("lifecycle null required field")
	}
	switch typ.Kind() {
	case reflect.Struct:
		obj, ok := raw.(map[string]any)
		if !ok {
			return fmt.Errorf("lifecycle object required")
		}
		if len(obj) != typ.NumField() {
			return fmt.Errorf("lifecycle unknown or missing fields")
		}
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			name := f.Tag.Get("json")
			v, ok := obj[name]
			if !ok {
				return fmt.Errorf("lifecycle missing %s", name)
			}
			if err := lifecycleShape(v, f.Type); err != nil {
				return err
			}
		}
	case reflect.Slice:
		rows, ok := raw.([]any)
		if !ok {
			return fmt.Errorf("lifecycle array required")
		}
		for _, r := range rows {
			if err := lifecycleShape(r, typ.Elem()); err != nil {
				return err
			}
		}
	case reflect.Float64:
		value, ok := raw.(float64)
		if !ok || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
			return fmt.Errorf("lifecycle finite nonnegative time required")
		}
	}
	return nil
}
func lifecycleHash(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 32 && s == strings.ToLower(s)
}
func lifecycleKey(i LifecycleIdentity) string {
	return fmt.Sprintf("%s/%s/%s/%d", i.Namespace, i.Workflow, i.Node, i.Iteration)
}
func lifecycleAttempt(i LifecycleIdentity) string {
	return fmt.Sprintf("%s/%d/%s", lifecycleKey(i), i.Attempt, i.TokenHash)
}

func CheckLifecycle(t LifecycleTimeline, audit LifecycleLeaseAudit) LifecycleReport {
	r := LifecycleReport{Schema: "dur053.k4-checker.v1", Complete: true, Violations: []string{}, Intervals: []LifecycleInterval{}}
	fail := func(s string) { r.Violations = append(r.Violations, s) }
	missing := func(s string) { r.Complete = false; fail(s) }
	if t.Schema != "dur053.k4-timeline.v1" || audit.Schema != "dur053.k4-lease-audit.v1" || audit.Episode != t.Episode || t.Namespace == "" {
		missing("lifecycle schema or episode mismatch")
	}
	if len(t.Sessions) == 0 {
		missing("lifecycle session inventory missing")
	}
	sessions := map[string]LifecycleSession{}
	for _, s := range t.Sessions {
		if _, ok := sessions[s.Session]; ok || s.Session == "" || s.PodUID == "" || s.ContainerID == "" {
			missing("lifecycle session identity invalid")
		}
		if s.LastAlive < s.Start || (s.Death != nil && (*s.Death < s.LastAlive || s.DeathEvidence == "")) {
			missing("lifecycle death bounds invalid")
		}
		sessions[s.Session] = s
	}
	validIdentity := func(i LifecycleIdentity) bool {
		return i.Namespace == t.Namespace && i.Workflow != "" && i.Node != "" && i.Iteration >= 0 && i.Attempt > 0 && lifecycleHash(i.TokenHash)
	}
	claims := map[string]LifecycleClaim{}
	for _, c := range t.Claims {
		key := lifecycleAttempt(c.Identity)
		if _, ok := claims[key]; ok || !validIdentity(c.Identity) {
			missing("lifecycle claim identity invalid")
		}
		if _, ok := sessions[c.Session]; !ok {
			missing("lifecycle claim session missing")
		}
		claims[key] = c
	}
	invocations := map[string]LifecycleInvocation{}
	byLogical := map[string][]LifecycleInvocation{}
	executed := map[string]bool{}
	for _, v := range t.Invocations {
		if _, ok := invocations[v.ID]; ok || v.ID == "" {
			missing("lifecycle duplicate invocation")
		}
		c, ok := claims[lifecycleAttempt(v.Identity)]
		if !ok || c.Session != v.Session || c.At > v.Start {
			missing("lifecycle invocation claim missing")
		}
		if _, ok := sessions[v.Session]; !ok {
			missing("lifecycle invocation session missing")
		}
		invocations[v.ID] = v
		executed[lifecycleAttempt(v.Identity)] = true
		key := lifecycleKey(v.Identity)
		byLogical[key] = append(byLogical[key], v)
	}
	inventories := map[string]bool{}
	acknowledged := map[string]bool{}
	for _, w := range t.Witness {
		s, ok := sessions[w.Session]
		if inventories[w.Session] || !ok || s.PodUID != w.PodUID || s.ContainerID != w.ContainerID || !w.Complete || w.Errors != 0 {
			missing("lifecycle witness inventory invalid")
		}
		inventories[w.Session] = true
		digest := sha256.Sum256([]byte(w.Data))
		if hex.EncodeToString(digest[:]) != w.SHA256 {
			missing("lifecycle witness hash mismatch")
		}
		starts := map[string]bool{}
		finishes := map[string]bool{}
		for _, line := range strings.Split(strings.TrimSpace(w.Data), "\n") {
			if line == "" {
				continue
			}
			var event struct {
				Event    string            `json:"event"`
				ID       string            `json:"id"`
				Identity LifecycleIdentity `json:"identity"`
				At       float64           `json:"at"`
			}
			if DecodeLifecycle([]byte(line), &event) != nil {
				missing("lifecycle witness row invalid")
				continue
			}
			v, ok := invocations[event.ID]
			if !ok || v.Session != w.Session || v.Identity != event.Identity {
				missing("lifecycle witness invocation mismatch")
			}
			switch event.Event {
			case "start":
				if starts[event.ID] || event.At != v.Start {
					missing("lifecycle witness start mismatch")
				}
				starts[event.ID] = true
			case "finish":
				if finishes[event.ID] || v.Finish == nil || event.At != *v.Finish {
					missing("lifecycle witness finish mismatch")
				}
				finishes[event.ID] = true
			default:
				missing("lifecycle witness event invalid")
			}
		}
		for _, id := range w.Acknowledged {
			if acknowledged[id] || !starts[id] {
				missing("lifecycle acknowledged start missing")
			}
			acknowledged[id] = true
		}
		for id := range starts {
			if !acknowledged[id] {
				missing("lifecycle unacknowledged start")
			}
		}
		for id, v := range invocations {
			if v.Session == w.Session && (!starts[id] || (v.Finish != nil && !finishes[id])) {
				missing("lifecycle invocation witness missing")
			}
		}
	}
	for id := range sessions {
		if !inventories[id] {
			missing("lifecycle dead session inventory missing")
		}
	}
	for id := range invocations {
		if !acknowledged[id] {
			missing("lifecycle acknowledged start missing")
		}
	}
	for key := range claims {
		if !executed[key] {
			r.ClaimedUnexecuted++
		}
	}
	history := append([]LifecycleHistory{}, t.History...)
	sort.Slice(history, func(i, j int) bool { return history[i].Order < history[j].Order })
	invalidated := map[string]int64{}
	canceled := map[string]bool{}
	cancelOrder := map[string]int64{}
	historyReceipts := map[string]LifecycleHistory{}
	orders := map[int64]bool{}
	for _, h := range history {
		if h.Order <= 0 || orders[h.Order] || !validIdentity(h.Identity) {
			missing("lifecycle history ordering invalid")
		}
		orders[h.Order] = true
		switch h.Kind {
		case "supersede":
			if _, exists := invalidated[lifecycleAttempt(h.Identity)]; !exists {
				invalidated[lifecycleAttempt(h.Identity)] = h.Order
			}
		case "cancel":
			if _, exists := cancelOrder[lifecycleKey(h.Identity)]; !exists {
				cancelOrder[lifecycleKey(h.Identity)] = h.Order
			}
			canceled[lifecycleKey(h.Identity)] = true
		case "receipt":
			if h.Receipt == "" {
				missing("lifecycle receipt identity missing")
			}
			if _, ok := historyReceipts[h.Receipt]; ok {
				missing("lifecycle duplicate receipt transition")
			}
			historyReceipts[h.Receipt] = h
		case "claim", "transition", "terminal":
		default:
			missing("lifecycle history kind invalid")
		}
	}
	winners := map[string]int{}
	committed := map[string]bool{}
	receipts := map[string]bool{}
	committedIDs := map[string]bool{}
	for _, v := range t.Receipts {
		if receipts[v.ID] || v.ID == "" || !validIdentity(v.Identity) {
			missing("lifecycle receipt identity invalid")
		}
		receipts[v.ID] = true
		if _, ok := claims[lifecycleAttempt(v.Identity)]; !ok {
			missing("lifecycle receipt claim missing")
		}
		switch v.Category {
		case "committed":
			committedIDs[v.ID] = true
			h, ok := historyReceipts[v.ID]
			if !ok || h.Identity != v.Identity || h.Order != v.Order || h.At != v.At {
				missing("lifecycle committed receipt history missing")
			}
			laterClaim := false
			for _, c := range t.Claims {
				if lifecycleKey(c.Identity) == lifecycleKey(v.Identity) && c.Identity.Attempt > v.Identity.Attempt && c.Order < v.Order {
					laterClaim = true
				}
			}
			if n, ok := invalidated[lifecycleAttempt(v.Identity)]; (ok && n < v.Order) || laterClaim {
				r.StaleCommitted++
			} else if n, ok := cancelOrder[lifecycleKey(v.Identity)]; ok && n < v.Order {
				r.StaleCommitted++
			}
			committed[lifecycleAttempt(v.Identity)] = true
			if v.Outcome == "success" {
				winners[lifecycleKey(v.Identity)]++
				if !canceled[lifecycleKey(v.Identity)] {
					r.Winners++
				}
			} else if v.Outcome == "retryable" {
				r.Retryable++
			} else {
				r.FinalFailures++
			}
		case "rejected":
			if v.Rejection == "stale_attempt" || v.Rejection == "canceled" {
				r.StaleRejected++
			} else {
				missing("lifecycle untyped rejection")
			}
		case "evidence_only":
			r.EvidenceOnly++
		default:
			missing("lifecycle receipt category invalid")
		}
	}
	for id := range historyReceipts {
		if !committedIDs[id] {
			missing("lifecycle receipt export missing")
		}
	}
	// Shared commit order across every workflow and lease transaction, per partition.
	leases := append([]LifecycleLease{}, audit.Rows...)
	sort.Slice(leases, func(i, j int) bool { return leases[i].Order < leases[j].Order })
	leaseOrders := map[int64]bool{}
	for _, l := range leases {
		if l.Order <= 0 || leaseOrders[l.Order] || l.Owner == "" || l.Epoch <= 0 || l.Transaction == "" {
			missing("lifecycle lease order incomplete")
		}
		leaseOrders[l.Order] = true
		if _, ok := sessions[l.Session]; !ok {
			missing("lifecycle lease session missing")
		}
		if l.Kind != "acquire" && l.Kind != "release" {
			missing("lifecycle lease kind invalid")
		}
	}
	for _, h := range history {
		if h.Owner == "" {
			if h.Kind == "transition" || h.Kind == "terminal" {
				missing("lifecycle owner-authorized transition lacks owner")
			}
			continue
		}
		var newest *LifecycleLease
		var matching *LifecycleLease
		var release *LifecycleLease
		for j := range leases {
			l := &leases[j]
			if l.Partition == h.Partition && l.Order <= h.Order && l.Owner == h.Owner && l.Epoch == h.Epoch {
				if l.Kind == "acquire" {
					matching = l
				} else if l.Kind == "release" {
					release = l
				}
			}
			if l.Kind == "acquire" && l.Partition == h.Partition && l.Order <= h.Order {
				if newest == nil || l.Order > newest.Order {
					newest = l
				}
			}
		}
		if matching == nil || (newest != nil && h.Epoch > newest.Epoch) {
			missing("lifecycle owner acquisition missing")
		}
		if newest != nil && (h.Epoch < newest.Epoch || h.Owner != newest.Owner) {
			r.SupersededTransitions++
		} else if release != nil && (matching == nil || release.Order > matching.Order) {
			r.SupersededTransitions++
		}
	}
	for key, vs := range byLogical {
		sort.Slice(vs, func(i, j int) bool {
			if vs[i].Start == vs[j].Start {
				return vs[i].ID < vs[j].ID
			}
			return vs[i].Start < vs[j].Start
		})
		if canceled[key] {
			r.CanceledIterations++
		} else {
			r.ExecutedIterations++
		}
		r.DuplicateExecutions += len(vs) - 1
		if winners[key] > 1 {
			fail("lifecycle multiple winning results")
		}
		for index, v := range vs {
			interval := LifecycleInterval{ID: v.ID, Canceled: canceled[key], Reexecution: index > 0}
			if v.Finish != nil {
				end := *v.Finish
				if s := sessions[v.Session]; s.Death != nil && end > *s.Death {
					missing("lifecycle finish occurs after session death")
				}
				interval.Lower = end - v.Start
				upper := interval.Lower
				interval.Upper = &upper
			} else {
				s := sessions[v.Session]
				last := math.Max(v.Start, v.LastAlive)
				interval.Lower = last - v.Start
				var end *float64
				if s.Death != nil && s.DeathEvidence != "" {
					end = s.Death
				}
				if v.Abort != nil && v.AbortEvidence != "" && (end == nil || *v.Abort < *end) {
					end = v.Abort
				}
				if end == nil {
					missing("lifecycle unfinished invocation lacks exact-session death or abort")
				} else if *end < last {
					missing("lifecycle interval bounds invalid")
				} else {
					upper := *end - v.Start
					interval.Upper = &upper
				}
			}
			interval.Abandoned = !committed[lifecycleAttempt(v.Identity)] && !canceled[key]
			if interval.Lower < 0 {
				missing("lifecycle interval bounds invalid")
			}
			r.Intervals = append(r.Intervals, interval)
		}
	}
	// Successful terminal work requires one independent winner for every executed iteration.
	for _, w := range t.Workflows {
		if w.State == "SUCCEEDED" {
			found := false
			for key, vs := range byLogical {
				if vs[0].Identity.Workflow == w.Workflow {
					found = true
					if winners[key] != 1 {
						missing("lifecycle successful iteration lacks one winner")
					}
				}
			}
			if !found {
				missing("lifecycle successful workflow lacks execution")
			}
		}
	}
	if r.StaleCommitted != 0 {
		fail("lifecycle stale committed results must be zero")
	}
	if r.SupersededTransitions != 0 {
		fail("lifecycle superseded-owner transitions must be zero")
	}
	sort.Slice(r.Intervals, func(i, j int) bool { return r.Intervals[i].ID < r.Intervals[j].ID })
	r.Valid = r.Complete && len(r.Violations) == 0
	sort.Strings(r.Violations)
	return r
}
