package k6v1

import (
	"bytes"
	"errors"
	"io/fs"
	"slices"
	"strings"
)

const (
	RosterPath       = "witness/inventory.json"
	IngressPath      = "witness/effect-ingress.jsonl"
	DecisionsPath    = "auth/decisions.jsonl"
	BrokerPath       = "broker/positions.jsonl"
	BrokerCopiesPath = "broker/copies.jsonl"
	TopologyPath     = "witness/topology.json"
	SamplesPath      = "samples/external.json"
)

// SourceBinding identifies one acquisition across the independent observers.
type SourceBinding struct {
	Episode   string `json:"episode"`
	Namespace string `json:"namespace"`
	ResetID   string `json:"reset_id"`
	BarrierID string `json:"barrier_id"`
}

func Binding(m Manifest) SourceBinding {
	return SourceBinding{m.Episode, m.Namespace, m.ResetID, m.BarrierID}
}

type StreamHeader struct {
	Schema    string        `json:"schema"`
	Binding   SourceBinding `json:"binding"`
	Source    string        `json:"source"`
	OpenedUTC string        `json:"opened_utc"`
}

type StreamSeal struct {
	Source     string `json:"source"`
	Mode       string `json:"mode"`
	First      int64  `json:"first"`
	Last       int64  `json:"last"`
	Attempted  int64  `json:"attempted"`
	Written    int64  `json:"written"`
	Errors     int64  `json:"errors"`
	Length     int64  `json:"length"`
	SHA256     string `json:"sha256"`
	ThroughUTC string `json:"through_utc"`
}

type Position struct {
	Sequence int64  `json:"sequence"`
	UTC      string `json:"utc"`
}

type Record[T any] struct {
	Position Position `json:"position"`
	Data     T        `json:"data"`
}

type Stream[T any] struct {
	Header StreamHeader `json:"header"`
	Rows   []Record[T]  `json:"rows"`
	Seal   StreamSeal   `json:"seal"`
}

func (s *Stream[T]) payload() ([]byte, error) {
	var out bytes.Buffer
	for _, row := range s.Rows {
		raw, err := Canonical(row)
		if err != nil {
			return nil, err
		}
		out.Write(raw)
	}
	return out.Bytes(), nil
}

// SealStream takes a collector-owned persisted prefix. The caller supplies the
// observed attempted/error counts and confirmation mode.
func SealStream[T any](s *Stream[T], mode, through string, attempted, failures int64) error {
	raw, err := s.payload()
	if err != nil {
		return err
	}
	count := int64(len(s.Rows))
	var first int64
	if count > 0 {
		first = 1
	}
	s.Seal = StreamSeal{s.Header.Source, mode, first, count, attempted, count, failures, int64(len(raw)), Hash(raw), through}
	return nil
}

func EncodeStream[T any](s Stream[T]) ([]byte, error) {
	header, err := Canonical(s.Header)
	if err != nil {
		return nil, err
	}
	data, err := s.payload()
	if err != nil {
		return nil, err
	}
	seal, err := Canonical(s.Seal)
	if err != nil {
		return nil, err
	}
	return bytes.Join([][]byte{header, data, seal}, nil), nil
}

func decodeStream[T any](raw []byte, jsonl bool, s *Stream[T]) bool {
	if !jsonl {
		return decodeCanonical(raw, s) == nil
	}
	lines := bytes.SplitAfter(raw, []byte{'\n'})
	if len(lines) < 3 || len(lines[len(lines)-1]) != 0 ||
		decodeCanonical(lines[0], &s.Header) != nil ||
		decodeCanonical(lines[len(lines)-2], &s.Seal) != nil {
		return false
	}
	s.Rows = make([]Record[T], len(lines)-3)
	for i := range s.Rows {
		if len(lines[i+1]) > 1<<20 || decodeCanonical(lines[i+1], &s.Rows[i]) != nil {
			return false
		}
	}
	return true
}

func (s Stream[T]) verify(m Manifest, schema string, valid func(T) bool) string {
	if s.Header.Schema != "dur053.k6-"+schema+".v1" || s.Header.Binding != Binding(m) ||
		!nonempty(s.Header.Source) || !validTime(s.Header.OpenedUTC) || s.Rows == nil {
		return "EXTERNAL_HEADER"
	}
	previous := s.Header.OpenedUTC
	for i, row := range s.Rows {
		if row.Position.Sequence != int64(i+1) {
			return "EXTERNAL_SEQUENCE_GAP"
		}
		if !validTime(row.Position.UTC) || row.Position.UTC < previous || !valid(row.Data) {
			return "EXTERNAL_ROW_SCHEMA"
		}
		previous = row.Position.UTC
	}
	if !validTime(s.Seal.ThroughUTC) || s.Seal.ThroughUTC < previous ||
		!slices.Contains([]string{"terminal", "receiver_final", "checkpoint"}, s.Seal.Mode) ||
		s.Seal.Attempted != int64(len(s.Rows)) || s.Seal.Errors != 0 {
		return "EXTERNAL_SEAL"
	}
	expected := s.Seal
	if SealStream(&s, expected.Mode, expected.ThroughUTC, expected.Attempted, expected.Errors) != nil || s.Seal != expected {
		return "EXTERNAL_SEAL"
	}
	return ""
}

type WorkerSession struct {
	ID                  string `json:"id"`
	PodUID              string `json:"pod_uid"`
	ContainerID         string `json:"container_id"`
	Restart             int64  `json:"restart"`
	BootID              string `json:"boot_id"`
	StartedUTC          string `json:"started_utc"`
	ContainerStartedUTC string `json:"container_started_utc"`
	Claimant            string `json:"claimant"`
}

func (s WorkerSession) valid() bool {
	return sessionName(s.ID) && identities(s.PodUID, s.ContainerID, s.BootID, s.Claimant) &&
		s.Restart >= 0 && validTime(s.StartedUTC) && validTime(s.ContainerStartedUTC) && s.ContainerStartedUTC <= s.StartedUTC
}

func sessionName(id string) bool {
	return uuid.MatchString(id) || len(id) == 32 && digest.MatchString(id+id)
}

func SessionPath(id string) string { return "witness/" + id + ".jsonl" }

func isSessionPath(path string) bool {
	id, found := strings.CutPrefix(path, "witness/")
	return found && strings.HasSuffix(id, ".jsonl") && sessionName(strings.TrimSuffix(id, ".jsonl"))
}

type Death struct {
	Observer      string `json:"observer"`
	ObservedUTC   string `json:"observed_utc"`
	Receiver      string `json:"receiver"`
	FinalSequence int64  `json:"final_sequence"`
	FinalSHA256   string `json:"final_sha256"`
}

// The list baseline is explicit. A watch update supplies the previously
// observed cursor even when the API's resource versions are opaque.
type WatchRow struct {
	Kind         string           `json:"kind"`
	From         string           `json:"from"`
	Cursor       string           `json:"cursor"`
	Session      *WorkerSession   `json:"session"`
	Death        *Death           `json:"death"`
	Runtime      *RuntimeIdentity `json:"runtime"`
	Registration *Registration    `json:"registration"`
	Exit         *RuntimeExit     `json:"exit,omitempty"`
	Shutdown     *ShutdownBinding `json:"shutdown,omitempty"`
	DeletionUTC  string           `json:"deletion_utc,omitempty"`
}

type RuntimeExit struct {
	FinishedUTC string `json:"finished_utc"`
	Code        int    `json:"code"`
	Signal      int    `json:"signal"`
}

// Corroborates checks the observation order and the kubelet's finish time.
// Kubernetes reports finishedAt at second precision.
func (e RuntimeExit) Corroborates(abortUTC, observedUTC string) bool {
	return abortUTC <= observedUTC && e.FinishedUTC <= observedUTC && abortUTC[:19] <= e.FinishedUTC[:19]
}

type ShutdownBinding struct {
	Invocation     string          `json:"invocation"`
	Attempt        AttemptIdentity `json:"attempt"`
	StartSequence  int64           `json:"start_sequence"`
	ExitSequence   int64           `json:"exit_sequence"`
	SignalSequence int64           `json:"signal_sequence,omitempty"`
	AbortSequence  int64           `json:"abort_sequence,omitempty"`
}

type Registration struct {
	PodUID            string `json:"pod_uid"`
	Claimant          string `json:"claimant"`
	ServiceAccountUID string `json:"service_account_uid"`
	Audience          string `json:"audience"`
	Observer          string `json:"observer"`
}

func (r Registration) matches(s WorkerSession) bool {
	return r.PodUID == s.PodUID && r.Claimant == s.Claimant &&
		identities(r.ServiceAccountUID, r.Observer) && r.Audience == "durable-worker-control"
}

type RuntimeIdentity struct {
	PodUID      string `json:"pod_uid"`
	ContainerID string `json:"container_id"`
	Restart     int64  `json:"restart"`
	StartedUTC  string `json:"started_utc"`
}

func (s WorkerSession) Runtime() RuntimeIdentity {
	return RuntimeIdentity{s.PodUID, s.ContainerID, s.Restart, s.ContainerStartedUTC}
}

func (w WatchRow) valid() bool {
	if !nonempty(w.Cursor) || !publicIdentity(w.From) ||
		(w.Exit != nil) != (w.Kind == "exit") || (w.Shutdown != nil) != (w.Kind == "shutdown") ||
		(w.DeletionUTC != "") != (w.Kind == "termination_requested") {
		return false
	}
	switch w.Kind {
	case "baseline", "bookmark", "gap":
		return w.Session == nil && w.Death == nil && w.Runtime == nil && w.Registration == nil
	case "runtime", "exit", "termination_requested":
		return w.Session == nil && w.Death == nil && w.Runtime != nil && w.Registration == nil &&
			identities(w.Runtime.PodUID, w.Runtime.ContainerID) && w.Runtime.Restart >= 0 && validTime(w.Runtime.StartedUTC) &&
			(w.Exit == nil || validTime(w.Exit.FinishedUTC) && w.Exit.FinishedUTC >= w.Runtime.StartedUTC && w.Exit.Code >= 0 && w.Exit.Signal >= 0) &&
			(w.DeletionUTC == "" || validTime(w.DeletionUTC) && w.DeletionUTC >= w.Runtime.StartedUTC)
	case "session":
		return w.Session != nil && w.Session.valid() && w.Death == nil && w.Runtime == nil &&
			w.Registration != nil && w.Registration.matches(*w.Session)
	case "death":
		return w.Session != nil && w.Session.valid() && w.Death != nil && w.Runtime == nil && w.Registration == nil &&
			identities(w.Death.Observer, w.Death.Receiver) && validTime(w.Death.ObservedUTC) &&
			w.Death.FinalSequence >= 1 && digest.MatchString(w.Death.FinalSHA256)
	case "shutdown":
		return w.Session != nil && w.Session.valid() && w.Runtime == nil && w.Registration == nil && w.Death == nil &&
			nonempty(w.Shutdown.Invocation) && w.Shutdown.Attempt.valid() && w.Shutdown.StartSequence > 1 && w.Shutdown.ExitSequence > 0
	}
	return false
}

type AttemptIdentity struct {
	Workflow  string `json:"workflow"`
	Node      string `json:"node"`
	Iteration int64  `json:"iteration"`
	Attempt   int64  `json:"attempt"`
	ClaimHash string `json:"claim_hash"`
	RequestID string `json:"claim_request_id"`
}

func (a AttemptIdentity) valid() bool {
	return identities(a.Workflow, a.Node, a.RequestID) && a.Iteration >= 0 && a.Attempt > 0 && digest.MatchString(a.ClaimHash)
}

type Traffic struct {
	Method        string      `json:"method"`
	Operation     string      `json:"operation"`
	MessageHash   string      `json:"message_hash"`
	RequestIDHash string      `json:"request_id_hash"`
	KeyHash       string      `json:"key_hash"`
	Intent        string      `json:"intent"`
	ResourceHash  string      `json:"resource_hash"`
	GrantHash     string      `json:"grant_hash"`
	Attempt       int64       `json:"attempt"`
	WorkerBinding string      `json:"worker_binding"`
	Request       WireDigest  `json:"request"`
	Wire          *WireDigest `json:"wire"`
}

func (t Traffic) valid() bool {
	return t.Method == "POST" && slices.Contains([]string{"apply", "lookup", "allocate"}, t.Operation) &&
		hashes(t.MessageHash, t.RequestIDHash, t.KeyHash, t.ResourceHash, t.GrantHash) &&
		(t.WorkerBinding == "" || hashes(t.WorkerBinding)) &&
		uuid.MatchString(t.Intent) && t.Attempt > 0 && t.Request.valid() &&
		(t.Wire == nil || t.Wire.valid() && t.Wire.Length >= t.Request.Length)
}

func (w WireDigest) validBody() bool {
	return w.Length >= 0 && digest.MatchString(w.SHA256) && (w.Length > 0 || w.SHA256 == Hash(nil))
}

type WorkerRow struct {
	Kind         string           `json:"kind"`
	Session      *WorkerSession   `json:"session"`
	Invocation   string           `json:"invocation"`
	Attempt      *AttemptIdentity `json:"attempt"`
	Traffic      *Traffic         `json:"traffic"`
	Response     *WireDigest      `json:"response"`
	ResponseWire *WireDigest      `json:"response_wire"`
}

func (w WorkerRow) valid() bool {
	if w.ResponseWire != nil && (w.Response == nil || !w.ResponseWire.valid() || w.ResponseWire.Length < w.Response.Length) {
		return false
	}
	switch w.Kind {
	case "session":
		return w.Session != nil && w.Session.valid() && w.Invocation == "" && w.Attempt == nil && w.Traffic == nil && w.Response == nil
	case "idle_probe", "lease_renewal":
		return w.Session == nil && w.Invocation == "" && w.Attempt == nil && w.Traffic == nil && w.Response == nil
	case "start", "finish", "abort":
		return w.Session == nil && nonempty(w.Invocation) && w.Attempt != nil && w.Attempt.valid() && w.Traffic == nil && w.Response == nil
	case "send", "receive", "timeout", "close":
		return w.Session == nil && nonempty(w.Invocation) && w.Attempt == nil && w.Traffic != nil && w.Traffic.valid() &&
			(w.Response != nil) == (w.Kind == "receive") && (w.Response == nil || w.Response.validBody())
	}
	return false
}

type IngressRow struct {
	Traffic        Traffic     `json:"traffic"`
	Ordinal        int64       `json:"ordinal"`
	Copy           int64       `json:"copy"`
	PeerHash       string      `json:"peer_hash"`
	ConnectionHash string      `json:"connection_hash"`
	Outcome        string      `json:"outcome"`
	Response       *WireDigest `json:"response"`
	ResponseWire   *WireDigest `json:"response_wire"`
	ApplicationID  string      `json:"application_id"`
	Acknowledged   bool        `json:"acknowledged"`
}

func (r IngressRow) Valid() bool {
	if !r.Traffic.valid() || r.Ordinal <= 0 || r.Copy <= 0 || !hashes(r.PeerHash, r.Traffic.WorkerBinding) ||
		(r.ConnectionHash == "" && r.Outcome != "denied" && r.Outcome != "timeout" && r.Outcome != "close") ||
		(r.ConnectionHash != "" && !hashes(r.ConnectionHash)) ||
		(r.ApplicationID != "" && !uuid.MatchString(r.ApplicationID)) {
		return false
	}
	switch r.Outcome {
	case "delivered", "duplicate_reply", "denied", "withheld":
		return r.Response != nil && r.Response.validBody() &&
			(r.ResponseWire == nil || r.ResponseWire.valid() && r.ResponseWire.Length >= r.Response.Length)
	case "close", "timeout":
		return r.Response == nil && r.ResponseWire == nil
	}
	return false
}

type DecisionRow struct {
	Intent            string `json:"intent"`
	ProposalHash      string `json:"proposal_hash"`
	GrantHash         string `json:"grant_hash"`
	Subject           string `json:"subject"`
	ServiceAccountUID string `json:"service_account_uid"`
	BoundUID          string `json:"bound_uid"`
	IssuerHash        string `json:"issuer_hash"`
	Audience          string `json:"audience"`
	Outcome           string `json:"outcome"`
	DecidedUTC        string `json:"decided_utc"`
}

func (d DecisionRow) Valid() bool {
	return uuid.MatchString(d.Intent) && hashes(d.ProposalHash, d.IssuerHash) &&
		identities(d.Subject, d.ServiceAccountUID, d.BoundUID) && d.Audience == "dur053-k6-approval" &&
		(validTime(d.DecidedUTC) && (d.Outcome == "APPROVED" && (d.GrantHash == "" || hashes(d.GrantHash)) ||
			d.Outcome == "REJECTED" && d.GrantHash == "") || d.Outcome == "PENDING" && d.DecidedUTC == "" && d.GrantHash == "")
}

func identities(values ...string) bool {
	for _, value := range values {
		if !nonempty(value) {
			return false
		}
	}
	return true
}

func hashes(values ...string) bool {
	for _, value := range values {
		if !digest.MatchString(value) {
			return false
		}
	}
	return true
}

type External struct {
	Roster       Stream[WatchRow]
	Sessions     map[string]Stream[WorkerRow]
	Ingress      *Stream[IngressRow]
	Decisions    *Stream[DecisionRow]
	Broker       *Stream[BrokerRow]
	BrokerCopies *Stream[BrokerCopyRow]
	Topology     *Stream[TopologyRow]
}

func (e External) Files() (map[string][]byte, error) {
	out := map[string][]byte{}
	add := func(path string, raw []byte, err error) error {
		if err == nil {
			out[path] = raw
		}
		return err
	}
	var problems []error
	if e.Roster.Header.Source != "" {
		raw, err := Canonical(e.Roster)
		problems = append(problems, add(RosterPath, raw, err))
	}
	for id, s := range e.Sessions {
		raw, err := EncodeStream(s)
		problems = append(problems, add(SessionPath(id), raw, err))
	}
	if e.Ingress != nil {
		raw, err := EncodeStream(*e.Ingress)
		problems = append(problems, add(IngressPath, raw, err))
	}
	if e.Decisions != nil {
		raw, err := EncodeStream(*e.Decisions)
		problems = append(problems, add(DecisionsPath, raw, err))
	}
	if e.Broker != nil {
		raw, err := EncodeStream(*e.Broker)
		problems = append(problems, add(BrokerPath, raw, err))
	}
	if e.BrokerCopies != nil {
		raw, err := EncodeStream(*e.BrokerCopies)
		problems = append(problems, add(BrokerCopiesPath, raw, err))
	}
	if e.Topology != nil {
		raw, err := Canonical(e.Topology)
		problems = append(problems, add(TopologyPath, raw, err))
	}
	return out, errors.Join(problems...)
}

type externalRead func(string) ([]byte, *Finding)

func readExternalStream[T any](read externalRead, m Manifest, path, schema string, valid func(T) bool, issues *[]Finding) *Stream[T] {
	raw, issue := read(path)
	if issue != nil {
		*issues = append(*issues, *issue)
		return nil
	}
	var s Stream[T]
	if !decodeStream(raw, strings.HasSuffix(path, ".jsonl"), &s) {
		*issues = append(*issues, *integrity("EXTERNAL_SCHEMA", path))
		return nil
	}
	if code := s.verify(m, schema, valid); code != "" {
		*issues = append(*issues, *integrity(code, path))
		return nil
	}
	return &s
}

// ReadExternal reads a single collector frame. Bundle verification supplies
// an indexed reader instead and applies the same decoders.
func ReadExternal(fsys fs.FS, m Manifest) (External, []Finding) {
	files := workerFiles(fsys)
	if _, err := fs.Stat(fsys, BrokerCopiesPath); err == nil {
		files = append(files, File{Path: BrokerCopiesPath})
	}
	return readExternal(externalFSReader(fsys), m, files)
}

func workerFiles(fsys fs.FS) []File {
	var inventory []File
	entries, _ := fs.ReadDir(fsys, "witness")
	for _, entry := range entries {
		inventory = append(inventory, File{Path: "witness/" + entry.Name()})
	}
	return inventory
}

func externalFSReader(fsys fs.FS) externalRead {
	return func(path string) ([]byte, *Finding) {
		raw, err := fs.ReadFile(fsys, path)
		if err != nil {
			return nil, missing(path)
		}
		return raw, nil
	}
}

func ReadWorkerLedgers(fsys fs.FS, m Manifest, roster Stream[WatchRow]) (map[string]Stream[WorkerRow], []Finding) {
	var issues []Finding
	ledgers := readLedgers(externalFSReader(fsys), m, roster, workerFiles(fsys), &issues)
	return ledgers, issues
}

func ReadDecisionStream(fsys fs.FS, m Manifest, path string) (*Stream[DecisionRow], []Finding) {
	var issues []Finding
	s := readExternalStream(externalFSReader(fsys), m, path, "decisions", DecisionRow.Valid, &issues)
	return s, issues
}

func ReadAllocatorStream(fsys fs.FS, m Manifest, path string) (*Stream[IngressRow], []Finding) {
	var issues []Finding
	s := readExternalStream(externalFSReader(fsys), m, path, "allocator", func(row IngressRow) bool {
		return row.Valid() && row.Traffic.Operation == "allocate"
	}, &issues)
	return s, issues
}

func readExternal(read externalRead, m Manifest, indexed []File) (External, []Finding) {
	e := External{}
	var issues []Finding
	if roster := readExternalStream(read, m, RosterPath, "roster", WatchRow.valid, &issues); roster != nil {
		e.Roster = *roster
	}
	e.Sessions = readLedgers(read, m, e.Roster, indexed, &issues)
	e.Broker = readExternalStream(read, m, BrokerPath, "broker", func(b BrokerRow) bool {
		return b.valid() && (b.OffsetPhase == nil || b.OffsetPhase.Valid(m))
	}, &issues)
	if slices.ContainsFunc(indexed, func(f File) bool { return f.Path == BrokerCopiesPath }) {
		e.BrokerCopies = readExternalStream(read, m, BrokerCopiesPath, "broker-copies", BrokerCopyRow.valid, &issues)
	}
	if m.Sink == nil {
		e.Topology = readExternalStream(read, m, TopologyPath, "topology", TopologyRow.valid, &issues)
	} else {
		e.Ingress = readExternalStream(read, m, IngressPath, "ingress", IngressRow.Valid, &issues)
		e.Decisions = readExternalStream(read, m, DecisionsPath, "decisions", DecisionRow.Valid, &issues)
	}
	return e, issues
}

func readLedgers(read externalRead, m Manifest, roster Stream[WatchRow], indexed []File, issues *[]Finding) map[string]Stream[WorkerRow] {
	ledgers := map[string]Stream[WorkerRow]{}
	ids := map[string]bool{}
	for _, row := range roster.Rows {
		if row.Data.Session != nil {
			ids[row.Data.Session.ID] = true
		}
	}
	for _, f := range indexed {
		if isSessionPath(f.Path) {
			ids[strings.TrimSuffix(strings.TrimPrefix(f.Path, "witness/"), ".jsonl")] = true
		}
	}
	for _, id := range sortedKeys(ids) {
		if session := readExternalStream(read, m, SessionPath(id), "worker", WorkerRow.valid, issues); session != nil {
			ledgers[id] = *session
		}
	}
	return ledgers
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}
