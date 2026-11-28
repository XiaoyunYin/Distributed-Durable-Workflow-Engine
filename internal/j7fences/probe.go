package j7fences

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"durable-agent-execution-engine/internal/restoreledger"
	"durable-agent-execution-engine/internal/state"
)

const (
	ownerReason = "J7_OWNER_PROBE"
	probeLease  = 15 * time.Second
	workGate    = 480 * time.Second
)

type Journal interface {
	Header() restoreledger.Header
	NowNS() (int64, error)
	RecordSend(restoreledger.Operation) (int64, error)
	RecordOutcome(restoreledger.Outcome) (int64, error)
	RecordWitness(restoreledger.Witness) error
}

// SaveEvidence writes and fsyncs a private evidence artifact before returning.
type SaveEvidence func(name string, body []byte) error

type repository interface {
	AcquireLease(context.Context, int16, string, time.Duration) (state.Lease, bool, error)
	ReleaseLease(context.Context, state.LeaseRef) error
	ApplyOwnerTransition(context.Context, state.OwnerTransitionInput) error
}

type Probe struct {
	journal Journal
	save    SaveEvidence
	repo    repository
	read    readState
}

// New uses the supplied connection for ordinary engine operations and
// independent SQL observations. Use the controller's sole live ledger writer.
func New(store *state.Store, journal Journal, save SaveEvidence) (*Probe, error) {
	if store == nil || journal == nil || save == nil {
		return nil, errors.New("fence probes require a store, live ledger and evidence writer")
	}
	return &Probe{journal: journal, save: save, repo: store, read: readPostgres(store, journal.NowNS)}, nil
}

type SourceOwner struct {
	Authority string    `json:"authority"`
	BootID    string    `json:"boot_id"`
	Database  Database  `json:"database"`
	Scope     Scope     `json:"scope"`
	Lease     Lease     `json:"lease"`
	DBTime    time.Time `json:"database_time"`
	StartedNS int64     `json:"started_ns"`
	EndedNS   int64     `json:"completed_ns"`
	Evidence  string    `json:"sample_sha256"`
}

type SourceClaim struct {
	Authority string    `json:"authority"`
	BootID    string    `json:"boot_id"`
	Database  Database  `json:"database"`
	Scope     Scope     `json:"scope"`
	Attempt   Attempt   `json:"attempt"`
	DBTime    time.Time `json:"database_time"`
	StartedNS int64     `json:"started_ns"`
	EndedNS   int64     `json:"completed_ns"`
	Evidence  string    `json:"sample_sha256"`
}

// Target binds the restored database identity and clocks from prior evidence.
type Target struct {
	Database Database `json:"database"`
	CutNS    int64    `json:"archive_cut_started_ns"`
	T0NS     int64    `json:"t0_ns"`
}

func (p *Probe) CaptureSourceOwner(ctx context.Context, scope Scope, db Database) (SourceOwner, error) {
	var result SourceOwner
	s, err := p.observe(ctx, scope, db, true, true)
	if err != nil {
		return result, err
	}
	w, _ := s.workflow()
	l, err := s.lease()
	if err != nil || l.Owner == "" || l.Epoch <= 0 || !l.Expires.After(s.DBTime) || w.State != state.StateRunnable {
		return result, errors.New("source L1 must be runnable under a live held owner")
	}
	h := p.journal.Header()
	ref := l.ref(scope.Partition)
	sha, err := p.witness("j7-source-owner", h.Source, "", s, &ref, nil)
	if err != nil {
		return result, err
	}
	return SourceOwner{h.Source, h.BootID, db, scope, l, s.DBTime, s.StartedNS, s.CompletedNS, sha}, nil
}

func (p *Probe) CaptureSourceClaim(ctx context.Context, scope Scope, db Database) (SourceClaim, error) {
	var result SourceClaim
	s, err := p.observe(ctx, scope, db, false, true)
	if err != nil {
		return result, err
	}
	w, _ := s.workflow()
	a, err := liveClaim(s, 1)
	if err != nil || w.State != state.StateWaitingActivity {
		return result, errors.New("source F1 must hold a live first pure claim")
	}
	h := p.journal.Header()
	ref := a.ref()
	sha, err := p.witness("j7-source-claim", h.Source, "", s, nil, &ref)
	if err != nil {
		return result, err
	}
	return SourceClaim{h.Source, h.BootID, db, scope, a, s.DBTime, s.StartedNS, s.CompletedNS, sha}, nil
}

func (p *Probe) observe(ctx context.Context, scope Scope, db Database, lease, clock bool) (sample, error) {
	s, err := p.read(ctx, scope, lease)
	if err != nil {
		return s, err
	}
	if err := checkSample(s, scope, db); err != nil {
		return s, err
	}
	if clock {
		err = checkClock(s)
	}
	return s, err
}

func (p *Probe) wait(ctx context.Context, scope Scope, db Database, lease bool,
	ready func(sample) (bool, error)) (sample, error) {
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return sample{}, err
		}
		s, err := p.observe(ctx, scope, db, lease, false)
		if err != nil {
			return s, err
		}
		ok, err := ready(s)
		if err != nil || ok {
			return s, errors.Join(err, ctx.Err())
		}
		select {
		case <-ctx.Done():
			return s, ctx.Err()
		case <-tick.C:
		}
	}
}

func (p *Probe) witness(name, authority, requestID string, s sample, l *restoreledger.LeaseRef,
	a *restoreledger.ClaimRef) (string, error) {
	raw, err := json.Marshal(s)
	if err != nil {
		return "", err
	}
	raw = append(raw, '\n')
	if err := p.save(name+".json", raw); err != nil {
		return "", err
	}
	sha := restoreledger.SHA256(raw)
	err = p.journal.RecordWitness(restoreledger.Witness{
		Kind: restoreledger.StateProbe, Phase: name, Authority: authority, RequestID: requestID,
		StartedNS: s.StartedNS, CompletedNS: s.CompletedNS, EvidenceSHA: sha, Timeline: s.Database.Timeline,
		Lease: l, Claim: a,
	})
	return sha, err
}

func (p *Probe) targetContext(ctx context.Context, original Database, authority, boot, sha string,
	from, to int64, target Target) (context.Context, context.CancelFunc, error) {
	h := p.journal.Header()
	now, err := p.journal.NowNS()
	if err != nil {
		return nil, nil, err
	}
	if authority != h.Source || boot != h.BootID || !restoreledger.IsSHA256(sha) ||
		from <= 0 || to < from || to-from > int64(maxClockBracket) ||
		to > target.CutNS || target.CutNS <= 0 || target.CutNS >= target.T0NS ||
		now < target.T0NS || now-target.T0NS >= int64(workGate) ||
		original.SystemID == 0 || target.Database.SystemID != original.SystemID ||
		target.Database.Timeline <= original.Timeline || original.Timeline == 0 {
		return nil, nil, errors.New("source witness, restored database or terminal deadline is unbound")
	}
	bounded, stop := context.WithTimeout(ctx, workGate-time.Duration(now-target.T0NS))
	return bounded, stop, nil
}

type operation struct {
	journal Journal
	start   int64
	id      string
}

func (p *Probe) send(op restoreledger.Operation, payload []byte) (operation, error) {
	op.Authority = p.journal.Header().Target
	op.RequestSHA256 = restoreledger.SHA256(payload)
	if _, err := p.journal.RecordSend(op); err != nil {
		return operation{}, err
	}
	start, err := p.journal.NowNS()
	return operation{p.journal, start, op.RequestID}, err
}

func (o operation) outcome(result restoreledger.Outcome, body []byte) error {
	result.RequestID, result.SentNS = o.id, o.start
	if body != nil {
		at, err := o.journal.NowNS()
		if err != nil {
			return err
		}
		result.CompletedNS, result.ResponseSHA256 = at, restoreledger.SHA256(body)
	}
	_, err := o.journal.RecordOutcome(result)
	return err
}

func (o operation) repositoryOutcome(result restoreledger.Outcome, body any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	return o.outcome(result, raw)
}

func stable(before, after sample) error {
	a, err := json.Marshal(before.Rows)
	if err != nil {
		return err
	}
	b, err := json.Marshal(after.Rows)
	if err != nil {
		return err
	}
	if before.Scope != after.Scope || before.Database != after.Database ||
		after.StartedNS < before.CompletedNS || !bytes.Equal(a, b) {
		return errors.New("database state or outbox changed across the rejected operation")
	}
	return nil
}

func resultEqual(got, want json.RawMessage) bool {
	// jsonb returns PostgreSQL's key order, while the HTTP response or fixture
	// can use another. Use the existing canonical payload hashing contract.
	a, err := state.CanonicalPayloadHash(got)
	b, otherErr := state.CanonicalPayloadHash(want)
	return err == nil && otherErr == nil && a == b
}

func liveClaim(s sample, attempt int64) (Attempt, error) {
	n, err := s.node()
	if err != nil || n.Current != attempt {
		return Attempt{}, errors.New("held claim is not the workflow's current attempt")
	}
	rows, err := s.attempts()
	if err != nil {
		return Attempt{}, err
	}
	for _, a := range rows {
		if a.Node == s.Scope.Node && a.Iteration == s.Scope.Iteration && a.Number == attempt &&
			a.Current && a.State == state.AttemptClaimed && a.Class == state.EffectPure &&
			a.Deadline.After(s.DBTime) && restoreledger.IsSHA256(a.TokenSHA) && a.Worker != "" && a.Request != "" &&
			a.Outcome == "NONE" && !hasResult(a.Result) {
			return a, nil
		}
	}
	return Attempt{}, fmt.Errorf("attempt %d does not have a live pure claim", attempt)
}

func hasResult(raw json.RawMessage) bool { return len(raw) != 0 && string(raw) != "null" }
