package j7fences

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"durable-agent-execution-engine/internal/restoreledger"
	"durable-agent-execution-engine/internal/state"
)

type OwnerEvidence struct {
	Source           SourceOwner            `json:"source"`
	Target           Target                 `json:"target"`
	Successor        restoreledger.LeaseRef `json:"successor"`
	ExpirySHA        string                 `json:"restored_expiry_sha256,omitempty"`
	BeforeSHA        string                 `json:"before_denial_sha256,omitempty"`
	AfterSHA         string                 `json:"after_denial_sha256,omitempty"`
	FreshSHA         string                 `json:"fresh_write_sha256,omitempty"`
	ReleasedSHA      string                 `json:"released_lease_sha256,omitempty"`
	FreshRevision    int64                  `json:"fresh_revision,omitempty"`
	ReleaseCheckedNS int64                  `json:"release_checked_ns,omitempty"`
	TerminalSHA      string                 `json:"terminal_sha256,omitempty"`
}

func stateRef(l restoreledger.LeaseRef) state.LeaseRef {
	return state.LeaseRef{PartitionID: l.PartitionID, OwnerID: l.OwnerID, Epoch: l.Epoch}
}

func payload(v any) []byte {
	// Probe repository requests are fixed JSON-compatible structs.
	raw, _ := json.Marshal(v)
	return raw
}

// RunOwner requires L1 scheduler admission to stay held through release. It
// submits one matched fenced write with each lease and never changes a deadline.
func (p *Probe) RunOwner(ctx context.Context, source SourceOwner, target Target) (out OwnerEvidence, runErr error) {
	out.Source, out.Target = source, target
	ctx, stop, err := p.targetContext(ctx, source.Database, source.Authority, source.BootID, source.Evidence,
		source.StartedNS, source.EndedNS, target)
	if err != nil {
		return out, err
	}
	defer stop()
	if source.Lease.Owner == "" || source.Lease.Epoch <= 0 || source.DBTime.IsZero() ||
		!source.Lease.Expires.After(source.DBTime) {
		return out, errors.New("original L1 lease lacks a live source-clock witness")
	}
	scope, db := source.Scope, target.Database
	old := source.Lease.ref(scope.Partition)
	authority := p.journal.Header().Target
	expired, err := p.wait(ctx, scope, db, true, func(s sample) (bool, error) {
		if err := checkClock(s); err != nil {
			return false, err
		}
		lease, err := s.lease()
		w, _ := s.workflow()
		if err != nil || lease.ref(scope.Partition) != old || lease.Expires.Before(source.Lease.Expires) ||
			w.State != state.StateRunnable {
			return false, errors.New("restored L1 changed before the archived lease expired")
		}
		return !lease.Expires.After(s.DBTime), nil
	})
	if err != nil {
		return out, err
	}
	out.ExpirySHA, err = p.witness("j7-owner-expiry", authority, "", expired, &old, nil)
	if err != nil {
		return out, err
	}
	ownerID := state.NewID()
	acquireBody := struct {
		Partition int16  `json:"partition_id"`
		Owner     string `json:"owner_id"`
		TTLNS     int64  `json:"ttl_ns"`
	}{scope.Partition, ownerID, int64(probeLease)}
	op, err := p.send(restoreledger.Operation{RequestID: "j7-owner-acquire", Kind: restoreledger.AcquireLease,
		Phase: "j7-owner-fence", WorkflowID: scope.Workflow}, payload(acquireBody))
	if err != nil {
		return out, err
	}
	acquired, ok, err := p.repo.AcquireLease(ctx, scope.Partition, ownerID, probeLease)
	if err != nil {
		return out, errors.Join(err, op.outcome(restoreledger.Outcome{Disposition: restoreledger.Ambiguous, Code: "LEASE_ACQUIRE_UNCERTAIN"}, nil))
	}
	fresh := restoreledger.LeaseRef{PartitionID: acquired.PartitionID, OwnerID: acquired.OwnerID, Epoch: acquired.Epoch}
	if !ok {
		err := op.repositoryOutcome(restoreledger.Outcome{Disposition: restoreledger.Rejected, Code: "LEASE_HELD"}, acquired)
		return out, errors.Join(errors.New("another owner acquired the restored L1 partition"), err)
	}
	out.Successor = fresh
	released := false
	defer func() {
		if released {
			return
		}
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()
		runErr = errors.Join(runErr, p.release(cleanup, scope.Workflow, fresh))
	}()
	if err := op.repositoryOutcome(restoreledger.Outcome{Disposition: restoreledger.Acknowledged,
		DurableID: scope.Workflow, Lease: &fresh}, acquired); err != nil {
		return out, err
	}
	if fresh.OwnerID != ownerID || fresh.OwnerID == old.OwnerID || fresh.Epoch <= old.Epoch ||
		fresh.PartitionID != scope.Partition {
		return out, errors.New("restored lease did not fence the original owner")
	}
	before, err := p.observe(ctx, scope, db, true, true)
	if err != nil {
		return out, err
	}
	w, _ := before.workflow()
	live, err := before.lease()
	if err != nil || live.ref(scope.Partition) != fresh || !live.Expires.Equal(acquired.LeaseExpiresAt) ||
		!live.Expires.After(before.DBTime) || w.State != state.StateRunnable {
		return out, errors.New("successor lease or L1 revision is not live for the challenge")
	}
	out.BeforeSHA, err = p.witness("j7-owner-before", authority, "", before, &fresh, nil)
	if err != nil {
		return out, err
	}
	input := state.OwnerTransitionInput{Lease: stateRef(old), WorkflowID: scope.Workflow, ExpectedRevision: w.Revision,
		NewState: state.StateRunnable, ActorID: "j7-owner-probe", Reason: ownerReason}
	denialErr := p.ownerWrite(ctx, input, old, "j7-owner-stale", true)
	after, err := p.observe(ctx, scope, db, true, true)
	if err != nil {
		return out, errors.Join(denialErr, err)
	}
	out.AfterSHA, err = p.witness("j7-owner-after-denial", authority, "j7-owner-stale", after, &fresh, nil)
	if err := errors.Join(denialErr, err, stable(before, after)); err != nil {
		return out, err
	}
	if !live.Expires.After(after.DBTime) {
		return out, errors.New("successor lease expired before its positive control")
	}
	input.Lease = stateRef(fresh)
	if err := p.ownerWrite(ctx, input, fresh, "j7-owner-fresh", false); err != nil {
		return out, err
	}
	committed, err := p.observe(ctx, scope, db, true, false)
	if err != nil {
		return out, err
	}
	out.FreshSHA, err = p.witness("j7-owner-committed", authority, "j7-owner-fresh", committed, &fresh, nil)
	if err != nil {
		return out, err
	}
	out.FreshRevision = w.Revision + 1
	if err := verifyOwnerCommit(before, committed, input, fresh); err != nil {
		return out, err
	}
	// A release is attempted once, even on an uncertain response.
	released = true
	if err := p.release(ctx, scope.Workflow, fresh); err != nil {
		return out, err
	}
	check, err := p.observe(ctx, scope, db, true, false)
	if err != nil {
		return out, err
	}
	out.ReleasedSHA, err = p.witness("j7-owner-released", authority, "j7-owner-release", check, &fresh, nil)
	if err != nil {
		return out, err
	}
	lease, err := check.lease()
	revision, _ := check.workflow()
	if err != nil || lease.Owner != "" || lease.Epoch != fresh.Epoch ||
		revision.State != state.StateRunnable || revision.Revision != out.FreshRevision {
		return out, errors.New("L1 was not left runnable with the successor lease released")
	}
	out.ReleaseCheckedNS = check.CompletedNS
	return out, nil
}

func (p *Probe) ownerWrite(ctx context.Context, input state.OwnerTransitionInput, ref restoreledger.LeaseRef,
	id string, stale bool) error {
	op, err := p.send(restoreledger.Operation{RequestID: id, Kind: restoreledger.OwnerWrite,
		Phase: "j7-owner-fence", WorkflowID: input.WorkflowID, Lease: &ref}, payload(input))
	if err != nil {
		return err
	}
	err = p.repo.ApplyOwnerTransition(ctx, input)
	switch {
	case errors.Is(err, state.ErrLeaseNotOwned):
		logErr := op.repositoryOutcome(restoreledger.Outcome{Disposition: restoreledger.Rejected, Code: "LEASE_NOT_OWNED"},
			map[string]string{"code": "LEASE_NOT_OWNED"})
		if stale {
			return logErr
		}
		return errors.Join(err, logErr)
	case err != nil:
		return errors.Join(err, op.outcome(restoreledger.Outcome{Disposition: restoreledger.Ambiguous, Code: "OWNER_WRITE_UNPROVED"}, nil))
	default:
		logErr := op.repositoryOutcome(restoreledger.Outcome{Disposition: restoreledger.Acknowledged, DurableID: input.WorkflowID},
			struct {
				Workflow string
				Revision int64
			}{input.WorkflowID, input.ExpectedRevision + 1})
		if stale {
			return errors.Join(errors.New("archived owner successfully mutated the restored workflow"), logErr)
		}
		return logErr
	}
}

func (p *Probe) release(ctx context.Context, workflow string, ref restoreledger.LeaseRef) error {
	op, err := p.send(restoreledger.Operation{RequestID: "j7-owner-release", Kind: restoreledger.ReleaseLease,
		Phase: "j7-owner-fence", WorkflowID: workflow, Lease: &ref}, payload(ref))
	if err != nil {
		return err
	}
	if err := p.repo.ReleaseLease(ctx, stateRef(ref)); err != nil {
		return errors.Join(err, op.outcome(restoreledger.Outcome{Disposition: restoreledger.Ambiguous, Code: "LEASE_RELEASE_UNPROVED"}, nil))
	}
	return op.repositoryOutcome(restoreledger.Outcome{Disposition: restoreledger.Acknowledged, DurableID: workflow},
		map[string]string{"code": "RELEASED"})
}

func verifyOwnerCommit(before, after sample, input state.OwnerTransitionInput, fresh restoreledger.LeaseRef) error {
	w, err := after.workflow()
	l, leaseErr := after.lease()
	rows, historyErr := historyRows(after)
	previous, previousErr := historyRows(before)
	if err != nil || leaseErr != nil || historyErr != nil || previousErr != nil ||
		l.ref(after.Scope.Partition) != fresh || w.Revision != input.ExpectedRevision+1 || w.State != input.NewState ||
		len(rows) != len(previous)+1 {
		return errors.New("valid owner did not commit exactly the intended workflow transition")
	}
	entry := rows[len(rows)-1]
	if entry.Revision != w.Revision || entry.ActorKind != "scheduler" || entry.Actor != input.ActorID ||
		entry.Epoch != fresh.Epoch || entry.Reason != input.Reason || entry.NewState != input.NewState {
		return errors.New("positive owner history does not match the fresh lease and request")
	}
	for _, relation := range []string{"attempts", "outbox", "publications", "lease", "lease_acquisitions", "result_evidence"} {
		if string(before.Rows[relation]) != string(after.Rows[relation]) {
			return fmt.Errorf("positive owner probe unexpectedly changed %s", relation)
		}
	}
	return nil
}

func historyRows(s sample) ([]history, error) {
	var h []history
	err := json.Unmarshal(s.Rows["history"], &h)
	return h, err
}

// FinishOwner runs after scheduler admission and joins a successful L1
// execution to the previously released positive-control lease.
func (p *Probe) FinishOwner(ctx context.Context, out OwnerEvidence) (OwnerEvidence, error) {
	if out.ReleaseCheckedNS <= 0 || out.FreshRevision <= 0 || !restoreledger.IsSHA256(out.ReleasedSHA) {
		return out, errors.New("L1 terminal observation requires a verified release")
	}
	src := out.Source
	ctx, stop, err := p.targetContext(ctx, src.Database, src.Authority, src.BootID, src.Evidence, src.StartedNS, src.EndedNS, out.Target)
	if err != nil {
		return out, err
	}
	defer stop()
	s, err := p.wait(ctx, src.Scope, out.Target.Database, false, func(s sample) (bool, error) {
		w, _ := s.workflow()
		if w.State == state.StateSucceeded {
			h, err := historyRows(s)
			if err == nil && len(h) > 0 {
				last := h[len(h)-1]
				if last.NewState == state.StateSucceeded && last.Revision == w.Revision &&
					last.Revision > out.FreshRevision && last.ActorKind == "scheduler" && last.Epoch > out.Successor.Epoch &&
					s.StartedNS >= out.ReleaseCheckedNS {
					return true, nil
				}
			}
			return false, errors.New("L1 success was not driven by a later scheduler after lease release")
		}
		return false, checkPending(w)
	})
	if err != nil {
		return out, err
	}
	out.TerminalSHA, err = p.witness("j7-owner-terminal", p.journal.Header().Target, "j7-owner-release", s, nil, nil)
	return out, err
}

func checkPending(w Workflow) error {
	switch w.State {
	case state.StateRunnable, state.StateWaitingActivity, state.StateWaitingTimer:
		return nil
	default:
		return fmt.Errorf("probe workflow %s reached unexpected state %s", w.ID, w.State)
	}
}
