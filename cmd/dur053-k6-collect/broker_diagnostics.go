package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"time"

	"durable-agent-execution-engine/internal/invariants/k6collect"
	"durable-agent-execution-engine/internal/invariants/k6v1"
)

const (
	brokerDiagnosticFile  = "broker-snapshot-diagnostics.json"
	brokerDiagnosticCount = 2
	brokerDiagnosticBytes = 16 << 10
	// The worker's Kafka group session timeout is 10 seconds. Allow one
	// expiry window for departures after drain, within Phase G's 30 s wait.
	drainConvergenceWait = 10 * time.Second
)

var errBrokerDiagnostics = errors.New("K6_COLLECT_BROKER_DIAGNOSTICS")

type diagnosticMemberRead struct {
	Bytes       []byte          `json:"bytes"`
	SHA256      string          `json:"sha256,omitempty"`
	Size        int             `json:"size"`
	Status      string          `json:"status"`
	Coordinator json.RawMessage `json:"coordinator"`
	usable      bool
	empty       bool
}

type brokerAttempt struct {
	AtUTC       string                 `json:"at_utc"`
	MemberReads []diagnosticMemberRead `json:"member_file_reads"`
	Before      json.RawMessage        `json:"before"`
	After       json.RawMessage        `json:"after"`
}

type brokerErrorHistory struct {
	Count int             `json:"count"`
	First []brokerAttempt `json:"first"`
	Last  []brokerAttempt `json:"last"`
}

type brokerHistory map[string]*brokerErrorHistory

func boundedView(value any) json.RawMessage {
	raw, err := json.Marshal(value)
	if err != nil || len(raw) > brokerDiagnosticBytes {
		return json.RawMessage(`{"truncated":true}`)
	}
	return raw
}

func (h brokerHistory) retain(path, code string, sample brokerAttempt) error {
	entry := h[code]
	if entry == nil {
		entry = &brokerErrorHistory{First: []brokerAttempt{}, Last: []brokerAttempt{}}
		h[code] = entry
	}
	entry.Count++
	if len(entry.First) < brokerDiagnosticCount {
		entry.First = append(entry.First, sample)
	} else if len(entry.Last) < brokerDiagnosticCount {
		entry.Last = append(entry.Last, sample)
	} else {
		copy(entry.Last, entry.Last[1:])
		entry.Last[len(entry.Last)-1] = sample
	}
	raw, err := k6v1.Canonical(h)
	if err != nil {
		return errBrokerDiagnostics
	}
	tmp := path + ".tmp"
	if os.WriteFile(tmp, raw, 0o600) != nil || os.Rename(tmp, path) != nil {
		return errBrokerDiagnostics
	}
	return nil
}

func readMembers(path string, attempt *brokerAttempt) k6collect.MemberGenerations {
	return func(context.Context) (int64, map[string]int64, *k6v1.CoordinatorObservation, error) {
		var state struct {
			Generation  int64                        `json:"generation"`
			Members     map[string]int64             `json:"members"`
			Coordinator *k6v1.CoordinatorObservation `json:"coordinator,omitempty"`
		}
		raw, err := os.ReadFile(path)
		diagnostic := diagnosticMemberRead{Status: "unavailable"}
		if err == nil {
			diagnostic.Bytes = bytes.Clone(raw[:min(len(raw), brokerDiagnosticBytes)])
			diagnostic.SHA256 = k6v1.Hash(raw)
			diagnostic.Size = len(raw)
			err = k6v1.DecodeStrict(raw, &state)
			diagnostic.Status = "invalid"
			if err == nil {
				diagnostic.Status = "read"
				diagnostic.Coordinator = boundedView(state.Coordinator)
				diagnostic.usable = state.Generation >= 0 && state.Members != nil
				diagnostic.empty = len(state.Members) == 0
			}
		}
		attempt.MemberReads = append(attempt.MemberReads, diagnostic)
		if err != nil {
			err = errors.New("K6_COLLECT_INPUT")
		}
		return state.Generation, state.Members, state.Coordinator, err
	}
}

func (o *collector) snapshotBroker(ctx context.Context, client k6collect.BrokerClient, ids map[string]string) (k6v1.BrokerRow, bool, error) {
	attempt := brokerAttempt{AtUTC: k6v1.UTC(time.Now()), MemberReads: []diagnosticMemberRead{}}
	var views k6collect.BrokerSnapshotViews
	row, err := k6collect.BrokerSnapshot(ctx, client, o.config.Group, ids, readMembers(o.config.Members, &attempt), &views)
	// Recheck at failure time so a bracket that straddles the fence is retained.
	if err == nil || !o.drained() {
		return row, false, err
	}
	attempt.Before, attempt.After = boundedView(views.Before), boundedView(views.After)
	if o.brokerFailures == nil {
		o.brokerFailures = brokerHistory{}
	}
	path := filepath.Join(filepath.Dir(o.config.Output), brokerDiagnosticFile)
	if writeErr := o.brokerFailures.retain(path, err.Error(), attempt); writeErr != nil {
		return row, false, writeErr
	}
	return row, maps.Equal(ids, o.config.Topics) && departureMismatch(err, views, attempt.MemberReads), err
}

func departureMismatch(err error, views k6collect.BrokerSnapshotViews, reads []diagnosticMemberRead) bool {
	switch err.Error() {
	case "K6_BROKER_GENERATION_RACED", "K6_BROKER_GENERATION_UNPROVED",
		"K6_BROKER_MEMBERS_UNJOINED", "K6_BROKER_ASSIGNMENTS_UNCORROBORATED":
		// These errors follow two matching successful descriptions. A missing
		// or malformed callback file cannot buy a convergence window.
		return views.After != nil && len(views.After.Groups) == 1 &&
			len(reads) == 2 && reads[0].usable && reads[1].usable &&
			(reads[0].empty || reads[1].empty || views.After.Groups[0].State == "Empty" ||
				err.Error() == "K6_BROKER_MEMBERS_UNJOINED")
	default:
		return false
	}
}

type drainMembershipPending struct{ error }

type collectionFailures struct {
	first      time.Time
	retryUntil time.Time
	last       error
}

func (f *collectionFailures) shouldExit(at time.Time, published, drained bool, err error) bool {
	if err == nil {
		*f = collectionFailures{}
		return false
	}
	f.last = err
	if f.first.IsZero() {
		f.first = at
	}
	if !published || !drained {
		return false
	}
	var pending drainMembershipPending
	if errors.As(err, &pending) {
		if f.retryUntil.IsZero() {
			f.retryUntil = at.Add(drainConvergenceWait)
		}
		return !at.Before(f.retryUntil)
	}
	return at.Sub(f.first) > k6v1.MaxSampleGap
}
