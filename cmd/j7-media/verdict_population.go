package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/invariants"
	"durable-agent-execution-engine/internal/restoreledger"
)

func (d drillInputs) exchanges(v *drillVerdict) (map[string]restoreledger.Exchange, error) {
	byID := map[string]restoreledger.Exchange{}
	for _, x := range d.ledger.Exchanges {
		if x.Disposition() != restoreledger.Acknowledged && x.Disposition() != restoreledger.Rejected {
			v.Ambiguous = append(v.Ambiguous, x.Operation.RequestID)
		}
		byID[x.Operation.RequestID] = x
	}
	if len(v.Ambiguous) != 0 {
		return byID, errors.New("sealed ledger contains unresolved client dispositions")
	}
	return byID, nil
}

func (d drillInputs) checkSubmissions(pre prewriteVerdict, byID map[string]restoreledger.Exchange, v *drillVerdict) error {
	h := d.ledger.Header
	roles := []string{"d1", "d2", "f1", "l1", "e1", "n1", "n2", "n3", "n4"}
	cutIDs := []string{d.cohorts.D1.Attempt.WorkflowID, d.cohorts.D2.Attempt.WorkflowID,
		d.cohorts.F1.WorkflowID, d.cohorts.L1.WorkflowID, d.cohorts.E1.Attempt.WorkflowID}
	if len(d.work.NonMarkers) != len(roles) {
		return errors.New("workload lacks the nine required non-marker identities")
	}
	want := map[string]restoreledger.Exchange{}
	for _, m := range d.plan.Markers {
		x := byID[m.RequestID]
		delete(byID, m.RequestID)
		if m.Sequence <= 12 {
			want[m.WorkflowID] = x
		}
	}
	var first int64
	for i, s := range d.work.NonMarkers {
		x := byID[s.RequestID]
		delete(byID, s.RequestID)
		o := x.Operation
		if s.Role != roles[i] || s.ID == "" || s.Key == "" || s.InitialNode == "" || s.Definition == "" ||
			!json.Valid(s.Input) || !restoreledger.IsSHA256(s.RequestSHA) || o.Kind != restoreledger.Submit ||
			o.WorkflowID != s.ID || o.Namespace != d.work.Fixture.Namespace || o.SubmissionKey != s.Key ||
			o.RequestSHA256 != s.RequestSHA || !x.Acked() || x.Outcome.PayloadHash == "" || want[s.ID].Outcome != nil {
			return fmt.Errorf("scheduled submission %s is missing, duplicated or unbound", roles[i])
		}
		if i < 5 {
			if s.ID != cutIDs[i] || o.Authority != h.Source || x.OutcomeSync >= d.cut.StartedNS {
				return fmt.Errorf("source submission differs from the archived cohort %s", roles[i])
			}
		} else {
			if o.Authority != h.Target || x.Outcome.SentNS < d.endpoint.OpenNS ||
				x.OutcomeSync > d.loss.T0NS+int64(360*time.Second) {
				return fmt.Errorf("target submit missed the 360-second gate for %s", roles[i])
			}
			if first == 0 {
				first = x.OutcomeSync
			}
			if x.OutcomeSync < first {
				return errors.New("fresh submission order differs from the frozen workload")
			}
			v.Fresh = append(v.Fresh, s.ID)
		}
		want[s.ID] = x
	}
	v.PhaseNS["first_new_write"] = first
	allowed := append(slices.Clone(pre.RetainedWorkflows), v.Fresh...)
	if len(want) != 21 || len(allowed) != 21 {
		return errors.New("terminal denominator is not 17 survivors and four new workflows")
	}
	for _, t := range []invariants.Trace{d.first, d.last} {
		if err := d.checkTerminal(t, want, allowed, v); err != nil {
			return err
		}
	}
	return nil
}

func (d drillInputs) checkTerminal(t invariants.Trace, want map[string]restoreledger.Exchange,
	ids []string, v *drillVerdict) error {
	if err := invariants.SnapshotIntegrity(t); err != nil {
		return err
	}
	raw, err := rawTable[workflowAtRestore](t, "engine.workflow_executions")
	if err != nil {
		return err
	}
	if err := checkPrewritePopulation(t, raw, ids, d.work.Fixture.Namespace); err != nil {
		return err
	}
	for _, w := range raw {
		x := want[w.ID]
		if x.Outcome == nil || w.Key != x.Operation.SubmissionKey || w.PayloadHash != x.Outcome.PayloadHash ||
			w.Version != 1 || w.State != "SUCCEEDED" {
			v.Incomplete = append(v.Incomplete, w.ID)
		}
	}
	if len(v.Incomplete) != 0 {
		return errors.New("terminal state or acknowledged payload identity differs")
	}
	for table, rows := range t.Snapshot {
		for _, row := range rows.Rows {
			var ref struct {
				ID string `json:"workflow_id"`
			}
			if err := json.Unmarshal(row, &ref); err != nil {
				return err
			}
			if ref.ID != "" && want[ref.ID].Outcome == nil {
				return fmt.Errorf("unexpected terminal snapshot identity %s in %s", ref.ID, table)
			}
		}
	}
	for _, s := range t.Submissions {
		x := want[s.WorkflowID]
		if x.Outcome == nil || s.Namespace != x.Operation.Namespace ||
			s.Key != x.Operation.SubmissionKey || s.Hash != x.Outcome.PayloadHash {
			return errors.New("terminal submission projection disagrees with the sealed acknowledgment")
		}
	}
	if len(t.Submissions) != len(want) {
		return errors.New("terminal submission projection undercounts the workload")
	}
	nodes, err := rawTable[nodeAtRestore](t, "engine.node_instances")
	if err != nil {
		return err
	}
	if _, _, err := checkMarkerPrefix(t, raw, nodes, ledgerMarkers(d.plan, d.ledger)); err != nil {
		return err
	}
	for _, m := range d.plan.Markers[:12] {
		if !slices.ContainsFunc(nodes, func(n nodeAtRestore) bool {
			return n.ID == m.WorkflowID && n.Node == "pure.echo" && n.Iteration == 0 &&
				equalMarkerInput(n.Result, m.ExpectedOutput)
		}) {
			return fmt.Errorf("recovered marker %s has no accepted echo output", m.WorkflowID)
		}
	}
	for _, p := range d.work.NonMarkers {
		if !slices.ContainsFunc(raw, func(w workflowAtRestore) bool { return w.ID == p.ID && w.Definition == p.Definition }) ||
			!slices.ContainsFunc(nodes, func(n nodeAtRestore) bool {
				return n.ID == p.ID && n.Node == p.InitialNode && n.Iteration == 0 && equalMarkerInput(n.Input, p.Input)
			}) {
			return fmt.Errorf("sealed non-marker workflow definition or input differs for %s", p.Role)
		}
	}
	for _, o := range t.Outbox {
		if o.PublishState != "PUBLISHED" {
			v.Unresolved = append(v.Unresolved, o.EventID)
		}
	}
	for _, r := range t.Reconciliation {
		if r.Status == "OPEN" {
			v.Unresolved = append(v.Unresolved, r.Reference)
		}
	}
	if len(v.Unresolved) != 0 {
		return errors.New("terminal snapshot has open reconciliation or unpublished work")
	}
	if result := invariants.Check(t); !result.Valid || len(result.NonTerminal) != 0 {
		return fmt.Errorf("existing invariant checker rejected recovered and new work: %s", strings.Join(result.Violations, ", "))
	}
	return nil
}

func (d drillInputs) terminalTime() (time.Time, error) {
	var at time.Time
	seen := map[string]bool{}
	history, err := rawTable[dispatchHistory](d.last, "engine.transition_history")
	if err != nil {
		return at, err
	}
	for _, h := range history {
		if (h.NewState == "SUCCEEDED") && !seen[h.WorkflowID] {
			if h.CreatedAt.IsZero() {
				return at, errors.New("terminal commit clock is missing")
			}
			seen[h.WorkflowID] = true
			if h.CreatedAt.After(at) {
				at = h.CreatedAt
			}
		}
	}
	if len(seen) != 21 {
		return at, errors.New("terminal history does not include all 21 workflow commits")
	}
	return at, nil
}
