package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/effects/k6sink"
	"durable-agent-execution-engine/internal/invariants"
	"durable-agent-execution-engine/internal/k6receipt"
	"durable-agent-execution-engine/internal/restoreledger"
	"durable-agent-execution-engine/internal/state"
)

type attemptAtCut struct {
	WorkflowID string    `json:"workflow_id"`
	NodeID     string    `json:"node_id"`
	Iteration  int       `json:"iteration"`
	Attempt    int64     `json:"attempt_number"`
	ClaimSHA   string    `json:"claim_token_sha256,omitempty"`
	Deadline   time.Time `json:"heartbeat_deadline"`
}

type dispatchAtCut struct {
	Attempt   attemptAtCut `json:"attempt"`
	EventID   string       `json:"event_id"`
	Topic     string       `json:"topic"`
	TopicID   string       `json:"topic_id"`
	Partition int          `json:"partition"`
	Offset    int64        `json:"offset"`
}

type ownerAtCut struct {
	WorkflowID string    `json:"workflow_id"`
	Partition  int       `json:"partition_id"`
	Owner      string    `json:"owner_id"`
	Epoch      int64     `json:"epoch"`
	Expires    time.Time `json:"lease_expires_at"`
}

type effectAtCut struct {
	Attempt       attemptAtCut `json:"attempt"`
	Intent        string       `json:"intent_id"`
	Resource      string       `json:"resource_id"`
	LogicalKey    string       `json:"logical_key"`
	CanonicalHash string       `json:"canonical_hash"`
	ScopeHash     string       `json:"scope_hash"`
	GrantSHA      string       `json:"grant_token_sha256"`
	Fence         int64        `json:"fence"`
}

type cohortsAtCut struct {
	Schema string        `json:"schema"`
	D1     dispatchAtCut `json:"d1"`
	D2     dispatchAtCut `json:"d2"`
	F1     attemptAtCut  `json:"f1"`
	L1     ownerAtCut    `json:"l1"`
	E1     effectAtCut   `json:"e1"`
}

type prewriteVerdict struct {
	RecoveredMarkers  []string          `json:"recovered_markers"`
	MissingMarkers    []string          `json:"missing_markers"`
	RetainedWorkflows []string          `json:"retained_workflows"`
	Orphan            k6receipt.Receipt `json:"orphan_receipt"`
}

type targetState struct {
	Version          int    `json:"server_version_num"`
	SystemID         uint64 `json:"system_identifier"`
	Timeline         uint32 `json:"timeline"`
	PreviousTimeline uint32 `json:"prev_timeline"`
	ReplayLSN        string `json:"replay_lsn"`
	DataDirectory    string `json:"data_directory"`
	DatabaseBytes    int64  `json:"database_bytes"`
	ClusterBytes     int64  `json:"cluster_database_bytes"`
	WALBytes         int64  `json:"wal_bytes"`
	Recovered        bool   `json:"recovered"`
	SocketOnly       bool   `json:"socket_only"`
	ReadOnly         bool   `json:"read_only"`
	Restricted       bool   `json:"restricted_observer"`
}

type observedTarget struct {
	Target targetState      `json:"target"`
	Trace  invariants.Trace `json:"trace"`
}

const observerSQL = `
SELECT json_build_object(
    'server_version_num', current_setting('server_version_num')::integer,
    'system_identifier', (SELECT system_identifier FROM pg_control_system()),
    'timeline', (SELECT timeline_id FROM pg_control_checkpoint()),
    'prev_timeline', (SELECT prev_timeline_id FROM pg_control_checkpoint()),
    'replay_lsn', pg_last_wal_replay_lsn(),
    'data_directory', current_setting('data_directory'),
    'database_bytes', pg_database_size(current_database()),
    'cluster_database_bytes', (SELECT sum(pg_database_size(oid)) FROM pg_database WHERE has_database_privilege(oid, 'CONNECT')),
    'wal_bytes', (SELECT COALESCE(sum(size), 0) FROM pg_ls_waldir()),
    'recovered', NOT pg_is_in_recovery(),
    'socket_only', inet_server_addr() IS NULL AND current_setting('listen_addresses') = '',
    'read_only', current_setting('transaction_read_only') = 'on',
    'restricted_observer', current_user = 'j7_observer'
        AND EXISTS (SELECT FROM pg_roles r WHERE r.rolname = current_user
            AND NOT (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls)
            AND NOT EXISTS (SELECT FROM pg_auth_members m JOIN pg_roles inherited ON inherited.oid = m.roleid
                WHERE m.member = r.oid AND (inherited.rolname <> 'pg_read_all_settings' OR m.admin_option)))
        AND NOT EXISTS (SELECT FROM pg_tables t WHERE t.schemaname IN ('engine','effects')
            AND has_table_privilege(format('%I.%I', t.schemaname, t.tablename),
                'INSERT, UPDATE, DELETE, TRUNCATE, TRIGGER'))
        AND NOT EXISTS (SELECT FROM pg_namespace n WHERE n.nspname NOT LIKE 'pg_%'
            AND has_schema_privilege(n.oid, 'CREATE'))
        AND NOT has_database_privilege(current_database(), 'CREATE')
);`

func observeTarget(ctx context.Context, database, namespace string) (observedTarget, error) {
	var out observedTarget
	if !fixtureName.MatchString(database) || namespace == "" {
		return out, errors.New("observer requires the approved database and namespace")
	}
	url := "host=" + targetSocket + " port=5432 sslmode=disable user=" + observerRole + " dbname=" + database
	store, err := state.NewCheckerFromURL(ctx, url)
	if err != nil {
		return out, err
	}
	defer store.Close()
	var raw []byte
	if err := store.Pool().QueryRow(ctx, observerSQL).Scan(&raw); err != nil {
		return out, err
	}
	if err := json.Unmarshal(raw, &out.Target); err != nil {
		return out, err
	}
	if !out.Target.Recovered || !out.Target.SocketOnly || !out.Target.ReadOnly || !out.Target.Restricted {
		return out, errors.New("target observer lacks recovery completion or read-only isolation")
	}
	out.Trace, err = invariants.LoadNamespace(ctx, store, namespace)
	return out, err
}

type workflowAtRestore struct {
	ID          string `json:"workflow_id"`
	Namespace   string `json:"namespace"`
	Key         string `json:"submission_key"`
	PayloadHash string `json:"submission_payload_hash"`
	Definition  string `json:"definition_id"`
	Version     int    `json:"definition_version"`
	Partition   int    `json:"partition_id"`
	State       string `json:"state"`
}

type nodeAtRestore struct {
	ID        string          `json:"workflow_id"`
	Node      string          `json:"node_id"`
	Iteration int             `json:"iteration"`
	Input     json.RawMessage `json:"input"`
	Result    json.RawMessage `json:"accepted_result"`
}

type attemptAtRestore struct {
	WorkflowID string          `json:"workflow_id"`
	NodeID     string          `json:"node_id"`
	Iteration  int             `json:"iteration"`
	Attempt    int64           `json:"attempt_number"`
	Claim      string          `json:"claim_token"`
	Worker     string          `json:"worker_id"`
	Request    string          `json:"worker_request_id"`
	Deadline   time.Time       `json:"heartbeat_deadline"`
	State      string          `json:"state"`
	Class      string          `json:"effect_class"`
	Current    bool            `json:"is_current"`
	Outcome    string          `json:"outcome_disposition"`
	Result     json.RawMessage `json:"result"`
	Proof      json.RawMessage `json:"k6_proof"`
	Fence      int64           `json:"k6_fence"`
	LogicalKey string          `json:"logical_effect_key"`
	ScopeHash  string          `json:"grant_scope_hash"`
}

func rawTable[T any](t invariants.Trace, name string) ([]T, error) {
	table, ok := t.Snapshot[name]
	if !ok {
		return nil, fmt.Errorf("pre-write snapshot is missing %s", name)
	}
	rows := make([]T, 0, len(table.Rows))
	for _, raw := range table.Rows {
		var row T
		if err := json.Unmarshal(raw, &row); err != nil {
			return nil, fmt.Errorf("decode %s: %w", name, err)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func verifyPrewrite(t invariants.Trace, markers []markerExpected, c cohortsAtCut, proof k6receipt.Proof,
	sink k6sink.Snapshot) (prewriteVerdict, error) {
	var v prewriteVerdict
	if len(markers) != 15 || c.Schema != "j7.cohorts-at-cut.v1" {
		return v, errors.New("pre-write verification requires the sealed 15 markers and held cohorts")
	}
	if err := invariants.SnapshotIntegrity(t); err != nil {
		return v, err
	}
	workflows, err := rawTable[workflowAtRestore](t, "engine.workflow_executions")
	if err != nil {
		return v, err
	}
	nodes, err := rawTable[nodeAtRestore](t, "engine.node_instances")
	if err != nil {
		return v, err
	}
	v.RecoveredMarkers, v.MissingMarkers, err = checkMarkerPrefix(t, workflows, nodes, markers)
	if err != nil {
		return v, err
	}
	want := append(slices.Clone(v.RecoveredMarkers), c.D1.Attempt.WorkflowID, c.D2.Attempt.WorkflowID, c.F1.WorkflowID,
		c.L1.WorkflowID, c.E1.Attempt.WorkflowID)
	v.RetainedWorkflows = want
	if err := checkPrewritePopulation(t, workflows, want, markers[0].Namespace); err != nil {
		return v, err
	}
	if err := checkPrewriteMigrations(t); err != nil {
		return v, err
	}
	attempts, err := rawTable[attemptAtRestore](t, "engine.activity_attempts")
	if err != nil {
		return v, err
	}
	for _, held := range []dispatchAtCut{c.D1, c.D2} {
		if err := checkHeldDispatch(t, attempts, held); err != nil {
			return v, err
		}
	}
	if _, err := checkHeldAttempt(attempts, c.F1, "CLAIMED", "PURE_ACTIVITY"); err != nil {
		return v, err
	}
	if err := checkHeldOwner(t, workflows, c.L1); err != nil {
		return v, err
	}
	v.Orphan, err = checkPrewriteOrphan(t, nodes, attempts, c.E1, proof, sink)
	return v, err
}

func checkMarkerPrefix(t invariants.Trace, workflows []workflowAtRestore, nodes []nodeAtRestore,
	markers []markerExpected) ([]string, []string, error) {
	byID := make(map[string]workflowAtRestore)
	for _, row := range workflows {
		byID[row.ID] = row
	}
	missing := make(map[string]bool)
	var kept, lost []string
	for i, m := range markers {
		w, exists := byID[m.ID]
		if i >= 12 {
			if exists {
				return nil, nil, fmt.Errorf("POST_CUT_MARKER_PRESENT: %s", m.ID)
			}
			lost = append(lost, m.ID)
			missing[m.ID] = true
			continue
		}
		if !exists {
			return nil, nil, fmt.Errorf("PRE_CUT_MARKER_MISSING: %s", m.ID)
		}
		if w.Namespace != m.Namespace || w.Key != m.Key || w.PayloadHash != m.PayloadSHA256 ||
			w.Definition != m.Definition || w.Version != 1 || (i < 8 && w.State != "SUCCEEDED") {
			return nil, nil, fmt.Errorf("pre-cut marker identity or terminal state differs: %s", m.ID)
		}
		n := slices.IndexFunc(nodes, func(n nodeAtRestore) bool { return n.ID == m.ID && n.Node == "pure.echo" && n.Iteration == 0 })
		if n < 0 || !equalMarkerInput(nodes[n].Input, m.Input) {
			return nil, nil, fmt.Errorf("pre-cut marker immutable input differs: %s", m.ID)
		}
		kept = append(kept, m.ID)
	}
	for table, rows := range t.Snapshot {
		for _, raw := range rows.Rows {
			var ref struct {
				ID string `json:"workflow_id"`
			}
			if err := json.Unmarshal(raw, &ref); err != nil {
				return nil, nil, err
			}
			if missing[ref.ID] {
				return nil, nil, fmt.Errorf("POST_CUT_MARKER_PRESENT: %s in %s", ref.ID, table)
			}
		}
	}
	return kept, lost, nil
}

func equalMarkerInput(a, b json.RawMessage) bool {
	var left, right map[string]json.RawMessage
	if json.Unmarshal(a, &left) != nil || json.Unmarshal(b, &right) != nil || len(left) != len(right) {
		return false
	}
	for k, v := range left {
		if string(v) != string(right[k]) {
			return false
		}
	}
	return true
}

func checkPrewritePopulation(t invariants.Trace, rows []workflowAtRestore, ids []string, namespace string) error {
	want := make(map[string]bool)
	for _, id := range ids {
		if id == "" || want[id] {
			return errors.New("pre-write denominator has an empty or duplicate identity")
		}
		want[id] = true
	}
	if len(rows) != len(ids) || len(t.Workflows) != len(ids) {
		return errors.New("pre-write workflow population differs from the 17 surviving IDs")
	}
	for _, row := range rows {
		if !want[row.ID] || row.Namespace != namespace {
			return fmt.Errorf("unexpected pre-write workflow %s", row.ID)
		}
		delete(want, row.ID)
	}
	return nil
}

func checkPrewriteMigrations(t invariants.Trace) error {
	type version struct {
		Number int `json:"version"`
	}
	versions, err := rawTable[version](t, "engine.schema_migrations")
	if err != nil {
		return err
	}
	if len(versions) != 23 {
		return errors.New("restored engine migration inventory must contain exactly 000001 through 000023")
	}
	slices.SortFunc(versions, func(a, b version) int { return a.Number - b.Number })
	for i, v := range versions {
		if v.Number != i+1 {
			return fmt.Errorf("restored engine migration inventory mismatch at %06d", i+1)
		}
	}
	return nil
}

func checkHeldAttempt(rows []attemptAtRestore, hold attemptAtCut, state, class string) (attemptAtRestore, error) {
	var result attemptAtRestore
	count := 0
	for _, row := range rows {
		if row.WorkflowID == hold.WorkflowID {
			result = row
			count++
		}
	}
	r := result
	if count != 1 || hold.Attempt != 1 || hold.NodeID == "" || hold.Deadline.IsZero() ||
		r.NodeID != hold.NodeID || r.Iteration != hold.Iteration || r.Attempt != hold.Attempt ||
		r.Deadline.Before(hold.Deadline) || r.State != state || r.Class != class || !r.Current ||
		r.Outcome != "NONE" || present(r.Result) || present(r.Proof) {
		return result, fmt.Errorf("restored held first attempt differs for %s", hold.WorkflowID)
	}
	if state == "CLAIMED" {
		// The same token can heartbeat between the source witness and C.
		if r.Claim == "" || restoreledger.SHA256([]byte(r.Claim)) != hold.ClaimSHA || r.Worker == "" || r.Request == "" {
			return result, fmt.Errorf("restored held claim differs for %s", hold.WorkflowID)
		}
	} else if r.Claim != "" || r.Worker != "" || r.Request != "" || hold.ClaimSHA != "" ||
		!r.Deadline.Equal(hold.Deadline) {
		return result, fmt.Errorf("restored dispatch already has a claim for %s", hold.WorkflowID)
	}
	return result, nil
}

func present(raw json.RawMessage) bool { return len(raw) != 0 && string(raw) != "null" }

func checkHeldDispatch(t invariants.Trace, attempts []attemptAtRestore, d dispatchAtCut) error {
	if _, err := checkHeldAttempt(attempts, d.Attempt, "DISPATCHABLE", "PURE_ACTIVITY"); err != nil {
		return err
	}
	if d.EventID == "" || d.TopicID == "" || d.Topic == "" || d.Partition < 0 || d.Partition >= 8 || d.Offset < 0 {
		return errors.New("original broker witness is incomplete")
	}
	var dispatches int
	for _, row := range t.Outbox {
		if row.WorkflowID != d.Attempt.WorkflowID {
			continue
		}
		if row.EventType == "attempt.redispatch" {
			return fmt.Errorf("redispatch already exists for %s", row.WorkflowID)
		}
		if row.EventType == "attempt.dispatch" {
			if row.EventID != d.EventID || row.Topic != d.Topic || row.PublishState != "PUBLISHED" {
				return fmt.Errorf("restored original dispatch differs for %s", row.WorkflowID)
			}
			dispatches++
		}
	}
	type publication struct {
		ID  string `json:"event_id"`
		Ack bool   `json:"broker_acknowledged"`
	}
	pubs, err := rawTable[publication](t, "engine.outbox_publications")
	if err != nil {
		return err
	}
	if dispatches != 1 || !slices.ContainsFunc(pubs, func(p publication) bool { return p.ID == d.EventID && p.Ack }) {
		return fmt.Errorf("restored original durable publication is absent for %s", d.Attempt.WorkflowID)
	}
	return nil
}

func checkHeldOwner(t invariants.Trace, workflows []workflowAtRestore, want ownerAtCut) error {
	type lease struct {
		Partition int       `json:"partition_id"`
		Owner     string    `json:"owner_id"`
		Epoch     int64     `json:"epoch"`
		Expires   time.Time `json:"lease_expires_at"`
	}
	rows, err := rawTable[lease](t, "engine.partition_leases")
	if err != nil {
		return err
	}
	if want.Owner == "" || want.Epoch <= 0 || want.Expires.IsZero() ||
		!slices.ContainsFunc(workflows, func(w workflowAtRestore) bool { return w.ID == want.WorkflowID && w.Partition == want.Partition }) ||
		!slices.ContainsFunc(rows, func(l lease) bool {
			return l.Partition == want.Partition && l.Owner == want.Owner && l.Epoch == want.Epoch && !l.Expires.Before(want.Expires)
		}) {
		return errors.New("restored partition lease differs from L1's archived owner")
	}
	return nil
}

func checkPrewriteOrphan(t invariants.Trace, nodes []nodeAtRestore, attempts []attemptAtRestore, e effectAtCut,
	proof k6receipt.Proof, sink k6sink.Snapshot) (k6receipt.Receipt, error) {
	r, err := checkHeldAttempt(attempts, e.Attempt, "CLAIMED", "COOPERATING_EFFECT")
	if err != nil {
		return k6receipt.Receipt{}, err
	}
	id := e.Attempt.WorkflowID
	if r.Fence != e.Fence || e.Fence <= 0 || r.LogicalKey != e.LogicalKey || r.ScopeHash != e.ScopeHash {
		return k6receipt.Receipt{}, errors.New("restored E1 claim disagrees with its sealed effect identity")
	}
	for _, node := range nodes {
		if node.ID == id && present(node.Result) {
			return k6receipt.Receipt{}, errors.New("E1 source result is unexpectedly present after restore")
		}
	}
	for _, row := range t.Effects {
		if row.WorkflowID == id && (row.ReceiptPresent || row.Outcome == "KNOWN_SUCCESS") {
			return k6receipt.Receipt{}, errors.New("E1 source effect receipt is unexpectedly present in restored engine")
		}
	}
	var approvals int
	for _, row := range t.Approvals {
		if row.WorkflowID == id && row.NodeID == e.Attempt.NodeID && row.Iteration == e.Attempt.Iteration &&
			row.IntentID == e.Intent && row.Decision == "APPROVED" && row.DispatchStatus == "GRANTED" &&
			row.ExpectedResourceRevision == "0" && row.CanonicalArgumentHash == e.CanonicalHash &&
			row.GrantScopeHash == e.ScopeHash && row.GrantToken != "" &&
			restoreledger.SHA256([]byte(row.GrantToken)) == e.GrantSHA {
			approvals++
		}
	}
	receipt := proof.Receipt
	if approvals != 1 || receipt.WorkflowID != id || receipt.IntentID != e.Intent || receipt.LogicalKey != e.LogicalKey ||
		receipt.ResourceID != e.Resource || receipt.CanonicalHash != e.CanonicalHash || receipt.ScopeHash != e.ScopeHash ||
		receipt.ExpectedRevision != 0 || receipt.AppliedRevision != 1 || receipt.ApplicationID == "" ||
		len(sink.Receipts) != 1 || sink.Receipts[0] != receipt {
		return k6receipt.Receipt{}, errors.New("E1 surviving authorization and retained orphan receipt do not join")
	}
	if err := errors.Join(sink.CheckApplications(1), sink.CheckRevisionChain()); err != nil {
		return k6receipt.Receipt{}, err
	}
	if len(sink.Allocations) != 1 || len(sink.Resources) != 1 ||
		sink.Allocations[0].IntentID != e.Intent || sink.Allocations[0].ResourceID != e.Resource ||
		sink.Allocations[0].Fence != e.Fence || sink.Allocations[0].Attempt != e.Attempt.Attempt ||
		!strings.EqualFold(sink.Allocations[0].GrantHash, e.GrantSHA) ||
		sink.Resources[0].ResourceID != e.Resource || sink.Resources[0].Revision != 1 ||
		sink.Journal[0].IntentID != e.Intent || sink.Journal[0].LogicalKey != e.LogicalKey {
		return k6receipt.Receipt{}, errors.New("E1 retained sink allocation and mutation do not join")
	}
	return receipt, nil
}
