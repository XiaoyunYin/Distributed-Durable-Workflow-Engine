package k6sink

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ResourceState struct {
	ResourceID       string `json:"resource_id"`
	InitialRevision  int64  `json:"initial_revision"`
	Revision         int64  `json:"revision"`
	LatestFence      int64  `json:"latest_fence"`
	InitialStateHash string `json:"initial_state_hash"`
	FinalStateHash   string `json:"final_state_hash"`
}

type Mutation struct {
	ResourceID   string `json:"resource_id"`
	OldRevision  int64  `json:"old_revision"`
	Revision     int64  `json:"revision"`
	OldStateHash string `json:"old_state_hash"`
	StateHash    string `json:"state_hash"`
	LogicalKey   string `json:"logical_key"`
	IntentID     string `json:"intent_id"`
}

type Call struct {
	CallID         int64     `json:"call_id"`
	Operation      string    `json:"operation"`
	WorkflowID     string    `json:"workflow_id"`
	LogicalKey     string    `json:"logical_key"`
	IntentID       string    `json:"intent_id"`
	ResourceID     string    `json:"resource_id"`
	MessageID      string    `json:"message_id"`
	Attempt        int64     `json:"attempt"`
	RequestID      string    `json:"request_id"`
	BodyHash       string    `json:"body_hash"`
	ConnectionHash string    `json:"connection_hash"`
	Outcome        string    `json:"outcome"`
	ApplicationID  *string   `json:"application_id"`
	ResponseHash   *string   `json:"response_hash"`
	ObservedAt     time.Time `json:"observed_at"`
}

type Allocation struct {
	IntentID   string `json:"intent_id"`
	GrantHash  string `json:"grant_hash"`
	Attempt    int64  `json:"attempt"`
	ResourceID string `json:"resource_id"`
	Fence      int64  `json:"fence"`
}

type RequestBinding struct {
	RequestID string `json:"request_id"`
	BodyHash  string `json:"body_hash"`
}

type RelationDigest struct {
	Relation string `json:"relation"`
	Count    int    `json:"count"`
	SHA256   string `json:"sha256"`
}

type Snapshot struct {
	Resources       []ResourceState  `json:"resources"`
	Journal         []Mutation       `json:"journal"`
	Receipts        []Receipt        `json:"receipts"`
	Calls           []Call           `json:"calls"`
	Allocations     []Allocation     `json:"allocations"`
	RequestBindings []RequestBinding `json:"request_bindings"`
	Relations       []RelationDigest `json:"relations"`
}

// Export reads the standalone sink DB using its dedicated single-connection
// read-only role. The snapshot includes inconsistent rows for offline checking.
func Export(ctx context.Context, dsn string) (Snapshot, error) {
	pool, err := openPool(ctx, dsn, "k6_sink_exporter", "dur053-k6-sink-exporter", 1)
	if err != nil {
		return Snapshot{}, err
	}
	defer pool.Close()
	return exportSnapshot(ctx, pool)
}

func exportSnapshot(ctx context.Context, pool *pgxpool.Pool) (Snapshot, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return Snapshot{}, errUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var snap Snapshot
	type query struct {
		relation, sql string
		read          func(context.Context, pgx.Tx, string, string) (RelationDigest, error)
	}
	queries := []query{
		{"resources", `SELECT resource_id, 0 AS initial_revision, revision, latest_fence, initial_state_hash, final_state_hash
			FROM k6_sink.resource_states ORDER BY resource_id COLLATE "C"`, readInto(&snap.Resources)},
		{"journal", `SELECT resource_id, old_revision, revision, old_state_hash, state_hash, logical_key, intent_id
			FROM k6_sink.journal ORDER BY resource_id COLLATE "C", revision`, readInto(&snap.Journal)},
		{"receipts", `SELECT ` + receiptColumns + ` FROM k6_sink.receipts
			ORDER BY workflow_id COLLATE "C", logical_key COLLATE "C"`, readInto(&snap.Receipts)},
		{"calls", `SELECT call_id, operation, workflow_id, logical_key, intent_id, resource_id, message_id,
			attempt, request_id, body_hash, connection_hash, outcome, application_id, response_hash, observed_at
			FROM k6_sink.calls ORDER BY call_id`, readInto(&snap.Calls)},
		{"allocations", `SELECT intent_id, grant_hash, attempt, resource_id, fence
			FROM k6_sink.allocations ORDER BY resource_id COLLATE "C", fence`, readInto(&snap.Allocations)},
		{"request_bindings", `SELECT request_id, body_hash FROM k6_sink.request_bindings
			ORDER BY request_id COLLATE "C"`, readInto(&snap.RequestBindings)},
	}
	for _, q := range queries {
		digest, err := q.read(ctx, tx, q.relation, q.sql)
		if err != nil {
			return Snapshot{}, errUnavailable
		}
		snap.Relations = append(snap.Relations, digest)
	}
	if err := tx.Commit(ctx); err != nil {
		return Snapshot{}, errUnavailable
	}
	return snap, nil
}

func readInto[T any](dest *[]T) func(context.Context, pgx.Tx, string, string) (RelationDigest, error) {
	return func(ctx context.Context, tx pgx.Tx, relation, query string) (RelationDigest, error) {
		rows, err := tx.Query(ctx, `SELECT row_to_json(ordered) FROM (`+query+`) AS ordered`)
		if err != nil {
			return RelationDigest{}, err
		}
		defer rows.Close()
		*dest = make([]T, 0)
		for rows.Next() {
			var value T
			if err := rows.Scan(&value); err != nil {
				return RelationDigest{}, err
			}
			*dest = append(*dest, value)
		}
		if err := rows.Err(); err != nil {
			return RelationDigest{}, err
		}
		encoded, err := json.Marshal(*dest)
		if err != nil {
			return RelationDigest{}, err
		}
		return RelationDigest{relation, len(*dest), hash(encoded)}, nil
	}
}

// CheckApplications reconciles the independent mutation count with the
// expected number of successful effects furnished by the caller.
func (s Snapshot) CheckApplications(expected int) error {
	applicationCount := len(s.Journal)
	if applicationCount != expected {
		return errors.New("sink journal application count disagrees with expected effects")
	}
	return nil
}

func (s Snapshot) CheckRevisionChain() error {
	entry := 0
	for _, resource := range s.Resources {
		revision, stateHash := resource.InitialRevision, resource.InitialStateHash
		for ; entry < len(s.Journal) && s.Journal[entry].ResourceID == resource.ResourceID; entry++ {
			write := s.Journal[entry]
			if write.OldRevision != revision || write.Revision != revision+1 || write.OldStateHash != stateHash {
				return errors.New("sink journal revision chain is not contiguous")
			}
			revision, stateHash = write.Revision, write.StateHash
		}
		if revision != resource.Revision || stateHash != resource.FinalStateHash {
			return errors.New("sink final state disagrees with journal chain")
		}
	}
	if entry != len(s.Journal) {
		return errors.New("sink journal has an unjoined mutation")
	}
	return nil
}
