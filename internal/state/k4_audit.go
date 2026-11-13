package state

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// K4Session identifies this process in both DB audit and scheduler acknowledgments.
// A restart always gets a new session, including restarts within one pod UID.
var K4Session = NewID()

func configureK4Audit(config *pgxpool.Config) error {
	flag := os.Getenv("DUR053_K4_CAMPAIGN")
	if flag != "" && flag != "0" && flag != "1" {
		return errors.New("DUR053_K4_CAMPAIGN must be 0 or 1")
	}
	if flag != "1" {
		return nil
	}
	if os.Getenv("POD_UID") == "" || os.Getenv("CONTAINER_ID") == "" {
		return errors.New("K4 campaign requires POD_UID and CONTAINER_ID")
	}
	config.ConnConfig.RuntimeParams["durable.k4_campaign"] = "on"
	config.ConnConfig.RuntimeParams["durable.k4_session"] = K4Session
	config.ConnConfig.RuntimeParams["durable.k4_pod_uid"] = os.Getenv("POD_UID")
	config.ConnConfig.RuntimeParams["durable.k4_container_id"] = os.Getenv("CONTAINER_ID")
	return nil
}

func (s *Store) recordK4Lease(ctx context.Context, tx pgx.Tx, kind string, ref LeaseRef) error {
	if tx.Conn().Config().RuntimeParams["durable.k4_campaign"] != "on" {
		return nil
	}
	_, err := tx.Exec(ctx, `INSERT INTO engine.k4_audit_queue
        (kind, partition_id, owner_id, epoch, session_id, pod_uid, container_id)
        VALUES ($1, $2, $3, $4, current_setting('durable.k4_session'),
                current_setting('durable.k4_pod_uid'), current_setting('durable.k4_container_id'))`,
		kind, ref.PartitionID, ref.OwnerID, ref.Epoch)
	return err
}

func recordK4Authorization(ctx context.Context, tx pgx.Tx, owner string) error {
	if tx.Conn().Config().RuntimeParams["durable.k4_campaign"] != "on" {
		return nil
	}
	_, err := tx.Exec(ctx, `SELECT set_config('durable.k4_owner', $1, true)`, owner)
	return err
}

// K4LeaseAudit is the closed Phase A lease schema. The caller supplies the DB
// wall-clock value corresponding to its episode monotonic zero, retaining that
// clock bracket separately. at is statement time; Order supplies commit order.
type K4LeaseAudit struct {
	Schema string            `json:"schema"`
	Rows   []K4LeaseAuditRow `json:"rows"`
}

type K4LeaseAuditRow struct {
	Order       int64   `json:"order"`
	At          float64 `json:"at"`
	Kind        string  `json:"kind"`
	Partition   int16   `json:"partition"`
	Owner       string  `json:"owner"`
	Epoch       int64   `json:"epoch"`
	Session     string  `json:"session"`
	Transaction string  `json:"transaction"`
}

// ExportK4LeaseAudit performs one read-only statement with a single MVCC
// snapshot; it can also be called on the existing read-only checker pool.
func (s *Store) ExportK4LeaseAudit(ctx context.Context, databaseOrigin time.Time) (K4LeaseAudit, error) {
	result := K4LeaseAudit{Schema: "dur053.k4-lease-audit.v1", Rows: []K4LeaseAuditRow{}}
	rows, err := s.pool.Query(ctx, `SELECT "order", at, kind, partition, owner, epoch, session, "transaction"
		FROM engine.k4_lease_audit ORDER BY "order"`)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var row K4LeaseAuditRow
		var at time.Time
		if err := rows.Scan(&row.Order, &at, &row.Kind, &row.Partition, &row.Owner, &row.Epoch, &row.Session, &row.Transaction); err != nil {
			return result, err
		}
		row.At = at.Sub(databaseOrigin).Seconds()
		if row.At < 0 {
			return result, fmt.Errorf("K4 database origin is after audit row %d", row.Order)
		}
		result.Rows = append(result.Rows, row)
	}
	return result, rows.Err()
}
