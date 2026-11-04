package state

import (
	"context"
	"errors"
	"time"
)

// DemandSample is a fresh namespace-wide database snapshot, never Kafka lag.
type DemandSample struct {
	Namespace                 string    `json:"namespace"`
	SampledAt                 time.Time `json:"sampled_at"`
	ActiveClaims              int64     `json:"active_claims"`
	ClaimableAttempts         int64     `json:"claimable_attempts"`
	Demand                    int64     `json:"demand"`
	OldestClaimableAgeSeconds float64   `json:"oldest_claimable_age_seconds"`
}

// DurableDemand bounds pool acquisition and the query together. A timeout is
// missing evidence, not an empty queue. EXISTS preserves attempt cardinality.
func (s *Store) DurableDemand(ctx context.Context, namespace string) (DemandSample, error) {
	if namespace == "" {
		return DemandSample{}, errors.New("demand namespace required")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	sample := DemandSample{Namespace: namespace}
	err := s.pool.QueryRow(ctx, `WITH sample AS MATERIALIZED (SELECT statement_timestamp() AS at),
        attempts AS (
         SELECT a.state, a.created_at, a.heartbeat_deadline
         FROM engine.activity_attempts a
         WHERE a.is_current AND a.state IN ('DISPATCHABLE','CLAIMED')
         AND EXISTS (SELECT 1 FROM engine.workflow_executions w
           WHERE w.workflow_id=a.workflow_id AND w.namespace=$1 AND w.state='WAITING_ACTIVITY')
         AND EXISTS (SELECT 1 FROM engine.node_instances n
           WHERE n.workflow_id=a.workflow_id AND n.node_id=a.node_id AND n.iteration=a.iteration
           AND n.current_attempt_number=a.attempt_number)
        )
        SELECT sample.at,
         count(*) FILTER (WHERE state='CLAIMED' AND heartbeat_deadline>sample.at),
         count(*) FILTER (WHERE state='DISPATCHABLE'),
         COALESCE(GREATEST(0,extract(epoch FROM sample.at-min(created_at) FILTER (WHERE state='DISPATCHABLE'))),0)::float8
        FROM sample LEFT JOIN attempts ON true GROUP BY sample.at`, namespace).Scan(
		&sample.SampledAt, &sample.ActiveClaims, &sample.ClaimableAttempts, &sample.OldestClaimableAgeSeconds)
	if err != nil {
		return DemandSample{}, err
	}
	sample.Demand = sample.ActiveClaims + sample.ClaimableAttempts
	return sample, nil
}
