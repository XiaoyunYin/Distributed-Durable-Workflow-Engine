package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// M3 transport records are deliberately snapshots. Relay and consumer code
// must not mutate them in memory and then mistake that mutation for a durable
// transition.
type OutboxPublishState string

const (
	EventTopic                           = "durable-agent.events.v1"
	TaskTopic                            = "durable-agent.tasks.v1"
	OutboxPending     OutboxPublishState = "PENDING"
	OutboxClaimed     OutboxPublishState = "CLAIMED"
	OutboxPublished   OutboxPublishState = "PUBLISHED"
	OutboxQuarantined OutboxPublishState = "QUARANTINED"
)

type OutboxEvent struct {
	EventID           string
	WorkflowID        string
	AggregateRevision int64
	EventType         string
	Topic             string
	SchemaVersion     int
	Payload           json.RawMessage
	PublishState      OutboxPublishState
	RelayAttempts     int
	ClaimOwner        string
	ClaimExpiresAt    *time.Time
	NextAttemptAt     time.Time
	LastError         string
	CreatedAt         time.Time
	PublishedAt       *time.Time
}

type OutboxPublication struct {
	PublicationID      string
	EventID            string
	RelayOwner         string
	RelayAttempt       int
	BrokerAcknowledged bool
	PublishedAt        time.Time
	Error              string
}

type InboxMessage struct {
	ConsumerID    string
	EventID       string
	Topic         string
	Partition     int
	Offset        int64
	EventType     string
	SchemaVersion int
	Payload       json.RawMessage
	Disposition   InboxDisposition
}

type InboxDisposition string

const (
	InboxAccepted       InboxDisposition = "ACCEPTED"
	InboxAlreadyHandled InboxDisposition = "ALREADY_HANDLED"
	InboxStale          InboxDisposition = "STALE"
	InboxQuarantined    InboxDisposition = "QUARANTINED"
)

type InboxResult struct {
	Disposition   InboxDisposition
	WakeupCreated bool
	NextOffset    int64
	CommitOffset  bool
}

type ConsumerOffset struct {
	ConsumerID string
	Topic      string
	Partition  int
	NextOffset int64
	UpdatedAt  time.Time
}

type WakeupState string

const (
	WakeupPending  WakeupState = "PENDING"
	WakeupClaimed  WakeupState = "CLAIMED"
	WakeupConsumed WakeupState = "CONSUMED"
)

type SchedulerWakeup struct {
	WakeupID       string
	WorkflowID     string
	PartitionID    int16
	EventID        string
	Reason         string
	State          WakeupState
	ClaimOwner     string
	ClaimEpoch     int64
	ClaimExpiresAt *time.Time
	CreatedAt      time.Time
	ConsumedAt     *time.Time
}

type ReconciliationKind string

const (
	ReconcileExpiredAttempt ReconciliationKind = "EXPIRED_ATTEMPT"
	ReconcilePendingOutbox  ReconciliationKind = "PENDING_OUTBOX"
	ReconcileLostWakeup     ReconciliationKind = "LOST_WAKEUP"
	ReconcileDueTimer       ReconciliationKind = "DUE_TIMER"
	ReconcilePoisonRecord   ReconciliationKind = "POISON_RECORD"
)

type ReconciliationItem struct {
	ItemID        string
	WorkflowID    string
	PartitionID   int16
	NodeID        string
	Iteration     *int
	AttemptNumber *int64
	Kind          ReconciliationKind
	Reference     string
	Status        string
	Detail        json.RawMessage
	CreatedAt     time.Time
	UpdatedAt     time.Time
	ResolvedAt    *time.Time
}

type DueAttempt struct {
	WorkflowID    string
	NodeID        string
	Iteration     int
	AttemptNumber int64
	PartitionID   int16
	WorkflowState WorkflowState
	Revision      int64
	Deadline      time.Time
}

type DueTimer struct {
	TimerID          string
	WorkflowID       string
	NodeID           string
	Iteration        int
	PartitionID      int16
	DueAt            time.Time
	Purpose          string
	OwningRevision   int64
	WorkflowRevision int64
}

type BackpressureLimits struct {
	PendingOutbox  int
	PendingWakeups int
	OpenItems      int
}

type Backlog struct {
	PartitionID    int16
	PendingOutbox  int
	PendingWakeups int
	OpenItems      int
}

var (
	ErrOutboxNotFound         = errors.New("outbox event not found")
	ErrEventConflict          = errors.New("event payload conflicts with durable outbox event")
	ErrRelayClaimLost         = errors.New("relay claim is stale or not owned")
	ErrInvalidEvent           = errors.New("invalid transport event")
	ErrWakeupNotFound         = errors.New("scheduler wake-up not found")
	ErrWakeupClaimLost        = errors.New("scheduler wake-up claim is stale or not owned")
	ErrBacklogLimit           = errors.New("transport backlog exceeds configured limit")
	ErrReconciliationNotFound = errors.New("reconciliation item not found")
	ErrConsumerOffsetNotFound = errors.New("consumer offset not found")
)

func (s *Store) ClaimOutbox(ctx context.Context, ownerID string, limit int, lease time.Duration) ([]OutboxEvent, error) {
	return s.claimOutbox(ctx, ownerID, limit, lease, nil)
}

// ClaimOutboxForPartition is the partition-scoped variant used by a
// scheduler-owned relay and by isolated integration fixtures. The default
// ClaimOutbox method remains a global relay queue for deployments where a
// relay is not colocated with a scheduler.
func (s *Store) ClaimOutboxForPartition(ctx context.Context, ownerID string, limit int,
	lease time.Duration, partitionID int16) ([]OutboxEvent, error) {
	if partitionID < 0 || partitionID >= 16 {
		return nil, errors.New("partition is outside the frozen map")
	}
	return s.claimOutbox(ctx, ownerID, limit, lease, &partitionID)
}

func (s *Store) claimOutbox(ctx context.Context, ownerID string, limit int, lease time.Duration,
	partitionID *int16) ([]OutboxEvent, error) {
	if ownerID == "" || limit <= 0 || limit > 1000 || lease <= 0 {
		return nil, errors.New("relay owner, bounded limit, and positive claim lease are required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `
		WITH candidates AS (
			SELECT event_id
			FROM engine.outbox
			WHERE next_attempt_at <= clock_timestamp()
			  AND (publish_state = 'PENDING'
			       OR (publish_state = 'CLAIMED' AND claim_expires_at <= clock_timestamp()))
			  AND ($4::smallint IS NULL OR EXISTS (
					SELECT 1 FROM engine.workflow_executions w
					WHERE w.workflow_id = engine.outbox.workflow_id AND w.partition_id = $4
				))
			ORDER BY created_at, event_id
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		UPDATE engine.outbox AS o
		SET publish_state = 'CLAIMED', claim_owner = $2::uuid,
			claim_expires_at = clock_timestamp() + ($3::double precision * interval '1 second'),
			relay_attempts = o.relay_attempts + 1
		FROM candidates
		WHERE o.event_id = candidates.event_id
		RETURNING o.event_id::text, o.workflow_id, o.aggregate_revision, o.event_type,
			o.topic, o.schema_version, o.payload, o.publish_state, o.relay_attempts,
			o.claim_owner::text, o.claim_expires_at, o.next_attempt_at, o.last_error,
			o.created_at, o.published_at`, limit, ownerID, durationSeconds(lease), partitionID)
	if err != nil {
		return nil, fmt.Errorf("claim outbox rows: %w", err)
	}
	defer rows.Close()
	var events []OutboxEvent
	for rows.Next() {
		event, scanErr := scanOutbox(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scan claimed outbox row: %w", scanErr)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claimed outbox rows: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return events, nil
}

func (s *Store) ListOutbox(ctx context.Context, workflowID string, limit int) ([]OutboxEvent, error) {
	if limit <= 0 || limit > 10000 {
		return nil, errors.New("outbox limit must be between 1 and 10000")
	}
	query := `
		SELECT event_id::text, workflow_id, aggregate_revision, event_type,
			topic, schema_version, payload, publish_state, relay_attempts,
			claim_owner::text, claim_expires_at, next_attempt_at, last_error,
			created_at, published_at
		FROM engine.outbox`
	args := []any{limit}
	if workflowID != "" {
		query += " WHERE workflow_id = $2"
		args = []any{limit, workflowID}
	}
	query += " ORDER BY created_at, event_id LIMIT $1"
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list outbox rows: %w", err)
	}
	defer rows.Close()
	var events []OutboxEvent
	for rows.Next() {
		event, scanErr := scanOutbox(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scan outbox row: %w", scanErr)
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (s *Store) ListPendingOutbox(ctx context.Context, partitionID int16, limit int) ([]OutboxEvent, error) {
	if partitionID < 0 || partitionID >= 16 || limit <= 0 || limit > 10000 {
		return nil, errors.New("partition and bounded pending-outbox limit are required")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT o.event_id::text, o.workflow_id, o.aggregate_revision, o.event_type,
			o.topic, o.schema_version, o.payload, o.publish_state, o.relay_attempts,
			o.claim_owner::text, o.claim_expires_at, o.next_attempt_at, o.last_error,
			o.created_at, o.published_at
		FROM engine.outbox o JOIN engine.workflow_executions w ON w.workflow_id = o.workflow_id
		WHERE w.partition_id = $1 AND o.publish_state IN ('PENDING','CLAIMED')
		ORDER BY o.created_at, o.event_id LIMIT $2`, partitionID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []OutboxEvent
	for rows.Next() {
		event, scanErr := scanOutbox(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (s *Store) FinalizeOutboxPublication(ctx context.Context, eventID, ownerID string, relayAttempt int, acknowledged bool, publishErr string, retryAfter time.Duration) error {
	if eventID == "" || ownerID == "" || relayAttempt <= 0 {
		return errors.New("event, relay owner, and positive relay attempt are required")
	}
	if retryAfter <= 0 {
		retryAfter = time.Second
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var currentOwner *string
	var currentAttempt int
	var currentState OutboxPublishState
	if err := tx.QueryRow(ctx, `
		SELECT claim_owner::text, relay_attempts, publish_state
		FROM engine.outbox WHERE event_id = $1 FOR UPDATE`, eventID).Scan(
		&currentOwner, &currentAttempt, &currentState); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrOutboxNotFound
		}
		return fmt.Errorf("lock outbox publication: %w", err)
	}
	if currentState == OutboxPublished {
		return tx.Commit(ctx)
	}
	if currentState != OutboxClaimed || currentOwner == nil || *currentOwner != ownerID || currentAttempt != relayAttempt {
		return ErrRelayClaimLost
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO engine.outbox_publications
			(publication_id, event_id, relay_owner, relay_attempt, broker_acknowledged, error)
		VALUES ($1, $2, $3::uuid, $4, $5, NULLIF($6, ''))
		ON CONFLICT (event_id, relay_attempt) DO UPDATE
		SET broker_acknowledged = EXCLUDED.broker_acknowledged,
			error = EXCLUDED.error,
			published_at = EXCLUDED.published_at`,
		NewID(), eventID, ownerID, relayAttempt, acknowledged, publishErr); err != nil {
		return fmt.Errorf("record outbox publication: %w", err)
	}
	if acknowledged {
		if _, err := tx.Exec(ctx, `
			UPDATE engine.outbox
			SET publish_state = 'PUBLISHED', claim_owner = NULL,
				claim_expires_at = NULL, published_at = clock_timestamp(),
				next_attempt_at = clock_timestamp(), last_error = NULL
			WHERE event_id = $1`, eventID); err != nil {
			return fmt.Errorf("mark outbox published: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE engine.reconciliation_items
			SET status = 'RESOLVED', resolved_at = clock_timestamp(), updated_at = clock_timestamp()
			WHERE kind = 'PENDING_OUTBOX' AND reference = $1 AND status = 'OPEN'`, "outbox/"+eventID); err != nil {
			return fmt.Errorf("resolve published outbox obligation: %w", err)
		}
	} else {
		if _, err := tx.Exec(ctx, `
			UPDATE engine.outbox
			SET publish_state = 'PENDING', claim_owner = NULL,
				claim_expires_at = NULL,
				next_attempt_at = clock_timestamp() + ($2::double precision * interval '1 second'),
				last_error = NULLIF($3, '')
			WHERE event_id = $1`, eventID, durationSeconds(retryAfter), publishErr); err != nil {
			return fmt.Errorf("release failed outbox claim: %w", err)
		}
	}
	return tx.Commit(ctx)
}

func (s *Store) MarkOutboxPublished(ctx context.Context, eventID, ownerID string, relayAttempt int) error {
	return s.FinalizeOutboxPublication(ctx, eventID, ownerID, relayAttempt, true, "", 0)
}

func (s *Store) QuarantineOutbox(ctx context.Context, eventID, ownerID string, relayAttempt int, reason string) error {
	if reason == "" {
		return errors.New("quarantine reason is required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var currentOwner *string
	var currentAttempt int
	var currentState OutboxPublishState
	if err := tx.QueryRow(ctx, `SELECT claim_owner::text, relay_attempts, publish_state FROM engine.outbox WHERE event_id = $1 FOR UPDATE`, eventID).Scan(&currentOwner, &currentAttempt, &currentState); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrOutboxNotFound
		}
		return err
	}
	if currentState != OutboxClaimed || currentOwner == nil || *currentOwner != ownerID || currentAttempt != relayAttempt {
		return ErrRelayClaimLost
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO engine.outbox_publications
			(publication_id, event_id, relay_owner, relay_attempt, broker_acknowledged, error)
		VALUES ($1, $2, $3::uuid, $4, false, $5)
		ON CONFLICT (event_id, relay_attempt) DO NOTHING`, NewID(), eventID, ownerID, relayAttempt, reason); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE engine.outbox
		SET publish_state = 'QUARANTINED', claim_owner = NULL, claim_expires_at = NULL,
			last_error = $2
		WHERE event_id = $1`, eventID, reason); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) RecordInbox(ctx context.Context, message InboxMessage) (InboxResult, error) {
	if message.ConsumerID == "" || message.EventID == "" || message.Topic == "" || message.Partition < 0 ||
		message.Offset < 0 || message.SchemaVersion < 0 {
		return InboxResult{}, ErrInvalidEvent
	}
	if len(message.Payload) == 0 {
		message.Payload = json.RawMessage(`{}`)
	}
	if !json.Valid(message.Payload) {
		return InboxResult{}, ErrInvalidEvent
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return InboxResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var workflowID string
	var eventType, storedTopic string
	var storedPayload []byte
	var schemaVersion int
	var partitionID int16
	if err := tx.QueryRow(ctx, `
		SELECT workflow_id, event_type, topic, schema_version, payload,
			(SELECT partition_id FROM engine.workflow_executions w WHERE w.workflow_id = o.workflow_id)
		FROM engine.outbox o WHERE event_id = $1 FOR UPDATE`, message.EventID).Scan(
		&workflowID, &eventType, &storedTopic, &schemaVersion, &storedPayload, &partitionID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return InboxResult{}, ErrOutboxNotFound
		}
		return InboxResult{}, fmt.Errorf("read outbox event for inbox: %w", err)
	}
	if storedTopic != message.Topic || eventType != message.EventType ||
		(message.SchemaVersion != 0 && schemaVersion != message.SchemaVersion) ||
		!jsonEqual(storedPayload, message.Payload) {
		return InboxResult{}, ErrEventConflict
	}
	var existing InboxDisposition
	var inserted bool
	var storedOffset *int64
	err = tx.QueryRow(ctx, `
		INSERT INTO engine.event_inbox
			(consumer_id, event_id, disposition, topic, kafka_partition, kafka_offset)
		VALUES ($1, $2, 'ACCEPTED', $3, $4, $5)
		ON CONFLICT (consumer_id, event_id) DO NOTHING
		RETURNING disposition`, message.ConsumerID, message.EventID, message.Topic, message.Partition, message.Offset).Scan(&existing)
	if err == nil {
		inserted = true
	} else if errors.Is(err, pgx.ErrNoRows) {
		var storedTopic *string
		if scanErr := tx.QueryRow(ctx, `
			SELECT disposition, topic, kafka_offset FROM engine.event_inbox
			WHERE consumer_id = $1 AND event_id = $2 FOR UPDATE`, message.ConsumerID, message.EventID).Scan(
			&existing, &storedTopic, &storedOffset); scanErr != nil {
			return InboxResult{}, fmt.Errorf("read existing inbox disposition: %w", scanErr)
		}
		// A broker retry can publish the same stable event ID at a new Kafka
		// offset. The topic and payload remain part of the event identity;
		// partition/offset identify this delivery, not the event itself.
		if storedTopic != nil && *storedTopic != message.Topic {
			return InboxResult{}, ErrEventConflict
		}
		existing = InboxAlreadyHandled
	} else {
		return InboxResult{}, fmt.Errorf("record inbox event: %w", err)
	}
	wakeupCreated := false
	if inserted && storedTopic == EventTopic {
		commandTag, err := tx.Exec(ctx, `
			INSERT INTO engine.scheduler_wakeups
				(wakeup_id, workflow_id, partition_id, event_id, reason)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (event_id) DO NOTHING`, NewID(), workflowID, partitionID, message.EventID, "EVENT:"+eventType)
		if err != nil {
			return InboxResult{}, fmt.Errorf("create scheduler wake-up: %w", err)
		}
		wakeupCreated = commandTag.RowsAffected() == 1
	}
	nextOffset, commitOffset, err := advanceConsumerOffset(ctx, tx, message.ConsumerID, message.Topic, message.Partition, message.Offset)
	if err != nil {
		return InboxResult{}, err
	}
	// A relay crash can publish one event twice. The second delivery has a
	// different broker offset but no second inbox row. When it is exactly the
	// next offset, consume that delivery as a known duplicate and continue
	// advancing over any already-recorded contiguous rows.
	if !inserted && storedOffset != nil && *storedOffset < message.Offset && nextOffset == message.Offset {
		nextOffset, err = advanceDuplicateConsumerOffset(ctx, tx, message.ConsumerID,
			message.Topic, message.Partition, message.Offset)
		if err != nil {
			return InboxResult{}, err
		}
		commitOffset = true
	}
	if err := tx.Commit(ctx); err != nil {
		return InboxResult{}, err
	}
	if !inserted {
		existing = InboxAlreadyHandled
	}
	return InboxResult{Disposition: existing, WakeupCreated: wakeupCreated,
		NextOffset: nextOffset, CommitOffset: commitOffset}, nil
}

// QuarantineMessage durably records a broker record that cannot be matched to
// a valid outbox event. It advances the same contiguous consumer watermark as
// a normal inbox row, allowing a poison record to be acknowledged without
// pretending it was an executable event.
func (s *Store) QuarantineMessage(ctx context.Context, message InboxMessage, reason string) (InboxResult, error) {
	if message.ConsumerID == "" || message.Topic == "" || message.Partition < 0 || message.Offset < 0 || reason == "" {
		return InboxResult{}, ErrInvalidEvent
	}
	payload := message.Payload
	if payload == nil {
		payload = []byte{}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return InboxResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `
		INSERT INTO engine.transport_quarantine
			(quarantine_id, consumer_id, event_id, topic, kafka_partition,
			 kafka_offset, event_type, payload, reason)
		VALUES ($1, NULLIF($2, ''), NULLIF($3, ''), $4, $5, $6, NULLIF($7, ''), $8, $9)
		ON CONFLICT (consumer_id, topic, kafka_partition, kafka_offset) DO NOTHING`,
		NewID(), message.ConsumerID, message.EventID, message.Topic, message.Partition,
		message.Offset, message.EventType, payload, reason); err != nil {
		return InboxResult{}, fmt.Errorf("record transport poison record: %w", err)
	}
	nextOffset, commitOffset, err := advanceConsumerOffset(ctx, tx, message.ConsumerID, message.Topic,
		message.Partition, message.Offset)
	if err != nil {
		return InboxResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return InboxResult{}, err
	}
	return InboxResult{Disposition: InboxQuarantined, NextOffset: nextOffset, CommitOffset: commitOffset}, nil
}

func (s *Store) EnsureConsumerOffset(ctx context.Context, consumerID, topic string, partition int, nextOffset int64) error {
	if consumerID == "" || topic == "" || partition < 0 || nextOffset < 0 {
		return errors.New("consumer offset identity and non-negative offset are required")
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO engine.consumer_offsets (consumer_id, topic, kafka_partition, next_offset)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (consumer_id, topic, kafka_partition) DO NOTHING`, consumerID, topic, partition, nextOffset)
	return err
}

func (s *Store) GetConsumerOffset(ctx context.Context, consumerID, topic string, partition int) (ConsumerOffset, error) {
	var offset ConsumerOffset
	err := s.pool.QueryRow(ctx, `
		SELECT consumer_id, topic, kafka_partition, next_offset, updated_at
		FROM engine.consumer_offsets WHERE consumer_id = $1 AND topic = $2 AND kafka_partition = $3`, consumerID, topic, partition).Scan(
		&offset.ConsumerID, &offset.Topic, &offset.Partition, &offset.NextOffset, &offset.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ConsumerOffset{}, ErrConsumerOffsetNotFound
	}
	return offset, err
}

func advanceConsumerOffset(ctx context.Context, tx pgx.Tx, consumerID, topic string, partition int, messageOffset int64) (int64, bool, error) {
	var next int64
	err := tx.QueryRow(ctx, `
		SELECT next_offset FROM engine.consumer_offsets
		WHERE consumer_id = $1 AND topic = $2 AND kafka_partition = $3 FOR UPDATE`, consumerID, topic, partition).Scan(&next)
	if errors.Is(err, pgx.ErrNoRows) {
		next = messageOffset
		if _, err := tx.Exec(ctx, `
			INSERT INTO engine.consumer_offsets (consumer_id, topic, kafka_partition, next_offset)
			VALUES ($1, $2, $3, $4)`, consumerID, topic, partition, next); err != nil {
			return 0, false, fmt.Errorf("initialize consumer offset: %w", err)
		}
	} else if err != nil {
		return 0, false, fmt.Errorf("lock consumer offset: %w", err)
	}
	if messageOffset < next {
		return next, true, nil
	}
	if messageOffset > next {
		return next, false, nil
	}
	for {
		var exists bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM engine.event_inbox
				WHERE consumer_id = $1 AND topic = $2 AND kafka_partition = $3
				  AND kafka_offset = $4 AND disposition <> 'QUARANTINED'
			) OR EXISTS (
				SELECT 1 FROM engine.transport_quarantine
				WHERE consumer_id = $1 AND topic = $2 AND kafka_partition = $3
				  AND kafka_offset = $4
			)`, consumerID, topic, partition, next).Scan(&exists); err != nil {
			return 0, false, fmt.Errorf("check contiguous inbox offset: %w", err)
		}
		if !exists {
			break
		}
		next++
	}
	if _, err := tx.Exec(ctx, `
		UPDATE engine.consumer_offsets SET next_offset = $4, updated_at = clock_timestamp()
		WHERE consumer_id = $1 AND topic = $2 AND kafka_partition = $3`, consumerID, topic, partition, next); err != nil {
		return 0, false, fmt.Errorf("advance consumer offset: %w", err)
	}
	return next, true, nil
}

func advanceDuplicateConsumerOffset(ctx context.Context, tx pgx.Tx, consumerID, topic string,
	partition int, messageOffset int64) (int64, error) {
	next := messageOffset + 1
	for {
		var exists bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM engine.event_inbox
				WHERE consumer_id = $1 AND topic = $2 AND kafka_partition = $3
				  AND kafka_offset = $4 AND disposition <> 'QUARANTINED'
			) OR EXISTS (
				SELECT 1 FROM engine.transport_quarantine
				WHERE consumer_id = $1 AND topic = $2 AND kafka_partition = $3
				  AND kafka_offset = $4
			)`, consumerID, topic, partition, next).Scan(&exists); err != nil {
			return 0, fmt.Errorf("check duplicate delivery offset: %w", err)
		}
		if !exists {
			break
		}
		next++
	}
	if _, err := tx.Exec(ctx, `
		UPDATE engine.consumer_offsets SET next_offset = $4, updated_at = clock_timestamp()
		WHERE consumer_id = $1 AND topic = $2 AND kafka_partition = $3`, consumerID, topic, partition, next); err != nil {
		return 0, fmt.Errorf("advance duplicate delivery offset: %w", err)
	}
	return next, nil
}

func (s *Store) ListInbox(ctx context.Context, consumerID string, limit int) ([]InboxMessage, error) {
	if consumerID == "" || limit <= 0 || limit > 10000 {
		return nil, errors.New("consumer and bounded inbox limit are required")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT i.consumer_id, i.event_id::text, i.topic, i.kafka_partition,
			i.kafka_offset, o.event_type, o.payload, i.disposition
		FROM engine.event_inbox i JOIN engine.outbox o ON o.event_id = i.event_id
		WHERE i.consumer_id = $1 ORDER BY i.recorded_at LIMIT $2`, consumerID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var messages []InboxMessage
	for rows.Next() {
		var message InboxMessage
		if err := rows.Scan(&message.ConsumerID, &message.EventID, &message.Topic, &message.Partition,
			&message.Offset, &message.EventType, &message.Payload, &message.Disposition); err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}
	return messages, rows.Err()
}

func (s *Store) ListInboxForWorkflow(ctx context.Context, workflowID string, limit int) ([]InboxMessage, error) {
	if workflowID == "" || limit <= 0 || limit > 10000 {
		return nil, errors.New("workflow and bounded inbox limit are required")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT i.consumer_id, i.event_id::text, i.topic, i.kafka_partition,
			i.kafka_offset, o.event_type, o.payload, i.disposition
		FROM engine.event_inbox i JOIN engine.outbox o ON o.event_id = i.event_id
		WHERE o.workflow_id = $1 ORDER BY i.recorded_at LIMIT $2`, workflowID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var messages []InboxMessage
	for rows.Next() {
		var message InboxMessage
		if err := rows.Scan(&message.ConsumerID, &message.EventID, &message.Topic, &message.Partition,
			&message.Offset, &message.EventType, &message.Payload, &message.Disposition); err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}
	return messages, rows.Err()
}

func (s *Store) ClaimWakeups(ctx context.Context, lease LeaseRef, ownerID string, limit int, claimLease time.Duration) ([]SchedulerWakeup, error) {
	if ownerID == "" || limit <= 0 || limit > 1000 || claimLease <= 0 {
		return nil, errors.New("wake-up owner, bounded limit, and positive claim lease are required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := lockLease(ctx, tx, lease); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `
		SELECT wakeup_id::text, workflow_id, partition_id, event_id::text, reason,
			state, claim_owner::text, claim_epoch, claim_expires_at, created_at, consumed_at
		FROM engine.scheduler_wakeups
		WHERE partition_id = $1 AND (state = 'PENDING' OR (state = 'CLAIMED' AND claim_expires_at <= clock_timestamp()))
		ORDER BY created_at, wakeup_id FOR UPDATE SKIP LOCKED LIMIT $2`, lease.PartitionID, limit)
	if err != nil {
		return nil, err
	}
	var wakeups []SchedulerWakeup
	for rows.Next() {
		var wakeup SchedulerWakeup
		var claimOwner *string
		var claimEpoch *int64
		if err := rows.Scan(&wakeup.WakeupID, &wakeup.WorkflowID, &wakeup.PartitionID, &wakeup.EventID,
			&wakeup.Reason, &wakeup.State, &claimOwner, &claimEpoch, &wakeup.ClaimExpiresAt,
			&wakeup.CreatedAt, &wakeup.ConsumedAt); err != nil {
			return nil, err
		}
		if claimOwner != nil {
			wakeup.ClaimOwner = *claimOwner
		}
		if claimEpoch != nil {
			wakeup.ClaimEpoch = *claimEpoch
		}
		wakeups = append(wakeups, wakeup)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for index := range wakeups {
		wakeup := &wakeups[index]
		var claimExpiresAt time.Time
		if err := tx.QueryRow(ctx, `
			UPDATE engine.scheduler_wakeups
			SET state = 'CLAIMED', claim_owner = $2::uuid, claim_epoch = $3,
				claim_expires_at = clock_timestamp() + ($4::double precision * interval '1 second')
			WHERE wakeup_id = $1
			RETURNING claim_expires_at`, wakeup.WakeupID, ownerID, lease.Epoch, durationSeconds(claimLease)).Scan(&claimExpiresAt); err != nil {
			return nil, err
		}
		wakeup.State = WakeupClaimed
		wakeup.ClaimOwner = ownerID
		wakeup.ClaimEpoch = lease.Epoch
		wakeup.ClaimExpiresAt = &claimExpiresAt
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return wakeups, nil
}

func (s *Store) ConsumeWakeup(ctx context.Context, lease LeaseRef, wakeupID, ownerID string) error {
	if wakeupID == "" || ownerID == "" {
		return errors.New("wake-up, owner, and lease are required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := lockLease(ctx, tx, lease); err != nil {
		return err
	}
	var partitionID int16
	var state WakeupState
	var claimOwner *string
	var claimEpoch *int64
	if err := tx.QueryRow(ctx, `
		SELECT partition_id, state, claim_owner::text, claim_epoch
		FROM engine.scheduler_wakeups WHERE wakeup_id = $1 FOR UPDATE`, wakeupID).Scan(
		&partitionID, &state, &claimOwner, &claimEpoch); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrWakeupNotFound
		}
		return err
	}
	if partitionID != lease.PartitionID {
		return ErrWakeupClaimLost
	}
	if state == WakeupConsumed {
		return tx.Commit(ctx)
	}
	if state != WakeupClaimed || claimOwner == nil || *claimOwner != ownerID || claimEpoch == nil || *claimEpoch != lease.Epoch {
		return ErrWakeupClaimLost
	}
	if _, err := tx.Exec(ctx, `
		UPDATE engine.scheduler_wakeups SET state = 'CONSUMED', claim_owner = NULL,
			claim_epoch = NULL, claim_expires_at = NULL, consumed_at = clock_timestamp()
		WHERE wakeup_id = $1`, wakeupID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE engine.reconciliation_items
		SET status = 'RESOLVED', resolved_at = clock_timestamp(), updated_at = clock_timestamp()
		WHERE kind = 'LOST_WAKEUP' AND reference = $1 AND status = 'OPEN'`, "wakeup/"+wakeupID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) ListWakeups(ctx context.Context, workflowID string, limit int) ([]SchedulerWakeup, error) {
	if workflowID == "" || limit <= 0 || limit > 10000 {
		return nil, errors.New("workflow and bounded wake-up limit are required")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT wakeup_id::text, workflow_id, partition_id, event_id::text, reason,
			state, claim_owner::text, claim_epoch, claim_expires_at, created_at, consumed_at
		FROM engine.scheduler_wakeups WHERE workflow_id = $1
		ORDER BY created_at, wakeup_id LIMIT $2`, workflowID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var wakeups []SchedulerWakeup
	for rows.Next() {
		var wakeup SchedulerWakeup
		var claimOwner *string
		var claimEpoch *int64
		if err := rows.Scan(&wakeup.WakeupID, &wakeup.WorkflowID, &wakeup.PartitionID, &wakeup.EventID,
			&wakeup.Reason, &wakeup.State, &claimOwner, &claimEpoch, &wakeup.ClaimExpiresAt,
			&wakeup.CreatedAt, &wakeup.ConsumedAt); err != nil {
			return nil, err
		}
		if claimOwner != nil {
			wakeup.ClaimOwner = *claimOwner
		}
		if claimEpoch != nil {
			wakeup.ClaimEpoch = *claimEpoch
		}
		wakeups = append(wakeups, wakeup)
	}
	return wakeups, rows.Err()
}

func (s *Store) ListPendingWakeups(ctx context.Context, partitionID int16, limit int) ([]SchedulerWakeup, error) {
	if partitionID < 0 || partitionID >= 16 || limit <= 0 || limit > 10000 {
		return nil, errors.New("partition and bounded pending-wakeup limit are required")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT wakeup_id::text, workflow_id, partition_id, event_id::text, reason,
			state, claim_owner::text, claim_epoch, claim_expires_at, created_at, consumed_at
		FROM engine.scheduler_wakeups
		WHERE partition_id = $1 AND (state = 'PENDING' OR
			(state = 'CLAIMED' AND claim_expires_at <= clock_timestamp()))
		ORDER BY created_at, wakeup_id LIMIT $2`, partitionID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var wakeups []SchedulerWakeup
	for rows.Next() {
		var wakeup SchedulerWakeup
		var claimOwner *string
		var claimEpoch *int64
		if err := rows.Scan(&wakeup.WakeupID, &wakeup.WorkflowID, &wakeup.PartitionID, &wakeup.EventID,
			&wakeup.Reason, &wakeup.State, &claimOwner, &claimEpoch, &wakeup.ClaimExpiresAt,
			&wakeup.CreatedAt, &wakeup.ConsumedAt); err != nil {
			return nil, err
		}
		if claimOwner != nil {
			wakeup.ClaimOwner = *claimOwner
		}
		if claimEpoch != nil {
			wakeup.ClaimEpoch = *claimEpoch
		}
		wakeups = append(wakeups, wakeup)
	}
	return wakeups, rows.Err()
}

func (s *Store) ListDueAttempts(ctx context.Context, partitionID int16, limit int) ([]DueAttempt, error) {
	if partitionID < 0 || partitionID >= 16 || limit <= 0 || limit > 1000 {
		return nil, errors.New("partition and bounded due-attempt limit are required")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT w.workflow_id, n.node_id, n.iteration, a.attempt_number,
			w.partition_id, w.state, w.revision, a.heartbeat_deadline
		FROM engine.workflow_executions w
		JOIN engine.node_instances n ON n.workflow_id = w.workflow_id
		JOIN engine.activity_attempts a ON a.workflow_id = n.workflow_id
			AND a.node_id = n.node_id AND a.iteration = n.iteration
			AND a.attempt_number = n.current_attempt_number AND a.is_current
		WHERE w.partition_id = $1 AND w.state NOT IN ('SUCCEEDED','FAILED','REJECTED','CANCELED','ABANDONED')
			AND a.state = 'CLAIMED' AND a.heartbeat_deadline IS NOT NULL
			AND a.heartbeat_deadline <= clock_timestamp()
		ORDER BY a.heartbeat_deadline LIMIT $2`, partitionID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var due []DueAttempt
	for rows.Next() {
		var item DueAttempt
		if err := rows.Scan(&item.WorkflowID, &item.NodeID, &item.Iteration, &item.AttemptNumber,
			&item.PartitionID, &item.WorkflowState, &item.Revision, &item.Deadline); err != nil {
			return nil, err
		}
		due = append(due, item)
	}
	return due, rows.Err()
}

func (s *Store) ListDueTimers(ctx context.Context, partitionID int16, limit int) ([]DueTimer, error) {
	if partitionID < 0 || partitionID >= 16 || limit <= 0 || limit > 1000 {
		return nil, errors.New("partition and bounded due-timer limit are required")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT t.timer_id::text, t.workflow_id, t.node_id, t.iteration, w.partition_id,
			t.due_at, t.purpose, t.owning_revision, w.revision
		FROM engine.timers t JOIN engine.workflow_executions w ON w.workflow_id = t.workflow_id
		WHERE w.partition_id = $1 AND t.consumed_at IS NULL AND t.due_at <= clock_timestamp()
		ORDER BY t.due_at LIMIT $2`, partitionID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var due []DueTimer
	for rows.Next() {
		var item DueTimer
		if err := rows.Scan(&item.TimerID, &item.WorkflowID, &item.NodeID, &item.Iteration,
			&item.PartitionID, &item.DueAt, &item.Purpose, &item.OwningRevision, &item.WorkflowRevision); err != nil {
			return nil, err
		}
		due = append(due, item)
	}
	return due, rows.Err()
}

func (s *Store) UpsertReconciliationItem(ctx context.Context, item ReconciliationItem) (ReconciliationItem, error) {
	if item.WorkflowID == "" || item.PartitionID < 0 || item.PartitionID >= 16 || item.Kind == "" || item.Reference == "" {
		return ReconciliationItem{}, errors.New("reconciliation item identity is required")
	}
	if len(item.Detail) == 0 {
		item.Detail = json.RawMessage(`{}`)
	}
	if !json.Valid(item.Detail) {
		return ReconciliationItem{}, errors.New("reconciliation detail must be valid JSON")
	}
	var result ReconciliationItem
	var nodeID *string
	var iteration *int
	var attemptNumber *int64
	err := s.pool.QueryRow(ctx, `
		INSERT INTO engine.reconciliation_items
			(item_id, workflow_id, partition_id, node_id, iteration, attempt_number, kind, reference, detail)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (kind, reference) DO UPDATE
		SET detail = EXCLUDED.detail, updated_at = clock_timestamp()
		RETURNING item_id::text, workflow_id, partition_id, node_id, iteration, attempt_number,
			kind, reference, status, detail, created_at, updated_at, resolved_at`,
		NewID(), item.WorkflowID, item.PartitionID, nullableString(item.NodeID), item.Iteration,
		item.AttemptNumber, item.Kind, item.Reference, item.Detail).Scan(
		&result.ItemID, &result.WorkflowID, &result.PartitionID, &nodeID, &iteration,
		&attemptNumber, &result.Kind, &result.Reference, &result.Status, &result.Detail,
		&result.CreatedAt, &result.UpdatedAt, &result.ResolvedAt)
	if err != nil {
		return ReconciliationItem{}, fmt.Errorf("upsert reconciliation item: %w", err)
	}
	if nodeID != nil {
		result.NodeID = *nodeID
	}
	result.Iteration = iteration
	result.AttemptNumber = attemptNumber
	return result, nil
}

func (s *Store) ResolveReconciliationItem(ctx context.Context, itemID, status string) error {
	if itemID == "" || (status != "RESOLVED" && status != "ABANDONED") {
		return errors.New("item ID and RESOLVED or ABANDONED status are required")
	}
	commandTag, err := s.pool.Exec(ctx, `
		UPDATE engine.reconciliation_items
		SET status = $2, resolved_at = clock_timestamp(), updated_at = clock_timestamp()
		WHERE item_id = $1 AND status = 'OPEN'`, itemID, status)
	if err != nil {
		return err
	}
	if commandTag.RowsAffected() == 0 {
		return ErrReconciliationNotFound
	}
	return nil
}

func (s *Store) ListReconciliationItems(ctx context.Context, partitionID int16, limit int) ([]ReconciliationItem, error) {
	if partitionID < 0 || partitionID >= 16 || limit <= 0 || limit > 10000 {
		return nil, errors.New("partition and bounded reconciliation limit are required")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT item_id::text, workflow_id, partition_id, node_id, iteration, attempt_number,
			kind, reference, status, detail, created_at, updated_at, resolved_at
		FROM engine.reconciliation_items WHERE partition_id = $1 AND status = 'OPEN'
		ORDER BY created_at LIMIT $2`, partitionID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []ReconciliationItem
	for rows.Next() {
		var item ReconciliationItem
		var nodeID *string
		if err := rows.Scan(&item.ItemID, &item.WorkflowID, &item.PartitionID, &nodeID,
			&item.Iteration, &item.AttemptNumber, &item.Kind, &item.Reference, &item.Status,
			&item.Detail, &item.CreatedAt, &item.UpdatedAt, &item.ResolvedAt); err != nil {
			return nil, err
		}
		if nodeID != nil {
			item.NodeID = *nodeID
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// ListReconciliationItemsForWorkflow includes resolved and abandoned rows so
// audit/checker consumers can verify the complete obligation history. The
// partition-scoped operational listing above intentionally returns OPEN rows
// only for bounded work queues.
func (s *Store) ListReconciliationItemsForWorkflow(ctx context.Context, workflowID string, limit int) ([]ReconciliationItem, error) {
	if workflowID == "" || limit <= 0 || limit > 10000 {
		return nil, errors.New("workflow and bounded reconciliation limit are required")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT item_id::text, workflow_id, partition_id, node_id, iteration, attempt_number,
			kind, reference, status, detail, created_at, updated_at, resolved_at
		FROM engine.reconciliation_items WHERE workflow_id = $1
		ORDER BY created_at, item_id LIMIT $2`, workflowID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []ReconciliationItem
	for rows.Next() {
		var item ReconciliationItem
		var nodeID *string
		if err := rows.Scan(&item.ItemID, &item.WorkflowID, &item.PartitionID, &nodeID,
			&item.Iteration, &item.AttemptNumber, &item.Kind, &item.Reference, &item.Status,
			&item.Detail, &item.CreatedAt, &item.UpdatedAt, &item.ResolvedAt); err != nil {
			return nil, err
		}
		if nodeID != nil {
			item.NodeID = *nodeID
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) GetBacklog(ctx context.Context, partitionID int16) (Backlog, error) {
	if partitionID < 0 || partitionID >= 16 {
		return Backlog{}, errors.New("partition is outside the frozen map")
	}
	var backlog Backlog
	backlog.PartitionID = partitionID
	err := s.pool.QueryRow(ctx, `
		SELECT
			(SELECT count(*) FROM engine.outbox o JOIN engine.workflow_executions w ON w.workflow_id = o.workflow_id
			 WHERE w.partition_id = $1 AND o.publish_state IN ('PENDING','CLAIMED')),
			(SELECT count(*) FROM engine.scheduler_wakeups WHERE partition_id = $1 AND state IN ('PENDING','CLAIMED')),
			(SELECT count(*) FROM engine.reconciliation_items WHERE partition_id = $1 AND status = 'OPEN')`, partitionID).Scan(
		&backlog.PendingOutbox, &backlog.PendingWakeups, &backlog.OpenItems)
	return backlog, err
}

func (s *Store) CheckBackpressure(ctx context.Context, partitionID int16, limits BackpressureLimits) (Backlog, error) {
	backlog, err := s.GetBacklog(ctx, partitionID)
	if err != nil {
		return Backlog{}, err
	}
	if (limits.PendingOutbox > 0 && backlog.PendingOutbox > limits.PendingOutbox) ||
		(limits.PendingWakeups > 0 && backlog.PendingWakeups > limits.PendingWakeups) ||
		(limits.OpenItems > 0 && backlog.OpenItems > limits.OpenItems) {
		return backlog, fmt.Errorf("%w: partition=%d outbox=%d wakeups=%d reconciliation=%d", ErrBacklogLimit,
			partitionID, backlog.PendingOutbox, backlog.PendingWakeups, backlog.OpenItems)
	}
	return backlog, nil
}

func scanOutbox(scanner interface{ Scan(...any) error }) (OutboxEvent, error) {
	var event OutboxEvent
	var claimOwner, lastError *string
	if err := scanner.Scan(&event.EventID, &event.WorkflowID, &event.AggregateRevision,
		&event.EventType, &event.Topic, &event.SchemaVersion, &event.Payload, &event.PublishState,
		&event.RelayAttempts, &claimOwner, &event.ClaimExpiresAt, &event.NextAttemptAt,
		&lastError, &event.CreatedAt, &event.PublishedAt); err != nil {
		return OutboxEvent{}, err
	}
	if claimOwner != nil {
		event.ClaimOwner = *claimOwner
	}
	if lastError != nil {
		event.LastError = *lastError
	}
	return event, nil
}

func timePtr(value time.Time) *time.Time {
	return &value
}
