// Package transport contains the Kafka-neutral relay and consumer protocol.
// PostgreSQL remains authoritative: a broker publication or acknowledgement
// never changes workflow state directly.
package transport

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"durable-agent-execution-engine/internal/state"
	"github.com/segmentio/kafka-go"
)

const (
	DefaultTopic       = state.EventTopic
	DefaultRelayLease  = time.Minute
	DefaultPollPeriod  = time.Second
	DefaultRetryPeriod = time.Second
	DefaultConsumerID  = "durable-agent-consumer"
)

type Broker interface {
	Publish(context.Context, state.OutboxEvent) error
	Close() error
}

// KafkaBroker publishes the stable outbox event ID as both the Kafka key and
// an explicit header. The writer waits for broker acknowledgement before
// returning, which makes the relay's post-ack crash boundary testable.
type KafkaBroker struct {
	writer       *kafka.Writer
	defaultTopic string
}

func NewKafkaBroker(brokers []string, defaultTopic string) (*KafkaBroker, error) {
	if len(brokers) == 0 {
		return nil, errors.New("at least one Kafka broker is required")
	}
	if defaultTopic == "" {
		defaultTopic = DefaultTopic
	}
	return &KafkaBroker{writer: &kafka.Writer{
		Addr: kafka.TCP(brokers...),
		// The relay supports separate task and event topics. kafka-go requires
		// a writer topic and message topic to be mutually exclusive, so the
		// event's durable topic is supplied on each message.
		Topic:        "",
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
		BatchTimeout: 10 * time.Millisecond,
	}, defaultTopic: defaultTopic}, nil
}

func (b *KafkaBroker) Publish(ctx context.Context, event state.OutboxEvent) error {
	if b == nil || b.writer == nil {
		return errors.New("Kafka broker is not initialized")
	}
	topic := event.Topic
	if topic == "" {
		topic = b.defaultTopic
	}
	return b.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(event.EventID),
		Value: event.Payload,
		Headers: []kafka.Header{
			{Key: "event-id", Value: []byte(event.EventID)},
			{Key: "event-type", Value: []byte(event.EventType)},
			{Key: "schema-version", Value: []byte(fmt.Sprint(event.SchemaVersion))},
			{Key: "workflow-id", Value: []byte(event.WorkflowID)},
		},
	})
}

func (b *KafkaBroker) Close() error {
	if b == nil || b.writer == nil {
		return nil
	}
	return b.writer.Close()
}

// MemoryBroker is a deterministic test broker. The hook runs after the
// message is appended, which models a broker acknowledgement that can be
// followed by a relay process crash.
type MemoryBroker struct {
	mu        sync.Mutex
	events    []state.OutboxEvent
	Failures  int
	OnPublish func(state.OutboxEvent) error
}

func (b *MemoryBroker) Publish(_ context.Context, event state.OutboxEvent) error {
	b.mu.Lock()
	if b.Failures > 0 {
		b.Failures--
		b.mu.Unlock()
		return errors.New("injected broker publish failure")
	}
	b.events = append(b.events, cloneEvent(event))
	hook := b.OnPublish
	b.mu.Unlock()
	if hook != nil {
		return hook(event)
	}
	return nil
}

func (b *MemoryBroker) Events() []state.OutboxEvent {
	b.mu.Lock()
	defer b.mu.Unlock()
	result := make([]state.OutboxEvent, len(b.events))
	for index := range b.events {
		result[index] = cloneEvent(b.events[index])
	}
	return result
}

func (b *MemoryBroker) Close() error { return nil }

func cloneEvent(event state.OutboxEvent) state.OutboxEvent {
	event.Payload = append([]byte(nil), event.Payload...)
	return event
}

type RelayConfig struct {
	OwnerID       string
	PartitionID   *int16
	BatchSize     int
	ClaimLease    time.Duration
	RetryBackoff  time.Duration
	PollInterval  time.Duration
	AfterBoundary func(string) error
	// OnError observes durable relay-loop failures while the loop remains
	// alive for a later retry. Runtime wiring uses this for logs/metrics.
	OnError func(error)
	// OnSuccess observes a completed relay pass. It is intentionally separate
	// from row-level publication metrics so a runtime can re-mark readiness
	// after a temporary dependency outage.
	OnSuccess func(RelayReport)
}

type Relay struct {
	Store     *state.Store
	Broker    Broker
	Config    RelayConfig
	errorMu   sync.Mutex
	runErrors uint64
}

type RelayReport struct {
	Claimed     int
	Published   int
	Failed      int
	Quarantined int
}

func NewRelay(store *state.Store, broker Broker, config RelayConfig) *Relay {
	if config.OwnerID == "" {
		config.OwnerID = state.NewID()
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 10
	}
	if config.ClaimLease <= 0 {
		config.ClaimLease = DefaultRelayLease
	}
	if config.RetryBackoff <= 0 {
		config.RetryBackoff = DefaultRetryPeriod
	}
	if config.PollInterval <= 0 {
		config.PollInterval = DefaultPollPeriod
	}
	return &Relay{Store: store, Broker: broker, Config: config}
}

func (r *Relay) RunOnce(ctx context.Context) (RelayReport, error) {
	if r == nil || r.Store == nil || r.Broker == nil {
		return RelayReport{}, errors.New("relay requires a store and broker")
	}
	var events []state.OutboxEvent
	var err error
	if r.Config.PartitionID == nil {
		events, err = r.Store.ClaimOutbox(ctx, r.Config.OwnerID, r.Config.BatchSize, r.Config.ClaimLease)
	} else {
		events, err = r.Store.ClaimOutboxForPartition(ctx, r.Config.OwnerID, r.Config.BatchSize,
			r.Config.ClaimLease, *r.Config.PartitionID)
	}
	if err != nil {
		return RelayReport{}, err
	}
	report := RelayReport{Claimed: len(events)}
	for _, event := range events {
		if !supportedEventType(event.EventType) {
			if quarantineErr := r.Store.QuarantineOutbox(ctx, event.EventID, r.Config.OwnerID, event.RelayAttempts, "unsupported event type: "+event.EventType); quarantineErr != nil {
				return report, quarantineErr
			}
			report.Quarantined++
			continue
		}
		if err := r.boundary("before_publish"); err != nil {
			return report, err
		}
		publishErr := r.Broker.Publish(ctx, event)
		if publishErr == nil {
			if err := r.boundary("after_broker_ack"); err != nil {
				return report, err
			}
			if err := r.Store.FinalizeOutboxPublication(ctx, event.EventID, r.Config.OwnerID,
				event.RelayAttempts, true, "", 0); err != nil {
				return report, err
			}
			report.Published++
			continue
		}
		if err := r.Store.FinalizeOutboxPublication(ctx, event.EventID, r.Config.OwnerID,
			event.RelayAttempts, false, publishErr.Error(), r.Config.RetryBackoff); err != nil {
			return report, err
		}
		report.Failed++
	}
	if r.Config.OnSuccess != nil {
		r.Config.OnSuccess(report)
	}
	return report, nil
}

func (r *Relay) boundary(name string) error {
	if r.Config.AfterBoundary == nil {
		return nil
	}
	return r.Config.AfterBoundary(name)
}

// Run reacts quickly to PostgreSQL NOTIFY but always keeps a ticker-based
// fallback. A notification is only a hint; every poll re-reads pending rows,
// so a missed notification cannot strand an outbox obligation.
func (r *Relay) Run(ctx context.Context) error {
	if r == nil || r.Store == nil || r.Broker == nil {
		return errors.New("relay requires a store and broker")
	}
	// A relay process must remain alive across a temporary broker or database
	// outage. RunOnce records row-level failures durably; this loop retries the
	// scan on the next notification or poll instead of turning an availability
	// blip into a permanently stopped relay.
	r.runOnceAndReportError(ctx)
	ticker := time.NewTicker(r.Config.PollInterval)
	defer ticker.Stop()
	wakeups := r.listen(ctx)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			r.runOnceAndReportError(ctx)
		case <-wakeups:
			r.runOnceAndReportError(ctx)
		}
	}
}

func (r *Relay) runOnceAndReportError(ctx context.Context) {
	if _, err := r.RunOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
		r.errorMu.Lock()
		r.runErrors++
		r.errorMu.Unlock()
		if r.Config.OnError != nil {
			r.Config.OnError(err)
		}
	}
}

// ErrorCount reports relay-loop pass failures observed since construction.
// It remains useful when no logging hook is configured and is intentionally
// separate from row-level publish failures, which are durably recorded.
func (r *Relay) ErrorCount() uint64 {
	if r == nil {
		return 0
	}
	r.errorMu.Lock()
	defer r.errorMu.Unlock()
	return r.runErrors
}

func (r *Relay) listen(ctx context.Context) <-chan struct{} {
	wakeups := make(chan struct{}, 1)
	go func() {
		connection, err := r.Store.Pool().Acquire(ctx)
		if err != nil {
			return
		}
		defer connection.Release()
		if _, err := connection.Exec(ctx, `LISTEN durable_agent_outbox`); err != nil {
			return
		}
		for {
			if _, err := connection.Conn().WaitForNotification(ctx); err != nil {
				return
			}
			select {
			case wakeups <- struct{}{}:
			default:
			}
		}
	}()
	return wakeups
}

func supportedEventType(eventType string) bool {
	_, ok := state.EventDefinitionFor(eventType)
	return ok
}

type Message struct {
	EventID       string
	Topic         string
	Partition     int
	Offset        int64
	EventType     string
	SchemaVersion int
	Payload       []byte
}

type MessageSource interface {
	Receive(context.Context) (Message, error)
	Commit(context.Context, Message, int64) error
	Close() error
}

// MemorySource provides deterministic delivery and commit observations for
// transport fault tests. It deliberately returns messages in the supplied
// order, allowing tests to inject an offset gap or a duplicate delivery.
type MemorySource struct {
	mu       sync.Mutex
	messages []Message
	commits  []int64
	closed   bool
}

func NewMemorySource(messages []Message) *MemorySource {
	copyMessages := make([]Message, len(messages))
	copy(copyMessages, messages)
	return &MemorySource{messages: copyMessages}
}

func (s *MemorySource) Receive(ctx context.Context) (Message, error) {
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return Message{}, errors.New("memory source is closed")
		}
		if len(s.messages) != 0 {
			message := s.messages[0]
			s.messages = s.messages[1:]
			s.mu.Unlock()
			return message, nil
		}
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return Message{}, ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func (s *MemorySource) Commit(_ context.Context, _ Message, nextOffset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("memory source is closed")
	}
	s.commits = append(s.commits, nextOffset)
	return nil
}

func (s *MemorySource) Commits() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int64(nil), s.commits...)
}

func (s *MemorySource) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}

type KafkaSource struct {
	reader *kafka.Reader
}

func NewKafkaSource(brokers []string, topic, groupID string) (*KafkaSource, error) {
	if len(brokers) == 0 || topic == "" || groupID == "" {
		return nil, errors.New("Kafka brokers, topic, and group ID are required")
	}
	return &KafkaSource{reader: kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers, Topic: topic, GroupID: groupID,
		MinBytes: 1, MaxBytes: 10e6, MaxWait: 250 * time.Millisecond,
		// A newly-created study group starts at records published after the
		// source starts; existing groups still resume from committed offsets.
		StartOffset: kafka.LastOffset,
	})}, nil
}

func (s *KafkaSource) Receive(ctx context.Context) (Message, error) {
	if s == nil || s.reader == nil {
		return Message{}, errors.New("Kafka source is not initialized")
	}
	m, err := s.reader.FetchMessage(ctx)
	if err != nil {
		return Message{}, err
	}
	eventID := string(headerValue(m.Headers, "event-id"))
	if eventID == "" {
		eventID = string(m.Key)
	}
	schemaVersion := 0
	if rawVersion := headerValue(m.Headers, "schema-version"); len(rawVersion) != 0 {
		var parseErr error
		schemaVersion, parseErr = strconv.Atoi(string(rawVersion))
		if parseErr != nil || schemaVersion <= 0 {
			// Preserve the delivery identity and let Consumer.Process durably
			// quarantine the poison record instead of losing it in Receive.
			schemaVersion = -1
		}
	} else {
		// Kafka messages produced by this relay always carry the schema marker;
		// a missing marker is a poison record, not an implicit legacy version.
		schemaVersion = -1
	}
	return Message{EventID: eventID, Topic: m.Topic, Partition: m.Partition,
		Offset: m.Offset, EventType: string(headerValue(m.Headers, "event-type")),
		SchemaVersion: schemaVersion, Payload: append([]byte(nil), m.Value...)}, nil
}

func (s *KafkaSource) Commit(ctx context.Context, message Message, nextOffset int64) error {
	if s == nil || s.reader == nil {
		return errors.New("Kafka source is not initialized")
	}
	if nextOffset <= message.Offset {
		return errors.New("next Kafka offset must be after the message")
	}
	return s.reader.CommitMessages(ctx, kafka.Message{Topic: message.Topic,
		Partition: message.Partition, Offset: nextOffset - 1})
}

func (s *KafkaSource) Close() error {
	if s == nil || s.reader == nil {
		return nil
	}
	return s.reader.Close()
}

func headerValue(headers []kafka.Header, key string) []byte {
	for _, header := range headers {
		if header.Key == key {
			return header.Value
		}
	}
	return nil
}

type ConsumerConfig struct {
	ConsumerID    string
	MaxConcurrent int
	Handler       func(context.Context, Message) error
}

type Consumer struct {
	Store  *state.Store
	Source MessageSource
	Config ConsumerConfig
}

type ConsumeReport struct {
	Message     Message
	Disposition state.InboxDisposition
	NextOffset  int64
	Committed   bool
}

func NewConsumer(store *state.Store, source MessageSource, config ConsumerConfig) *Consumer {
	if config.ConsumerID == "" {
		config.ConsumerID = DefaultConsumerID
	}
	if config.MaxConcurrent <= 0 {
		config.MaxConcurrent = 4
	}
	return &Consumer{Store: store, Source: source, Config: config}
}

func (c *Consumer) Process(ctx context.Context, message Message) (ConsumeReport, error) {
	if c == nil || c.Store == nil || c.Source == nil {
		return ConsumeReport{}, errors.New("consumer requires a store and source")
	}
	result, err := c.Store.RecordInbox(ctx, state.InboxMessage{ConsumerID: c.Config.ConsumerID,
		EventID: message.EventID, Topic: message.Topic, Partition: message.Partition,
		Offset: message.Offset, EventType: message.EventType, SchemaVersion: message.SchemaVersion,
		Payload: message.Payload})
	if err != nil {
		if !errors.Is(err, state.ErrInvalidEvent) && !errors.Is(err, state.ErrOutboxNotFound) &&
			!errors.Is(err, state.ErrEventConflict) {
			return ConsumeReport{}, err
		}
		poison, quarantineErr := c.Store.QuarantineMessage(ctx, state.InboxMessage{
			ConsumerID: c.Config.ConsumerID, EventID: message.EventID, Topic: message.Topic,
			Partition: message.Partition, Offset: message.Offset, EventType: message.EventType,
			Payload: message.Payload}, err.Error())
		if quarantineErr != nil {
			return ConsumeReport{}, quarantineErr
		}
		if poison.CommitOffset {
			if commitErr := c.Source.Commit(ctx, message, poison.NextOffset); commitErr != nil {
				return ConsumeReport{}, commitErr
			}
		}
		return ConsumeReport{Message: message, Disposition: poison.Disposition,
			NextOffset: poison.NextOffset, Committed: poison.CommitOffset}, nil
	}
	if result.CommitOffset {
		if err := c.Source.Commit(ctx, message, result.NextOffset); err != nil {
			return ConsumeReport{}, err
		}
	}
	if c.Config.Handler != nil && result.Disposition == state.InboxAccepted {
		if err := c.Config.Handler(ctx, message); err != nil {
			return ConsumeReport{Message: message, Disposition: result.Disposition,
				NextOffset: result.NextOffset, Committed: result.CommitOffset}, err
		}
	}
	return ConsumeReport{Message: message, Disposition: result.Disposition,
		NextOffset: result.NextOffset, Committed: result.CommitOffset}, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	if c == nil || c.Store == nil || c.Source == nil {
		return errors.New("consumer requires a store and source")
	}
	semaphore := make(chan struct{}, c.Config.MaxConcurrent)
	errorsCh := make(chan error, 1)
	for {
		message, err := c.Source.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		select {
		case semaphore <- struct{}{}:
		case err := <-errorsCh:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
		go func(message Message) {
			defer func() { <-semaphore }()
			if _, err := c.Process(ctx, message); err != nil {
				select {
				case errorsCh <- err:
				default:
				}
			}
		}(message)
	}
}
