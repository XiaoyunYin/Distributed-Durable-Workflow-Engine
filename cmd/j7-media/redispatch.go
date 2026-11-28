package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"time"

	"durable-agent-execution-engine/internal/invariants"
	"durable-agent-execution-engine/internal/restoreledger"
	"durable-agent-execution-engine/internal/state"
	"durable-agent-execution-engine/internal/transport"
)

const redispatchWindow = 45 * time.Second

type redispatchNotice struct {
	ObservedNS int64      `json:"observed_ns"`
	Event      kafkaEvent `json:"event"`
}

type redispatchWatch struct {
	Schema   string            `json:"schema"`
	BootID   string            `json:"boot_id"`
	FreshSHA string            `json:"fresh_kafka_sha256"`
	OpenNS   int64             `json:"scheduler_open_ns"`
	D1       *redispatchNotice `json:"d1,omitempty"`
	D2       *redispatchNotice `json:"d2,omitempty"`
	Broker   kafkaSnapshot     `json:"broker_observation"`
}

type redispatchResult struct {
	WorkflowID         string           `json:"workflow_id"`
	Attempt            int64            `json:"attempt_number"`
	ClaimSHA           string           `json:"claim_token_sha256"`
	RedispatchRevision int64            `json:"redispatch_revision"`
	ResultRevision     int64            `json:"result_revision"`
	ConsumedRevision   int64            `json:"consumed_revision"`
	Notice             redispatchNotice `json:"notice"`
}

type redispatchReceipt struct {
	Schema      string           `json:"schema"`
	BootID      string           `json:"boot_id"`
	FreshSHA    string           `json:"fresh_kafka_sha256"`
	WatchSHA    string           `json:"redispatch_watch_sha256"`
	EngineSHA   string           `json:"engine_snapshot_sha256"`
	Broker      kafkaSnapshot    `json:"broker_observation"`
	D1          redispatchResult `json:"d1"`
	D2          redispatchResult `json:"d2"`
	CompletedNS int64            `json:"completed_ns,omitempty"`
}

func verifyKafkaIdentity(initial, current kafkaSnapshot) error {
	if initial.Cluster != current.Cluster || initial.Endpoint != current.Endpoint ||
		initial.AfterNS > current.BeforeNS || !current.CopiesComplete || len(initial.Partitions) != 16 ||
		len(current.Partitions) != 16 {
		return errors.New("Kafka observation is not on the witnessed fresh broker")
	}
	for i, p := range initial.Partitions {
		q := current.Partitions[i]
		if p.Topic != q.Topic || p.TopicID != q.TopicID || p.Partition != q.Partition || p.First != q.First || q.End < p.End {
			return errors.New("fresh Kafka topic identity or offset prefix changed")
		}
	}
	return nil
}

func findRedispatch(broker kafkaSnapshot, hold dispatchAtCut) (*kafkaEvent, error) {
	var found *kafkaEvent
	for i := range broker.Events {
		e := &broker.Events[i]
		if e.EventID == hold.EventID {
			return nil, fmt.Errorf("archived dispatch was replayed for %s", hold.Attempt.WorkflowID)
		}
		if e.Topic != state.TaskTopic || e.WorkflowID != hold.Attempt.WorkflowID {
			continue
		}
		if e.Kind != "attempt.redispatch" || !sameAttempt(e.Attempt, hold.Attempt) {
			return nil, fmt.Errorf("first fresh task must redispatch the archived attempt for %s", hold.Attempt.WorkflowID)
		}
		if found != nil {
			// Kafka can carry an identical retry of the same durable outbox
			// event. The worker inbox must account for that physical offset.
			if e.EventID != found.EventID || e.TopicID != found.TopicID || e.Partition != found.Partition ||
				e.Offset <= found.Offset || e.PayloadSHA != found.PayloadSHA {
				return nil, fmt.Errorf("unexpected additional redispatch for %s", hold.Attempt.WorkflowID)
			}
			continue
		}
		found = e
	}
	return found, nil
}

func sameKafkaEvent(a, b kafkaEvent) bool {
	return a.Topic == b.Topic && a.TopicID == b.TopicID && a.Partition == b.Partition && a.Offset == b.Offset &&
		a.EventID == b.EventID && a.Kind == b.Kind && a.WorkflowID == b.WorkflowID &&
		a.BrokerTime.Equal(b.BrokerTime) && a.PayloadSHA == b.PayloadSHA &&
		((a.Attempt == nil && b.Attempt == nil) || (a.Attempt != nil && b.Attempt != nil && *a.Attempt == *b.Attempt))
}

func watchRedispatch(ctx context.Context, snapshot func(context.Context) (kafkaSnapshot, error),
	clock restoreledger.Clock, in restoreInputs, fresh freshKafkaReceipt, sha string, opened int64) (redispatchWatch, error) {
	w := redispatchWatch{Schema: "j7.redispatch-watch.v1", BootID: in.header.BootID, FreshSHA: sha, OpenNS: opened}
	at, err := clock()
	if err != nil {
		return w, err
	}
	if opened < fresh.CompletedNS || opened-fresh.T0NS > int64(300*time.Second) {
		return w, errors.New("scheduler open did not follow fresh Kafka admission within the endpoint gate")
	}
	ctx, cancel, err := phaseDeadline(ctx, at, opened, redispatchWindow)
	if err != nil {
		return w, err
	}
	defer cancel()
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		w.Broker, err = snapshot(ctx)
		if err != nil {
			return w, err
		}
		if err := verifyKafkaIdentity(fresh.Broker, w.Broker); err != nil {
			return w, err
		}
		if w.Broker.BeforeNS < opened || w.Broker.AfterNS-opened > int64(redispatchWindow) {
			return w, errors.New("fresh Kafka redispatch observation missed the 45-second window")
		}
		for _, item := range []struct {
			hold   dispatchAtCut
			notice **redispatchNotice
		}{{in.cohorts.D1, &w.D1}, {in.cohorts.D2, &w.D2}} {
			event, err := findRedispatch(w.Broker, item.hold)
			if err != nil {
				return w, err
			}
			if event != nil && *item.notice == nil {
				*item.notice = &redispatchNotice{w.Broker.AfterNS, *event}
			} else if *item.notice != nil && (event == nil || !sameKafkaEvent((*item.notice).Event, *event)) {
				return w, errors.New("previously witnessed redispatch disappeared or changed")
			}
		}
		if w.D1 != nil && w.D2 != nil {
			return w, nil
		}
		select {
		case <-ctx.Done():
			return w, errors.New("both redispatch notices must be observed within 45 seconds")
		case <-tick.C:
		}
	}
}

type inboxKey struct {
	group, topic string
	partition    int
	offset       int64
}
type offsetKey struct {
	group, topic string
	partition    int
}

func checkFreshPositions(before, after invariants.Trace, broker kafkaSnapshot) error {
	legacy := transport.ConsumerGeneration{}
	g, _ := transport.ParseConsumerGeneration(transport.J7RestoreGeneration)
	topicGroup := map[string]string{state.TaskTopic: g.WorkerID(), state.EventTopic: g.SchedulerID()}
	legacyGroup := func(id string) bool { return id == legacy.SchedulerID() || id == legacy.WorkerID() }
	for _, table := range []string{"engine.event_inbox", "engine.consumer_offsets"} {
		filter := func(t invariants.Trace) ([]string, error) {
			rows, err := rawTable[json.RawMessage](t, table)
			if err != nil {
				return nil, err
			}
			var got []string
			for _, raw := range rows {
				var key struct {
					Group string `json:"consumer_id"`
				}
				if err := json.Unmarshal(raw, &key); err != nil {
					return nil, err
				}
				if legacyGroup(key.Group) {
					var normalized bytes.Buffer
					if err := json.Compact(&normalized, raw); err != nil {
						return nil, err
					}
					got = append(got, normalized.String())
				}
			}
			slices.Sort(got)
			return got, nil
		}
		previous, err := filter(before)
		if err != nil {
			return err
		}
		current, err := filter(after)
		if err != nil || !slices.Equal(previous, current) {
			return fmt.Errorf("legacy rows changed in %s", table)
		}
	}
	if !slices.Equal(broker.Groups, []string{g.SchedulerID(), g.WorkerID()}) {
		return errors.New("fresh broker group identities differ from the J7 target")
	}
	ends := map[offsetKey]int64{}
	for _, p := range broker.Partitions {
		ends[offsetKey{topicGroup[p.Topic], p.Topic, p.Partition}] = p.End
	}
	positions := map[offsetKey]int64{}
	for _, p := range after.ConsumerOffsets {
		if legacyGroup(p.ConsumerID) {
			continue
		}
		k := offsetKey{p.ConsumerID, p.Topic, p.Partition}
		end, ok := ends[k]
		if _, duplicate := positions[k]; duplicate || !ok || p.NextOffset < 0 || p.NextOffset > end {
			return errors.New("new consumer watermark exceeds the witnessed Kafka range or uses unexpected routing")
		}
		positions[k] = p.NextOffset
	}
	events := map[inboxKey]kafkaEvent{}
	for _, event := range broker.Events {
		key := inboxKey{topicGroup[event.Topic], event.Topic, event.Partition, event.Offset}
		if _, seen := events[key]; seen {
			return errors.New("duplicate Kafka physical offset in copied broker evidence")
		}
		events[key] = event
	}
	exact := map[inboxKey]string{}
	original := map[string]invariants.InboxRecord{}
	for _, i := range after.Inbox {
		if legacyGroup(i.ConsumerID) {
			continue
		}
		loc := inboxKey{i.ConsumerID, i.Topic, i.Partition, i.Offset}
		b, ok := events[loc]
		if !ok || i.EventID != b.EventID || i.Disposition != "ACCEPTED" || exact[loc] != "" {
			return errors.New("fresh inbox row does not match an accepted physical Kafka record")
		}
		exact[loc] = i.EventID
		original[i.ConsumerID+"\x00"+i.EventID] = i
	}
	for p, end := range ends {
		next := positions[p]
		if end > 0 && (next == 0 || exact[inboxKey{p.group, p.topic, p.partition, 0}] == "") {
			return fmt.Errorf("fresh %s/%d lacks an accepted first offset at zero", p.topic, p.partition)
		}
		for offset := int64(0); offset < next; offset++ {
			key := inboxKey{p.group, p.topic, p.partition, offset}
			event, ok := events[key]
			if !ok {
				return errors.New("fresh durable watermark crosses an unwitnessed Kafka offset")
			}
			if exact[key] == event.EventID {
				continue
			}
			first, reused := original[p.group+"\x00"+event.EventID]
			earlier := events[inboxKey{p.group, p.topic, first.Partition, first.Offset}]
			if !reused || first.Topic != p.topic || first.Partition != p.partition || first.Offset >= offset ||
				earlier.EventID != event.EventID || earlier.PayloadSHA != event.PayloadSHA || earlier.Kind != event.Kind {
				return errors.New("fresh durable watermark crossed a gap without a matching earlier disposition")
			}
		}
	}
	return nil
}

type dispatchHistory struct {
	WorkflowID string    `json:"workflow_id"`
	Revision   int64     `json:"revision"`
	ActorKind  string    `json:"actor_kind"`
	Epoch      int64     `json:"scheduler_epoch"`
	NodeID     string    `json:"node_id"`
	Iteration  int       `json:"iteration"`
	Attempt    int64     `json:"attempt_number"`
	Reason     string    `json:"reason"`
	NewState   string    `json:"new_state"`
	CreatedAt  time.Time `json:"created_at"`
}

func verifyRedispatchedWorkflow(t invariants.Trace, hold dispatchAtCut, known redispatchNotice,
	broker kafkaSnapshot, open int64) (redispatchResult, error) {
	r := redispatchResult{WorkflowID: hold.Attempt.WorkflowID, Attempt: hold.Attempt.Attempt, Notice: known}
	message, err := findRedispatch(broker, hold)
	if err != nil || message == nil || !sameKafkaEvent(*message, known.Event) ||
		known.ObservedNS < open || known.ObservedNS-open > int64(redispatchWindow) {
		return r, errors.New("timely redispatch does not match the retained Kafka copy")
	}
	var dispatches, records, consumed, terminal int
	var finished dispatchHistory
	history, err := rawTable[dispatchHistory](t, "engine.transition_history")
	if err != nil {
		return r, err
	}
	for _, row := range history {
		h := hold.Attempt
		if row.WorkflowID != h.WorkflowID {
			continue
		}
		switch row.Reason {
		case "REDISPATCH_UNCLAIMED", "ATTEMPT_RESULT_RECORDED", "RESULT_CONSUMED":
			if row.NodeID != h.NodeID || row.Iteration != h.Iteration || row.Attempt != h.Attempt ||
				row.CreatedAt.Before(h.Deadline) {
				return r, errors.New("redispatch or result history disagrees with the archived deadline and attempt")
			}
		}
		switch row.Reason {
		case "REDISPATCH_UNCLAIMED":
			if row.ActorKind != "scheduler" {
				return r, errors.New("redispatch lacks scheduler authority")
			}
			dispatches++
			r.RedispatchRevision = row.Revision
		case "ATTEMPT_RESULT_RECORDED":
			if row.ActorKind != "worker" {
				return r, errors.New("result lacks worker authority")
			}
			records++
			r.ResultRevision = row.Revision
		case "RESULT_CONSUMED":
			if row.ActorKind != "scheduler" || row.Epoch <= 0 || row.NewState != "RUNNABLE" {
				return r, errors.New("redispatch did not end in a scheduler-consumed success")
			}
			consumed++
			r.ConsumedRevision = row.Revision
		case "GRAPH_ADVANCED":
			if row.NewState == "SUCCEEDED" {
				if row.ActorKind != "scheduler" || row.Epoch <= 0 || row.CreatedAt.Before(h.Deadline) {
					return r, errors.New("redispatch graph completion lacks scheduler authority")
				}
				terminal++
				finished = row
			}
		}
	}
	if dispatches != 1 || records != 1 || consumed != 1 || r.RedispatchRevision >= r.ResultRevision ||
		r.ResultRevision >= r.ConsumedRevision || terminal != 1 || finished.Revision <= r.ConsumedRevision {
		return r, errors.New("redispatch does not have one ordered dispatch, recorded result and consumed result")
	}
	var attempts int
	for _, a := range t.Attempts {
		if a.WorkflowID != r.WorkflowID {
			continue
		}
		attempts++
		if a.NodeID != hold.Attempt.NodeID || a.Iteration != hold.Attempt.Iteration || a.AttemptNumber != r.Attempt ||
			a.ClaimToken == "" || a.WorkerID == "" || a.WorkerRequestID == "" || a.State != "SUCCEEDED" ||
			a.Outcome != "KNOWN_SUCCESS" || !a.ResultRecorded {
			return r, errors.New("redispatch did not preserve and complete its original attempt number")
		}
		r.ClaimSHA = credentialDigest(a.ClaimToken)
	}
	if attempts != 1 || !slices.ContainsFunc(t.Results, func(a invariants.AcceptedResult) bool {
		return a.WorkflowID == r.WorkflowID && a.NodeID == hold.Attempt.NodeID && a.Iteration == hold.Attempt.Iteration && a.Accepted
	}) || !slices.ContainsFunc(t.Workflows, func(a invariants.WorkflowRecord) bool {
		return a.WorkflowID == r.WorkflowID && a.State == "SUCCEEDED" && a.Revision == finished.Revision
	}) {
		return r, errors.New("redispatched workflow has no accepted terminal result")
	}
	var outbox int
	for _, row := range t.Outbox {
		if row.WorkflowID == r.WorkflowID && row.EventType == "attempt.redispatch" {
			outbox++
			if row.EventID != message.EventID || row.Topic != message.Topic || row.AggregateRevision != r.RedispatchRevision ||
				row.PublishState != "PUBLISHED" {
				return r, errors.New("fresh dispatch is not the durable published outbox row")
			}
		}
	}
	type dispatchPayload struct {
		EventID string          `json:"event_id"`
		Payload json.RawMessage `json:"payload"`
	}
	eventRows, err := rawTable[dispatchPayload](t, "engine.outbox")
	if err != nil {
		return r, err
	}
	if !slices.ContainsFunc(eventRows, func(row dispatchPayload) bool {
		var attempt eventAttempt
		return row.EventID == message.EventID && json.Unmarshal(row.Payload, &attempt) == nil && attempt == *message.Attempt
	}) {
		return r, errors.New("fresh task payload differs from the durable redispatch record")
	}
	type publish struct {
		ID  string `json:"event_id"`
		Ack bool   `json:"broker_acknowledged"`
	}
	rows, err := rawTable[publish](t, "engine.outbox_publications")
	if err != nil {
		return r, err
	}
	if outbox != 1 || !slices.ContainsFunc(rows, func(p publish) bool { return p.ID == message.EventID && p.Ack }) {
		return r, errors.New("redispatch has no durable broker acknowledgment")
	}
	g, _ := transport.ParseConsumerGeneration(transport.J7RestoreGeneration)
	if !slices.ContainsFunc(t.Inbox, func(i invariants.InboxRecord) bool {
		return i.ConsumerID == g.WorkerID() && i.EventID == message.EventID && i.Topic == message.Topic &&
			i.Partition == message.Partition && i.Offset == message.Offset && i.Disposition == "ACCEPTED"
	}) {
		return r, errors.New("redispatch has no fresh worker-control disposition")
	}
	return r, nil
}

func runRedispatchStage(ctx context.Context, args []string, out, diagnostics io.Writer) error {
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	flags.SetOutput(diagnostics)
	config := flags.String("config", "", "private absolute Kafka drill configuration")
	freshSHA := flags.String("fresh-sha256", "", "digest of fresh Kafka admission receipt")
	openNS := flags.Int64("scheduler-open-ns", 0, "host monotonic scheduler endpoint opening")
	watchPath := flags.String("watch-receipt", "", "absolute path for the Kafka notice timing witness")
	watchSHA := flags.String("watch-sha256", "", "digest of the redispatch timing witness")
	enginePath := flags.String("engine-snapshot", "", "absolute path of a read-only engine export after D1 and D2 complete")
	engineSHA := flags.String("engine-sha256", "", "digest of the engine export")
	resultPath := flags.String("receipt", "", "absolute path for the redispatch verification receipt")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 || !filepath.IsAbs(*config) || !filepath.IsAbs(*watchPath) {
		return errors.New("redispatch stage requires absolute configuration and watch-receipt paths")
	}
	now, boot, err := restoreledger.HostClock()
	if err != nil {
		return err
	}
	var cfg kafkaConfig
	cfgSHA, err := readFaultJSON(*config, &cfg)
	if err != nil {
		return err
	}
	if err := cfg.validate(); err != nil {
		return err
	}
	in, err := loadRestoreInputs(cfg.RestoreConfig, boot)
	if err != nil {
		return err
	}
	var fresh freshKafkaReceipt
	sha, err := readFaultJSON(cfg.FreshReceipt, &fresh)
	if err != nil {
		return err
	}
	if cfg.Ledger != in.fault.Ledger || fresh.Schema != "j7.fresh-kafka.v1" || sha != *freshSHA ||
		fresh.ConfigSHA != cfgSHA || fresh.BootID != boot || fresh.T0NS != in.loss.T0NS ||
		fresh.Authority != in.header.Target || fresh.CompletedNS < fresh.Broker.AfterNS ||
		fresh.Broker.Endpoint != cfg.Target.hostEndpoint() || fresh.Broker.BeforeNS <= fresh.OldEndpointDeniedNS ||
		fresh.OldEndpointDeniedNS < fresh.OldVolumeAbsentNS || fresh.OldVolumeAbsentNS < fresh.T0NS {
		return errors.New("redispatch requires a completed fresh-Kafka receipt on this boot")
	}
	emit := json.NewEncoder(out)
	b, err := openJ7Broker(cfg.Target.hostEndpoint())
	if err != nil {
		return err
	}
	defer b.ids.Close()
	if args[0] == "watch-redispatch" {
		_, digest, err := faultReceipt(*watchPath, func() (redispatchWatch, error) {
			return watchRedispatch(ctx, func(ctx context.Context) (kafkaSnapshot, error) { return b.snapshot(ctx, now, true) },
				now, in, fresh, sha, *openNS)
		})
		if err != nil {
			return err
		}
		return emit.Encode(map[string]string{"redispatch_watch_sha256": digest})
	}
	if !filepath.IsAbs(*enginePath) || !filepath.IsAbs(*resultPath) {
		return errors.New("redispatch verification requires absolute snapshot and result paths")
	}
	var watch redispatchWatch
	digest, err := readFaultJSON(*watchPath, &watch)
	if err != nil {
		return err
	}
	if digest != *watchSHA || watch.Schema != "j7.redispatch-watch.v1" || watch.BootID != boot ||
		watch.FreshSHA != sha || watch.OpenNS < fresh.CompletedNS || watch.D1 == nil || watch.D2 == nil ||
		watch.Broker.BeforeNS < watch.OpenNS || watch.Broker.AfterNS-watch.OpenNS > int64(redispatchWindow) ||
		watch.D1.ObservedNS > watch.Broker.AfterNS || watch.D2.ObservedNS > watch.Broker.AfterNS {
		return errors.New("redispatch timing evidence is incomplete or unbound")
	}
	var restore restoreReceipt
	restoreSHA, err := readFaultJSON(filepath.Join(in.config.Evidence, "restore.json"), &restore)
	if err != nil || restoreSHA != fresh.RestoreSHA {
		return errors.New("redispatch requires the same pre-write engine export")
	}
	prewrite, err := loadTraceArtifact(filepath.Join(in.config.Evidence, "engine-pre-write.json"), restore.SnapshotSHA)
	if err != nil {
		return err
	}
	after, err := loadTraceArtifact(*enginePath, *engineSHA)
	if err != nil {
		return err
	}
	if err := invariants.SnapshotIntegrity(after); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	_, digest, err = faultReceipt(*resultPath, func() (redispatchReceipt, error) {
		r := redispatchReceipt{Schema: "j7.redispatch.v1", BootID: boot, FreshSHA: sha, WatchSHA: *watchSHA, EngineSHA: *engineSHA}
		r.Broker, err = b.snapshot(ctx, now, true)
		if err != nil {
			return r, err
		}
		if err := verifyKafkaIdentity(fresh.Broker, r.Broker); err != nil {
			return r, err
		}
		if err := checkFreshPositions(prewrite, after, r.Broker); err != nil {
			return r, err
		}
		r.D1, err = verifyRedispatchedWorkflow(after, in.cohorts.D1, *watch.D1, r.Broker, watch.OpenNS)
		if err != nil {
			return r, err
		}
		r.D2, err = verifyRedispatchedWorkflow(after, in.cohorts.D2, *watch.D2, r.Broker, watch.OpenNS)
		if err != nil {
			return r, err
		}
		r.CompletedNS, err = now()
		return r, err
	})
	if err != nil {
		return err
	}
	return emit.Encode(map[string]string{"redispatch_receipt_sha256": digest})
}
