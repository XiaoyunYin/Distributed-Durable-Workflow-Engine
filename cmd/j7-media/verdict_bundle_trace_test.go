package main

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/invariants"
	"durable-agent-execution-engine/internal/restoreledger"
	"durable-agent-execution-engine/internal/state"
	"durable-agent-execution-engine/internal/transport"
)

func (f *drillFixture) transportFixture(o orphanFixture) (cohortsAtCut, sourceKafkaReceipt, freshKafkaReceipt, redispatchWatch, kafkaSnapshot) {
	old, fresh := brokerCase("source", f.ns(57)), brokerCase("target", f.ns(93))
	old.CopiesComplete, fresh.CopiesComplete = true, true
	final := fresh
	final.Partitions = slices.Clone(fresh.Partitions)
	g, _ := transport.ParseConsumerGeneration(transport.J7RestoreGeneration)
	final.Groups = []string{g.SchedulerID(), g.WorkerID()}
	h := &f.header
	src := sourceKafkaReceipt{Schema: "j7.source-kafka.v1", ConfigSHA: testDigest, BootID: h.BootID, Authority: h.Source,
		Binding: kafkaBinding{ID: "source-kafka", Volume: sourceVolume{Name: "j7-source-kafka"}, RuntimeEndpoint: "j7-old:19092"}}
	dst := freshKafkaReceipt{Schema: "j7.fresh-kafka.v1", ConfigSHA: testDigest, BootID: h.BootID, Authority: h.Target,
		CompletedNS: f.ns(93) + 10, T0NS: f.ns(76), OldVolumeAbsentNS: f.ns(92), OldEndpointDeniedNS: f.ns(92) + 5,
		Target: kafkaBinding{ID: "target-kafka", Volume: sourceVolume{Name: "j7-target-kafka"}, RuntimeEndpoint: "j7-new:19092"}}
	f.files["consumers.json"] = []byte(`[
		{"role":"scheduler","env":[{"name":"RUNTIME_KAFKA_GENERATION","value":"j7-restore"},{"name":"KAFKA_BOOTSTRAP_SERVERS","value":"j7-new:19092"}]},
		{"role":"worker","env":[{"name":"RUNTIME_KAFKA_GENERATION","value":"j7-restore"},{"name":"KAFKA_BOOTSTRAP_SERVERS","value":"j7-new:19092"}]},
		{"role":"worker-control","env":[{"name":"RUNTIME_KAFKA_GENERATION","value":"j7-restore"}]}
	]`)
	dst.TargetConsumersSHA = restoreledger.SHA256(f.files["consumers.json"])
	c := cohortsAtCut{Schema: "j7.cohorts-at-cut.v1", E1: o.hold,
		F1: attemptAtCut{WorkflowID: "f1", NodeID: "pure.echo", Attempt: 1,
			ClaimSHA: restoreledger.SHA256([]byte("f1-first-token")), Deadline: f.clock.wall(f.ns(99))},
		L1: ownerAtCut{WorkflowID: "l1", Partition: 5, Owner: "l1-old-owner", Epoch: 1, Expires: f.clock.wall(f.ns(90))}}
	wp := redispatchWatch{Schema: "j7.redispatch-watch.v1", BootID: h.BootID, OpenNS: f.ns(95)}
	for i, slot := range []*dispatchAtCut{&c.D1, &c.D2} {
		id := fmt.Sprintf("d%d", i+1)
		base := eventAttempt{WorkflowID: id, NodeID: "pure.echo", Attempt: 1}
		part := 8
		oldEvent := kafkaEvent{WorkflowID: id, EventID: id + "-source-dispatch", Topic: state.TaskTopic,
			Kind: "attempt.dispatch", TopicID: old.Partitions[part].TopicID, Offset: int64(i),
			PayloadSHA: testDigest, BrokerTime: f.clock.wall(f.ns(51)), Attempt: &base}
		old.Events = append(old.Events, oldEvent)
		old.Partitions[part].End++
		*slot = dispatchAtCut{Attempt: attemptAtCut{WorkflowID: id, NodeID: base.NodeID, Attempt: 1,
			Deadline: f.clock.wall(f.ns(98))}, EventID: oldEvent.EventID, Topic: oldEvent.Topic, TopicID: oldEvent.TopicID,
			Offset: oldEvent.Offset}
		newEvent := oldEvent
		newEvent.Kind, newEvent.EventID, newEvent.TopicID = "attempt.redispatch", id+"-target-dispatch", final.Partitions[part].TopicID
		newEvent.BrokerTime = f.clock.wall(f.ns(100))
		final.Events = append(final.Events, newEvent)
		final.Partitions[part].End++
		notice := &redispatchNotice{ObservedNS: f.ns(100), Event: newEvent}
		if i == 0 {
			wp.D1 = notice
		} else {
			wp.D2 = notice
		}
	}
	src.Broker, dst.Broker, wp.Broker = old, fresh, final
	wp.Broker.BeforeNS, wp.Broker.AfterNS = f.ns(100)-1, f.ns(100)+1
	return c, src, dst, wp, final
}

type fixtureTrace struct {
	f *drillFixture
	t invariants.Trace
}

func (b *fixtureTrace) add(table string, value any) {
	raw, err := json.Marshal(value)
	if err != nil {
		b.f.t.Fatal(err)
	}
	row := b.t.Snapshot[table]
	row.Rows = append(row.Rows, raw)
	b.t.Snapshot[table] = row
}

func (b *fixtureTrace) seal() invariants.Trace {
	for key, row := range b.t.Snapshot {
		raw, err := json.Marshal(row.Rows)
		if err != nil {
			b.f.t.Fatal(err)
		}
		row.Count, row.SHA256 = len(row.Rows), restoreledger.SHA256(raw)
		b.t.Snapshot[key] = row
	}
	return b.t
}

func (f *drillFixture) newTrace() *fixtureTrace {
	b := &fixtureTrace{f: f, t: invariants.Trace{Snapshot: map[string]invariants.SnapshotTable{}}}
	// The complete export must include empty relations as well as populated ones.
	for _, name := range strings.Fields(`
		engine.schema_migrations engine.workflow_executions engine.workflow_definitions engine.node_instances engine.activity_attempts
		engine.partition_leases engine.transition_history engine.outbox engine.event_inbox engine.cancellation_requests engine.lease_acquisitions
		engine.timers engine.checkpoints engine.approval_action_intents engine.consumer_offsets engine.scheduler_wakeups engine.reconciliation_items
		engine.outbox_publications engine.attempt_result_evidence engine.effect_records engine.effect_call_attempts engine.effect_resource_fences
		engine.sandbox_effect_state engine.effect_resolution_audit engine.transport_quarantine engine.retry_policies engine.dur050_admission_slots
		effects.effect_records effects.effect_call_attempts effects.effect_resource_fences effects.sandbox_effect_state effects.effect_resolution_audit
	`) {
		b.t.Snapshot[name] = invariants.SnapshotTable{Rows: []json.RawMessage{}}
	}
	for i := 1; i <= 23; i++ {
		b.add("engine.schema_migrations", map[string]int{"version": i})
	}
	for _, r := range []invariants.ConsumerOffsetRecord{
		{ConsumerID: "runtime-workers-v1", Topic: state.TaskTopic, NextOffset: 2},
		{ConsumerID: "runtime-schedulers-v1", Topic: state.EventTopic, NextOffset: 1},
	} {
		b.t.ConsumerOffsets = append(b.t.ConsumerOffsets, r)
		b.add("engine.consumer_offsets", map[string]any{"consumer_id": r.ConsumerID, "topic": r.Topic, "next_offset": r.NextOffset})
	}
	return b
}

func (b *fixtureTrace) workflow(s plannedSubmission, namespace, st string, revision int64, partition int) {
	hash := "sub-v1:" + testDigest
	b.t.Workflows = append(b.t.Workflows, invariants.WorkflowRecord{WorkflowID: s.ID, State: st, Revision: revision})
	b.t.Submissions = append(b.t.Submissions, invariants.SubmissionRecord{Namespace: namespace, Key: s.Key, Hash: hash, WorkflowID: s.ID})
	b.add("engine.workflow_executions", workflowAtRestore{ID: s.ID, Namespace: namespace, Key: s.Key,
		Definition: s.Definition, PayloadHash: hash, Version: 1, Partition: partition, State: st})
	node := nodeAtRestore{ID: s.ID, Node: s.InitialNode, Input: s.Input}
	if st == "SUCCEEDED" {
		node.Result = node.Input
		b.t.Results = append(b.t.Results, invariants.AcceptedResult{WorkflowID: s.ID, NodeID: s.InitialNode, Accepted: true})
	}
	b.add("engine.node_instances", node)
}

func (b *fixtureTrace) event(workflow, event, kind, topic string, rev int64, payload any) {
	b.t.Outbox = append(b.t.Outbox, invariants.OutboxRecord{WorkflowID: workflow, EventID: event, EventType: kind,
		Topic: topic, AggregateRevision: rev, PublishState: "PUBLISHED", PayloadValid: true})
	b.add("engine.outbox", map[string]any{"workflow_id": workflow, "event_id": event, "payload": payload})
	b.add("engine.outbox_publications", map[string]any{"event_id": event, "broker_acknowledged": true})
}

func (b *fixtureTrace) attempt(row attemptAtRestore) {
	b.t.Attempts = append(b.t.Attempts, invariants.AttemptRecord{WorkflowID: row.WorkflowID, NodeID: row.NodeID,
		AttemptNumber: row.Attempt, State: row.State, ClaimToken: row.Claim, WorkerID: row.Worker,
		WorkerRequestID: row.Request, Outcome: row.Outcome, IsCurrent: row.Current, ResultRecorded: present(row.Result)})
	b.add("engine.activity_attempts", row)
}

func (b *fixtureTrace) history(s plannedSubmission, rev int64, actor, from, to, reason string, sec int64, attempt int64) {
	var epoch int64
	r := invariants.HistoryRecord{WorkflowID: s.ID, Revision: rev, ActorKind: actor, OldState: from, NewState: to, Reason: reason}
	if actor == "scheduler" {
		epoch = 2
		r.SchedulerEpoch = &epoch
	}
	b.t.History = append(b.t.History, r)
	b.add("engine.transition_history", dispatchHistory{WorkflowID: s.ID, Revision: rev, ActorKind: actor,
		NodeID: s.InitialNode, Attempt: attempt, Epoch: epoch, Reason: reason, NewState: to, CreatedAt: b.f.clock.wall(b.f.ns(sec))})
}

func (f *drillFixture) engineFixtures(media *faultFixture, work drillWorkload, c cohortsAtCut, watch redispatchWatch) (invariants.Trace, invariants.Trace) {
	pre, final := f.newTrace(), f.newTrace()
	ns := work.Fixture.Namespace
	pure := func(b *fixtureTrace, s plannedSubmission, terminalSec int64) {
		b.workflow(s, ns, "SUCCEEDED", 2, 1)
		b.history(s, 1, "client", "", "RUNNABLE", "WORKFLOW_CREATED", 5, 0)
		b.event(s.ID, s.ID+"-created", "workflow.created", state.EventTopic, 1, struct{}{})
		b.history(s, 2, "scheduler", "RUNNABLE", "SUCCEEDED", "FIXTURE_FINISHED", terminalSec, 0)
	}
	for _, m := range media.plan.Markers[:12] {
		s := plannedSubmission{ID: m.WorkflowID, Key: m.SubmissionKey, Definition: work.Fixture.DefinitionID,
			InitialNode: "pure.echo", Input: m.ExpectedOutput}
		pure(pre, s, 57)
		pure(final, s, 57)
	}
	for i, h := range []dispatchAtCut{c.D1, c.D2} {
		s := work.NonMarkers[i]
		msg := []*redispatchNotice{watch.D1, watch.D2}[i].Event
		first := attemptAtRestore{WorkflowID: s.ID, NodeID: s.InitialNode, Attempt: 1,
			Deadline: h.Attempt.Deadline, Class: "PURE_ACTIVITY", State: "DISPATCHABLE", Outcome: "NONE", Current: true}
		pre.workflow(s, ns, "WAITING_ACTIVITY", 2, 1)
		pre.attempt(first)
		pre.event(s.ID, h.EventID, "attempt.dispatch", state.TaskTopic, 2, msg.Attempt)
		final.workflow(s, ns, "SUCCEEDED", 6, 1)
		first.State, first.Outcome, first.Current, first.Claim, first.Worker, first.Request, first.Result =
			"SUCCEEDED", "KNOWN_SUCCESS", false, s.ID+"-token", "worker", s.ID+"-claim", s.Input
		final.attempt(first)
		final.history(s, 1, "client", "", "RUNNABLE", "WORKFLOW_CREATED", 4, 0)
		final.event(s.ID, s.ID+"-created", "workflow.created", state.EventTopic, 1, struct{}{})
		final.history(s, 2, "scheduler", "RUNNABLE", "WAITING_ACTIVITY", "FIXTURE_DISPATCH", 5, 1)
		final.event(s.ID, h.EventID, "attempt.dispatch", state.TaskTopic, 2, msg.Attempt)
		final.history(s, 3, "scheduler", "WAITING_ACTIVITY", "WAITING_ACTIVITY", "REDISPATCH_UNCLAIMED", 99, 1)
		final.event(s.ID, msg.EventID, msg.Kind, msg.Topic, 3, msg.Attempt)
		final.history(s, 4, "worker", "WAITING_ACTIVITY", "WAITING_ACTIVITY", "ATTEMPT_RESULT_RECORDED", 102, 1)
		final.event(s.ID, s.ID+"-result", "activity.result", state.EventTopic, 4, struct{}{})
		final.history(s, 5, "scheduler", "WAITING_ACTIVITY", "RUNNABLE", "RESULT_CONSUMED", 103, 1)
		final.event(s.ID, s.ID+"-consumed", "workflow.result_consumed", state.EventTopic, 5, struct{}{})
		final.history(s, 6, "scheduler", "RUNNABLE", "SUCCEEDED", "GRAPH_ADVANCED", 104, 0)
		final.event(s.ID, s.ID+"-advanced", "graph.advanced", state.EventTopic, 6, struct{}{})
		final.t.Inbox = append(final.t.Inbox, invariants.InboxRecord{ConsumerID: "runtime-workers-v1-j7-restore",
			EventID: msg.EventID, Topic: msg.Topic, Offset: msg.Offset, Disposition: "ACCEPTED"})
		final.add("engine.event_inbox", map[string]any{"event_id": msg.EventID, "consumer_id": "runtime-workers-v1-j7-restore"})
	}
	final.t.ConsumerOffsets = append(final.t.ConsumerOffsets, invariants.ConsumerOffsetRecord{
		ConsumerID: "runtime-workers-v1-j7-restore", Topic: state.TaskTopic, NextOffset: 2})
	final.add("engine.consumer_offsets", map[string]any{"consumer_id": "runtime-workers-v1-j7-restore", "topic": state.TaskTopic, "next_offset": 2})
	pre.workflow(work.NonMarkers[2], ns, "WAITING_ACTIVITY", 2, 1)
	pre.attempt(attemptAtRestore{WorkflowID: "f1", NodeID: "pure.echo", Attempt: 1,
		Claim: "f1-first-token", Worker: "worker", Request: "f1-claim",
		Deadline: c.F1.Deadline, State: "CLAIMED", Outcome: "NONE", Class: "PURE_ACTIVITY", Current: true})
	pre.workflow(work.NonMarkers[3], ns, "RUNNABLE", 1, c.L1.Partition)
	pre.add("engine.partition_leases", map[string]any{"partition_id": c.L1.Partition, "owner_id": c.L1.Owner,
		"epoch": c.L1.Epoch, "lease_expires_at": c.L1.Expires})
	e1 := c.E1
	pre.workflow(work.NonMarkers[4], ns, "WAITING_ACTIVITY", 5, 1)
	pre.attempt(attemptAtRestore{WorkflowID: "e1", NodeID: e1.Attempt.NodeID, Attempt: 1,
		Claim: "first token", Worker: "worker", Request: "e1-claim", Deadline: e1.Attempt.Deadline, State: "CLAIMED",
		Outcome: "NONE", Class: "COOPERATING_EFFECT", Current: true, Fence: e1.Fence, LogicalKey: e1.LogicalKey, ScopeHash: e1.ScopeHash})
	pre.t.Approvals = append(pre.t.Approvals, invariants.ApprovalRecord{WorkflowID: "e1", NodeID: e1.Attempt.NodeID,
		IntentID: e1.Intent, CanonicalArgumentHash: e1.CanonicalHash, GrantScopeHash: e1.ScopeHash,
		ExpectedResourceRevision: "0", Decision: "APPROVED", DispatchStatus: "GRANTED", GrantToken: "grant"})
	pre.add("engine.approval_action_intents", map[string]any{"workflow_id": "e1", "grant_token": "grant"})
	for _, s := range work.NonMarkers[2:] {
		pure(final, s, 107)
	}
	return pre.seal(), final.seal()
}

func (f *drillFixture) quietFixtures(trace invariants.Trace, b kafkaSnapshot, sinkSHA string) (quietObservation, quietObservation) {
	q := func(phase, name string, second int64) quietObservation {
		at := f.ns(second)
		r := quietObservation{Schema: "j7.quiet-observation.v1", Authority: f.header.Target, BootID: f.header.BootID,
			SystemID: 123, Timeline: 2, StartedNS: at, CompletedNS: at + 1000, DBTime: f.clock.wall(at),
			EngineSHA: f.put(name, trace), SinkSHA: sinkSHA, Database: 25, Cluster: 50, WAL: walSegmentBytes, SinkBytes: 10,
			Broker: b}
		r.Broker.BeforeNS, r.Broker.AfterNS = at+10, at+20
		f.witness(f.header.Target, "quiet-"+phase, f.put("quiet-"+phase+".json", r), 2, r.StartedNS, r.CompletedNS)
		return r
	}
	return q("start", "engine-quiet-start.json", 110), q("end", "engine-final.json", 147)
}

func (f *drillFixture) resourcesFixture(end int64) {
	w := resourceWindow{Schema: "j7.resource-samples.v1", CPUs: 4, InstalledKiB: 12 << 20, StartupFreeKiB: 8 << 20,
		EngineLimit: 100, SinkLimit: 16, RoleLimits: map[string]quota{
			"engine-db": {1000, 1 << 30}, "sink-db": {500, 512 << 20}, "kafka": {1000, 1536 << 20},
			"api": {500, 256 << 20}, "scheduler": {500, 256 << 20}, "relay": {500, 256 << 20},
			"worker-1": {500, 512 << 20}, "worker-2": {500, 512 << 20}, "sink": {500, 512 << 20},
		}}
	phases := map[int64]string{10: "backup", 76: "outage", 86: "replay", 148: "export"}
	for sec := int64(0); f.ns(sec) < end+int64(2*time.Second); sec++ {
		if sec%5 != 0 && phases[sec] == "" {
			continue
		}
		w.Samples = append(w.Samples, resourceSample{AtNS: f.ns(sec), Phase: phases[sec], FreeKiB: 8 << 20,
			AvailableKiB: 3 << 20, EngineSessions: 12, SinkSessions: 2})
	}
	f.put("resource-samples.json", w)
}
