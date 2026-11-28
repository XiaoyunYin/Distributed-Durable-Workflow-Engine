package main

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"time"

	"durable-agent-execution-engine/internal/effects/k6sink"
	"durable-agent-execution-engine/internal/invariants"
	"durable-agent-execution-engine/internal/k6ingress"
	"durable-agent-execution-engine/internal/restoreledger"
	"durable-agent-execution-engine/internal/state"
	"durable-agent-execution-engine/internal/transport"
)

func (d drillInputs) checkClosedEvidence(v *drillVerdict) error {
	x, err := d.exchanges(v)
	if err != nil {
		return err
	}
	pre, err := d.checkPrewrite(v)
	if err != nil {
		v.Status = "failed"
		return err
	}
	if err := d.checkSubmissions(pre, x, v); err != nil {
		v.Status = "failed"
		return err
	}
	if err := d.checkMilestones(v); err != nil {
		return err
	}
	if err := d.checkFences(x); err != nil {
		v.Status = "failed"
		return err
	}
	if err := d.checkTransport(); err != nil {
		v.Status = "failed"
		return err
	}
	if err := d.checkOrphan(); err != nil {
		v.Unresolved = append(v.Unresolved, d.cohorts.E1.Attempt.WorkflowID)
		v.Status = "failed"
		return err
	}
	return nil
}

func (d drillInputs) checkMilestones(v *drillVerdict) error {
	h, loss, r, p := d.ledger.Header, d.loss, d.restore, d.endpoint
	t0 := loss.T0NS
	if t0 < d.work.Clock.StartNS || t0-d.work.Clock.StartNS > int64(8*time.Minute) ||
		r.CompletedNS < loss.ArchiveSealNS || r.CompletedNS-t0 > int64(180*time.Second) ||
		p.Schema != "j7.target-endpoint.v1" || p.BootID != h.BootID || p.Authority != h.Target ||
		p.RestoreSHA != d.b.original("restore.json") || p.KafkaSHA != d.b.original("kafka-fresh.json") ||
		p.SystemID != r.State.SystemID || p.Timeline != r.State.Timeline ||
		p.OldDeniedNS < r.CompletedNS || p.ForeignDeniedNS < r.CompletedNS ||
		p.OpenNS < max(d.freshKafka.CompletedNS, p.OldDeniedNS, p.ForeignDeniedNS) ||
		p.OpenNS-t0 > int64(300*time.Second) {
		return errors.New("loss, isolated restore, endpoint exclusion or open gate missed")
	}
	if err := d.witness("j7-target-endpoint", d.b.original("endpoint.json"), h.Target, r.State.Timeline,
		min(p.OldDeniedNS, p.ForeignDeniedNS), p.OpenNS, restoreledger.StateProbe); err != nil {
		return err
	}
	terminal, err := d.terminalTime()
	if err != nil {
		return err
	}
	terminalBound := d.work.Clock.StartNS + int64(terminal.Sub(d.work.Clock.StartUTC)+clockBound)
	for _, item := range []struct {
		obs                           quietObservation
		receipt, engineFile, sinkFile string
	}{
		{d.start, "quiet-start.json", "engine-quiet-start.json", "sink-quiet-start.json"},
		{d.end, "quiet-end.json", "engine-final.json", "final-sink.json"},
	} {
		q := item.obs
		if q.Schema != "j7.quiet-observation.v1" || q.BootID != h.BootID || q.Authority != h.Target ||
			q.SystemID != r.State.SystemID || q.Timeline != r.State.Timeline || q.StartedNS < terminalBound ||
			q.CompletedNS < q.StartedNS || q.EngineSHA != d.b.original(item.engineFile) ||
			q.SinkSHA != d.b.original(item.sinkFile) || !restoreledger.IsSHA256(q.SinkSHA) ||
			q.Database <= 0 || q.Database > maxDatabaseSize || q.Cluster < q.Database || q.WAL < walSegmentBytes ||
			q.SinkBytes <= 0 || q.DBTime.Before(d.work.Clock.wall(q.StartedNS).Add(-clockBound)) ||
			q.DBTime.After(d.work.Clock.wall(q.CompletedNS).Add(clockBound)) ||
			q.Broker.BeforeNS < q.StartedNS || q.Broker.AfterNS > q.CompletedNS || !q.Broker.CopiesComplete {
			return fmt.Errorf("independent observation is incomplete: %s", item.receipt)
		}
		phase := item.receipt[:len(item.receipt)-len(".json")]
		if err := d.witness(phase, d.b.original(item.receipt), h.Target, q.Timeline, q.StartedNS, q.CompletedNS,
			restoreledger.StateProbe); err != nil {
			return err
		}
	}
	if terminalBound-t0 > int64(480*time.Second) || terminalBound < v.PhaseNS["first_new_write"] ||
		d.start.SinkSHA != d.end.SinkSHA ||
		d.end.StartedNS-d.start.CompletedNS < int64(30*time.Second) || d.end.CompletedNS-t0 > int64(600*time.Second) {
		return errors.New("last terminal commit or full quiet interval missed its cutoff")
	}
	for name, stamp := range map[string]int64{
		"outage": t0, "source_volume_absent": loss.VolumeAbsent, "first_replay": r.FirstReplayNS,
		"database_ready": r.ReadyObservedNS, "replay_verified": r.RecoveredNS, "pre_write_verified": r.CompletedNS,
		"fresh_kafka_ready": d.freshKafka.CompletedNS, "endpoint_open": p.OpenNS,
		"all_terminal_upper_bound": terminalBound, "quiet_start": d.start.CompletedNS, "quiet_end": d.end.CompletedNS,
	} {
		v.PhaseNS[name] = stamp
	}
	v.Bytes["database_final"], v.Bytes["cluster_final"], v.Bytes["wal_final"], v.Bytes["sink_final"] =
		d.end.Database, d.end.Cluster, d.end.WAL, d.end.SinkBytes
	if d.watch.D1 == nil || d.watch.D2 == nil ||
		max(d.watch.D1.ObservedNS, d.watch.D2.ObservedNS)-t0 > int64(360*time.Second) {
		return errors.New("both redispatches must be witnessed by the 360-second gate")
	}
	v.PhaseNS["d1_redispatch"], v.PhaseNS["d2_redispatch"] = d.watch.D1.ObservedNS, d.watch.D2.ObservedNS
	return nil
}

func (d drillInputs) checkTransport() error {
	h, b := d.ledger.Header, d.b
	old, fresh, watch, done := d.oldKafka, d.freshKafka, d.watch, d.redispatch
	var rendered []renderedConsumer
	if err := b.decode("consumers.json", &rendered); err != nil {
		return err
	}
	if err := verifyGenerationRender(rendered, fresh.Target.RuntimeEndpoint); err != nil {
		return err
	}
	if old.Schema != "j7.source-kafka.v1" || old.BootID != h.BootID || old.Authority != h.Source ||
		!old.Broker.CopiesComplete || old.Broker.AfterNS > d.cut.StartedNS ||
		fresh.Schema != "j7.fresh-kafka.v1" || fresh.BootID != h.BootID || fresh.Authority != h.Target ||
		fresh.SourceSHA != b.original("kafka-source.json") || fresh.RestoreSHA != b.original("restore.json") ||
		!restoreledger.IsSHA256(fresh.ConfigSHA) || fresh.ConfigSHA != old.ConfigSHA ||
		fresh.TargetConsumersSHA != b.original("consumers.json") || fresh.T0NS != d.loss.T0NS ||
		fresh.OldVolumeAbsentNS < d.restore.CompletedNS || fresh.OldEndpointDeniedNS < fresh.OldVolumeAbsentNS ||
		fresh.Broker.BeforeNS < fresh.OldEndpointDeniedNS || fresh.CompletedNS < fresh.Broker.AfterNS ||
		old.Broker.Cluster == "" || fresh.Broker.Cluster == "" || old.Broker.Cluster == fresh.Broker.Cluster ||
		old.Binding.ID == fresh.Target.ID || old.Binding.Volume.Name == fresh.Target.Volume.Name ||
		old.Binding.RuntimeEndpoint == fresh.Target.RuntimeEndpoint ||
		len(fresh.Broker.Events) != 0 || len(fresh.Broker.Groups) != 0 ||
		len(old.Broker.Partitions) != 16 || len(fresh.Broker.Partitions) != 16 {
		return errors.New("fresh broker and retired source evidence are incomplete or share identity")
	}
	for _, p := range fresh.Broker.Partitions {
		if p.First != 0 || p.End != 0 || p.TopicID == "" ||
			slices.ContainsFunc(old.Broker.Partitions, func(old kafkaPartition) bool { return old.TopicID == p.TopicID }) {
			return errors.New("target did not start with fresh topic IDs and zero offsets")
		}
	}
	for _, s := range []dispatchAtCut{d.cohorts.D1, d.cohorts.D2} {
		if err := verifySourceDispatch(old.Broker, s); err != nil {
			return err
		}
	}
	if watch.Schema != "j7.redispatch-watch.v1" || watch.BootID != h.BootID ||
		watch.FreshSHA != b.original("kafka-fresh.json") || watch.OpenNS != d.endpoint.OpenNS ||
		watch.Broker.BeforeNS < watch.OpenNS || watch.Broker.AfterNS-watch.OpenNS > int64(redispatchWindow) ||
		watch.D1.ObservedNS > watch.Broker.AfterNS || watch.D2.ObservedNS > watch.Broker.AfterNS ||
		done.Schema != "j7.redispatch.v1" || done.BootID != h.BootID || done.FreshSHA != watch.FreshSHA ||
		done.WatchSHA != b.original("redispatch-watch.json") || done.EngineSHA != b.original("engine-final.json") {
		return errors.New("redispatch timing or terminal receipt is unbound")
	}
	if err := errors.Join(verifyKafkaIdentity(fresh.Broker, watch.Broker), verifyKafkaIdentity(watch.Broker, d.start.Broker),
		verifyKafkaIdentity(d.start.Broker, d.end.Broker), verifyKafkaIdentity(d.end.Broker, done.Broker)); err != nil {
		return err
	}
	if !slices.Equal(d.start.Broker.Partitions, d.end.Broker.Partitions) ||
		!slices.Equal(d.end.Broker.Partitions, done.Broker.Partitions) || len(d.start.Broker.Events) != len(d.end.Broker.Events) ||
		len(d.end.Broker.Events) != len(done.Broker.Events) {
		return errors.New("fresh broker changed during the quiet interval")
	}
	for i, event := range d.start.Broker.Events {
		if !sameKafkaEvent(event, d.end.Broker.Events[i]) || !sameKafkaEvent(event, done.Broker.Events[i]) {
			return errors.New("fresh broker event changed during the quiet interval")
		}
	}
	if err := checkFreshPositions(d.pre, d.last, done.Broker); err != nil {
		return err
	}
	g, _ := transport.ParseConsumerGeneration(transport.J7RestoreGeneration)
	for _, p := range done.Broker.Partitions {
		group := g.SchedulerID()
		if p.Topic == state.TaskTopic {
			group = g.WorkerID()
		}
		if p.End != 0 && !slices.ContainsFunc(d.last.ConsumerOffsets, func(row invariants.ConsumerOffsetRecord) bool {
			return row.ConsumerID == group && row.Topic == p.Topic && row.Partition == p.Partition && row.NextOffset == p.End
		}) {
			return errors.New("unread J7 messages remain at the broker")
		}
	}
	d1, err1 := verifyRedispatchedWorkflow(d.last, d.cohorts.D1, *watch.D1, done.Broker, watch.OpenNS)
	d2, err2 := verifyRedispatchedWorkflow(d.last, d.cohorts.D2, *watch.D2, done.Broker, watch.OpenNS)
	if err := errors.Join(err1, err2); err != nil {
		return err
	}
	if !sameDispatchResult(d1, done.D1) || !sameDispatchResult(d2, done.D2) {
		return errors.New("independent redispatch replay differs from retained receipt")
	}
	for _, name := range []string{"engine.transition_history", "engine.outbox", "engine.outbox_publications",
		"engine.event_inbox", "engine.consumer_offsets", "engine.reconciliation_items", "engine.activity_attempts",
		"engine.workflow_executions", "engine.node_instances"} {
		if d.first.Snapshot[name].SHA256 != d.last.Snapshot[name].SHA256 {
			return fmt.Errorf("durable work changed during the quiet interval in %s", name)
		}
	}
	return nil
}

func sameDispatchResult(a, b redispatchResult) bool {
	return a.WorkflowID == b.WorkflowID && a.Attempt == b.Attempt && a.ClaimSHA == b.ClaimSHA &&
		a.RedispatchRevision == b.RedispatchRevision && a.ResultRevision == b.ResultRevision &&
		a.ConsumedRevision == b.ConsumedRevision && a.Notice.ObservedNS == b.Notice.ObservedNS &&
		sameKafkaEvent(a.Notice.Event, b.Notice.Event)
}

func (d drillInputs) checkOrphan() error {
	var record orphanVerdict
	var source, rebind orphanObservation
	var start, end k6sink.Snapshot
	var engine orphanDB
	b, e := d.b, d.cohorts.E1
	for _, item := range []struct {
		name string
		out  any
	}{
		{"orphan.json", &record}, {"source-engine.json", &source}, {"reader-rebind.json", &rebind},
		{"source-sink.json", &start}, {"final-sink.json", &end}, {"final-engine.json", &engine},
	} {
		if err := b.decode(item.name, item.out); err != nil {
			return err
		}
	}
	if record.Schema != "j7.e1-reconciliation.v1" || record.BootID != d.ledger.Header.BootID ||
		record.Workflow != e.Attempt.WorkflowID || record.RestoreSHA != b.original("restore.json") ||
		record.SourceSHA != b.original("source-engine.json") || record.RebindSHA != b.original("reader-rebind.json") ||
		record.EngineSHA != b.original("final-engine.json") || record.SinkSHA != b.original("final-sink.json") ||
		record.IngressSHA != b.original("effect-ingress.jsonl") || source.SinkSHA != b.original("source-sink.json") ||
		record.OriginalResultSHA != d.loss.FinalSource.EffectSHA256 || record.RestoredResultSHA != engine.ResultSHA ||
		record.CompletedNS < d.start.CompletedNS+int64(30*time.Second) || record.CompletedNS > d.end.StartedNS || len(record.Unresolved) != 0 ||
		record.ReplacementApplies == nil || *record.ReplacementApplies != 0 ||
		record.Mutations != 1 || record.Revision != 1 {
		return errors.New("orphan reconciliation receipt is incomplete or unbound")
	}
	if err := d.witness(orphanSourcePhase, record.SourceSHA, d.ledger.Header.Source, source.DB.Timeline,
		source.DB.StartedNS, source.CompletedNS, restoreledger.StateProbe); err != nil {
		return err
	}
	if err := d.witness(orphanRebindPhase, record.RebindSHA, d.ledger.Header.Target, rebind.DB.Timeline,
		rebind.DB.StartedNS, rebind.CompletedNS, restoreledger.AuthorityChange); err != nil {
		return err
	}
	if len(rebind.DB.Attempts) != 1 || rebind.DB.SystemID != d.restore.State.SystemID ||
		rebind.DB.Timeline != d.restore.State.Timeline || rebind.DB.Address == source.DB.Address ||
		rebind.DB.Grant != source.DB.Grant || rebind.DB.StartedNS < d.restore.CompletedNS ||
		rebind.CompletedNS > d.endpoint.OpenNS || source.CompletedNS > d.cut.StartedNS {
		return errors.New("unproved E1 grant switchover and held first attempt")
	}
	key, err := hex.DecodeString(d.work.SinkKey)
	if err != nil {
		return err
	}
	var verified orphanVerdict
	t0 := d.work.Clock.wall(d.loss.T0NS)
	next, err := verifyRecoveredEffect(engine, e, d.proof, record.OriginalResultSHA, ed25519.PublicKey(key),
		rebind.DB, t0.Add(-clockBound), t0.Add(480*time.Second-clockBound), &verified)
	if err != nil {
		return err
	}
	sourceApply, lookup, err := verifyRetainedSink(start, d.sinkBefore, end, e, d.proof.Receipt, next)
	if err != nil {
		return err
	}
	raw, err := b.read("effect-ingress.jsonl")
	if err != nil {
		return err
	}
	rows, err := k6ingress.ReadJournal(bytes.NewReader(raw), d.work.RelayPeer, d.work.SinkPeer)
	if err != nil {
		return err
	}
	if err := checkIngress(rows, e, sourceApply, lookup, d.work.Clock.wall(d.cut.CompletedNS), d.work.Clock.wall(d.loss.KillNS),
		d.work.Clock.wall(rebind.CompletedNS), d.work.Clock.wall(engine.StartedNS)); err != nil {
		return err
	}
	if record.LookupCallID != lookup.CallID || record.ApplicationID != d.proof.Receipt.ApplicationID ||
		record.ResultRevision != verified.ResultRevision || record.ConsumedRevision != verified.ConsumedRevision ||
		!record.ConsumedAt.Equal(verified.ConsumedAt) || record.ReplacementClaim != next.ClaimSHA ||
		record.ReplacementFence != next.Fence || record.ReplacementAttempt != next.Attempt ||
		record.StateHash != end.Resources[0].FinalStateHash {
		return errors.New("independent orphan replay differs from the retained receipt")
	}
	return nil
}
