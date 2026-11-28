package main

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/j7fences"
	"durable-agent-execution-engine/internal/j7markers"
	"durable-agent-execution-engine/internal/k6auth"
	"durable-agent-execution-engine/internal/k6receipt"
	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/restoreledger"
	"durable-agent-execution-engine/internal/state"
	"durable-agent-execution-engine/internal/transport"
	"github.com/jackc/pgx/v5"
)

type effectCommit struct {
	proof      k6receipt.Proof
	start, end int64
	err        error
}

func sourceMarkerActions() map[int]string {
	return map[int]string{4: "submit-e1", 5: "approve-e1", 8: "claim-f1", 9: "hold-l1-and-offer-d1-d2",
		10: "capture-precut", 11: "heartbeat-held-e1", 12: "release-e1", 14: "freeze-source-writers", 15: "capture-qualification-tail"}
}

func (d *liveDrill) runMarkers(ctx context.Context) error {
	store, err := d.sourceStore(ctx)
	if err != nil {
		return err
	}
	defer store.Close()
	s := j7markers.Sender{Client: d.apps.api.client, SourceURL: d.apps.api.origin,
		BearerToken: func(context.Context) (string, error) { return d.apps.api.token, nil }}
	s.PlaceCut = func(ctx context.Context, ack int64) (restoreledger.Witness, error) {
		raw, err := d.mediaCall(ctx, "cut")
		var w restoreledger.Witness
		if err != nil {
			return w, err
		}
		if err := evidenceJSON(raw, &w); err != nil {
			return w, err
		}
		if w.StartedNS < ack {
			return w, errors.New("archive cut did not follow durable M12")
		}
		return w, nil
	}
	actions := sourceMarkerActions()
	s.AfterMarker = func(ctx context.Context, sequence int, _ int64) error {
		switch actions[sequence] {
		case "submit-e1":
			_, err := d.submit(ctx, "e1", false)
			return err
		case "approve-e1":
			return d.approveEffect(ctx, store)
		case "claim-f1":
			return d.holdFirstWorker(ctx, store)
		case "hold-l1-and-offer-d1-d2":
			return d.offerDeferred(ctx, store)
		case "capture-precut":
			if err := d.heartbeatEffect(ctx); err != nil {
				return err
			}
			return d.captureCohorts(ctx, store)
		case "heartbeat-held-e1":
			return d.heartbeatEffect(ctx)
		case "release-e1":
			d.effectDone = make(chan effectCommit, 1)
			go func() { d.effectDone <- d.commitSourceEffect(ctx, store) }()
			return nil
		case "freeze-source-writers":
			return d.freezeSource(ctx)
		case "capture-qualification-tail":
			if d.id == "q1" {
				return d.captureQualificationTail(ctx)
			}
		}
		return nil
	}
	_, err = s.Run(ctx, d.ledger, d.work.markers, d.work.work.MarkerOrigin)
	if err != nil && d.effectDone != nil {
		// The asynchronous apply owns no ledger writes. Wait for its positive
		// or uncertain response before closing the connection it sampled.
		select {
		case <-d.effectDone:
		case <-time.After(9 * time.Second):
		}
	}
	return err
}

func (d *liveDrill) heartbeatEffect(ctx context.Context) error {
	scope := d.scope("e1")
	raw, err := json.Marshal(map[string]any{"attempt_number": d.e1.Attempt, "claim_token": d.e1.Token, "extension_ms": 60000})
	if err != nil {
		return err
	}
	status, reply, err := d.apps.control.post(ctx, attemptPath(scope.Workflow, scope.Node, 0)+"/heartbeat", "", raw)
	var live struct {
		Deadline time.Time `json:"heartbeat_deadline"`
	}
	if err != nil || status != 200 || json.Unmarshal(reply, &live) != nil || !live.Deadline.After(d.e1.Deadline) {
		return errors.New("held source E1 claim heartbeat was not acknowledged")
	}
	d.e1.Deadline = live.Deadline
	return nil
}

func (d *liveDrill) approveEffect(ctx context.Context, store *state.Store) error {
	s := d.work.submission("e1")
	var intent, hash string
	if err := pollFixture(ctx, 2*time.Second, func(ctx context.Context) (bool, error) {
		err := store.Pool().QueryRow(ctx, `SELECT intent_id::text,proposal_hash FROM engine.approval_action_intents
			WHERE workflow_id=$1 AND node_id=$2 AND iteration=0`, s.ID, state.K6Activity).Scan(&intent, &hash)
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return err == nil, err
	}); err != nil {
		return err
	}
	return d.h.approveFixture(ctx, d.apps.ns, d.stamp, d.db, s.ID, intent, hash)
}
func (d *liveDrill) waitDispatch(ctx context.Context, store *state.Store, role string, attempt int64, ttl time.Duration) error {
	s := d.work.submission(role)
	return pollFixture(ctx, ttl, func(ctx context.Context) (bool, error) {
		var ready bool
		err := store.Pool().QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM engine.activity_attempts
			WHERE workflow_id=$1 AND node_id=$2 AND iteration=0 AND attempt_number=$3
			AND is_current AND state='DISPATCHABLE' AND heartbeat_deadline>clock_timestamp())`, s.ID, s.InitialNode, attempt).Scan(&ready)
		return ready, err
	})
}
func (d *liveDrill) sourceDB() j7fences.Database {
	return j7fences.Database{SystemID: d.backup.Source.SystemID, Timeline: d.backup.Source.Timeline}
}
func (d *liveDrill) scope(role string) j7fences.Scope {
	s := d.work.submission(role)
	part, _ := partition.ID(s.ID)
	return j7fences.Scope{Namespace: d.apps.ns, Workflow: s.ID, Node: s.InitialNode, Partition: int16(part)}
}
func (d *liveDrill) holdFirstWorker(ctx context.Context, store *state.Store) error {
	if _, err := d.submit(ctx, "f1", false); err != nil {
		return err
	}
	if err := d.waitDispatch(ctx, store, "f1", 1, 3*time.Second); err != nil {
		return err
	}
	s := d.scope("f1")
	var err error
	d.f1, _, err = claimFixture(ctx, d.apps.control, s.Workflow, s.Node, 0, 1)
	if err != nil {
		return err
	}
	probe, err := d.probe(store)
	if err != nil {
		return err
	}
	d.workerSource, err = probe.CaptureSourceClaim(ctx, s, d.sourceDB())
	return err
}
func (d *liveDrill) offerDeferred(ctx context.Context, store *state.Store) error {
	scope := d.scope("l1")
	if _, ok, err := store.AcquireLease(ctx, scope.Partition, state.NewID(), 60*time.Second); err != nil || !ok {
		return errors.New("cannot hold the reserved source L1 partition")
	}
	for _, name := range []string{"l1", "d1", "d2"} {
		if _, err := d.submit(ctx, name, false); err != nil {
			return err
		}
	}
	probe, err := d.probe(store)
	if err != nil {
		return err
	}
	d.ownerSource, err = probe.CaptureSourceOwner(ctx, scope, d.sourceDB())
	if err != nil {
		return err
	}
	ef := d.scope("e1")
	d.e1, _, err = claimFixture(ctx, d.apps.control, ef.Workflow, ef.Node, 0, 1)
	return err
}
func (d *liveDrill) captureCohorts(ctx context.Context, store *state.Store) error {
	start, err := d.now()
	if err != nil {
		return err
	}
	if _, err := d.mediaCall(ctx, "kafka-source"); err != nil {
		return err
	}
	var old sourceKafkaReceipt
	d.media.Source, err = readFaultJSON(d.p.raw("kafka-source.json"), &old)
	if err != nil {
		return err
	}
	q := cohortsAtCut{Schema: "j7.cohorts-at-cut.v1"}
	for i, item := range []struct {
		role string
		out  *dispatchAtCut
	}{{"d1", &q.D1}, {"d2", &q.D2}} {
		s := d.scope(item.role)
		var event string
		a := attemptAtCut{WorkflowID: s.Workflow, NodeID: s.Node, Attempt: 1}
		err := store.Pool().QueryRow(ctx, `SELECT a.heartbeat_deadline,o.event_id::text
			FROM engine.activity_attempts a
			JOIN engine.outbox o ON (o.workflow_id=a.workflow_id AND o.event_type='attempt.dispatch'
				AND (o.payload->>'attempt_number')::bigint=a.attempt_number)
			WHERE a.workflow_id=$1 AND a.node_id=$2 AND a.iteration=0 AND a.attempt_number=1
				AND a.is_current AND a.state='DISPATCHABLE' AND a.claim_token IS NULL
				AND a.heartbeat_deadline>clock_timestamp() AND o.publish_state='PUBLISHED'`, s.Workflow, s.Node).Scan(&a.Deadline, &event)
		if err != nil {
			return fmt.Errorf("source D%d was not published and still unclaimed: %w", i+1, err)
		}
		for _, e := range old.Broker.Events {
			if e.EventID == event {
				*item.out = dispatchAtCut{Attempt: a, EventID: e.EventID, Topic: e.Topic, TopicID: e.TopicID, Partition: e.Partition, Offset: e.Offset}
				break
			}
		}
		if err := verifySourceDispatch(old.Broker, *item.out); err != nil {
			return err
		}
	}
	w, l := d.workerSource, d.ownerSource
	q.F1 = attemptAtCut{WorkflowID: w.Scope.Workflow, NodeID: w.Scope.Node, Iteration: w.Scope.Iteration,
		Attempt: 1, ClaimSHA: w.Attempt.TokenSHA, Deadline: w.Attempt.Deadline}
	q.L1 = ownerAtCut{WorkflowID: l.Scope.Workflow, Partition: int(l.Scope.Partition), Owner: l.Lease.Owner, Epoch: l.Lease.Epoch, Expires: l.Lease.Expires}
	e := d.e1.Dispatch
	if e == nil || e.Attempt != 1 {
		return errors.New("source E1 has no authenticated allocation")
	}
	q.E1 = effectAtCut{Attempt: attemptAtCut{WorkflowID: e.WorkflowID, NodeID: e.NodeID, Iteration: e.Iteration,
		Attempt: 1, ClaimSHA: restoreledger.SHA256([]byte(d.e1.Token)), Deadline: d.e1.Deadline},
		Intent: e.IntentID, Resource: e.ResourceID, LogicalKey: e.LogicalKey, CanonicalHash: e.CanonicalHash, ScopeHash: e.ScopeHash,
		GrantSHA: restoreledger.SHA256([]byte(e.GrantToken)), Fence: e.Fence}
	d.cohorts = q
	sha, err := savePrivateJSON(d.p.cfg("cohorts.json"), q)
	if err != nil {
		return err
	}
	if err := d.copyEvidence(d.p.secret, map[string]string{"cohorts.json": "cohorts.json"}); err != nil {
		return err
	}
	client, err := d.grantReaderAddress(ctx)
	if err != nil {
		return err
	}
	if err := writeControllerJSON(d.media.config("orphan"), orphanConfig{Clock: d.stamp, Restore: d.media.config("restore"),
		Cohorts: d.p.cfg("cohorts.json"), Ledger: d.p.raw("ack.jsonl"), SourceDSN: d.db.sourceOrphanPath, TargetDSN: d.db.targetOrphanPath,
		SinkDSN: d.db.sinkExportPath, SinkClient: client, RelayPeer: relayURI, SinkPeer: sinkURI, Ingress: d.effect.journal,
		Namespace: d.apps.ns, Evidence: d.p.orphan}); err != nil {
		return err
	}
	if _, err := d.mediaCall(ctx, "orphan-source"); err != nil {
		return err
	}
	if err := d.copyEvidence(d.p.orphan, map[string]string{"source-engine.json": "source-engine.json", "source-sink.json": "source-sink.json"}); err != nil {
		return err
	}
	var src orphanObservation
	sourceSHA, err := readFaultJSON(d.p.raw("source-engine.json"), &src)
	if err != nil {
		return err
	}
	if err := d.witness(orphanSourcePhase, d.ledger.Header().Source, src.DB.Timeline, src.DB.StartedNS, src.CompletedNS, sourceSHA); err != nil {
		return err
	}
	end, err := d.now()
	if err != nil {
		return err
	}
	return d.witness("pre-cut-cohorts", d.ledger.Header().Source, d.backup.Source.Timeline, start, end, sha)
}
func (d *liveDrill) grantReaderAddress(ctx context.Context) (string, error) {
	dsn, err := readPrivateDSN(d.db.sourceOrphanPath)
	if err != nil {
		return "", err
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return "", err
	}
	defer conn.Close(ctx)
	rows, err := conn.Query(ctx, `SELECT DISTINCT host(client_addr) FROM pg_stat_activity
		WHERE usename=$1 AND application_name='dur053-k6-sink-grant-reader' AND client_addr IS NOT NULL`, k6auth.ReaderRole)
	if err != nil {
		return "", err
	}
	addresses, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil || len(addresses) != 1 || net.ParseIP(addresses[0]) == nil {
		return "", errors.New("retained sink has no single TLS grant reader address")
	}
	return addresses[0], nil
}

func (d *liveDrill) commitSourceEffect(ctx context.Context, store *state.Store) (x effectCommit) {
	defer func() {
		if x.err == nil {
			x.end, x.err = d.now()
		}
	}()
	x.start, x.err = d.now()
	if x.err != nil {
		return x
	}
	key := d.apps.receiptKey.Public().(ed25519.PublicKey)
	x.proof, x.err = d.effect.perform(ctx, key, d.apps.sinkToken, "apply", d.e1)
	if x.err != nil {
		return x
	}
	body, err := json.Marshal(x.proof)
	if err != nil {
		x.err = err
		return x
	}
	s := d.scope("e1")
	x.err = resultFixture(ctx, d.apps.control, s.Workflow, s.Node, 0, d.e1, body)
	if x.err != nil {
		return x
	}
	x.err = pollFixture(ctx, 8*time.Second, func(ctx context.Context) (bool, error) {
		var status string
		err := store.Pool().QueryRow(ctx, "SELECT state FROM engine.workflow_executions WHERE workflow_id=$1", s.Workflow).Scan(&status)
		return status == "SUCCEEDED", err
	})
	return x
}
func (d *liveDrill) freezeSource(ctx context.Context) error {
	var e effectCommit
	select {
	case e = <-d.effectDone:
		d.effectDone = nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(2 * time.Second):
		return errors.New("E1 outcome was not committed before the source writer freeze")
	}
	if e.err != nil {
		return e.err
	}
	sum, err := savePrivateJSON(d.p.raw("source-proof.json"), e.proof)
	if err != nil {
		return err
	}
	h := d.ledger.Header()
	if err := d.witness("post-cut-effect", h.Source, d.backup.Source.Timeline, e.start, e.end, sum); err != nil {
		return err
	}
	start, err := d.now()
	if err != nil {
		return err
	}
	d.freezing.Store(true)
	names := []string{"source-scheduler", "source-relay", "source-worker-control", "source-worker-1", "source-worker-2"}
	args := []string{"-n", d.apps.ns, "delete", "pod"}
	args = append(args, names...)
	args = append(args, "--grace-period=1", "--wait=true", "--timeout=3s")
	if _, err := d.h.kubectl(ctx, args...); err != nil {
		return err
	}
	nodes, err := d.h.kubectl(ctx, "-n", d.apps.ns, "get", "pods", "-o", "name")
	if err != nil {
		return err
	}
	for _, name := range names {
		for _, live := range splitNames(nodes) {
			if live == "pod/"+name {
				return errors.New("old application writer is still live")
			}
		}
	}
	sum, err = savePrivateJSON(d.p.cfg("source-writers-frozen.json"), map[string]any{"deleted": names, "retained": splitNames(nodes)})
	if err != nil {
		return err
	}
	end, err := d.now()
	if err != nil {
		return err
	}
	return d.witness("writers-frozen", h.Source, d.backup.Source.Timeline, start, end, sum)
}
func splitNames(raw []byte) []string { return strings.Fields(string(raw)) }
func (d *liveDrill) captureQualificationTail(ctx context.Context) error {
	start, err := d.now()
	if err != nil {
		return err
	}
	bounded, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	observer, err := state.NewCheckerFromURL(bounded, d.db.observer.local(d.db.source, engineDatabase, d.db.ca, true))
	if err != nil {
		return err
	}
	defer observer.Close()
	trace, err := d.trace(bounded, observer)
	if err != nil {
		return err
	}
	sha, err := savePrivateJSON(d.p.raw("source-tail.private.json"), trace)
	if err != nil {
		return err
	}
	end, err := d.now()
	if err != nil {
		return err
	}
	return d.witness("j7-source-tail", d.ledger.Header().Source, d.backup.Source.Timeline, start, end, sha)
}

func (d *liveDrill) reconcile(ctx context.Context) error {
	now, err := d.now()
	if err != nil {
		return err
	}
	ctx, cancel, err := phaseDeadline(ctx, now, d.loss.T0NS, 480*time.Second)
	if err != nil {
		return err
	}
	defer cancel()
	store, err := d.targetStore(ctx)
	if err != nil {
		return err
	}
	defer store.Close()
	probe, err := d.probe(store)
	if err != nil {
		return err
	}
	var boundary cutReceipt
	if _, err := readFaultJSON(d.p.raw("cut.json"), &boundary); err != nil {
		return err
	}
	target := j7fences.Target{Database: j7fences.Database{SystemID: d.restored.State.SystemID, Timeline: d.restored.State.Timeline},
		CutNS: boundary.StartedNS, T0NS: d.loss.T0NS}
	owner, err := probe.RunOwner(ctx, d.ownerSource, target)
	if err != nil {
		return err
	}
	watch := make(chan error, 1)
	stageFiles := d.media
	go func() { watch <- invokeControllerStage(ctx, stageFiles, "watch-redispatch", io.Discard, io.Discard) }()
	if err := d.apps.startRuntime(ctx, true); err != nil {
		return err
	}
	var n1 restoreledger.Exchange
	for _, role := range []string{"n1", "n2", "n3", "n4"} {
		x, err := d.submit(ctx, role, true)
		if err != nil {
			return err
		}
		if role == "n1" {
			n1 = x
		}
	}
	if err := d.recoverEffect(ctx, store); err != nil {
		return err
	}
	worker, err := probe.RunWorker(ctx, j7fences.WorkerInput{Source: d.workerSource, Target: target, ArchivedToken: d.f1.Token,
		N1Workflow: d.work.submission("n1").ID, N1Ack: n1, Result: d.work.submission("f1").Input,
		Control: j7fences.WorkerControl{Origin: d.apps.control.origin, Identity: d.apps.workerIdentity, Client: d.apps.control.client,
			Bearer: func(context.Context) (string, error) { return d.apps.workerToken, nil }}})
	if err != nil {
		return err
	}
	if _, err := savePrivateJSON(d.p.raw("worker.json"), worker); err != nil {
		return err
	}
	owner, err = probe.FinishOwner(ctx, owner)
	if err != nil {
		return err
	}
	if _, err := savePrivateJSON(d.p.raw("owner.json"), owner); err != nil {
		return err
	}
	if err := <-watch; err != nil {
		return err
	}
	d.media.Watch, err = fileSHA(d.p.evidence, "redispatch-watch.json")
	return err
}
func (d *liveDrill) recoverEffect(ctx context.Context, store *state.Store) error {
	if err := d.waitDispatch(ctx, store, "e1", 2, 100*time.Second); err != nil {
		return err
	}
	s := d.scope("e1")
	cl, _, err := claimFixture(ctx, d.apps.control, s.Workflow, s.Node, 0, 2)
	if err != nil {
		return err
	}
	proof, err := d.effect.perform(ctx, d.apps.receiptKey.Public().(ed25519.PublicKey), d.apps.sinkToken, "lookup", cl)
	if err != nil {
		return err
	}
	body, err := json.Marshal(proof)
	if err != nil {
		return err
	}
	return resultFixture(ctx, d.apps.control, s.Workflow, s.Node, 0, cl, body)
}
func (d *liveDrill) brokerDrained(ctx context.Context, store *state.Store) (bool, error) {
	b, err := openJ7Broker(d.fresh.Broker.Endpoint)
	if err != nil {
		return false, err
	}
	defer b.ids.Close()
	s, err := b.snapshot(ctx, d.now, false)
	if err != nil {
		return false, err
	}
	if len(s.Partitions) != 16 {
		return false, errors.New("incomplete fresh broker snapshot")
	}
	g, err := transport.ParseConsumerGeneration(transport.J7RestoreGeneration)
	if err != nil {
		return false, err
	}
	for _, p := range s.Partitions {
		id := g.SchedulerID()
		if p.Topic == state.TaskTopic {
			id = g.WorkerID()
		}
		var next int64
		err := store.Pool().QueryRow(ctx, `SELECT COALESCE((SELECT next_offset FROM engine.consumer_offsets
			WHERE consumer_id=$1 AND topic=$2 AND kafka_partition=$3),0)`, id, p.Topic, p.Partition).Scan(&next)
		if err != nil {
			return false, err
		}
		if next != p.End {
			return false, nil
		}
	}
	return true, nil
}
