package main

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/invariants/k6collect"
	"durable-agent-execution-engine/internal/invariants/k6live"
	"durable-agent-execution-engine/internal/invariants/k6v1"
	"github.com/jackc/pgx/v5"
)

const (
	parityIntent   = "00000000-0000-0000-0000-000000000011"
	parityApp      = "00000000-0000-0000-0000-000000000012"
	parityDecision = "2026-01-01T12:00:00.000000000Z"
)

func TestK6QuietLiveParity(t *testing.T) {
	for _, tc := range []struct {
		name, groupFixture string
	}{
		{"false", ""},
		{"GroupMembership", "valid"},
		{"GroupAssignmentMismatch", "mismatch"},
		{"GroupMissingProof", "missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			engine, dsn := k6TestDatabase(t)
			bindK6Database(t, engine, "engine-instance", "reset-a")
			m := k6Manifest(engine.Config().Database, false)
			if tc.groupFixture != "" {
				m.Ordinal, m.Seed, m.Cell, m.Episode = 80, 530081, "medium-10", "medium-10-0"
			}
			cfg := k6live.Config{Schema: k6live.ConfigVersion, DatabaseURL: dsn}
			plan, arrivals := k6Sources(m, time.Now().Add(-time.Minute), "pure.echo")
			workflow := plan.Windows[0].Submissions[0]
			seedParityAttempt(t, engine, workflow, "PURE_ACTIVITY", 1)
			cfg.ExternalDir = t.TempDir()
			sources := parityExternal(t, &m, false, workflow.Key)
			if tc.groupFixture != "" {
				broker := &sources.Broker.Rows[0].Data
				broker.State, broker.Generation = "stable", 2
				member := k6v1.BrokerMember{ID: "member", ClientID: "worker", Generation: 2,
					Assignments: []k6v1.PartitionIdentity{{TopicID: "topic", Partition: 0}}}
				broker.Members = []k6v1.BrokerMember{member}
				if tc.groupFixture != "missing" {
					if tc.groupFixture == "mismatch" {
						member.Assignments = []k6v1.PartitionIdentity{}
					}
					broker.Coordinator = &k6v1.CoordinatorObservation{Generation: 2, Members: []k6v1.BrokerMember{member}}
				}
				sealFixtureStream(t, sources.Broker, time.Now())
			}
			cfg.Manifest = m
			cfg.EpisodeDir = filepath.Join(t.TempDir(), "public")
			cfg = writeK6Sources(t, cfg, plan, arrivals)
			publishFixtureFrames(t, cfg, sources)
			path, stdout, diag, code := invokeLiveK6(t, cfg)
			if tc.groupFixture != "" {
				r := verifyVerdictParity(t, path, stdout, code)
				switch tc.groupFixture {
				case "mismatch":
					requirePredicateFinding(t, r, k6v1.Violation, "GroupMembership", "GROUP_COORDINATOR_DESCRIPTION_MISMATCH")
				case "missing":
					requirePredicateFinding(t, r, k6v1.MissingProof, "GroupMembership", "required_proof_missing:GroupMembership")
				default:
					requireExternalParityProofGaps(t, r, code)
				}
				return
			}
			bundle := inspectK6Output(t, path, stdout, code)
			r := k6v1.Verify(bundle)
			requireExternalParityProofGaps(t, r, code)
			if diag != "" || !slices.Equal(r.InvariantsCovered, []string{"I1", "I11"}) {
				t.Fatalf("clean live external cut: exit %d %s %+v", code, diag, r.Findings)
			}
		})
	}
}

func seedParityAttempt(t *testing.T, engine *pgx.Conn, workflow k6v1.Submission, class string, attempt int64) {
	t.Helper()
	execK6SQL(t, engine, `INSERT INTO engine.workflow_definitions(definition_id,version,definition_hash,graph,activity_versions,effect_classes)
		VALUES ('definition',1,'hash','{"nodes":["pure.echo"]}','{"pure.echo":"v1"}',jsonb_build_object('pure.echo',$1::text))`, class)
	minimalExecution(t, engine, workflow)
	execK6SQL(t, engine, `INSERT INTO engine.node_instances(workflow_id,node_id,iteration,state,input)
		VALUES ($1,'pure.echo',0,'SUCCEEDED','{}')`, workflow.Key)
	execK6SQL(t, engine, `INSERT INTO engine.activity_attempts(workflow_id,node_id,iteration,attempt_number,state,effect_class,claim_token,worker_id,worker_request_id,logical_effect_key)
		VALUES ($1,'pure.echo',0,$4,'SUCCEEDED',$2,$3,'worker','request','key')`, workflow.Key, class, claimSecret, attempt)
	execK6SQL(t, engine, `INSERT INTO engine.k4_audit(kind,at,partition_id,owner_id,epoch,session_id,pod_uid,container_id,transaction_id,claim_identity,finalized_at)
		VALUES ('claim',clock_timestamp(),0,'',0,'session','uid','container','1',
		jsonb_build_object('workflow_id',$1::text,'node_id','pure.echo','iteration',0,'attempt_number',$3::bigint,
		'token_hash',$2::text,'worker_id','worker','worker_request_id','request'),clock_timestamp())`, workflow.Key, k6v1.Hash([]byte(claimSecret)), attempt)
}

const paritySession = "00000000-0000-0000-0000-000000000001"

func TestK6EffectLiveParity(t *testing.T) {
	if os.Getenv("DURABLE_K6_VERDICT_ARTIFACT") == "" {
		t.Skip("requires hosted PostgreSQL and network-isolated checker")
	}
	for _, tc := range []struct{ name, predicate, code string }{
		{name: "Clean"},
		{name: "RecoveredLostReply"},
		{"N11DuplicateMutation", "OneMutationPerKey", "SINK_KEY_MUTATED_TWICE"},
		{"N12RevisionJump", "RevisionChain", "SINK_REVISION_ENDPOINT"},
		{"N12ForeignKey", "RevisionChain", "SINK_ORPHAN_JOURNAL"},
		{"N12MissingWrite", "OneMutationPerKey", "EFFECT_SUCCESS_WITHOUT_MUTATION_RECEIPT"},
		{"N12MissingWinner", "OneMutationPerKey", "EFFECT_WORKFLOW_WITHOUT_WINNER"},
		{"N12WrongReceipt", "ReceiptSignature", "EFFECT_RECEIPT_MISMATCH"},
		{"N12ForeignReceipt", "DeliveryClosure", "DELIVERY_RECEIPT_IDENTITY"},
		{"N12FenceGap", "RevisionChain", "SINK_FENCE_SEQUENCE"},
		{"N12FenceEndpoint", "RevisionChain", "SINK_FENCE_ENDPOINT"},
		{"N12UnissuedFence", "RevisionChain", "SINK_APPLIED_STALE_FENCE"},
		{"N13Unacknowledged", "DeliveryClosure", "INGRESS_UNACKNOWLEDGED"},
		{"N13OmittedIngress", "DeliveryClosure", "INGRESS_ORDINAL_GAP"},
		{"N13ReusedCall", "DeliveryClosure", "INGRESS_CALL_COPY"},
		{"N13MismatchedCopy", "DeliveryClosure", "INGRESS_COPY_ORDER"},
		{"N13UnknownDenial", "DeliveryClosure", "DELIVERY_WITHOUT_ENGINE_KEY"},
		{"N13WrongCallOutcome", "DeliveryClosure", "DELIVERY_RECEIPT_OUTCOME"},
		{"N13DuplicateApplication", "DeliveryClosure", "DELIVERY_DUPLICATE_APPLICATION"},
		{"N13WrongAllocationOutcome", "DeliveryClosure", "DELIVERY_ALLOCATION_OUTCOME"},
		{"N13WrongRequestBinding", "DeliveryClosure", "DELIVERY_REQUEST_BINDING"},
		{"N14ClientTimeout", "DeliveryClosure", "COMMITTED_NONDELIVERY_UNPROVEN"},
		{"N14WrongRecoveryKey", "DeliveryClosure", "DELIVERY_LOGICAL_KEY"},
		{"N14SuppressedLookup", "DeliveryClosure", "LOST_APPLICATION_WITHOUT_LOOKUP"},
		{"N14WrongProofOperation", "DeliveryClosure", "EFFECT_SUCCESS_WITHOUT_DELIVERY"},
		{"N14ReplacementWithoutMiss", "DeliveryClosure", "SINK_REPLACEMENT_WITHOUT_MISS"},
		{"N14UnresolvedWithoutCommit", "DeliveryClosure", "UNRESOLVED_APPLY_DELIVERY"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			engine, dsn := k6TestDatabase(t)
			bindK6Database(t, engine, "engine-instance", "reset-a")
			sink, sinkDSN := k6SinkDatabase(t, engine)
			m := k6Manifest(engine.Config().Database, false)
			m.Ordinal, m.Seed, m.Cell, m.Episode, m.Workload = 90, 530091, "medium-12", "medium-12-0", "COOPERATING_EFFECT"
			m.Sink = &k6v1.DatabaseIdentity{Database: sink.Config().Database, Instance: "sink-instance"}
			m.RequiredSources = k6v1.RequiredSources(false, true)
			plan, arrivals := k6Sources(m, time.Now().Add(-time.Minute), "resource")
			workflow := plan.Windows[0].Submissions[0]
			sources := parityExternal(t, &m, true, workflow.Key)
			attempt := int64(1)
			if tc.name == "N14ReplacementWithoutMiss" {
				attempt = 2
				for i := range sources.Ingress.Rows {
					sources.Ingress.Rows[i].Data.Traffic.Attempt = attempt
				}
				ledger := sources.Sessions[paritySession]
				for _, row := range ledger.Rows {
					if row.Data.Attempt != nil {
						row.Data.Attempt.Attempt = attempt
					}
					if row.Data.Traffic != nil {
						row.Data.Traffic.Attempt = attempt
					}
				}
			}
			seedParityAttempt(t, engine, workflow, m.Workload, attempt)
			seedParityEffect(t, engine, sink, workflow.Key, sources)
			op := "apply"
			loseReply := false
			switch tc.name {
			case "RecoveredLostReply", "N14ClientTimeout", "N14WrongRecoveryKey", "N14SuppressedLookup":
				op, loseReply = "lookup", true
			case "N14WrongProofOperation":
				op = "lookup"
			}
			provisionFixtureReceipt(t, engine, sink, &m, workflow.Key, op, attempt)
			changeEffectFixture(t, engine, sink, &sources, tc.name, loseReply)
			cfg := k6live.Config{
				Schema: k6live.ConfigVersion, DatabaseURL: dsn, SinkDatabaseURL: sinkDSN, Manifest: m,
				ExternalDir: t.TempDir(), EpisodeDir: filepath.Join(t.TempDir(), "public"),
			}
			cfg = writeK6Sources(t, cfg, plan, arrivals)
			publishFixtureFrames(t, cfg, sources)
			path, stdout, diag, code := invokeLiveK6(t, cfg)
			if diag != "" || code != k6v1.Fail {
				t.Fatalf("effect fixture exit %d: %s %s", code, diag, stdout)
			}
			r := verifyVerdictParity(t, path, stdout, code)
			if tc.code == "" {
				requireExternalParityProofGaps(t, r, code)
				if label := r.Labels[7]; label.Outcome != "covered/pass" || label.PopulationCount != 1 {
					t.Fatalf("I8 award: %+v", label)
				}
				return
			}
			requirePredicateFinding(t, r, k6v1.Violation, tc.predicate, tc.code)
			if r.Labels[7].Outcome != "covered/fail" {
				t.Fatalf("I8 violation: %+v", r.Labels[7])
			}
		})
	}
}

func changeEffectFixture(t *testing.T, engine, sink *pgx.Conn, sources *k6v1.External, variant string, loseReply bool) {
	t.Helper()
	ingress := sources.Ingress
	ledger := sources.Sessions[paritySession]
	if loseReply {
		ingress.Rows[1].Data.Outcome, ingress.Rows[1].Data.Nondelivery = "withheld", variant != "N14ClientTimeout"
		ledger.Rows[3].Data.Kind, ledger.Rows[3].Data.Response = "timeout", nil
	}
	switch variant {
	case "N11DuplicateMutation":
		execK6SQL(t, sink, "UPDATE k6_sink.resources SET revision=2")
	case "N12RevisionJump":
		execK6SQL(t, sink, `ALTER TABLE k6_sink.resources DISABLE TRIGGER journal_resource_write;
			UPDATE k6_sink.resources SET revision=2; ALTER TABLE k6_sink.resources ENABLE TRIGGER journal_resource_write`)
	case "N12ForeignKey":
		execK6SQL(t, sink, "UPDATE k6_sink.journal SET logical_key='foreign-key'")
	case "N12MissingWrite":
		execK6SQL(t, sink, "DELETE FROM k6_sink.journal")
	case "N12MissingWinner":
		execK6SQL(t, engine, "UPDATE engine.activity_attempts SET state='FAILED_FINAL'")
	case "N12WrongReceipt":
		execK6SQL(t, sink, "UPDATE k6_sink.receipts SET scope_hash=repeat('d',64)")
	case "N12ForeignReceipt":
		execK6SQL(t, sink, "UPDATE k6_sink.receipts SET workflow_id='foreign-workflow'")
	case "N12FenceGap":
		execK6SQL(t, sink, "UPDATE k6_sink.allocations SET fence=2")
	case "N12FenceEndpoint":
		execK6SQL(t, sink, "UPDATE k6_sink.resources SET latest_fence=2")
	case "N12UnissuedFence":
		execK6SQL(t, sink, "DELETE FROM k6_sink.allocations")
	case "N13Unacknowledged":
		ingress.Rows[1].Data.Acknowledged = false
	case "N13OmittedIngress":
		ingress.Rows = slices.Delete(ingress.Rows, 1, 2)
		ingress.Rows[1].Position.Sequence = 2
	case "N13ReusedCall":
		copy := ingress.Rows[1]
		copy.Position.Sequence, copy.Data.Copy, copy.Data.Outcome = 4, 2, "duplicate_reply"
		ingress.Rows = append(ingress.Rows, copy)
	case "N13MismatchedCopy":
		copy := &ingress.Rows[2].Data
		copy.Ordinal, copy.Copy, copy.Outcome = 2, 2, "duplicate_reply"
		copy.Traffic.WorkerBinding = ingress.Rows[1].Data.Traffic.WorkerBinding
	case "N13UnknownDenial":
		traffic := ingress.Rows[2].Data.Traffic
		traffic.Intent = "00000000-0000-0000-0000-000000000123"
		body := testWire("K6_AUTH_DENIED\n")
		ingress.Rows[2].Data.Traffic = traffic
		ingress.Rows[2].Data.Outcome, ingress.Rows[2].Data.ApplicationID, ingress.Rows[2].Data.Response = "denied", "", &body
		ledger.Rows[4].Data.Traffic, ledger.Rows[5].Data.Traffic, ledger.Rows[5].Data.Response = &traffic, &traffic, &body
		execK6SQL(t, sink, "DELETE FROM k6_sink.calls WHERE operation='lookup'")
		execK6SQL(t, sink, "DELETE FROM k6_sink.request_bindings WHERE request_id='request-lookup'")
	case "N13WrongCallOutcome":
		execK6SQL(t, sink, "UPDATE k6_sink.calls SET outcome='DUPLICATE' WHERE operation='lookup'")
	case "N13DuplicateApplication":
		execK6SQL(t, sink, "UPDATE k6_sink.calls SET outcome='APPLIED' WHERE operation='lookup'")
	case "N13WrongAllocationOutcome":
		body := testWire(`{"fence":1}`)
		ingress.Rows[2].Data.ApplicationID, ingress.Rows[2].Data.Response = "", &body
		ledger.Rows[5].Data.Response = &body
		execK6SQL(t, sink, "UPDATE k6_sink.calls SET outcome='ALLOCATED',application_id=NULL,response_hash=NULL WHERE operation='lookup'")
	case "N13WrongRequestBinding":
		execK6SQL(t, sink, "UPDATE k6_sink.request_bindings SET body_hash=repeat('d',64) WHERE request_id='request-apply'")
	case "N14WrongRecoveryKey":
		ingress.Rows[2].Data.Traffic.KeyHash = k6v1.Hash([]byte("foreign-key"))
	case "N14SuppressedLookup":
		execK6SQL(t, sink, "DELETE FROM k6_sink.calls WHERE operation='lookup'")
	case "N14WrongProofOperation":
		ingress.Rows = ingress.Rows[:2]
		ledger.Rows = slices.Delete(ledger.Rows, 4, 6)
		ledger.Rows[4].Position.Sequence = 5
		execK6SQL(t, sink, "DELETE FROM k6_sink.calls WHERE operation='lookup'")
		execK6SQL(t, sink, "DELETE FROM k6_sink.request_bindings WHERE request_id='request-lookup'")
	case "N14UnresolvedWithoutCommit":
		i := &ingress.Rows[1].Data
		i.Outcome, i.Response, i.ApplicationID = "close", nil, ""
		ledger.Rows[3].Data.Kind, ledger.Rows[3].Data.Response = "timeout", nil
		execK6SQL(t, sink, "DELETE FROM k6_sink.calls WHERE operation='apply'")
		execK6SQL(t, sink, "DELETE FROM k6_sink.request_bindings WHERE request_id='request-apply'")
	}
	sealFixtureStream(t, &ledger, time.Now())
	sealFixtureStream(t, ingress, time.Now())
	sources.Sessions[paritySession] = ledger
}

func publishFixtureFrames(t *testing.T, cfg k6live.Config, sources k6v1.External) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	publish := func(i int64) error {
		now := k6v1.UTC(time.Now())
		sources.Roster.Seal.ThroughUTC, sources.Broker.Seal.ThroughUTC = now, now
		if sources.BrokerCopies != nil {
			sources.BrokerCopies.Seal.ThroughUTC = now
		}
		if sources.Ingress != nil {
			sources.Ingress.Seal.ThroughUTC, sources.Decisions.Seal.ThroughUTC = now, now
		} else {
			sources.Topology.Seal.ThroughUTC = now
		}
		return k6collect.Publish(cfg.ExternalDir, i, cfg.Manifest, sources)
	}
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	if err := publish(1); err != nil {
		done <- nil
		t.Fatal(err)
	}
	go func() {
		ticker := time.NewTicker(400 * time.Millisecond)
		defer ticker.Stop()
		for i := int64(2); ; i++ {
			select {
			case <-ctx.Done():
				done <- nil
				return
			case <-ticker.C:
				if err := publish(i); err != nil {
					done <- err
					return
				}
			}
		}
	}()
}

func requireExternalParityProofGaps(t *testing.T, r k6v1.Result, code int) {
	t.Helper()
	if code != k6v1.Fail || len(r.Findings) != 2 || r.Safety == "FAIL" || r.Completeness != "FAIL" {
		t.Fatalf("synthetic fixture proof gaps: exit %d %+v", code, r.Findings)
	}
	requirePredicateFinding(t, r, k6v1.MissingProof, "AtomicHistory", "required_proof_missing:engine/engine.k4_audit.jsonl")
	requirePredicateFinding(t, r, k6v1.MissingProof, "CurrentClaimWinner", "required_proof_missing:CurrentClaimWinner")
}

func sealFixtureStream[T any](t *testing.T, s *k6v1.Stream[T], at time.Time) {
	t.Helper()
	if err := k6v1.SealStream(s, "terminal", k6v1.UTC(at), int64(len(s.Rows)), 0); err != nil {
		t.Fatal(err)
	}
}

func parityExternal(t *testing.T, m *k6v1.Manifest, effect bool, workflow string) k6v1.External {
	t.Helper()
	const id = paritySession
	at := time.Now().Add(-time.Minute)
	opened := k6v1.UTC(at)
	session := k6v1.WorkerSession{ID: id, PodUID: "uid", ContainerID: "container", BootID: "boot", StartedUTC: opened, ContainerStartedUTC: opened, Claimant: "worker"}
	roster := k6collect.NewStream[k6v1.WatchRow](*m, "roster", "kube-observer", at)
	k6collect.Append(&roster, at, k6v1.WatchRow{Kind: "baseline", Cursor: "1"})
	runtime := session.Runtime()
	k6collect.Append(&roster, at, k6v1.WatchRow{Kind: "runtime", From: "1", Cursor: "2", Runtime: &runtime})
	k6collect.Append(&roster, at, k6v1.WatchRow{Kind: "session", From: "2", Cursor: "2", Session: &session,
		Registration: &k6v1.Registration{PodUID: session.PodUID, Claimant: session.Claimant, ServiceAccountUID: "sa-uid", Audience: "durable-worker-control", Observer: "reviewer"}})
	ledger := k6collect.NewStream[k6v1.WorkerRow](*m, "worker", "receiver", at)
	k6collect.Append(&ledger, at, k6v1.WorkerRow{Kind: "session", Session: &session})
	identity := k6v1.AttemptIdentity{Workflow: workflow, Node: "pure.echo", Attempt: 1, ClaimHash: k6v1.Hash([]byte(claimSecret)), RequestID: "request"}
	k6collect.Append(&ledger, at, k6v1.WorkerRow{Kind: "start", Invocation: "invocation", Attempt: &identity})
	broker := k6collect.NewStream[k6v1.BrokerRow](*m, "broker", "reader", at)
	k6collect.Append(&broker, at, k6v1.BrokerRow{Cluster: "cluster", Group: "group", State: "empty",
		BeforeUTC: opened, AfterUTC: opened, Partitions: []k6v1.BrokerPartition{{Topic: "events", TopicID: "topic"}},
		Members: []k6v1.BrokerMember{}})
	sealFixtureStream(t, &roster, at)
	sealFixtureStream(t, &broker, at)
	sources := k6v1.External{Roster: roster, Sessions: map[string]k6v1.Stream[k6v1.WorkerRow]{id: ledger}, Broker: &broker}
	if effect {
		ingress := k6collect.NewStream[k6v1.IngressRow](*m, "ingress", "relay-receiver", at)
		decisions := k6collect.NewStream[k6v1.DecisionRow](*m, "decisions", "adapter", at)
		grant := k6v1.Hash([]byte(grantSecret))
		k6collect.Append(&decisions, at, k6v1.DecisionRow{Intent: parityIntent, ProposalHash: k6v1.Hash([]byte("proposal")),
			Subject: "system:serviceaccount:episode:k6-approver", ServiceAccountUID: "approver-uid", BoundUID: "secret-uid",
			IssuerHash: k6v1.Hash([]byte("https://kubernetes.default.svc.cluster.local")), Audience: "dur053-k6-approval", Outcome: "APPROVED", DecidedUTC: parityDecision})
		for i, op := range []string{"allocate", "apply", "lookup"} {
			message := "invocation"
			if op == "allocate" {
				message = "allocate"
			}
			traffic := k6v1.Traffic{Method: "POST", Operation: op, MessageHash: k6v1.Hash([]byte(message)),
				RequestIDHash: k6v1.Hash([]byte("request-" + op)), KeyHash: k6v1.Hash([]byte("key")), Intent: parityIntent,
				ResourceHash: k6v1.Hash([]byte("resource")), GrantHash: grant, Attempt: 1, WorkerBinding: k6v1.Hash([]byte("worker-" + op)), Request: testWire("body-" + op)}
			reply := testWire("receipt")
			app := parityApp
			if op == "allocate" {
				reply = testWire(`{"fence":1}`)
				app = ""
			} else {
				k6collect.Append(&ledger, at, k6v1.WorkerRow{Kind: "send", Invocation: message, Traffic: &traffic})
				k6collect.Append(&ledger, at, k6v1.WorkerRow{Kind: "receive", Invocation: message, Traffic: &traffic, Response: &reply})
			}
			k6collect.Append(&ingress, at, k6v1.IngressRow{Traffic: traffic, Ordinal: int64(i + 1), Copy: 1,
				PeerHash: k6v1.Hash([]byte("peer")), ConnectionHash: k6v1.Hash([]byte("connection-" + op)),
				Outcome: "delivered", Response: &reply, ApplicationID: app, Acknowledged: true})
		}
		sealFixtureStream(t, &ingress, at)
		sealFixtureStream(t, &decisions, at)
		sources.Ingress, sources.Decisions = &ingress, &decisions
	} else {
		topology := k6collect.NewStream[k6v1.TopologyRow](*m, "topology", "platform", at)
		rendered, digest, err := k6collect.RenderedTopology("rendered", []k6v1.Component{
			{Kind: "database", Name: "engine", Role: "engine"}, {Kind: "deployment", Name: "worker", Role: "worker"}, {Kind: "service", Name: "api", Role: "api"},
		})
		if err != nil {
			t.Fatal(err)
		}
		m.TopologySHA256 = digest
		k6collect.Append(&topology, at, rendered)
		observed := rendered
		observed.Kind, observed.Observer = "observed", "cluster"
		k6collect.Append(&topology, at, observed)
		sealFixtureStream(t, &topology, at)
		sources.Topology = &topology
	}
	k6collect.Append(&ledger, at, k6v1.WorkerRow{Kind: "finish", Invocation: "invocation", Attempt: &identity})
	sealFixtureStream(t, &ledger, at)
	sources.Sessions[id] = ledger
	return sources
}

func seedParityEffect(t *testing.T, engine, sink *pgx.Conn, workflow string, sources k6v1.External) {
	t.Helper()
	d := sources.Decisions.Rows[0].Data
	execK6SQL(t, engine, `INSERT INTO engine.approval_action_intents(intent_id,workflow_id,node_id,iteration,proposal_hash,target,canonical_arguments,
		valid_until,decision,approver_id,decided_at,grant_token,k6_logical_key)
		VALUES ($1,$2,'pure.echo',0,$3,'resource','{}',clock_timestamp(),'APPROVED',$4,$5,$6,'key')`,
		parityIntent, workflow, d.ProposalHash, d.Subject, time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC), grantSecret)
	execK6SQL(t, sink, `INSERT INTO k6_sink.resources(resource_id) VALUES ('resource')`)
	execK6SQL(t, sink, `UPDATE k6_sink.resources SET state='{"value":"k6"}',revision=1,latest_fence=1,logical_key='key',intent_id=$1 WHERE resource_id='resource'`, parityIntent)
	execK6SQL(t, sink, `INSERT INTO k6_sink.receipts(workflow_id,logical_key,resource_id,canonical_hash,scope_hash,intent_id,expected_revision,applied_revision,application_id)
		VALUES ($1,'key','resource',repeat('a',64),repeat('b',64),$2,0,1,$3)`, workflow, parityIntent, parityApp)
	execK6SQL(t, sink, `INSERT INTO k6_sink.allocations VALUES ($1,$2,$3,'resource',1)`,
		parityIntent, k6v1.Hash([]byte(grantSecret)), sources.Ingress.Rows[0].Data.Traffic.Attempt)
	for i, row := range sources.Ingress.Rows {
		v := row.Data
		op := v.Traffic.Operation
		request, message := "request-"+op, "invocation"
		var app, response *string
		if op == "allocate" {
			message = "allocate"
		} else {
			app = &v.ApplicationID
			response = &v.Response.SHA256
		}
		execK6SQL(t, sink, `INSERT INTO k6_sink.request_bindings VALUES ($1,$2)`, request, k6v1.Hash([]byte(op+"\x00"+v.Traffic.Request.SHA256)))
		execK6SQL(t, sink, `INSERT INTO k6_sink.calls(operation,workflow_id,logical_key,intent_id,resource_id,message_id,attempt,request_id,body_hash,connection_hash,outcome,application_id,response_hash)
			VALUES ($1,$2,'key',$3,'resource',$4,$11,$5,$6,$7,$8,$9,$10)`, op, workflow, parityIntent, message, request, v.Traffic.Request.SHA256, v.ConnectionHash,
			[]string{"ALLOCATED", "APPLIED", "FOUND"}[i], app, response, v.Traffic.Attempt)
	}
}
