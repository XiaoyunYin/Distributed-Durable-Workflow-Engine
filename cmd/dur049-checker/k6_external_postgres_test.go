package main

import (
	"context"
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
		name         string
		effect       bool
		groupFixture string
	}{
		{"false", false, ""},
		{"true", true, ""},
		{"GroupMembership", false, "valid"},
		{"GroupAssignmentMismatch", false, "mismatch"},
		{"GroupMissingProof", false, "missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			effect := tc.effect
			engine, dsn := k6TestDatabase(t)
			bindK6Database(t, engine, "engine-instance", "reset-a")
			m := k6Manifest(engine.Config().Database, false)
			if tc.groupFixture != "" {
				m.Ordinal, m.Seed, m.Cell, m.Episode = 80, 530081, "medium-10", "medium-10-0"
			}
			cfg := k6live.Config{Schema: k6live.ConfigVersion, DatabaseURL: dsn}
			var sink *pgx.Conn
			if effect {
				var sinkDSN string
				sink, sinkDSN = k6SinkDatabase(t, engine)
				m.Ordinal, m.Seed, m.Cell, m.Episode, m.Workload = 90, 530091, "medium-12", "medium-12-0", "COOPERATING_EFFECT"
				m.Sink = &k6v1.DatabaseIdentity{Database: sink.Config().Database, Instance: "sink-instance"}
				m.RequiredSources = k6v1.RequiredSources(false, true)
				cfg.SinkDatabaseURL = sinkDSN
			}
			target := "pure.echo"
			if effect {
				target = "resource"
			}
			plan, arrivals := k6Sources(m, time.Now().Add(-time.Minute), target)
			workflow := plan.Windows[0].Submissions[0]
			execK6SQL(t, engine, `INSERT INTO engine.workflow_definitions(definition_id,version,definition_hash,graph,activity_versions,effect_classes)
				VALUES ('definition',1,'hash','{"nodes":["pure.echo"]}','{"pure.echo":"v1"}','{"pure.echo":"PURE_ACTIVITY"}')`)
			minimalExecution(t, engine, workflow)
			{
				class := "PURE_ACTIVITY"
				if effect {
					class = "COOPERATING_EFFECT"
				}
				execK6SQL(t, engine, `INSERT INTO engine.node_instances(workflow_id,node_id,iteration,state,input)
					VALUES ($1,'pure.echo',0,'SUCCEEDED','{}')`, workflow.Key)
				execK6SQL(t, engine, `INSERT INTO engine.activity_attempts(workflow_id,node_id,iteration,attempt_number,state,effect_class,claim_token,worker_id,worker_request_id,logical_effect_key)
					VALUES ($1,'pure.echo',0,1,'SUCCEEDED',$2,$3,'worker','request','key')`, workflow.Key, class, claimSecret)
				execK6SQL(t, engine, `INSERT INTO engine.k4_audit(kind,at,partition_id,owner_id,epoch,session_id,pod_uid,container_id,transaction_id,claim_identity,finalized_at)
					VALUES ('claim',clock_timestamp(),0,'',0,'session','uid','container','1',
					jsonb_build_object('workflow_id',$1::text,'node_id','pure.echo','iteration',0,'attempt_number',1,
					'token_hash',$2::text,'worker_id','worker','worker_request_id','request'),clock_timestamp())`, workflow.Key, k6v1.Hash([]byte(claimSecret)))
			}
			cfg.ExternalDir = t.TempDir()
			sources := parityExternal(t, &m, effect, workflow.Key)
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
			if effect {
				seedParityEffect(t, engine, sink, workflow.Key, sources)
				provisionFixtureReceipt(t, engine, sink, &m, workflow.Key)
			}
			cfg.Manifest = m
			cfg.EpisodeDir = filepath.Join(t.TempDir(), "public")
			cfg = writeK6Sources(t, cfg, plan, arrivals)
			m = cfg.Manifest
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			publish := func(i int64) error {
				now := k6v1.UTC(time.Now())
				sources.Roster.Seal.ThroughUTC, sources.Broker.Seal.ThroughUTC = now, now
				if effect {
					sources.Ingress.Seal.ThroughUTC, sources.Decisions.Seal.ThroughUTC = now, now
				} else {
					sources.Topology.Seal.ThroughUTC = now
				}
				return k6collect.Publish(cfg.ExternalDir, i, m, sources)
			}
			if err := publish(1); err != nil {
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
			t.Cleanup(func() {
				cancel()
				if err := <-done; err != nil {
					t.Error(err)
				}
			})
			path, stdout, diag, code := invokeLiveK6(t, cfg)
			if tc.groupFixture != "" {
				r := verifyVerdictParity(t, path, stdout, code)
				switch tc.groupFixture {
				case "mismatch":
					requirePredicateFinding(t, r, k6v1.Violation, "GroupMembership", "GROUP_COORDINATOR_DESCRIPTION_MISMATCH")
				case "missing":
					requirePredicateFinding(t, r, k6v1.MissingProof, "GroupMembership", "required_proof_missing:GroupMembership")
				default:
					if code != k6v1.Incomplete || len(r.Findings) != 0 {
						t.Fatalf("corroborated group: exit %d %+v", code, r.Findings)
					}
				}
				return
			}
			bundle := inspectK6Output(t, path, stdout, code)
			if r := k6v1.Verify(bundle); code != k6v1.Incomplete || diag != "" || len(r.Findings) != 0 ||
				!slices.Equal(r.InvariantsCovered, []string{"I1", "I11"}) {
				t.Fatalf("clean live external cut: exit %d %s %+v", code, diag, r.Findings)
			}
		})
	}
}

func sealFixtureStream[T any](t *testing.T, s *k6v1.Stream[T], at time.Time) {
	t.Helper()
	if err := k6v1.SealStream(s, "terminal", k6v1.UTC(at), int64(len(s.Rows)), 0); err != nil {
		t.Fatal(err)
	}
}

func parityExternal(t *testing.T, m *k6v1.Manifest, effect bool, workflow string) k6v1.External {
	t.Helper()
	const id = "00000000-0000-0000-0000-000000000001"
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
	execK6SQL(t, sink, `INSERT INTO k6_sink.allocations VALUES ($1,$2,1,'resource',1)`, parityIntent, k6v1.Hash([]byte(grantSecret)))
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
			VALUES ($1,$2,'key',$3,'resource',$4,1,$5,$6,$7,$8,$9,$10)`, op, workflow, parityIntent, message, request, v.Traffic.Request.SHA256, v.ConnectionHash,
			[]string{"ALLOCATED", "APPLIED", "FOUND"}[i], app, response)
	}
}
