package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/state"
	"github.com/segmentio/kafka-go"
)

const (
	definition    = "j12-fixture"
	node          = "pure.echo"
	workflowPath  = "/v1/workflows/"
	approvalPath  = "/v1/approvals/"
	missing       = "j12-absent"
	missingIntent = "b6168641-7f0b-4b95-a2b6-d96d1ec32ef4"
)

type fixtures struct {
	requester, approver, foreign     string
	leases                           map[string]state.LeaseRef
	intents                          map[string]state.ApprovalIntent
	delivery                         object
	cancelID, claimToken, grantToken string
	attempt                          int64
	workerIdentity, operatorIdentity string
}

type fixtureHTTPFailure struct {
	path, code string
	status     int
}

func (f fixtureHTTPFailure) Error() string { return "fixture request returned an unexpected status" }
func (f fixtureHTTPFailure) Unwrap() error { return contractMismatch{} }

func submitBody(id, namespace string) object {
	return object{"workflow_id": id, "namespace": namespace, "submission_key": id, "payload": object{}, "definition_id": definition,
		"definition_version": 1, "initial_node_id": node, "actor_id": "forged-actor"}
}

func nodePath(id, action string) string {
	return workflowPath + id + "/nodes/" + node + "/iterations/0/" + action
}

func leaseBody(lease state.LeaseRef) object {
	return object{"partition_id": lease.PartitionID, "owner_id": lease.OwnerID, "epoch": lease.Epoch, "actor_id": "forged-actor"}
}

func (f *fixtures) approvalBody(id string) object {
	b := leaseBody(f.leases[id])
	b["expected_revision"], b["target"], b["canonical_arguments"], b["expected_resource_revision"] = 1, "sandbox.write", object{"resource": id}, "0"
	b["valid_until"] = time.Now().UTC().Add(30 * time.Minute)
	return b
}

func (f *fixtures) applyBody(id string) object {
	b := leaseBody(f.leases[id])
	b["expected_revision"], b["logical_effect_key"] = 2, "j12-sandbox-effect"
	return b
}

func decisionBody(intent state.ApprovalIntent, decision string) object {
	return object{"proposal_hash": intent.ProposalHash, "decision": decision, "approver_id": "forged-approver"}
}

func cancelBody() object { return object{"client_key": "j12-cancel", "observed_revision": 1} }

func (h *host) callerIdentity(role string) string {
	return fmt.Sprintf("system:serviceaccount:%s:%s-caller:pod:%s-probe:uid:%s", h.name, role, role, h.podUIDs[role+"-probe"])
}

func prepareFixtures(ctx context.Context, h *host, c *credentials) (*fixtures, error) {
	f := &fixtures{leases: map[string]state.LeaseRef{}, intents: map[string]state.ApprovalIntent{}, workerIdentity: h.callerIdentity("worker"), operatorIdentity: h.callerIdentity("operator")}
	for _, item := range []struct {
		out         *string
		subject, ns string
	}{{&f.requester, "requester", "tenant-a"}, {&f.approver, "approver", "tenant-a"}, {&f.foreign, "requester", "tenant-b"}} {
		token, err := c.token("current", "current", "RS256", claims(item.subject, item.ns, time.Now()))
		if err != nil {
			return nil, err
		}
		*item.out = token
	}
	// These calls only set up fixtures for routes with no public definition,
	// lease or attempt-creation operation. The witness never calls the store.
	store, err := state.NewFromURL(ctx, h.dbURL)
	if err != nil {
		return nil, errors.New("open J12 fixture writer")
	}
	defer store.Close()
	if err := store.CreateDefinition(ctx, state.DefinitionInput{DefinitionID: definition, Version: 1, DefinitionHash: definition,
		Graph:            json.RawMessage(`{"entry":"pure.echo","nodes":[{"id":"pure.echo","kind":"activity","next":"done"},{"id":"done","kind":"success"}]}`),
		ActivityVersions: json.RawMessage(`{"pure.echo":"v1"}`), EffectClasses: json.RawMessage(`{"pure.echo":"PURE_ACTIVITY"}`)}); err != nil {
		return nil, fmt.Errorf("seed J12 definition: %w", err)
	}
	owner := state.NewID()
	parts := map[uint64]state.LeaseRef{}
	for _, id := range []string{"j12-own", "j12-self", "j12-foreign", "j12-legacy", "j12-cancel", "j12-approval-create", "j12-worker"} {
		part, err := partition.ID(id)
		if err != nil {
			return nil, err
		}
		lease, ok := parts[part]
		if !ok {
			claimed, acquired, err := store.AcquireLease(ctx, int16(part), owner, 25*time.Minute)
			if err != nil || !acquired {
				return nil, errors.New("acquire J12 fixture lease")
			}
			lease = state.LeaseRef{PartitionID: claimed.PartitionID, OwnerID: claimed.OwnerID, Epoch: claimed.Epoch}
			parts[part] = lease
		}
		f.leases[id] = lease
		if id == "j12-legacy" {
			_, err := store.CreateWorkflow(ctx, state.CreateWorkflowInput{WorkflowID: id, Namespace: "tenant-a", SubmissionKey: id, SubmissionPayloadHash: "legacy", DefinitionID: definition,
				DefinitionVersion: 1, PartitionID: lease.PartitionID, InitialNodeID: node, ActorID: "forged-legacy"})
			if err != nil {
				return nil, fmt.Errorf("seed J12 legacy row: %w", err)
			}
		} else {
			ns, token := "tenant-a", f.requester
			if id == "j12-foreign" {
				ns, token = "tenant-b", f.foreign
			}
			reply, err := h.request(ctx, "tenant", probeRequest{Listener: "public", Method: "POST", Path: "/v1/workflows", Token: token, Body: submitBody(id, ns)})
			if err != nil {
				return nil, err
			}
			if reply.Status != 201 {
				return nil, fixtureHTTPFailure{path: "/v1/workflows", code: errorCode(reply), status: reply.Status}
			}
		}
	}
	for _, id := range []string{"j12-own", "j12-self", "j12-foreign", "j12-legacy"} {
		reply, err := h.request(ctx, "operator", probeRequest{Listener: "operator", Method: "POST", Path: nodePath(id, "approval"), Body: f.approvalBody(id)})
		var intent state.ApprovalIntent
		if err != nil {
			return nil, err
		}
		if reply.Status != 201 {
			return nil, fixtureHTTPFailure{path: nodePath(id, "approval"), code: errorCode(reply), status: reply.Status}
		}
		if json.Unmarshal(reply.Body, &intent) != nil || intent.WorkflowID != id || intent.IntentID == "" || intent.ProposalHash == "" {
			return nil, fmt.Errorf("seed J12 pending approval %s: missing bound intent", id)
		}
		f.intents[id] = intent
	}
	lease := f.leases["j12-worker"]
	waiting := state.StateWaitingActivity
	if err := store.ApplyOwnerTransition(ctx, state.OwnerTransitionInput{Lease: lease, WorkflowID: "j12-worker", ExpectedRevision: 1,
		NewState: waiting, ActorID: "fixture", Reason: "SCHEDULE_ACTIVITY", NodeID: node, NodeState: &waiting}); err != nil {
		return nil, fmt.Errorf("seed J12 worker transition: %w", err)
	}
	attempt, err := store.CreateAttempt(ctx, state.AttemptInput{Lease: lease, WorkflowID: "j12-worker", NodeID: node, ExpectedRevision: 2,
		EffectClass: state.EffectPure, DispatchLease: time.Minute, ActorID: "fixture"})
	if err != nil {
		return nil, fmt.Errorf("seed J12 worker attempt: %w", err)
	}
	f.attempt = attempt.AttemptNumber
	f.delivery, err = h.deliveryFixture(ctx)
	return f, err
}

type fixtureResolver string

func (r fixtureResolver) LookupHost(context.Context, string) ([]string, error) {
	return []string{string(r)}, nil
}

func (h *host) deliveryFixture(ctx context.Context) (object, error) {
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	dialer := kafka.Dialer{Timeout: 5 * time.Second, Resolver: fixtureResolver(h.kafkaIP)}
	conn, err := dialer.DialLeader(ctx, "tcp", h.kafkaIP+":9092", state.TaskTopic, 0)
	if err != nil {
		return nil, errors.New("read J12 task partition")
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	_ = conn.SetReadDeadline(deadline)
	message, err := conn.ReadMessage(1 << 20)
	if err != nil {
		return nil, errors.New("J12 relay produced no task delivery")
	}
	id := string(message.Key)
	var ack bool
	if !poll(ctx, 10*time.Second, func(ctx context.Context) bool {
		err := h.observer.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM engine.outbox o JOIN engine.outbox_publications p USING (event_id)
			WHERE o.event_id::text=$1 AND o.workflow_id='j12-worker' AND o.event_type='attempt.dispatch'
			  AND o.publish_state='PUBLISHED' AND p.broker_acknowledged AND o.payload=$2::jsonb)`, id, string(message.Value)).Scan(&ack)
		return err == nil && ack
	}) {
		return nil, errors.New("J12 task did not match an acknowledged durable publication")
	}
	headers := map[string]string{}
	for _, field := range message.Headers {
		headers[field.Key] = string(field.Value)
	}
	version, err := strconv.Atoi(headers["schema-version"])
	if id == "" || err != nil || headers["event-id"] != id || headers["event-type"] != "attempt.dispatch" {
		return nil, errors.New("malformed J12 task event")
	}
	return object{"event_id": id, "topic": state.TaskTopic, "partition": message.Partition, "offset": message.Offset,
		"event_type": headers["event-type"], "schema_version": version, "payload": message.Value}, nil
}
