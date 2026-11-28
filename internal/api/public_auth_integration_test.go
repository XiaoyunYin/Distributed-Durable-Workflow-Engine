package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/state"
)

func TestJ12PostgresTenantAuthorisation(t *testing.T) {
	databaseURL := os.Getenv("J12_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("requires an isolated J12 PostgreSQL database with exactly the base migrations")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := state.NewFromURL(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var isolated bool
	err = store.Pool().QueryRow(ctx, `SELECT
		(SELECT array_agg(version::integer ORDER BY version) FROM engine.schema_migrations) = ARRAY(SELECT generate_series(1,23))
		AND NOT EXISTS (SELECT 1 FROM engine.workflow_executions)`).Scan(&isolated)
	if err != nil || !isolated {
		t.Fatalf("J12 integration requires an empty base-profile database: %t %v", isolated, err)
	}
	if err := store.CheckTenantRequesterSchema(ctx); err == nil {
		t.Fatal("schema check accepted the base profile for J12")
	}
	migrate := func(ctx context.Context, direction string) error {
		path := filepath.Join("..", "..", "migrations", "j12", "000026_j12_workflow_requesters."+direction+".sql")
		sql, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		_, err = store.Pool().Exec(ctx, string(sql))
		return err
	}
	if err := migrate(ctx, "up"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := migrate(cleanupCtx, "down"); err != nil {
			t.Errorf("restore base migrations: %v", err)
		}
	}()
	if err := store.CheckTenantRequesterSchema(ctx); err != nil {
		t.Fatal(err)
	}
	verifier, key := j12JWTFixture(t)
	reviewer, remote := identityFixture(t)
	now := time.Now()
	tenantToken := func(subject, namespace string) string {
		return j12Token(t, key, "current", j12Claims(subject, namespace, now))
	}
	requester, approver := tenantToken("requester", "tenant-a"), tenantToken("approver", "tenant-a")
	foreign := tenantToken("requester", "tenant-b")
	projected := projectedToken(reviewer.config.Issuer, remote.username, reviewer.config.Audience, now.Add(time.Minute).Unix())
	public := NewServer(store).WithRoutes(RoutesPublic).WithPublicAPIAuth(true, verifier).Handler()
	operator := NewServer(store).WithRoutes(RoutesOperator).WithPublicAPIAuth(true, nil).WithAuthentication(reviewer).Handler()
	legacy := NewServer(store).Handler()
	definitionID := "j12-" + state.NewID()
	if err := store.CreateDefinition(ctx, state.DefinitionInput{DefinitionID: definitionID, Version: 1, DefinitionHash: definitionID,
		Graph: []byte(`{"nodes":["root"]}`), ActivityVersions: []byte(`{"root":"v1"}`),
		EffectClasses: []byte(`{"root":"PURE_ACTIVITY"}`)}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := store.Pool().Exec(cleanupCtx, `DELETE FROM engine.workflow_executions WHERE definition_id = $1`, definitionID); err != nil {
			t.Errorf("clean J12 workflows: %v", err)
		}
		if _, err := store.Pool().Exec(cleanupCtx, `DELETE FROM engine.workflow_definitions WHERE definition_id = $1`, definitionID); err != nil {
			t.Errorf("clean J12 definition: %v", err)
		}
	}()
	check := func(response *httptest.ResponseRecorder, want int) []byte {
		t.Helper()
		if response.Code != want {
			var envelope errorResponse
			_ = json.Unmarshal(response.Body.Bytes(), &envelope)
			t.Fatalf("HTTP status %d (%s), want %d", response.Code, envelope.Error.Code, want)
		}
		return response.Body.Bytes()
	}
	var leases = map[int16]state.Lease{}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, lease := range leases {
			if err := store.ReleaseLease(cleanupCtx, state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch}); err != nil {
				t.Errorf("release J12 fixture lease: %v", err)
			}
		}
	}()
	acquireWorkflowLease := func(workflowID string) state.Lease {
		t.Helper()
		part, err := partition.ID(workflowID)
		if err != nil {
			t.Fatal(err)
		}
		lease, held := leases[int16(part)]
		if !held {
			lease, held, err = store.AcquireLease(ctx, int16(part), state.NewID(), time.Minute)
			if err != nil || !held {
				t.Fatalf("acquire J12 fixture lease: %t %v", held, err)
			}
			leases[int16(part)] = lease
		}
		return lease
	}
	makeIntent := func(workflowID string) (state.ApprovalIntent, state.Lease) {
		t.Helper()
		lease := acquireWorkflowLease(workflowID)
		path := "/v1/workflows/" + workflowID + "/nodes/root/iterations/0/approval"
		body := fmt.Sprintf(`{"partition_id":%d,"owner_id":%q,"epoch":%d,"expected_revision":1,"target":"sandbox.write","canonical_arguments":{"resource":"j12"},"expected_resource_revision":"0","valid_until":%q}`, lease.PartitionID, lease.OwnerID, lease.Epoch, now.Add(5*time.Minute).UTC().Format(time.RFC3339Nano))
		var intent state.ApprovalIntent
		if err := json.Unmarshal(check(j12Request(operator, "POST", path, projected, body), 201), &intent); err != nil {
			t.Fatal(err)
		}
		return intent, lease
	}
	submission := func(id string) string {
		return fmt.Sprintf(`{"workflow_id":%q,"submission_key":%q,"definition_id":%q,"definition_version":1,"initial_node_id":"root","payload":{},"actor_id":"forged","namespace":"tenant-a"}`, id, id, definitionID)
	}
	ownID, foreignID, legacyID, cancelID := state.NewID(), state.NewID(), state.NewID(), state.NewID()
	check(j12Request(public, "POST", "/v1/workflows", requester, submission(ownID)), 201)
	check(j12Request(public, "POST", "/v1/workflows", requester, submission(ownID)), 200)
	check(j12Request(public, "POST", "/v1/workflows", approver, submission(ownID)), 409)
	check(j12Request(public, "POST", "/v1/workflows", foreign, submission(foreignID)), 403)
	check(j12Request(public, "POST", "/v1/workflows", foreign, strings.Replace(submission(foreignID), "tenant-a", "tenant-b", 1)), 201)
	check(j12Request(legacy, "POST", "/v1/workflows", "", submission(legacyID)), 201)
	check(j12Request(public, "POST", "/v1/workflows", requester, submission(legacyID)), 409)
	check(j12Request(public, "POST", "/v1/workflows", requester, submission(cancelID)), 201)
	var persistedIssuer, subject, ns, historyActor string
	err = store.Pool().QueryRow(ctx, `SELECT r.issuer, r.subject, w.namespace, h.actor_id
		FROM engine.j12_workflow_requesters r JOIN engine.workflow_executions w USING (workflow_id)
		JOIN engine.transition_history h USING (workflow_id) WHERE r.workflow_id=$1 AND h.revision=1`, ownID).
		Scan(&persistedIssuer, &subject, &ns, &historyActor)
	if err != nil || persistedIssuer != j12Issuer || subject != "requester" || ns != "tenant-a" || historyActor != subject {
		t.Fatalf("durable requester and history mismatch: %q %q %q %q %v", persistedIssuer, subject, ns, historyActor, err)
	}
	ownIntent, lease := makeIntent(ownID)
	foreignIntent, _ := makeIntent(foreignID)
	legacyIntent, _ := makeIntent(legacyID)
	decision := func(intent state.ApprovalIntent, credential, value string, want int) []byte {
		t.Helper()
		body := fmt.Sprintf(`{"approver_id":"forged","proposal_hash":%q,"decision":%q}`, intent.ProposalHash, value)
		return check(j12Request(public, "POST", "/v1/approvals/"+intent.IntentID+"/decision", credential, body), want)
	}
	decision(ownIntent, requester, "APPROVED", 403)
	decision(ownIntent, requester, "REJECTED", 403)
	decision(foreignIntent, approver, "APPROVED", 404)
	decision(legacyIntent, approver, "APPROVED", 403)
	for _, intent := range []state.ApprovalIntent{ownIntent, foreignIntent, legacyIntent} {
		var unchanged bool
		err := store.Pool().QueryRow(ctx, `SELECT i.decision='PENDING' AND i.approver_id IS NULL
			AND i.dispatch_status='NOT_GRANTED' AND i.grant_token IS NULL
			AND NOT EXISTS (SELECT 1 FROM engine.effect_records e WHERE e.workflow_id=i.workflow_id)
			FROM engine.approval_action_intents i WHERE i.intent_id=$1`, intent.IntentID).Scan(&unchanged)
		if err != nil || !unchanged {
			t.Fatalf("denied decision altered durable state: %t %v", unchanged, err)
		}
	}
	check(j12Request(public, "GET", "/v1/workflows/"+foreignID, requester, ""), 404)
	check(j12Request(public, "GET", "/v1/workflows/"+foreignID+"/history", requester, ""), 404)
	check(j12Request(public, "POST", "/v1/workflows/"+foreignID+"/cancel-request", requester, j12Cancel), 404)
	var cancellations int
	if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.cancellation_requests WHERE workflow_id = $1`, foreignID).Scan(&cancellations); err != nil || cancellations != 0 {
		t.Fatalf("foreign cancellation altered durable state: %d %v", cancellations, err)
	}
	check(j12Request(public, "GET", "/v1/workflows/"+cancelID, requester, ""), 200)
	check(j12Request(public, "GET", "/v1/workflows/"+cancelID+"/history", requester, ""), 200)
	accepted := check(j12Request(public, "POST", "/v1/workflows/"+cancelID+"/cancel-request", requester, j12Cancel), 202)
	var cancellation state.CancellationRequest
	if err := json.Unmarshal(accepted, &cancellation); err != nil || cancellation.WorkflowID != cancelID || cancellation.RequestID == "" {
		t.Fatalf("cancellation receipt did not bind to the authorised id: %+v %v", cancellation, err)
	}
	cancelLease := acquireWorkflowLease(cancelID)
	cancelPath := "/v1/cancellations/" + cancellation.RequestID + "/apply"
	cancelBody := fmt.Sprintf(`{"partition_id":%d,"owner_id":%q,"epoch":%d}`, cancelLease.PartitionID, cancelLease.OwnerID, cancelLease.Epoch)
	check(j12Request(operator, "POST", cancelPath, requester, cancelBody), 401)
	check(j12Request(operator, "POST", cancelPath, projected, cancelBody), 200)
	var cancellationStatus string
	if err := store.Pool().QueryRow(ctx, `SELECT status FROM engine.cancellation_requests WHERE request_id=$1 AND workflow_id=$2`, cancellation.RequestID, cancelID).
		Scan(&cancellationStatus); err != nil || cancellationStatus != "APPLIED" {
		t.Fatalf("internal apply did not update the cancellation attached to the path ID: %q %v", cancellationStatus, err)
	}
	decision(ownIntent, approver, "APPROVED", 200)
	var durableApprover string
	if err := store.Pool().QueryRow(ctx, `SELECT approver_id FROM engine.approval_action_intents WHERE intent_id=$1 AND decision='APPROVED'`, ownIntent.IntentID).
		Scan(&durableApprover); err != nil || durableApprover != "approver" {
		t.Fatalf("wrong durable approver: %q %v", durableApprover, err)
	}
	applyPath := "/v1/approvals/" + ownIntent.IntentID + "/apply"
	applyBody := fmt.Sprintf(`{"partition_id":%d,"owner_id":%q,"epoch":%d,"expected_revision":2,"workflow_id":%q,"node_id":"root","intent_id":%q,"logical_effect_key":"j12-effect"}`, lease.PartitionID, lease.OwnerID, lease.Epoch, ownID, ownIntent.IntentID)
	check(j12Request(operator, "POST", applyPath, approver, applyBody), 401)
	var grant state.ApprovalGrant
	if err := json.Unmarshal(check(j12Request(operator, "POST", applyPath, projected, applyBody), 200), &grant); err != nil || grant.GrantToken == "" {
		t.Fatalf("internal apply did not issue a dispatch grant: %v", err)
	}
	if response := decision(ownIntent, approver, "APPROVED", 200); strings.Contains(string(response), grant.GrantToken) {
		t.Fatal("public decision replay disclosed the dispatch grant")
	}
}
