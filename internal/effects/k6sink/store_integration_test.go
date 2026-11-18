package k6sink

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/k6auth"
	"durable-agent-execution-engine/internal/state"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type testDB struct {
	admin, service, exporter *pgxpool.Pool
	name, adminURL           string
}

func testPool(t *testing.T, adminURL, database, role string) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(adminURL)
	if err != nil {
		t.Fatal("invalid sink test DB URL")
	}
	if database != "" {
		cfg.ConnConfig.Database = database
	}
	cfg.MaxConns = 4
	cfg.ConnConfig.RuntimeParams["timezone"] = "UTC"
	cfg.ConnConfig.RuntimeParams["lock_timeout"] = "2000"
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "4000"
	if role != "" {
		cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
			_, err := conn.Exec(ctx, "SET ROLE "+pgx.Identifier{role}.Sanitize())
			return err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal("could not create sink test DB pool")
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatal("could not connect to sink test DB")
	}
	return pool
}

func newTestDB(t *testing.T) testDB {
	t.Helper()
	adminURL := os.Getenv("DUR053_SINK_TEST_ADMIN_URL")
	if adminURL == "" {
		adminURL = os.Getenv("DURABLE_MUTATION_DATABASE_URL")
	}
	if adminURL == "" {
		t.Skip("requires DUR053_SINK_TEST_ADMIN_URL or the mutation gate database")
	}
	bootstrap := testPool(t, adminURL, "", "")
	name := "dur053_sink_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	quoted := pgx.Identifier{name}.Sanitize()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := bootstrap.Exec(ctx, "CREATE DATABASE "+quoted); err != nil {
		t.Fatal("could not create isolated sink test DB")
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := bootstrap.Exec(ctx, "DROP DATABASE "+quoted+" WITH (FORCE)"); err != nil {
			t.Error("could not remove isolated sink test DB")
		}
	})
	admin := testPool(t, adminURL, name, "")
	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "sink-migrations", "*.up.sql"))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		sql, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := admin.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("sink migration: %v", err)
		}
	}
	return testDB{admin, testPool(t, adminURL, name, "k6_sink_service"),
		testPool(t, adminURL, name, "k6_sink_exporter"), name, adminURL}
}

func (db testDB) snapshot(t *testing.T) Snapshot {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	snap, err := exportSnapshot(ctx, db.exporter)
	if err != nil {
		t.Fatalf("sink export: %v", err)
	}
	return snap
}

func serviceHandler(t *testing.T, pool *pgxpool.Pool, grants GrantReader) *handler {
	t.Helper()
	handler, err := newHandler(testHTTP, testReviewer(), grants, &Store{pool})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func issueFence(t *testing.T, clients testClients, req Request, allocationRequestID string) int64 {
	t.Helper()
	req.Fence, req.RequestID = 0, allocationRequestID
	body := request(t, clients.engine, clients.url, "allocate", "", asJSON(t, req), http.StatusOK)
	var issued struct{ Fence int64 }
	if err := json.Unmarshal(body, &issued); err != nil || issued.Fence <= 0 {
		t.Fatal("missing issued fence")
	}
	return issued.Fence
}

func workerCall(t *testing.T, clients testClients, operation string, req Request, status int) []byte {
	t.Helper()
	return request(t, clients.ingress, clients.url, operation, workerToken(t, nil), asJSON(t, req), status)
}

func TestK6SinkConcurrentApplies(t *testing.T) {
	db := newTestDB(t)
	grants := testGrants{}
	req := grants.add(t, testRequest(), time.Now().Add(time.Minute))
	clients := newTestServer(t, serviceHandler(t, db.service, grants))
	req.Fence = issueFence(t, clients, req, "alloc-1")
	const n = 12
	body, token := asJSON(t, req), workerToken(t, nil)
	start := make(chan struct{})
	results := make([]reply, n)
	errs := make([]error, n)
	var group sync.WaitGroup
	for i := range n {
		group.Go(func() {
			<-start
			results[i], errs[i] = post(clients.ingress, clients.url+"/v1/apply", token, body)
		})
	}
	close(start)
	group.Wait()
	for i, response := range results {
		if errs[i] != nil || response.status != http.StatusOK || !bytes.Equal(results[0].body, response.body) {
			t.Fatalf("delivery %d: status=%d same receipt=%v err=%v", i, response.status, bytes.Equal(results[0].body, response.body), errs[i])
		}
	}
	snap := db.snapshot(t)
	if err := snap.CheckApplications(1); err != nil || len(snap.Receipts) != 1 || len(snap.Calls) != n+1 {
		t.Fatalf("concurrent calls: journal=%d receipts=%d calls=%d", len(snap.Journal), len(snap.Receipts), len(snap.Calls))
	}
}

func TestK6SinkCleanTwoDeliveries(t *testing.T) {
	db := newTestDB(t)
	grants := testGrants{}
	req := grants.add(t, testRequest(), time.Now().Add(time.Minute))
	clients := newTestServer(t, serviceHandler(t, db.service, grants))
	req.Fence = issueFence(t, clients, req, "alloc-1")
	body, token := asJSON(t, req), workerToken(t, nil)
	first := request(t, clients.ingress, clients.url, "apply", token, body, http.StatusOK)
	second := request(t, clients.ingress, clients.url, "apply", token, body, http.StatusOK)
	snap := db.snapshot(t)
	if err := snap.CheckApplications(1); err != nil {
		t.Fatalf("expected one journaled application, counted %d", len(snap.Journal))
	}
	if err := snap.CheckRevisionChain(); err != nil {
		t.Fatal("initial/final revision disagrees with journal count")
	}
	if len(snap.Receipts) != 1 || len(snap.Calls) != 3 || !bytes.Equal(first, second) {
		t.Fatal("expected one receipt and two byte-equivalent successful deliveries")
	}
	a, b := snap.Calls[1], snap.Calls[2]
	if a.CallID == b.CallID || a.ConnectionHash == b.ConnectionHash || a.ConnectionHash == "" ||
		a.Outcome != "APPLIED" || b.Outcome != "DUPLICATE" || a.RequestID != b.RequestID ||
		a.MessageID != b.MessageID || a.LogicalKey != b.LogicalKey || a.Attempt != b.Attempt ||
		a.ObservedAt.IsZero() || b.ObservedAt.Before(a.ObservedAt) ||
		a.BodyHash != hash(body) || a.BodyHash != b.BodyHash || a.ResponseHash == nil ||
		b.ResponseHash == nil || *a.ResponseHash != hash(first) || *a.ResponseHash != *b.ResponseHash ||
		a.ApplicationID == nil || b.ApplicationID == nil || *a.ApplicationID != snap.Receipts[0].ApplicationID ||
		*a.ApplicationID != *b.ApplicationID ||
		snap.Receipts[0].ExpectedRevision != 0 || snap.Receipts[0].AppliedRevision != 1 {
		t.Fatal("duplicate delivery evidence did not join distinct calls and connections to the original receipt")
	}
	if !reflect.DeepEqual(snap, db.snapshot(t)) {
		t.Fatal("ordered export changed between snapshots")
	}
	wantCounts := []int{1, 1, 1, 3, 1, 2}
	rows := []any{snap.Resources, snap.Journal, snap.Receipts, snap.Calls, snap.Allocations, snap.RequestBindings}
	names := []string{"resources", "journal", "receipts", "calls", "allocations", "request_bindings"}
	if len(snap.Relations) != len(wantCounts) {
		t.Fatal("missing export relation")
	}
	for i, rel := range snap.Relations {
		if rel.Relation != names[i] || rel.Count != wantCounts[i] || rel.SHA256 != hash(asJSON(t, rows[i])) {
			t.Fatalf("wrong export digest for %s", names[i])
		}
	}
	encoded := asJSON(t, snap)
	if bytes.Contains(encoded, []byte(req.GrantToken)) || bytes.Contains(encoded, []byte("DO-NOT-EXPORT")) ||
		bytes.Contains(encoded, []byte(token)) {
		t.Fatal("snapshot exported secret material")
	}
}

func TestK6SinkFenceAndAllocation(t *testing.T) {
	db := newTestDB(t)
	grants := testGrants{}
	req := grants.add(t, testRequest(), time.Now().Add(time.Minute))
	clients := newTestServer(t, serviceHandler(t, db.service, grants))
	old := issueFence(t, clients, req, "alloc-1")
	newAttempt := req
	newAttempt.Attempt, newAttempt.RequestID = 2, "apply-2"
	current := issueFence(t, clients, newAttempt, "alloc-2")
	// Both request-ID retry and retried claim with a new message use the
	// grant/attempt tuple even when a newer attempt has already allocated.
	if issueFence(t, clients, req, "alloc-1") != old || issueFence(t, clients, req, "alloc-1-retry") != old {
		t.Fatal("allocation retry changed the issued fence")
	}
	before := db.snapshot(t)
	req.Fence = old
	status, err := post(clients.ingress, clients.url+"/v1/apply", workerToken(t, nil), asJSON(t, req))
	if err != nil || status.status != http.StatusConflict || string(status.body) != "K6_SINK_STALE_FENCE\n" {
		t.Fatal("lower issued fence mutated protected state")
	}
	after := db.snapshot(t)
	if !reflect.DeepEqual(before.Resources, after.Resources) || len(after.Journal) != 0 ||
		len(after.Receipts) != 0 || len(after.Allocations) != 2 || current != old+1 {
		t.Fatal("stale request changed allocator or state")
	}
	newAttempt.Fence = old
	workerCall(t, clients, "apply", newAttempt, http.StatusForbidden)
	newAttempt.Fence = current
	receipt := workerCall(t, clients, "apply", newAttempt, http.StatusOK)
	recovery := req
	recovery.RequestID, recovery.Fence = "lookup-1", old
	if !bytes.Equal(receipt, workerCall(t, clients, "lookup", recovery, http.StatusOK)) {
		t.Fatal("same grant could not look up the original receipt")
	}
}

func TestK6SinkRequestAndKeyConflicts(t *testing.T) {
	db := newTestDB(t)
	grants := testGrants{}
	expiry := time.Now().Add(time.Minute)
	req := grants.add(t, testRequest(), expiry)
	conflict := req
	conflict.IntentID, conflict.GrantToken = "11234567-0123-4123-8123-012345678901", "private-grant-b"
	conflict.ResourceID, conflict.RequestID = "another-resource", "conflict-key"
	conflict.Arguments = json.RawMessage(`{"changed":1}`)
	conflict = grants.add(t, conflict, expiry)
	sameResource := conflict
	sameResource.IntentID, sameResource.GrantToken = "21234567-0123-4123-8123-012345678901", "private-grant-c"
	sameResource.LogicalKey, sameResource.ResourceID, sameResource.RequestID = "another-key", req.ResourceID, "conflict-revision"
	sameResource = grants.add(t, sameResource, expiry)
	clients := newTestServer(t, serviceHandler(t, db.service, grants))
	req.Fence = issueFence(t, clients, req, "alloc-1")
	workerCall(t, clients, "apply", req, http.StatusOK)
	changed := req
	changed.MessageID = "different-message"
	if string(workerCall(t, clients, "apply", changed, http.StatusConflict)) != "K6_SINK_REQUEST_CONFLICT\n" {
		t.Fatal("changed-body replay was not recorded as a request conflict")
	}
	conflict.Fence = issueFence(t, clients, conflict, "alloc-2")
	if string(workerCall(t, clients, "apply", conflict, http.StatusConflict)) != "K6_SINK_KEY_CONFLICT\n" {
		t.Fatal("conflicting stable key was not rejected")
	}
	sameResource.Fence = issueFence(t, clients, sameResource, "alloc-3")
	if string(workerCall(t, clients, "apply", sameResource, http.StatusConflict)) != "K6_SINK_REVISION_CONFLICT\n" {
		t.Fatal("current fence with an outdated expected revision was accepted")
	}
	if snap := db.snapshot(t); len(snap.Journal) != 1 || len(snap.Receipts) != 1 || snap.CheckRevisionChain() != nil {
		t.Fatal("conflict changed the protected state or receipt")
	}
}

func TestK6SinkConflictingAllocationDoesNotCreateResource(t *testing.T) {
	db := newTestDB(t)
	grants := testGrants{}
	expiry := time.Now().Add(time.Minute)
	first := grants.add(t, testRequest(), expiry)
	second := first
	second.IntentID = "11234567-0123-4123-8123-012345678901"
	second.GrantToken, second.ResourceID, second.RequestID = "private-grant-b", "new-resource", "alloc-1"
	second = grants.add(t, second, expiry)
	clients := newTestServer(t, serviceHandler(t, db.service, grants))
	issueFence(t, clients, first, "alloc-1")
	request(t, clients.engine, clients.url, "allocate", "", asJSON(t, second), http.StatusConflict)
	snap := db.snapshot(t)
	if len(snap.Resources) != 1 || len(snap.Allocations) != 1 || len(snap.RequestBindings) != 1 ||
		len(snap.Calls) != 2 || snap.Calls[1].Outcome != "REQUEST_CONFLICT" {
		t.Fatal("allocation request-ID conflict left a resource, binding or fence change")
	}
}

func TestK6SinkCompetingAuthorizedOperations(t *testing.T) {
	db := newTestDB(t)
	grants := testGrants{}
	expiry := time.Now().Add(time.Minute)
	req := grants.add(t, testRequest(), expiry)
	other := req
	other.IntentID, other.GrantToken = "11234567-0123-4123-8123-012345678901", "private-grant-b"
	other.LogicalKey, other.RequestID = "other-key", "other-request"
	other = grants.add(t, other, expiry)
	clients := newTestServer(t, serviceHandler(t, db.service, grants))
	req.Fence = issueFence(t, clients, req, "alloc-1")
	other.Fence = issueFence(t, clients, other, "alloc-2")
	a, b := asJSON(t, req), asJSON(t, other)
	token := workerToken(t, nil)
	start := make(chan struct{})
	var group sync.WaitGroup
	for i, body := range [][]byte{a, b} {
		group.Go(func() {
			<-start
			response, err := post(clients.ingress, clients.url+"/v1/apply", token, body)
			expected := []int{http.StatusConflict, http.StatusOK}[i]
			if err != nil || response.status != expected {
				t.Errorf("competing apply %d: status=%d want=%d err=%v", i, response.status, expected, err)
			}
		})
	}
	close(start)
	group.Wait()
	if snap := db.snapshot(t); len(snap.Journal) != 1 || len(snap.Receipts) != 1 || snap.Receipts[0].LogicalKey != other.LogicalKey {
		t.Fatal("competing authorized operations did not serialize to one revision")
	}
}

func TestK6SinkAtomicJournalAndReceipt(t *testing.T) {
	db := newTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := db.admin.Exec(ctx, `CREATE FUNCTION k6_sink.reject_receipt() RETURNS trigger
		LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test receipt failure'; END; $$;
		CREATE TRIGGER reject_receipt BEFORE INSERT ON k6_sink.receipts
		FOR EACH ROW EXECUTE FUNCTION k6_sink.reject_receipt();`); err != nil {
		t.Fatal(err)
	}
	grants := testGrants{}
	req := grants.add(t, testRequest(), time.Now().Add(time.Minute))
	clients := newTestServer(t, serviceHandler(t, db.service, grants))
	req.Fence = issueFence(t, clients, req, "alloc-1")
	before := db.snapshot(t)
	workerCall(t, clients, "apply", req, http.StatusServiceUnavailable)
	if !reflect.DeepEqual(before, db.snapshot(t)) {
		t.Fatal("receipt failure left a committed resource write or journal row")
	}
}

func TestK6SinkPrevalidationAndExpiredRecoveryLeaveDatabaseUnchanged(t *testing.T) {
	db := newTestDB(t)
	grants := testGrants{}
	expiry := time.Now().Add(time.Minute)
	req := grants.add(t, testRequest(), expiry)
	var equalExpiry atomic.Bool
	reader := grantFunc(func(ctx context.Context, input k6auth.ExactGrantRequest) (k6auth.ExactGrant, error) {
		g, err := grants.ReadExactGrant(ctx, input)
		if equalExpiry.Load() {
			g.DBTime = expiry
		}
		return g, err
	})
	clients := newTestServer(t, serviceHandler(t, db.service, reader))
	req.Fence = issueFence(t, clients, req, "alloc-1")
	before := db.snapshot(t)
	bad := req
	bad.ExpectedRevision = "1"
	workerCall(t, clients, "apply", bad, http.StatusForbidden)
	if snap := db.snapshot(t); !reflect.DeepEqual(before, snap) {
		t.Fatal("pre-validation rejection reached sink ledger or state")
	}
	workerCall(t, clients, "apply", req, http.StatusOK)
	before = db.snapshot(t)
	equalExpiry.Store(true)
	workerCall(t, clients, "apply", req, http.StatusForbidden)
	req.RequestID = "lookup-expired"
	workerCall(t, clients, "lookup", req, http.StatusForbidden)
	if snap := db.snapshot(t); !reflect.DeepEqual(before, snap) {
		t.Fatal("expired retry or lookup changed sink ledger or state")
	}
}

func TestK6SinkExpiryRecheckedUnderLock(t *testing.T) {
	db := newTestDB(t)
	grants := testGrants{}
	expiry := time.Now().Add(time.Minute)
	req := grants.add(t, testRequest(), expiry)
	clients := newTestServer(t, serviceHandler(t, db.service, grants))
	req.Fence = issueFence(t, clients, req, "alloc-1")
	before := db.snapshot(t)
	var reads atomic.Int32
	reader := grantFunc(func(ctx context.Context, input k6auth.ExactGrantRequest) (k6auth.ExactGrant, error) {
		read := reads.Add(1)
		g, err := grants.ReadExactGrant(ctx, input)
		if read == 2 {
			g.DBTime = expiry
		}
		return g, err
	})
	expiring := newTestServer(t, serviceHandler(t, db.service, reader))
	workerCall(t, expiring, "apply", req, http.StatusForbidden)
	if reads.Load() != 2 || !reflect.DeepEqual(before, db.snapshot(t)) {
		t.Fatal("expiry at the locked recheck left a sink write")
	}
}

func TestK6SinkSQLWriteCutoff(t *testing.T) {
	db := newTestDB(t)
	grants := testGrants{}
	req := grants.add(t, testRequest(), time.Now().Add(time.Minute))
	clients := newTestServer(t, serviceHandler(t, db.service, grants))
	req.Fence = issueFence(t, clients, req, "alloc-1")
	before := db.snapshot(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	auth, err := authorize(ctx, grants, req, testHTTP.ClockUncertainty)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.admin.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&auth.sinkCutoff); err != nil {
		t.Fatal(err)
	}
	del := delivery{"apply", hash(asJSON(t, req)), "test-connection", req, auth, false}
	_, err = (&Store{db.service}).execute(ctx, del, func(context.Context) (time.Time, error) {
		return auth.deadline, nil
	})
	if !errors.Is(err, errDenied) || !reflect.DeepEqual(before, db.snapshot(t)) {
		t.Fatal("sink DB accepted a write at or after its cutoff")
	}
}

func TestK6SinkAckLossRecovery(t *testing.T) {
	db := newTestDB(t)
	grants := testGrants{}
	req := grants.add(t, testRequest(), time.Now().Add(time.Minute))
	handler := serviceHandler(t, db.service, grants)
	clients := newTestServer(t, handler)
	req.Fence = issueFence(t, clients, req, "alloc-1")
	lost := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.ServeHTTP(dropReply{w}, r)
	}))
	if _, err := post(lost.ingress, lost.url+"/v1/apply", workerToken(t, nil), asJSON(t, req)); err == nil {
		t.Fatal("client received an acknowledgment")
	}
	snap := db.snapshot(t)
	if len(snap.Journal) != 1 || len(snap.Calls) != 2 || snap.Calls[1].ResponseHash == nil {
		t.Fatal("response was dropped without a committed application")
	}
	retry := workerCall(t, clients, "apply", req, http.StatusOK)
	req.RequestID = "lookup-1"
	lookup := workerCall(t, clients, "lookup", req, http.StatusOK)
	if !bytes.Equal(retry, lookup) || hash(retry) != *snap.Calls[1].ResponseHash ||
		db.snapshot(t).CheckApplications(1) != nil {
		t.Fatal("ack loss did not recover the original receipt")
	}
}

type dropReply struct{ http.ResponseWriter }

func (w dropReply) Write(_ []byte) (int, error) {
	conn, _, err := http.NewResponseController(w.ResponseWriter).Hijack()
	if err == nil {
		_ = conn.Close()
	}
	return 0, err
}

func TestK6SinkProcessRestart(t *testing.T) {
	db := newTestDB(t)
	expiry := time.Now().Add(time.Minute).UTC().Format(time.RFC3339Nano)
	run := func(action, expectedHash string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestK6SinkRestartHelper$")
		cmd.Env = append(os.Environ(),
			"DUR053_SINK_RESTART_ACTION="+action, "DUR053_SINK_RESTART_DB="+db.name,
			"DUR053_SINK_TEST_ADMIN_URL="+db.adminURL, "DUR053_SINK_RESTART_EXPIRY="+expiry,
			"DUR053_SINK_RESTART_RECEIPT_HASH="+expectedHash)
		return cmd.Run()
	}
	var exited *exec.ExitError
	if err := run("crash", ""); !errors.As(err, &exited) || exited.ExitCode() != 73 {
		t.Fatalf("sink process did not exit at the commit/response boundary: %v", err)
	}
	snap := db.snapshot(t)
	if len(snap.Journal) != 1 || len(snap.Receipts) != 1 || len(snap.Calls) != 2 || snap.Calls[1].ResponseHash == nil {
		t.Fatal("sink crash did not leave a durable commit")
	}
	if err := run("lookup", *snap.Calls[1].ResponseHash); err != nil {
		t.Fatalf("replacement sink process could not recover the receipt: %v", err)
	}
	after := db.snapshot(t)
	if after.CheckApplications(1) != nil || len(after.Calls) != 3 || after.Calls[2].Outcome != "FOUND" {
		t.Fatal("process restart triggered another application")
	}
}

func TestK6SinkRestartHelper(t *testing.T) {
	action := os.Getenv("DUR053_SINK_RESTART_ACTION")
	if action == "" {
		return // Invoked as a subprocess by TestK6SinkProcessRestart.
	}
	expiry, err := time.Parse(time.RFC3339Nano, os.Getenv("DUR053_SINK_RESTART_EXPIRY"))
	if err != nil {
		t.Fatal(err)
	}
	pool := testPool(t, os.Getenv("DUR053_SINK_TEST_ADMIN_URL"), os.Getenv("DUR053_SINK_RESTART_DB"), "k6_sink_service")
	grants := testGrants{}
	req := grants.add(t, testRequest(), expiry)
	handler := serviceHandler(t, pool, grants)
	clients := newTestServer(t, handler)
	switch action {
	case "crash":
		req.Fence = issueFence(t, clients, req, "alloc-1")
		crashing := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handler.ServeHTTP(exitReply{w}, r)
		}))
		workerCall(t, crashing, "apply", req, http.StatusOK)
		t.Fatal("sink did not exit before response")
	case "lookup":
		req.Fence, req.RequestID = 1, "lookup-after-restart"
		body := workerCall(t, clients, "lookup", req, http.StatusOK)
		if hash(body) != os.Getenv("DUR053_SINK_RESTART_RECEIPT_HASH") {
			t.Fatal("replacement sink returned a different receipt")
		}
	default:
		t.Fatal("unexpected subprocess mode")
	}
}

type exitReply struct{ http.ResponseWriter }

func (exitReply) Write(_ []byte) (int, error) {
	os.Exit(73)
	return 0, nil
}

func TestK6SinkDatabasePrivileges(t *testing.T) {
	db := newTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, check := range []struct {
		pool *pgxpool.Pool
		sql  string
	}{
		{db.service, `DELETE FROM k6_sink.journal`},
		{db.service, `INSERT INTO k6_sink.journal (resource_id) VALUES ('resource')`},
		{db.service, `ALTER TABLE k6_sink.resources DISABLE TRIGGER journal_resource_write`},
		{db.service, `UPDATE k6_sink.receipts SET expected_revision=2`},
		{db.exporter, `SELECT state FROM k6_sink.resources`},
		{db.exporter, `UPDATE k6_sink.resources SET latest_fence=3`},
	} {
		_, err := check.pool.Exec(ctx, check.sql)
		var denied *pgconn.PgError
		if !errors.As(err, &denied) || denied.Code != "42501" {
			t.Fatalf("expected SQLSTATE 42501 for %s, got %v", check.sql, err)
		}
	}
}

func TestK6SinkReplacementLookupDoesNotApply(t *testing.T) {
	db := newTestDB(t)
	grants := testGrants{}
	req := testRequest()
	req.NodeID = state.K6Activity
	req = grants.add(t, req, time.Now().Add(time.Minute))
	_, signer, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cfg := testHTTP
	cfg.ReceiptKey = signer
	handler, err := newHandler(cfg, testReviewer(), grants, &Store{db.service})
	if err != nil {
		t.Fatal(err)
	}
	clients := newTestServer(t, handler)
	first := req
	first.Fence = issueFence(t, clients, first, "allocate-first")
	req.Attempt = 2
	req.Fence = issueFence(t, clients, req, "allocate-replacement")
	req.RequestID = "blind-application"
	workerCall(t, clients, "apply", req, http.StatusForbidden)
	req.RequestID = "replacement-missing-lookup"
	response, err := post(clients.ingress, clients.url+"/v1/lookup", workerToken(t, nil), asJSON(t, req))
	if err != nil || response.status != http.StatusNotFound || len(db.snapshot(t).Journal) != 0 {
		t.Fatal("missing lookup applied a new effect")
	}
	first.RequestID = "old-fence-application"
	workerCall(t, clients, "apply", first, http.StatusConflict)
	req.RequestID = "known-missing-application"
	committed := workerCall(t, clients, "apply", req, http.StatusOK)
	req.Attempt++
	req.Fence = issueFence(t, clients, req, "allocate-recovery")
	req.RequestID = "recovery-lookup"
	found := workerCall(t, clients, "lookup", req, http.StatusOK)
	if !bytes.Equal(committed, found) || len(db.snapshot(t).Journal) != 1 {
		t.Fatal("replacement lookup did not recover the same committed receipt")
	}
}
