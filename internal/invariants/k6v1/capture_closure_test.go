package k6v1

import (
	"encoding/json"
	"maps"
	"slices"
	"strings"
	"testing"
	"testing/fstest"
)

func TestK6CaptureClosure(t *testing.T) {
	n := func(s string) json.Number { return json.Number(s) }
	key := []any{"workflow", "node", n("0"), n("1")}
	attempt := Row{"workflow_id": "workflow", "node_id": "node", "iteration": n("0"), "attempt_number": n("1")}
	write := Row{"event_order": n("10"), "transaction_id": "1", "workflow_id": "workflow",
		"partition_id": n("0"), "subject": activityAttempts, "operation": "update", "row_key": key,
		"before_image": map[string]any{"state": "CLAIMED", "claim_token_hash": "token", "worker_request_id": "request", "receipt_id": "receipt"},
		"after_image":  map[string]any{"state": "SUCCEEDED"},
	}
	h := maps.Clone(attempt)
	h["transition_id"], h["revision"], h["reason"] = "transition", n("2"), resultRecorded
	audit := Row{"transaction_id": "1", "event_order": n("11"), "finalized_at": "time", "kind": "history", "transition_id": "transition"}
	result := maps.Clone(write)
	result["subject"], result["operation"], result["event_order"] = resultRequest, "ACCEPTED", n("12")
	result["after_image"] = map[string]any{"attempt_state": "SUCCEEDED", "token_hash": "token", "worker_request_id": "request", "receipt_id": "receipt"}
	timer := Row{"subject": timers, "operation": "insert", "partition_id": n("0"), "event_order": n("10"),
		"transaction_id": "1", "row_key": []any{"timer"}}
	owner := Row{"subject": timers, "operation": "insert", "partition_id": n("0"), "event_order": n("10"),
		"transaction_id": "1", "owner_id": "owner", "epoch": n("1")}

	for _, tc := range []struct {
		name, code string
		check      func(c captureClosure)
	}{
		{"DirtyQueue", "CAPTURE_QUEUE_NOT_DRAINED", func(c captureClosure) {
			c.rows[CaptureQueue] = []Row{{"id": n("1")}}
			c.checkCaptureClosure()
		}},
		{"MissingWrite", "CAPTURE_ROW_MISSING", func(c captureClosure) {
			c.rows[activityAttempts] = []Row{attempt}
			c.checkEndpoints()
		}},
		{"BrokenChain", "CAPTURE_IMAGE_CHAIN", func(c captureClosure) {
			c.last[capturedKey(write)] = Row{"after_image": map[string]any{"state": "DISPATCHABLE"}}
			c.checkImageChain(write)
		}},
		{"InvalidInsert", "CAPTURE_IMAGE_CHAIN", func(c captureClosure) {
			row := maps.Clone(write)
			row["operation"] = "insert"
			c.checkImageChain(row)
		}},
		{"InvalidOperation", "CAPTURE_OPERATION", func(c captureClosure) {
			row := maps.Clone(write)
			row["operation"] = "copy"
			c.checkImageChain(row)
		}},
		{"BadEndpoint", "CAPTURE_ENDPOINT", func(c captureClosure) {
			c.last[capturedKey(write)] = write
			end := maps.Clone(attempt)
			end["state"] = "CLAIMED"
			c.rows[activityAttempts] = []Row{end}
			c.checkEndpoints()
		}},
		{"OrphanWrite", "CAPTURE_ORPHAN", func(c captureClosure) {
			c.last[capturedKey(write)] = write
			c.rows[activityAttempts], c.rows[WriteCapture] = nil, []Row{write}
			c.checkEndpoints()
		}},
		{"OrderCollision", "CAPTURE_ORDER_COLLISION", func(c captureClosure) {
			row := maps.Clone(audit)
			row["event_order"] = write["event_order"]
			c.rows[commitAudit], c.rows[WriteCapture] = []Row{row}, []Row{write}
			c.checkOrder()
		}},
		{"SplitTransaction", "CAPTURE_TRANSACTION_ORDER", func(c captureClosure) {
			row := maps.Clone(audit)
			row["transaction_id"] = "2"
			c.rows[commitAudit], c.rows[WriteCapture] = []Row{row}, []Row{write, result}
			c.checkOrder()
		}},
		{"OmittedOwnerClass", "OWNER_INVENTORY_WRITE_MISSING", func(c captureClosure) {
			c.rows[timers], c.rows[WriteCapture] = nil, []Row{timer}
			c.checkOwnerInventory()
		}},
		{"OmittedFencedClass", "OWNER_INVENTORY_WRITE_MISSING", func(c captureClosure) {
			row := maps.Clone(audit)
			row["owner_id"] = "owner"
			c.rows[commitAudit], c.rows[activityAttempts], c.rows[WriteCapture] = []Row{row}, nil, []Row{write}
			c.checkOwnerInventory()
		}},
		{"OrphanInventory", "OWNER_INVENTORY_ORPHAN", func(c captureClosure) {
			c.rows[OwnerWrites] = []Row{owner}
			c.checkOwnerInventory()
		}},
		{"WrongOwnerTransaction", "OWNER_INVENTORY_BINDING", func(c captureClosure) {
			row := maps.Clone(owner)
			row["transaction_id"] = "2"
			c.checkOwnerWrite(timer, row, nil, nil)
		}},
		{"StaleEpoch", "OWNER_INVENTORY_STALE_EPOCH", func(c captureClosure) {
			prior := maps.Clone(owner)
			prior["epoch"] = n("2")
			c.checkOwnerWrite(timer, owner, map[any]Row{n("0"): prior}, nil)
		}},
		{"UnboundLease", "OWNER_LEASE_AUDIT_BINDING", func(c captureClosure) {
			row, a := maps.Clone(timer), maps.Clone(owner)
			for _, item := range []Row{row, a} {
				item["subject"], item["operation"] = leases, "acquire"
			}
			c.checkOwnerWrite(row, a, map[any]Row{}, nil)
		}},
		{"WrongHistoryTransaction", "CAPTURE_HISTORY_TRANSACTION", func(c captureClosure) {
			row := maps.Clone(write)
			row["subject"], row["row_key"], row["transaction_id"] = history, []any{"transition"}, "2"
			c.rows[WriteCapture], c.byTransition["transition"] = []Row{row}, audit
			c.checkTransactionLinks()
		}},
		{"WrongNodeTransaction", "CAPTURE_NODE_TRANSACTION", func(c captureClosure) {
			row := maps.Clone(write)
			row["subject"] = nodeRows
			c.rows[WriteCapture] = []Row{row}
			c.checkTransactionLinks()
		}},
		{"WrongAttemptTransaction", "CAPTURE_ATTEMPT_TRANSACTION", func(c captureClosure) {
			c.checkAttemptTransaction(write, nil)
		}},
		{"WrongOutboxTransaction", "CAPTURE_OUTBOX_TRANSACTION", func(c captureClosure) {
			row := maps.Clone(write)
			row["subject"], row["operation"], row["after_image"] = outbox, "insert", map[string]any{"aggregate_revision": n("2"), "key": key[0]}
			c.rows[WriteCapture] = []Row{row}
			c.checkTransactionLinks()
		}},
		{"MissingResult", "CAPTURE_RESULT_MISSING", func(c captureClosure) {
			c.rows[history], c.byTransition["transition"] = []Row{h}, audit
			c.checkTransactionLinks()
		}},
		{"WrongResultTransaction", "CAPTURE_RESULT_TRANSACTION", func(c captureClosure) {
			c.checkCapturedResult(result, []Row{write}, nil)
		}},
		{"MissingResultAttempt", "CAPTURE_RESULT_ATTEMPT_WRITE", func(c captureClosure) {
			c.checkCapturedResult(result, nil, []Row{h})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newClosureFixture()
			tc.check(c)
			wantCaptureCode(t, c, tc.code)
		})
	}
	for _, field := range []string{"token_hash", "worker_request_id", "receipt_id"} {
		t.Run(field, func(t *testing.T) {
			row, image := maps.Clone(result), maps.Clone(captureImage(result, "after_image"))
			image[field] = "wrong"
			row["after_image"] = map[string]any(image)
			c := newClosureFixture()
			c.checkCapturedResult(row, []Row{write}, []Row{h})
			wantCaptureCode(t, c, "CAPTURE_RESULT_IDENTITY")
		})
	}
	t.Run("CleanResult", func(t *testing.T) {
		c := newClosureFixture()
		c.checkCapturedResult(result, []Row{write}, []Row{h})
		if len(c.result.Findings) != 0 {
			t.Fatalf("clean result capture: %+v", c.result.Findings)
		}
	})
}

func newClosureFixture() captureClosure {
	r := newResult(Manifest{}, "")
	e := newEngineOrder(&r, map[string][]Row{}, nil, nil, func(predicate, code, relation string, row Row) {
		r.record(Violation, predicate, code, RelationPath(relation), nil)
	})
	return captureClosure{e, map[captureKey]Row{}, map[captureTransaction][]Row{}, map[captureTransaction][]Row{}}
}

func wantCaptureCode(t *testing.T, c captureClosure, code string) {
	t.Helper()
	if !slices.ContainsFunc(c.result.Findings, func(f Finding) bool { return f.Kind == Violation && f.Code == code }) {
		t.Fatalf("expected capture violation %s, got %+v", code, c.result.Findings)
	}
}

func TestK6CapturePublicShape(t *testing.T) {
	raw := `{"event_order":1,"transaction_id":"5","subject":"physical_delivery","operation":"ACCEPTED",
		"row_key":["consumer","topic",0,1],"workflow_id":null,"partition_id":null,
		"before_image":null,"after_image":{"event_id":"event"}}`
	for _, tc := range []struct{ name, raw string }{
		{"SecretField", strings.Replace(raw, `"event"`, `"event","raw_token":"secret"`, 1)},
		{"KeyType", strings.Replace(raw, `"topic",0`, `"topic","0"`, 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value, err := parse([]byte(tc.raw))
			if err != nil {
				t.Fatal(err)
			}
			if ValidateRow(CaptureRelations()[2], value.(map[string]any)) == nil {
				t.Fatal("capture projection accepted a malformed row")
			}
		})
	}
}

func TestK6CaptureIdlePrefix(t *testing.T) {
	for _, spec := range CaptureRelations()[1:] {
		before := NewWatermark(spec)
		end := NewWatermark(spec)
		first := Row{"event_order": json.Number("1"), "subject": history, "operation": "insert"}
		before.Add(first, "")
		end.Add(first, "")
		end.Add(Row{"event_order": json.Number("2"), "subject": leases, "operation": "renewal"}, "")
		if !equalWatermark(before.Finish(), end.Finish()) {
			t.Fatal("capture lease renewal invalidated the cut")
		}
		end.Add(Row{"event_order": json.Number("3"), "subject": leases, "operation": "takeover"}, "")
		if equalWatermark(before.Finish(), end.Finish()) {
			t.Fatal("capture takeover did not invalidate the cut")
		}
	}
}

func TestK6CaptureIndexPath(t *testing.T) {
	for _, spec := range CaptureRelations() {
		raw, err := Canonical(Index{Schema: IndexVersion, Files: []File{{Path: RelationPath(spec.Name), SHA256: Hash(nil)}}})
		if err != nil {
			t.Fatal(err)
		}
		r := Verify(fstest.MapFS{IndexPath: &fstest.MapFile{Data: raw}})
		if len(r.Findings) != 1 || r.Findings[0].Code != "required_proof_missing:"+ManifestPath {
			t.Fatalf("capture index entry was rejected: %+v", r.Findings)
		}
	}
}
