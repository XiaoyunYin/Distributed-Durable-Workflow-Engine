package k6v1

import (
	"cmp"
	"reflect"
	"slices"
	"time"
)

const capturePredicate = "EngineCaptureClosure"

type captureKey struct {
	subject string
	key     string
}

func capturedKey(row Row) captureKey {
	raw, _ := encode(row["row_key"])
	return captureKey{row["subject"].(string), string(raw)}
}

func observedKey(subject string, row Row) captureKey {
	key := make([]any, 0, len(captureProjections[subject].keys))
	for _, col := range captureProjections[subject].keys {
		key = append(key, row[col.Name])
	}
	raw, _ := encode(key)
	return captureKey{subject, string(raw)}
}

func captureImage(row Row, name string) Row {
	image, _ := row[name].(map[string]any)
	return image
}

type captureTransaction struct{ xid, workflow any }

type captureClosure struct {
	engineOrder
	last      map[captureKey]Row
	writes    map[captureTransaction][]Row
	histories map[captureTransaction][]Row
}

func (e engineOrder) checkCaptureClosure() {
	c := captureClosure{e, map[captureKey]Row{}, map[captureTransaction][]Row{}, map[captureTransaction][]Row{}}
	for _, row := range e.rows[CaptureQueue] {
		e.violate(capturePredicate, "CAPTURE_QUEUE_NOT_DRAINED", CaptureQueue, row)
	}
	if _, present := e.rows[WriteCapture]; !present {
		return
	}
	for _, h := range e.rows[history] {
		if a := e.byTransition[h["transition_id"]]; committed(a) {
			tx := captureTransaction{a["transaction_id"], h["workflow_id"]}
			c.histories[tx] = append(c.histories[tx], h)
		}
	}
	for _, row := range e.rows[WriteCapture] {
		tx := captureTransaction{row["transaction_id"], row["workflow_id"]}
		c.writes[tx] = append(c.writes[tx], row)
		if _, durable := e.rows[row["subject"].(string)]; durable {
			c.checkImageChain(row)
		}
	}
	c.checkEndpoints()
	c.checkOrder()
	c.checkOwnerInventory()
	c.checkTransactionLinks()
}

func (c captureClosure) checkImageChain(row Row) {
	key := capturedKey(row)
	prior := c.last[key]
	before, after := captureImage(row, "before_image"), captureImage(row, "after_image")
	switch row["operation"] {
	case "insert":
		if before != nil || after == nil || captureImage(prior, "after_image") != nil {
			c.violate(capturePredicate, "CAPTURE_IMAGE_CHAIN", WriteCapture, row)
		}
	case "update", "delete", "acquire", "renewal", "takeover", "release":
		if before == nil || (after == nil) != (row["operation"] == "delete") ||
			(prior == nil && key.subject != leases) ||
			(prior != nil && !reflect.DeepEqual(captureImage(prior, "after_image"), before)) {
			c.violate(capturePredicate, "CAPTURE_IMAGE_CHAIN", WriteCapture, row)
		}
	default:
		c.violate(capturePredicate, "CAPTURE_OPERATION", WriteCapture, row)
	}
	c.last[key] = row
}

func (c captureClosure) checkEndpoints() {
	observed := map[captureKey]bool{}
	for _, spec := range EngineRelations(false) {
		if _, tracked := captureProjections[spec.Name]; !tracked {
			continue
		}
		for _, row := range c.rows[spec.Name] {
			key := observedKey(spec.Name, row)
			observed[key] = true
			last := c.last[key]
			if last == nil && spec.Name == leases && row["owner_id"] == nil && number(row["epoch"]) == 0 {
				continue // Bootstrap rows predate episode capture.
			}
			switch {
			case last == nil:
				c.violate(capturePredicate, "CAPTURE_ROW_MISSING", spec.Name, row)
			case !captureMatchesEndpoint(spec.Name, captureImage(last, "after_image"), row):
				c.violate(capturePredicate, "CAPTURE_ENDPOINT", WriteCapture, last)
			}
		}
	}
	for _, row := range c.rows[WriteCapture] {
		key := capturedKey(row)
		if c.last[key]["event_order"] == row["event_order"] && row["after_image"] != nil && !observed[key] {
			c.violate(capturePredicate, "CAPTURE_ORPHAN", WriteCapture, row)
		}
	}
}

func captureMatchesEndpoint(subject string, image, endpoint Row) bool {
	if image == nil {
		return false
	}
	for _, col := range captureProjections[subject].fields {
		value, exists := endpoint[col.Name]
		if !exists {
			continue // A capture-only digest or physical Kafka key.
		}
		sampled := image[col.Name]
		if col.Kind == Timestamp && sampled != nil {
			t, err := time.Parse(time.RFC3339Nano, sampled.(string))
			if err != nil {
				return false
			}
			sampled = UTC(t)
		}
		if sampled != value {
			return false
		}
	}
	return true
}

func (c captureClosure) checkOrder() {
	type entry struct {
		row      Row
		relation string
	}
	var entries []entry
	for _, relation := range []string{commitAudit, WriteCapture} {
		for _, row := range c.rows[relation] {
			entries = append(entries, entry{row, relation})
		}
	}
	slices.SortStableFunc(entries, func(a, b entry) int {
		return cmp.Compare(number(a.row["event_order"]), number(b.row["event_order"]))
	})
	seen := map[any]bool{}
	var xid any
	var order int64
	for _, e := range entries {
		next := number(e.row["event_order"])
		if next <= order {
			c.violate(capturePredicate, "CAPTURE_ORDER_COLLISION", e.relation, e.row)
		}
		order = next
		if e.row["transaction_id"] != xid {
			xid = e.row["transaction_id"]
			if xid == "" || seen[xid] {
				c.violate(capturePredicate, "CAPTURE_TRANSACTION_ORDER", e.relation, e.row)
			}
			seen[xid] = true
		}
	}
}

func (c captureClosure) checkOwnerInventory() {
	const predicate = "OwnerAtCommit"
	inventory, observed := map[any]Row{}, map[any]Row{}
	fenced := map[any]bool{}
	acquired := map[[3]any]Row{}
	authority := map[any]Row{}
	for _, a := range c.rows[commitAudit] {
		if a["kind"] == "acquire" || a["kind"] == "release" {
			acquired[[3]any{a["transaction_id"], a["partition_id"], a["kind"]}] = a
		}
		if a["kind"] == "history" && a["owner_id"] != "" {
			fenced[a["transaction_id"]] = true
		}
	}
	for _, row := range c.rows[OwnerWrites] {
		inventory[row["event_order"]] = row
		if row["subject"] != leases && row["subject"] != acquisitions {
			fenced[row["transaction_id"]] = true
		}
	}
	for _, row := range c.rows[WriteCapture] {
		order, subject := row["event_order"], row["subject"].(string)
		observed[order] = row
		if _, businessRow := c.rows[subject]; !businessRow {
			continue
		}
		a := inventory[order]
		if a == nil {
			if fenced[row["transaction_id"]] || ownerWriteRequired(row) {
				c.violate(predicate, "OWNER_INVENTORY_WRITE_MISSING", WriteCapture, row)
			}
			continue
		}
		c.checkOwnerWrite(row, a, authority, acquired)
	}
	for _, row := range c.rows[OwnerWrites] {
		write := observed[row["event_order"]]
		subject, _ := write["subject"].(string)
		if _, businessRow := c.rows[subject]; !businessRow {
			c.violate(predicate, "OWNER_INVENTORY_ORPHAN", OwnerWrites, row)
		}
	}
}

func ownerWriteRequired(row Row) bool {
	switch row["subject"] {
	case leases, acquisitions, timers, "engine.retry_policies":
		return true
	case wakeups:
		return row["operation"] != "insert"
	case history:
		return captureImage(row, "after_image")["scheduler_epoch"] != nil
	case approvalIntents:
		before, after := captureImage(row, "before_image"), captureImage(row, "after_image")
		return row["operation"] == "insert" || after["grant_token_hash"] != before["grant_token_hash"] ||
			after["grant_expires_at"] != before["grant_expires_at"]
	case reconciliation:
		return captureImage(row, "after_image")["kind"] == "EXPIRED_ATTEMPT" && row["operation"] == "insert"
	default:
		return false
	}
}

func (c captureClosure) checkOwnerWrite(row, a Row, authority map[any]Row, acquired map[[3]any]Row) {
	const predicate = "OwnerAtCommit"
	partition := a["partition_id"]
	if a["transaction_id"] != row["transaction_id"] || a["subject"] != row["subject"] ||
		a["operation"] != row["operation"] || partition != row["partition_id"] {
		c.violate(predicate, "OWNER_INVENTORY_BINDING", OwnerWrites, a)
		return
	}
	if row["subject"] == leases {
		kind := row["operation"]
		if kind == "takeover" {
			kind = "acquire"
		}
		if kind == "acquire" || kind == "release" {
			audit := acquired[[3]any{row["transaction_id"], partition, kind}]
			if audit["owner_id"] != a["owner_id"] || audit["epoch"] != a["epoch"] {
				c.violate(predicate, "OWNER_LEASE_AUDIT_BINDING", OwnerWrites, a)
			}
			if kind == "acquire" {
				authority[partition] = a
				return
			}
		}
	}
	prior := authority[partition]
	if prior["owner_id"] != a["owner_id"] || prior["epoch"] != a["epoch"] {
		c.violate(predicate, "OWNER_INVENTORY_STALE_EPOCH", OwnerWrites, a)
	}
	if row["operation"] == "release" {
		delete(authority, partition)
	}
}

func (c captureClosure) checkTransactionLinks() {
	for _, row := range c.rows[WriteCapture] {
		tx := captureTransaction{row["transaction_id"], row["workflow_id"]}
		histories := c.histories[tx]
		key := row["row_key"].([]any)
		after := captureImage(row, "after_image")
		switch row["subject"] {
		case history:
			a := c.byTransition[key[0]]
			if !committed(a) || a["transaction_id"] != tx.xid {
				c.violate(capturePredicate, "CAPTURE_HISTORY_TRANSACTION", WriteCapture, row)
			}
		case nodeRows:
			if len(histories) == 0 {
				c.violate(capturePredicate, "CAPTURE_NODE_TRANSACTION", WriteCapture, row)
			}
		case outbox:
			if row["operation"] == "insert" && (after["key"] != key[0] ||
				!slices.ContainsFunc(histories, func(h Row) bool { return h["revision"] == after["aggregate_revision"] })) {
				c.violate(dispatchPredicate, "CAPTURE_OUTBOX_TRANSACTION", WriteCapture, row)
			}
		case resultRequest:
			c.checkCapturedResult(row, c.writes[tx], histories)
		}
	}
	for _, h := range c.rows[history] {
		if h["reason"] != resultRecorded || !committed(c.byTransition[h["transition_id"]]) {
			continue
		}
		tx := captureTransaction{c.byTransition[h["transition_id"]]["transaction_id"], h["workflow_id"]}
		if !slices.ContainsFunc(c.writes[tx], func(row Row) bool {
			return row["subject"] == resultRequest && row["operation"] == "ACCEPTED" && captureMatchesAttempt(row, h)
		}) {
			c.violate("CurrentClaimWinner", "CAPTURE_RESULT_MISSING", history, h)
		}
	}
}

func captureMatchesAttempt(row, attempt Row) bool {
	key := row["row_key"].([]any)
	return key[0] == attempt["workflow_id"] && key[1] == attempt["node_id"] &&
		key[2] == attempt["iteration"] && key[3] == attempt["attempt_number"]
}

func (c captureClosure) checkCapturedResult(row Row, writes, histories []Row) {
	const predicate = "CurrentClaimWinner"
	if row["operation"] != "ACCEPTED" {
		return
	}
	result := captureImage(row, "after_image")
	if !slices.ContainsFunc(histories, func(h Row) bool { return h["reason"] == resultRecorded && captureMatchesAttempt(row, h) }) {
		c.violate(predicate, "CAPTURE_RESULT_TRANSACTION", WriteCapture, row)
		return
	}
	key := capturedKey(row).key
	for _, write := range writes {
		if write["subject"] != activityAttempts || capturedKey(write).key != key {
			continue
		}
		before, after := captureImage(write, "before_image"), captureImage(write, "after_image")
		if before["state"] != "CLAIMED" || after["state"] != result["attempt_state"] {
			continue
		}
		if before["claim_token_hash"] != result["token_hash"] || before["worker_request_id"] != result["worker_request_id"] {
			c.violate(predicate, "CAPTURE_RESULT_IDENTITY", WriteCapture, row)
		}
		return
	}
	c.violate(predicate, "CAPTURE_RESULT_ATTEMPT_WRITE", WriteCapture, row)
}
