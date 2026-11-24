package k6collect

import (
	"errors"
	"io"
	"time"

	"durable-agent-execution-engine/internal/invariants/k6v1"
	"durable-agent-execution-engine/internal/k6ingress"
)

type relayCopy struct {
	open, reply, result k6ingress.Record
	dropped             bool
}

type deliveryKey struct {
	session string
	ordinal uint64
	copy    int
}

// IngressSnapshot projects the receiver-owned relay journal. The reader must
// see the entire receiver prefix. An open exchange prevents a terminal seal.
// Empty journals need the separately measured producer closure passed in
// drained, just as the direct allocator channel does.
func IngressSnapshot(in io.Reader, stream *k6v1.Stream[k6v1.IngressRow], relay, sink string, at time.Time,
	direct []k6v1.Record[k6v1.IngressRow], drained bool) error {
	raw, err := k6ingress.ReadJournal(in, relay, sink)
	if err != nil {
		return err
	}
	rows := []k6v1.Record[k6v1.IngressRow]{}
	copies := map[deliveryKey]relayCopy{}
	sessions := map[string]bool{}
	var ordinal int64
	ordinals := map[[2]string]int64{}
	closed := map[deliveryKey]bool{}
	for _, r := range raw {
		if r.Event == "seal" {
			sessions[r.Session] = true
			continue
		}
		sessions[r.Session] = false
		key := deliveryKey{r.Session, r.Ordinal, max(r.Copy, 1)}
		if closed[key] {
			return errors.New("K6_INGRESS_AFTER_CLOSE")
		}
		copy := copies[key]
		switch r.Event {
		case "inbound", "copy_open":
			if copy.open.Sequence != 0 || r.IntentID == "" {
				return errors.New("K6_INGRESS_OPEN")
			}
			copy.open = r
		case "sink_response", "local_response":
			if copy.reply.Sequence != 0 || copy.open.Sequence == 0 {
				return errors.New("K6_INGRESS_RESPONSE")
			}
			copy.reply = r
		case "worker_delivery", "fault_drop", "extra_reply":
			if copy.dropped || copy.result.Sequence != 0 || copy.reply.Sequence == 0 ||
				copy.reply.BodyHash != r.BodyHash || copy.reply.WireHash != r.WireHash ||
				copy.reply.Status != r.Status || copy.reply.Length != r.Length || copy.reply.WireLength != r.WireLength {
				return errors.New("K6_INGRESS_OUTCOME")
			}
			copy.result = r
		case "worker_drop":
			if copy.dropped || copy.result.Event == "worker_delivery" || copy.result.Event == "extra_reply" {
				return errors.New("K6_INGRESS_DROP_ORDER")
			}
			copy.dropped = true
		case "close":
			if copy.open.Sequence == 0 {
				return errors.New("K6_INGRESS_CLOSE")
			}
			exchange := [2]string{r.Session, copy.open.WorkerBinding}
			if ordinals[exchange] == 0 {
				ordinal++
				ordinals[exchange] = ordinal
			}
			row := projectCopy(copy, ordinals[exchange], int64(key.copy))
			rows = append(rows, k6v1.Record[k6v1.IngressRow]{Position: k6v1.Position{UTC: k6v1.UTC(r.Time)}, Data: row})
			delete(copies, key)
			closed[key] = true
			continue
		}
		if copy.open.Sequence == 0 || r.ConnectionHash != copy.open.ConnectionHash || r.WorkerBinding != copy.open.WorkerBinding ||
			r.IntentID != copy.open.IntentID || r.GrantHash != copy.open.GrantHash || r.Attempt != copy.open.Attempt ||
			r.RequestIDHash != copy.open.RequestIDHash || r.InvocationHash != copy.open.InvocationHash ||
			r.KeyHash != copy.open.KeyHash || r.ResourceHash != copy.open.ResourceHash {
			return errors.New("K6_INGRESS_COPY_BINDING")
		}
		copies[key] = copy
	}
	for _, terminal := range sessions {
		drained = drained && terminal
	}
	for _, item := range direct {
		if item.Data.Traffic.Operation != "allocate" {
			return errors.New("K6_ALLOCATOR_RECORD")
		}
		rows = append(rows, item)
	}
	// Merge by collector timestamp. Direct records and relay records are
	// persisted once and retained verbatim, so a prefix cannot reorder.
	if err := mergeIngress(stream, rows); err != nil {
		return err
	}
	mode := "checkpoint"
	if drained && len(copies) == 0 {
		mode = "terminal"
	}
	return k6v1.SealStream(stream, mode, k6v1.UTC(at), int64(len(stream.Rows)), 0)
}

func projectCopy(c relayCopy, ordinal, copy int64) k6v1.IngressRow {
	o := c.open
	row := k6v1.IngressRow{Ordinal: ordinal, Copy: copy, PeerHash: k6v1.Hash([]byte(o.RelayPeer)),
		OpenSequence: o.Sequence, OpenedUTC: k6v1.UTC(o.Time),
		ConnectionHash: o.ConnectionHash, Acknowledged: true, Outcome: "close", Nondelivery: c.dropped,
		Traffic: k6v1.Traffic{Method: o.Method, Operation: o.Operation, MessageHash: o.InvocationHash, RequestIDHash: o.RequestIDHash,
			KeyHash: o.KeyHash, Intent: o.IntentID, ResourceHash: o.ResourceHash, GrantHash: o.GrantHash, Attempt: o.Attempt,
			WorkerBinding: o.WorkerBinding, Request: k6v1.WireDigest{Length: int64(o.Length), SHA256: o.BodyHash},
			Wire: &k6v1.WireDigest{Length: int64(o.WireLength), SHA256: o.WireHash}},
	}
	if c.reply.Sequence != 0 {
		reply := c.reply
		row.Response = &k6v1.WireDigest{Length: int64(reply.Length), SHA256: reply.BodyHash}
		row.ResponseWire = &k6v1.WireDigest{Length: int64(reply.WireLength), SHA256: reply.WireHash}
		row.ApplicationID = reply.ApplicationID
		row.Acknowledged = c.result.Event != "fault_drop" || c.dropped
		switch {
		case c.result.Event == "extra_reply":
			row.Outcome = "duplicate_reply"
		case c.result.Event != "worker_delivery":
			row.Outcome = "withheld"
		case reply.Status == 403 || c.reply.Event == "local_response":
			row.Outcome = "denied"
		default:
			row.Outcome = "delivered"
		}
	}
	return row
}

func mergeIngress(out *k6v1.Stream[k6v1.IngressRow], complete []k6v1.Record[k6v1.IngressRow]) error {
	type key struct {
		connection, worker, exchange string
		copy                         int64
	}
	identify := func(r k6v1.IngressRow) key {
		return key{r.ConnectionHash, r.Traffic.WorkerBinding, r.Traffic.RequestIDHash, r.Copy}
	}
	seen := map[key]k6v1.Record[k6v1.IngressRow]{}
	ordinal := map[string]int64{}
	for _, row := range complete {
		k := identify(row.Data)
		if _, ok := seen[k]; ok {
			return errors.New("K6_INGRESS_DUPLICATE_COPY")
		}
		seen[k] = row
	}
	for _, previous := range out.Rows {
		k := identify(previous.Data)
		current, ok := seen[k]
		if !ok {
			return errors.New("K6_INGRESS_PREFIX_LOST")
		}
		current.Data.Ordinal, current.Position = previous.Data.Ordinal, previous.Position
		old, _ := k6v1.Canonical(previous)
		new, _ := k6v1.Canonical(current)
		if string(old) != string(new) {
			return errors.New("K6_INGRESS_PREFIX_CHANGED")
		}
		ordinal[k.worker] = previous.Data.Ordinal
		delete(seen, k)
	}
	var counter int64
	for _, value := range ordinal {
		counter = max(counter, value)
	}
	for _, row := range complete {
		k := identify(row.Data)
		if _, ok := seen[k]; !ok {
			continue
		}
		at, err := time.Parse(k6v1.TimeLayout, row.Position.UTC)
		if err != nil {
			return errors.New("K6_INGRESS_CLOCK")
		}
		if len(out.Rows) > 0 {
			prior, _ := time.Parse(k6v1.TimeLayout, out.Rows[len(out.Rows)-1].Position.UTC)
			if at.Before(prior) {
				at = prior
			}
		}
		if ordinal[k.worker] == 0 {
			counter++
			ordinal[k.worker] = counter
		}
		row.Data.Ordinal = ordinal[k.worker]
		Append(out, at, row.Data)
	}
	return nil
}
