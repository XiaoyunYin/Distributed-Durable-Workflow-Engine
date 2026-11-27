package dur053

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"durable-agent-execution-engine/internal/invariants"
)

const (
	j2PrecommitBytes = 64 << 20
	j2OutcomeBatch   = 64
)

type j2SyncWriter interface {
	io.Writer
	Sync() error
}

type j2BoundedWriter struct {
	writer io.Writer
	left   int
}

func (w *j2BoundedWriter) Write(data []byte) (int, error) {
	if len(data) > w.left {
		return 0, errors.New("J2 precommit exceeds 64 MiB")
	}
	n, err := w.writer.Write(data)
	w.left -= n
	return n, err
}

type j2Flush struct {
	Event          string `json:"event"`
	Batch          int    `json:"batch"`
	LockAcquired   *int64 `json:"lock_acquired_ns"`
	SyncEntry      *int64 `json:"sync_entry_ns"`
	SyncReturn     *int64 `json:"sync_return_ns"`
	DirReturn      *int64 `json:"directory_sync_return_ns,omitempty"`
	PrecommitStart *int64 `json:"precommit_start_ns,omitempty"`
	PrecommitBytes int    `json:"precommit_bytes,omitempty"`
	PeakRSSKiB     int64  `json:"peak_rss_kib,omitempty"`
}

type j2Outcome struct {
	Event          string `json:"event"`
	Sequence       int    `json:"sequence"`
	Key            string `json:"key"`
	Intent         *int64 `json:"dispatch_intent_ns"`
	Transport      *int64 `json:"transport_entry_ns"`
	Refusal        *int64 `json:"refusal_ns"`
	Written        *int64 `json:"request_written_ns"`
	Completed      *int64 `json:"completed_ns"`
	WriteCallbacks int    `json:"write_callbacks"`
	Status         int    `json:"status"`
	Outcome        string `json:"outcome"`
	Error          string `json:"error"`
	Acknowledged   string `json:"acknowledged_workflow_id,omitempty"`
	InFlight       int    `json:"in_flight_at_release"`
	Batch          int    `json:"batch"`
}

func commitJ2Plan(file j2SyncWriter, syncDir func() error, plan invariants.Arrivals, slots []j2Slot,
	scheduleHash string, start time.Time, clock Clock,
) (j2Flush, error) {
	begin := clock.Now()
	buffer := bufio.NewWriterSize(file, 64<<10)
	limit := &j2BoundedWriter{writer: buffer, left: j2PrecommitBytes}
	digest := sha256.New()
	stream := io.MultiWriter(limit, digest)
	if err := streamJ2Plan(stream, plan, scheduleHash, start); err != nil {
		return j2Flush{}, err
	}
	encode := json.NewEncoder(stream)
	for i, row := range plan.Rows {
		if err := encode.Encode(map[string]any{
			"event": "intent", "sequence": i, "key": row.Key, "workflow_id": row.WorkflowID,
			"at_ns": int64(slots[i].offset), "cutoff_ns": int64(slots[i].cutoff),
			"request_sha256": sumHex(row.Request), "prepared_ns": clock.Now().Sub(start).Nanoseconds(),
		}); err != nil {
			return j2Flush{}, err
		}
	}
	if err := json.NewEncoder(limit).Encode(map[string]any{
		"event": "precommit", "count": len(slots), "sha256": hex.EncodeToString(digest.Sum(nil)),
	}); err != nil {
		return j2Flush{}, err
	}
	if err := buffer.Flush(); err != nil {
		return j2Flush{}, err
	}
	flush := j2Flush{Event: "flush", SyncEntry: offsetNS(start, clock.Now())}
	if err := file.Sync(); err != nil {
		return j2Flush{}, err
	}
	flush.SyncReturn = offsetNS(start, clock.Now())
	if err := syncDir(); err != nil {
		return j2Flush{}, err
	}
	flush.DirReturn = offsetNS(start, clock.Now())
	flush.PrecommitStart = offsetNS(start, begin)
	flush.PrecommitBytes = j2PrecommitBytes - limit.left
	flush.PeakRSSKiB = precommitPeakRSS()
	return flush, nil
}

// Keep the historical plan shape for independent identity replay while encoding
// it a row at a time. The explicit Rows field shadows the embedded slice.
func streamJ2Plan(stream io.Writer, plan invariants.Arrivals, scheduleHash string, start time.Time) error {
	header, err := json.Marshal(struct {
		invariants.Arrivals
		Rows *int `json:"rows,omitempty"`
	}{Arrivals: plan})
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(stream, `{"event":"plan","schema":%q,"schedule_sha256":%q,"origin_ns":%d,"plan":%s,"rows":[`,
		j2DispatchSchema, scheduleHash, start.UnixNano(), header[:len(header)-1]); err != nil {
		return err
	}
	for i, row := range plan.Rows {
		if i != 0 {
			if _, err := io.WriteString(stream, ","); err != nil {
				return err
			}
		}
		row.ScheduledAt = start.Add(time.Duration(row.ScheduledNS)).UTC()
		data, err := json.Marshal(row)
		if err != nil {
			return err
		}
		if _, err := stream.Write(data); err != nil {
			return err
		}
	}
	_, err = io.WriteString(stream, "]}}\n")
	return err
}

func writeJ2Outcomes(file j2SyncWriter, precommit j2Flush, outcomes []j2Outcome, ready <-chan int,
	start time.Time, clock Clock,
) error {
	buffer := bufio.NewWriterSize(file, 64<<10)
	digest := sha256.New()
	appendRow := json.NewEncoder(io.MultiWriter(buffer, digest)).Encode
	if err := appendRow(precommit); err != nil {
		return err
	}
	batch, pending, total := 1, 0, 0
	flush := func() error {
		if pending == 0 {
			return nil
		}
		if err := buffer.Flush(); err != nil {
			return err
		}
		timing := j2Flush{Event: "flush", Batch: batch, SyncEntry: offsetNS(start, clock.Now())}
		if err := file.Sync(); err != nil {
			return err
		}
		timing.SyncReturn = offsetNS(start, clock.Now())
		batch++
		pending = 0
		return appendRow(timing)
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case seq, ok := <-ready:
			if !ok {
				if err := flush(); err != nil {
					return err
				}
				if total != len(outcomes) {
					return errors.Join(errors.New("J2 dispatch interrupted before all outcomes"), buffer.Flush(), file.Sync())
				}
				if err := json.NewEncoder(buffer).Encode(map[string]any{
					"event": "closed", "count": total, "sha256": hex.EncodeToString(digest.Sum(nil)),
				}); err != nil {
					return err
				}
				if err := buffer.Flush(); err != nil {
					return err
				}
				return file.Sync()
			}
			row := outcomes[seq]
			row.Batch = batch
			if err := appendRow(row); err != nil {
				return err
			}
			total++
			pending++
			if pending == j2OutcomeBatch {
				if err := flush(); err != nil {
					return err
				}
			}
		case <-ticker.C:
			if err := flush(); err != nil {
				return err
			}
		}
	}
}
