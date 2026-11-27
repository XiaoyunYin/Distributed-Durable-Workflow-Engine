package dur053

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/invariants"
)

type dispatchFile struct {
	bytes.Buffer
	flushes int
	flush   func(int) error
}

func (f *dispatchFile) Sync() error {
	f.flushes++
	if f.flush != nil {
		return f.flush(f.flushes)
	}
	return nil
}

func newDispatchTest(t *testing.T, periods ...J2Segment) (J2DispatchResult, invariants.Arrivals, []j2Slot, time.Time, *virtualJ2Clock) {
	t.Helper()
	c := testConfig()
	c.Concurrency = j2Cap
	_, plan, slots, err := buildJ2Plan(c, periods, true)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Unix(1770000000, 0)
	return J2DispatchResult{J2Result: J2Result{Offered: len(slots), Status: "FAIL"},
			Schema: j2DispatchSchema, ScheduleSHA256: strings.Repeat("a", 64), AllAccepted: "FAIL"},
		plan, slots, start, &virtualJ2Clock{now: start.Add(-5 * time.Second)}
}

func TestJ2DispatchCutoffs(t *testing.T) {
	_, _, grid, _, _ := newDispatchTest(t, J2Segment{18.63484081654246, 720 * time.Second})
	_, _, continuous, _, _ := newDispatchTest(t,
		J2Segment{10.091970208646075, 1920 * time.Second}, J2Segment{24.33102566649034, 300 * time.Second})
	for name, pair := range map[string][2]j2Slot{
		"last grid": {grid[len(grid)-1], {719995417835, 720049080756}},
		"last hold": {continuous[19376], {1919942251058, 1920000000000}},
		"first C":   {continuous[19377], {1920000000000, 1920041099788}},
		"last C":    {continuous[len(continuous)-1], {2219987353597, 2220028453385}},
	} {
		if pair[0] != pair[1] {
			t.Errorf("%s: got %+v want %+v", name, pair[0], pair[1])
		}
	}
}

func TestJ2PrecommitFencesDispatch(t *testing.T) {
	for _, scenario := range []string{"slow-ready", "slow-overrun", "sync-error", "directory-error", "short-write"} {
		t.Run(scenario, func(t *testing.T) {
			result, plan, slots, start, clock := newDispatchTest(t, J2Segment{4, time.Second})
			file := &dispatchFile{flush: func(n int) error {
				if n != 1 {
					return nil
				}
				switch scenario {
				case "slow-ready":
					clock.advance(start.Add(-j2DispatchLead))
				case "slow-overrun":
					clock.advance(start.Add(-j2DispatchLead + 1))
				case "sync-error":
					return errors.New("sync unavailable")
				}
				return nil
			}}
			var storage j2SyncWriter = file
			if scenario == "short-write" {
				storage = shortJ2Write{file}
			}
			directory := false
			got, err := dispatchJ2(context.Background(), result, plan, slots, start, clock, storage, func() error {
				directory = true
				if scenario == "directory-error" {
					return errors.New("directory unavailable")
				}
				return nil
			}, func(_ context.Context, row invariants.Arrival, at, _ time.Time) j2Attempt {
				if scenario != "slow-ready" || !directory {
					t.Error("HTTP released before a valid precommit")
				}
				return acceptedJ2(row, at)
			})
			if scenario == "slow-ready" {
				if err != nil || !got.DispatchEnabled || got.Status != "PASS" || got.Accepted != 4 {
					t.Fatalf("ready precommit: %+v %v", got, err)
				}
			} else if err == nil || got.DispatchEnabled || got.Accepted != 0 {
				t.Fatalf("failed precommit: %+v %v", got, err)
			}
		})
	}
}

type shortJ2Write struct{ *dispatchFile }

func (s shortJ2Write) Write(p []byte) (int, error) { return s.dispatchFile.Write(p[:len(p)-1]) }

func acceptedJ2(row invariants.Arrival, at time.Time) j2Attempt {
	return j2Attempt{entered: at, written: at, callbacks: 1, status: 201, ack: row.Key}
}

func TestJ2BackgroundSyncHoldsNoLease(t *testing.T) {
	result, plan, slots, start, clock := newDispatchTest(t, J2Segment{128, time.Second})
	entered, unblocked := make(chan struct{}), make(chan struct{})
	completed := make(chan struct{}, len(slots))
	file := &dispatchFile{flush: func(n int) error {
		if n == 2 {
			close(entered)
			<-unblocked
		}
		return nil
	}}
	clock.before = func(i int, at time.Time) time.Time {
		if i > 0 {
			<-completed
		}
		if i == j2OutcomeBatch {
			<-entered
		}
		if i == len(slots)-1 {
			close(unblocked)
		}
		return at
	}
	got, err := dispatchJ2(context.Background(), result, plan, slots, start, clock, file, func() error { return nil },
		func(_ context.Context, row invariants.Arrival, at, _ time.Time) j2Attempt {
			// The HTTP request completes before the writer resumes. Signal the
			// clock in the next goroutine so it also sees the lease release.
			completed <- struct{}{}
			return acceptedJ2(row, at)
		})
	if err != nil || got.Status != "PASS" || got.Accepted != 128 || file.flushes != 4 {
		t.Fatalf("background sync: %+v %v, sync count %d", got, err, file.flushes)
	}
}

func TestJ2CapAcrossStepAndBackgroundFailure(t *testing.T) {
	result, plan, slots, start, clock := newDispatchTest(t, J2Segment{16, time.Second}, J2Segment{4, time.Second})
	release := make(chan struct{})
	file := &dispatchFile{flush: func(n int) error {
		if n == 2 {
			return errors.New("outcome sync failed")
		}
		return nil
	}}
	clock.before = func(i int, at time.Time) time.Time {
		if i == len(slots)-1 {
			defer close(release)
			return at.Add(slots[i].cutoff - slots[i].offset)
		}
		return at
	}
	got, err := dispatchJ2(context.Background(), result, plan, slots, start, clock, file, func() error { return nil },
		func(_ context.Context, row invariants.Arrival, at, _ time.Time) j2Attempt {
			if row.Sequence >= j2Cap {
				t.Error("leased request crossed a full cap")
			}
			<-release
			return acceptedJ2(row, at)
		})
	if err == nil || !strings.Contains(err.Error(), "outcome sync failed") || !got.DispatchEnabled || bytes.Contains(file.Bytes(), []byte(`"event":"closed"`)) {
		t.Fatalf("background error not retained as partial: %+v %v", got, err)
	}
	if bytes.Count(file.Bytes(), []byte(`"error":"capacity-missed"`)) != 3 || !bytes.Contains(file.Bytes(), []byte(`"error":"missed-deadline"`)) {
		t.Fatal("cap or deadline refusal missing")
	}
}

func TestJ2WriteGuardBoundariesAndRace(t *testing.T) {
	cutoff := time.Now()
	for _, tc := range []struct {
		name string
		at   time.Time
		err  error
		want int32
	}{
		{"on-time", cutoff.Add(-1), nil, j2WrittenInTime},
		{"at-cutoff", cutoff, nil, j2WriteExpired},
		{"late", cutoff.Add(1), nil, j2WriteExpired},
		{"failed-write", cutoff.Add(-1), io.ErrShortWrite, j2WriteExpired},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			guard := &j2WriteGuard{cancel: cancel, cutoff: cutoff}
			guard.observe(tc.at, tc.err)
			guard.expire()
			if guard.state.Load() != tc.want || (ctx.Err() == nil) != (tc.want == j2WrittenInTime) ||
				(guard.first.Load() == nil) != (tc.err != nil) {
				t.Fatalf("guard state=%v err=%v", guard.state.Load(), ctx.Err())
			}
		})
	}
	for range 100 {
		ctx, cancel := context.WithCancel(context.Background())
		guard := &j2WriteGuard{cancel: cancel, cutoff: cutoff}
		var racing sync.WaitGroup
		racing.Go(guard.expire)
		racing.Go(func() { guard.observe(cutoff.Add(-1), nil) })
		racing.Wait()
		if guard.first.Load() == nil || (ctx.Err() == nil) != (guard.state.Load() == j2WrittenInTime) {
			t.Error("lost write observation or guard state during race")
		}
		cancel()
	}
}

func TestJ2HTTPObservesWriteAndAllowsResponseAfterCutoff(t *testing.T) {
	_, plan, _, _, _ := newDispatchTest(t, J2Segment{1, time.Second})
	row := plan.Rows[0]
	received := make(chan struct{})
	reply := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if !bytes.Equal(body, row.Request) || r.Header.Get("X-Dur053-K4-Request") != row.Key {
			t.Error("changed HTTP wire body or identity")
		}
		close(received)
		<-reply
		w.WriteHeader(201)
		fmt.Fprintf(w, `{"workflow":{"workflow_id":%q}}`, row.Key)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	at := time.Now()
	cutoff := at.Add(200 * time.Millisecond)
	send := newJ2HTTPSender(Config{URL: server.URL, Timeout: 2 * time.Second})
	go func() {
		select {
		case <-received:
		case <-ctx.Done():
		}
		time.Sleep(max(0, time.Until(cutoff)+time.Millisecond))
		close(reply)
	}()
	result := send(ctx, row, at, cutoff)
	if result.entered.Before(at) || result.written.Before(result.entered) || !result.written.Before(cutoff) ||
		result.status != 201 || result.ack != row.Key || result.callbacks != 1 || result.reason != "" {
		t.Fatalf("unexpected HTTP result: %+v", result)
	}
}

func TestJ2HTTPRefusalAndFailedWrite(t *testing.T) {
	_, plan, _, _, _ := newDispatchTest(t, J2Segment{1, time.Second})
	called := false
	transport := j2TransportFunc(func(req *http.Request) (*http.Response, error) {
		called = true
		if req.GetBody != nil {
			t.Error("HTTP request can be rewound for retry")
		}
		httptrace.ContextClientTrace(req.Context()).WroteRequest(httptrace.WroteRequestInfo{Err: io.ErrShortWrite})
		<-req.Context().Done()
		return nil, req.Context().Err()
	})
	send := j2HTTPSender(Config{URL: "http://localhost", Timeout: time.Second}, transport)
	at := time.Now()
	if got := send(context.Background(), plan.Rows[0], at, at); called || got.refused.IsZero() || !got.entered.IsZero() {
		t.Fatalf("expired slot reached HTTP: %+v", got)
	}
	got := send(context.Background(), plan.Rows[0], at, time.Now().Add(50*time.Millisecond))
	if !called || got.reason != "write-cutoff" || !got.written.IsZero() || got.callbacks != 1 {
		t.Fatalf("failed write was misclassified: %+v", got)
	}
}

func TestJ2CrashLeavesOnlyDurablePrecommit(t *testing.T) {
	if path := os.Getenv("J2_CRASH_TEST_FILE"); path != "" {
		result, plan, slots, start, clock := newDispatchTest(t, J2Segment{1, time.Second})
		file, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = dispatchJ2(context.Background(), result, plan, slots, start, clock, file, func() error {
			if os.Getenv("J2_CRASH_TEST_POINT") == "precommit" {
				os.Exit(91)
			}
			return nil
		}, func(_ context.Context, row invariants.Arrival, at, _ time.Time) j2Attempt {
			// Simulate a recorded HTTP write in memory and terminate before the
			// attempt becomes an outcome. There is no graceful close.
			if attempt := acceptedJ2(row, at); !attempt.written.IsZero() {
				os.Exit(92)
			}
			return j2Attempt{}
		})
		t.Fatal("failed to reach crash point")
	}
	for i, point := range []string{"precommit", "write"} {
		t.Run(point, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "sender.jsonl")
			child := exec.Command(os.Args[0], "-test.run=^TestJ2CrashLeavesOnlyDurablePrecommit$")
			child.Env = append(os.Environ(), "J2_CRASH_TEST_FILE="+path, "J2_CRASH_TEST_POINT="+point)
			var code *exec.ExitError
			if output, err := child.CombinedOutput(); !errors.As(err, &code) || code.ExitCode() != 91+i {
				t.Fatalf("unexpected child exit: %s %v", output, err)
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			lines := bytes.Split(bytes.TrimSpace(raw), []byte("\n"))
			var seal struct {
				Event string `json:"event"`
				Hash  string `json:"sha256"`
			}
			if len(lines) != 3 || json.Unmarshal(lines[2], &seal) != nil || seal.Event != "precommit" ||
				seal.Hash != sumHex(append(bytes.Join(lines[:2], []byte("\n")), '\n')) {
				t.Fatal("crash left an invalid or closed journal")
			}
		})
	}
}

type countSyncWriter struct{ bytes int }

func (w *countSyncWriter) Write(p []byte) (int, error) { w.bytes += len(p); return len(p), nil }
func (*countSyncWriter) Sync() error                   { return nil }

func TestJ2MaximumPrecommitFitsBound(t *testing.T) {
	result, plan, slots, start, clock := newDispatchTest(t, J2Segment{j2DispatchMax, time.Second})
	w := &countSyncWriter{}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	begin := time.Now()
	_, err := commitJ2Plan(w, func() error { return nil }, plan, slots, result.ScheduleSHA256, start, clock)
	runtime.ReadMemStats(&after)
	if err != nil || len(slots) != j2DispatchMax || w.bytes > j2PrecommitBytes || w.bytes == 0 {
		t.Fatalf("invalid bounded precommit: bytes %d, err %v", w.bytes, err)
	}
	t.Logf("max population encoded %d bytes in %s (total Go allocation delta %d bytes)",
		w.bytes, time.Since(begin), after.TotalAlloc-before.TotalAlloc)
}
