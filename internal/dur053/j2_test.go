package dur053

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"
)

type virtualJ2Clock struct {
	mu     sync.Mutex
	now    time.Time
	before func(int, time.Time) time.Time
	waits  int
}

func (c *virtualJ2Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *virtualJ2Clock) advance(at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if at.After(c.now) {
		c.now = at
	}
}

func (c *virtualJ2Clock) Wait(ctx context.Context, at time.Time) error {
	if c.before != nil {
		at = c.before(c.waits, at)
	}
	c.waits++
	c.advance(at)
	return ctx.Err()
}

type j2TestJournal struct {
	data   bytes.Buffer
	before func([]byte) int
	synced func(j2TestEvent)
}

type j2TestEvent struct {
	Event   string    `json:"event"`
	Key     string    `json:"key"`
	Outcome string    `json:"outcome"`
	Error   string    `json:"error"`
	Status  int       `json:"status"`
	Sent    time.Time `json:"sent"`
}

func (j *j2TestJournal) Write(p []byte) (int, error) {
	if j.before != nil {
		p = p[:j.before(p)]
	}
	return j.data.Write(p)
}

func (j *j2TestJournal) Sync() error {
	if j.synced != nil {
		lines := j.data.Bytes()
		last := lines[bytes.LastIndexByte(lines[:len(lines)-1], '\n')+1:]
		var record j2TestEvent
		if err := json.Unmarshal(last, &record); err != nil {
			return err
		}
		j.synced(record)
	}
	return nil
}

func j2Response(body []byte) (int, []byte, error) {
	var req struct {
		ID string `json:"workflow_id"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return 0, nil, err
	}
	return 201, []byte(fmt.Sprintf(`{"workflow":{"workflow_id":%q}}`, req.ID)), nil
}

func readJ2TestEvents(t *testing.T, data []byte) []j2TestEvent {
	t.Helper()
	var events []j2TestEvent
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte("\n")) {
		var record j2TestEvent
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatal(err)
		}
		events = append(events, record)
	}
	return events
}

func TestJ2SingleSegmentStockK4ByteParity(t *testing.T) {
	for _, reject := range []bool{false, true} {
		t.Run(fmt.Sprintf("reject=%t", reject), func(t *testing.T) {
			const count = 5
			start := time.Unix(1_700_000_000, 17).UTC()
			c := testConfig()
			c.RunID, c.Seed, c.Rate, c.Count = "parity-run", 246, 2.5, count
			stockPlan, err := Plan(c)
			if err != nil {
				t.Fatal(err)
			}
			period := J2Segment{Rate: c.Rate, Duration: 2 * time.Second}
			j2Config, plan, slots, err := j2Plan(c, []J2Segment{period})
			if err != nil {
				t.Fatal(err)
			}
			send := func(_ context.Context, body []byte) (int, []byte, error) {
				if reject && bytes.Contains(body, []byte("-000002")) {
					return 429, []byte("{}"), nil
				}
				return j2Response(body)
			}
			makeClock := func() (*virtualJ2Clock, *j2TestJournal) {
				completed := make(chan struct{}, count)
				clock := &virtualJ2Clock{now: start, before: func(i int, at time.Time) time.Time {
					if i > 0 {
						<-completed
					}
					return at
				}}
				journal := &j2TestJournal{synced: func(row j2TestEvent) {
					if row.Status != 0 {
						completed <- struct{}{}
					}
				}}
				return clock, journal
			}
			clock, stock := makeClock()
			if err := runK4Plan(context.Background(), c, stockPlan, start, &K4Journal{File: stock}, clock, send,
				func() (time.Time, error) { return start.Add(period.Duration), nil }); err != nil {
				t.Fatal(err)
			}
			clock, j2 := makeClock()
			result, err := runJ2Plan(context.Background(), j2Config, plan, slots, start, &K4Journal{File: j2}, clock, send)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(stock.data.Bytes(), j2.data.Bytes()) {
				t.Fatalf("stock K4 journal:\n%s\nJ2 journal:\n%s", &stock.data, &j2.data)
			}
			n := 0
			events := readJ2TestEvents(t, stock.data.Bytes())
			for _, row := range events {
				if row.Outcome == "accepted" {
					n++
				}
			}
			want := "FAIL"
			if n == count && events[len(events)-1].Event == "closed" {
				want = "PASS"
			}
			if result != (J2Result{Offered: count, Accepted: n, Status: want}) {
				t.Fatalf("J2 verdict differs from stock journal: %+v", result)
			}
		})
	}
}

func TestJ2StepSharesSlotsAndSequence(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	c := testConfig()
	c.Concurrency = 16
	c, plan, slots, err := j2Plan(c, []J2Segment{
		{Rate: 16, Duration: time.Second},
		{Rate: 4, Duration: 250 * time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(slots) != 17 || slots[15].cutoff != time.Second || slots[16].offset != time.Second ||
		plan.Rows[16].Sequence != 16 || plan.Rows[16].Activity != Activity(16, c.Arm) {
		t.Fatalf("invalid transition: %+v %#v", slots, plan.Rows[16])
	}
	started := make(chan struct{}, 16)
	release := make(chan struct{})
	clock := &virtualJ2Clock{now: start, before: func(i int, at time.Time) time.Time {
		if i > 0 {
			<-started
		}
		return at
	}}
	journal := &j2TestJournal{synced: func(row j2TestEvent) {
		if row.Error == "capacity-missed" {
			close(release)
		}
	}}
	send := func(_ context.Context, body []byte) (int, []byte, error) {
		started <- struct{}{}
		<-release
		return j2Response(body)
	}
	result, err := runJ2Plan(context.Background(), c, plan, slots, start, &K4Journal{File: journal}, clock, send)
	if err != nil || result != (J2Result{Offered: 17, Accepted: 16, Status: "FAIL"}) {
		t.Fatalf("step did not inherit the occupied cap: %+v %v", result, err)
	}
	misses := 0
	for _, row := range readJ2TestEvents(t, journal.data.Bytes()) {
		if row.Error == "capacity-missed" {
			misses++
			if row.Key != plan.Rows[16].Key || !row.Sent.IsZero() {
				t.Fatalf("wrong capacity miss: %+v", row)
			}
		}
	}
	if misses != 1 {
		t.Fatalf("capacity misses = %d", misses)
	}
}

func TestJ2CutoffAfterIntentSyncAtRateBoundary(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	c, plan, slots, err := j2Plan(testConfig(), []J2Segment{
		{Rate: 3, Duration: 200 * time.Millisecond},
		{Rate: 5, Duration: 100 * time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(slots) != 2 || slots[0].cutoff != 200*time.Millisecond || slots[1].offset != slots[0].cutoff ||
		slots[1].cutoff != 300*time.Millisecond {
		t.Fatalf("incorrect half-open cutoffs: %+v", slots)
	}
	skipped := make(chan struct{})
	clock := &virtualJ2Clock{now: start, before: func(i int, at time.Time) time.Time {
		if i == 1 {
			<-skipped
		}
		return at
	}}
	journal := &j2TestJournal{synced: func(row j2TestEvent) {
		if row.Key == plan.Rows[0].Key && row.Outcome == "uncertain" {
			clock.advance(start.Add(slots[0].cutoff))
		}
		if row.Error == "cutoff-after-intent" {
			close(skipped)
		}
	}}
	send := func(_ context.Context, body []byte) (int, []byte, error) {
		if !bytes.Contains(body, []byte(plan.Rows[1].Key)) {
			return 0, nil, fmt.Errorf("expired hold arrival reached HTTP")
		}
		return j2Response(body)
	}
	result, err := runJ2Plan(context.Background(), c, plan, slots, start, &K4Journal{File: journal}, clock, send)
	if err != nil || result.Accepted != 1 || result.Status != "FAIL" {
		t.Fatalf("cutoff result = %+v, %v", result, err)
	}
	events := readJ2TestEvents(t, journal.data.Bytes())
	if events[2].Error != "cutoff-after-intent" || events[2].Outcome != "unsent" || !events[2].Sent.IsZero() {
		t.Fatalf("cutoff evidence = %+v", events[2])
	}
}

func TestJ2InterruptedJournalRetainsPlanAndIntent(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	c, plan, slots, err := j2Plan(testConfig(), []J2Segment{
		{Rate: 1, Duration: time.Second},
		{Rate: 1, Duration: time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	interrupted := make(chan struct{})
	clock := &virtualJ2Clock{now: start, before: func(i int, at time.Time) time.Time {
		if i == 1 {
			<-interrupted
		}
		return at
	}}
	journal := &j2TestJournal{before: func(p []byte) int {
		if bytes.Contains(p, []byte(`"outcome":"accepted"`)) {
			cancel()
			close(interrupted)
			return len(p) / 2
		}
		return len(p)
	}}
	send := func(_ context.Context, body []byte) (int, []byte, error) { return j2Response(body) }
	result, err := runJ2Plan(ctx, c, plan, slots, start, &K4Journal{File: journal}, clock, send)
	if err == nil || result.Status != "FAIL" || result.Accepted != 0 {
		t.Fatalf("interrupted result = %+v, %v", result, err)
	}
	lines := bytes.Split(journal.data.Bytes(), []byte("\n"))
	if len(lines) != 3 || json.Valid(lines[2]) {
		t.Fatalf("missing torn suffix: %s", &journal.data)
	}
	events := readJ2TestEvents(t, bytes.Join(lines[:2], []byte("\n")))
	if events[0].Event != "plan" || events[1].Outcome != "uncertain" || events[1].Key != plan.Rows[0].Key {
		t.Fatalf("durable journal prefix = %+v", events)
	}
}

type j2RoundTripFunc func(*http.Request) (*http.Response, error)

func (f j2RoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestJ2TransportChecksCutoffBeforeDelegating(t *testing.T) {
	var called bool
	c := testConfig()
	c.BearerTokenFile = ""
	send := k4HTTPSender(c, j2CutoffTransport{base: j2RoundTripFunc(func(*http.Request) (*http.Response, error) {
		called = true
		return nil, io.EOF
	})})
	ctx := context.WithValue(context.Background(), j2CutoffKey{}, time.Now().Add(-time.Second))
	status, _, err := send(ctx, []byte(`{"workflow_id":"late"}`))
	if status != 0 || err == nil || called {
		t.Fatalf("expired request crossed the transport cutoff: %d %v %t", status, err, called)
	}
}
