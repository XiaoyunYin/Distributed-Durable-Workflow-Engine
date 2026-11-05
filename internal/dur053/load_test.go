package dur053

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func testConfig() Config {
	return Config{URL: "http://127.0.0.1:8080", RunID: "test", Namespace: "dur053-test", Arm: "short", Seed: 530001, Rate: 10, Count: 4, Concurrency: 4, CPUUnits: 10000, Timeout: 5 * time.Second}
}
func TestK2cAbsoluteSchedule(t *testing.T) {
	c := testConfig()
	c.Count = 10000
	c.Rate = 3
	plan, err := Plan(c)
	if err != nil {
		t.Fatal(err)
	}
	for i, row := range plan.Rows {
		if row.ScheduledNS != int64(float64(i)*1e9/3) {
			t.Fatal("absolute schedule drift")
		}
		if row.Activity != Activity(i, "short") {
			t.Fatal("workload mix changed")
		}
	}
	for _, arm := range []string{"short", "medium", "long", "cpu"} {
		for i := 0; i < 30; i++ {
			want := "dur048.sleep"
			if arm == "cpu" {
				want = "dur053.cpu"
			} else if i%10 == 8 {
				want = "pure.echo"
			} else if i%10 == 9 {
				want = "pure.add"
			}
			if Activity(i, arm) != want {
				t.Fatal("workload mix guard bypassed")
			}
		}
	}
}
func TestK2cNoCoordinatedOmission(t *testing.T) {
	c := testConfig()
	plan, _ := Plan(c)
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	done := make(chan struct{})
	var resultStatus string
	go func() {
		result := RunWithClock(context.Background(), c, plan, realClock{}, func(_ context.Context, b []byte) (int, []byte, error) {
			started <- struct{}{}
			<-release
			var req map[string]any
			_ = json.Unmarshal(b, &req)
			return 201, []byte(fmt.Sprintf(`{"workflow":{"workflow_id":%q}}`, req["workflow_id"])), nil
		})
		resultStatus = result.Status
		close(done)
	}()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for i := 0; i < c.Count; i++ {
		select {
		case <-started:
		case <-timer.C:
			close(release)
			<-done
			t.Fatal("completion blocked scheduled arrival (coordinated omission)")
		}
	}
	close(release)
	<-done
	if resultStatus != "PASS" {
		t.Fatal("absolute arrivals missed despite available capacity")
	}
}
func TestK2cCapacityNeverBackfills(t *testing.T) {
	c := testConfig()
	c.Concurrency = 1
	plan, _ := Plan(c)
	var sends atomic.Int32
	result := RunWithClock(context.Background(), c, plan, realClock{}, func(_ context.Context, _ []byte) (int, []byte, error) {
		sends.Add(1)
		time.Sleep(600 * time.Millisecond)
		return 201, []byte(`{"workflow":{"workflow_id":"` + plan.Rows[0].WorkflowID + `"}}`), nil
	})
	if sends.Load() != 1 || result.Status != "FAIL" {
		t.Fatal("saturated generator backfilled or hid arrival loss")
	}
	for _, r := range result.Rows[1:] {
		if r.Outcome != "CAPACITY_MISSED" {
			t.Fatal("capacity miss not recorded")
		}
	}
}

type lateClock struct{ now time.Time }

func (c *lateClock) Now() time.Time { return c.now }
func (c *lateClock) Wait(_ context.Context, at time.Time) error {
	c.now = at.Add(time.Second)
	return nil
}
func TestK2cLateArrivalsNeverBackfill(t *testing.T) {
	c := testConfig()
	plan, _ := Plan(c)
	clock := &lateClock{now: time.Now()}
	var sends atomic.Int32
	result := RunWithClock(context.Background(), c, plan, clock, func(_ context.Context, _ []byte) (int, []byte, error) { sends.Add(1); return 500, nil, nil })
	// Only the final arrival has no subsequent interval to expire it.
	if sends.Load() != 1 || result.Status != "FAIL" {
		t.Fatal("late generator replayed expired arrivals")
	}
	for _, r := range result.Rows[:3] {
		if r.Outcome != "MISSED_DEADLINE" {
			t.Fatal("late generator replayed expired arrivals")
		}
	}
}

func TestK2cConfigGuards(t *testing.T) {
	tests := []func(*Config){func(c *Config) { c.Rate = 0 }, func(c *Config) { c.Count = 0 }, func(c *Config) { c.Count = 1000001 }, func(c *Config) { c.Concurrency = 0 }, func(c *Config) { c.Concurrency = 1025 }, func(c *Config) { c.CPUUnits = 0 }, func(c *Config) { c.CPUUnits = 1000001 }, func(c *Config) { c.Timeout = 0 }, func(c *Config) { c.Namespace = "" }, func(c *Config) { c.RunID = "bad/id" }, func(c *Config) { c.Arm = "invalid" }, func(c *Config) { c.URL = "file:///unsafe" }, func(c *Config) { c.Rate = 0.000000000001 }}
	for _, change := range tests {
		c := testConfig()
		change(&c)
		if _, err := Plan(c); err == nil {
			t.Fatal("campaign configuration guard bypassed")
		}
	}
}
func TestK2cResponseManifest(t *testing.T) {
	c := testConfig()
	c.Count = 1
	for _, kind := range []string{"rejected", "server-error", "wrong-workflow", "bad-json", "transport-error"} {
		plan, _ := Plan(c)
		result := RunWithClock(context.Background(), c, plan, realClock{}, func(_ context.Context, _ []byte) (int, []byte, error) {
			switch kind {
			case "rejected":
				return 429, []byte(`{}`), nil
			case "server-error":
				return 503, []byte(`{}`), nil
			case "wrong-workflow":
				return 201, []byte(`{"workflow":{"workflow_id":"other"}}`), nil
			case "bad-json":
				return 201, []byte(`broken`), nil
			default:
				return 0, nil, fmt.Errorf("transport lost")
			}
		})
		want := "UNCERTAIN"
		if kind == "rejected" {
			want = "REJECTED"
		}
		if result.Status != "FAIL" || len(result.Rows) != 1 || result.Rows[0].Outcome != want || result.Rows[0].SendNS == nil || result.Rows[0].FinishedNS == nil || !json.Valid(result.Rows[0].Response) {
			t.Fatal("submission response evidence guard bypassed")
		}
	}
}

type dispatchClock struct {
	start time.Time
	reads atomic.Int32
	child chan struct{}
}

func (c *dispatchClock) Now() time.Time {
	n := c.reads.Add(1)
	if n <= 2 {
		return c.start
	}
	if n == 3 {
		close(c.child)
		return c.start.Add(time.Second)
	}
	return c.start.Add(100 * time.Millisecond)
}
func (c *dispatchClock) Wait(_ context.Context, at time.Time) error {
	if at.After(c.start) {
		<-c.child
	}
	return nil
}
func TestK2cDelayedSendDeadline(t *testing.T) {
	c := testConfig()
	c.Count = 2
	plan, _ := Plan(c)
	clock := &dispatchClock{start: time.Now(), child: make(chan struct{})}
	var sends atomic.Int32
	result := RunWithClock(context.Background(), c, plan, clock, func(_ context.Context, b []byte) (int, []byte, error) {
		sends.Add(1)
		var req map[string]any
		_ = json.Unmarshal(b, &req)
		return 201, []byte(fmt.Sprintf(`{"workflow":{"workflow_id":%q}}`, req["workflow_id"])), nil
	})
	row := result.Rows[0]
	if row.Outcome != "MISSED_SEND_DEADLINE" || row.SendNS != nil || row.SentAt != nil || sends.Load() != 1 {
		t.Fatal("expired sender backfilled or fabricated send time")
	}
}
