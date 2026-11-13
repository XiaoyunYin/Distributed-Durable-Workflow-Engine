package dur053

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestK4JournalAppendIsLossResistant(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	j := K4Journal{File: f}
	if err = j.Append(map[string]any{"event": "plan", "count": 64}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			if e := j.Append(map[string]any{"event": "uncertain", "id": index}); e != nil {
				t.Error(e)
			}
		}(i)
	}
	wg.Wait()
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	decoder := json.NewDecoder(reader)
	var plan map[string]any
	if err = decoder.Decode(&plan); err != nil || plan["event"] != "plan" {
		t.Fatal("K4 durable plan missing", err)
	}
	seen := map[int]bool{}
	for i := 0; i < 64; i++ {
		var row struct {
			Event string
			ID    int
		}
		if err = decoder.Decode(&row); err != nil {
			t.Fatal("K4 append lost row", err)
		}
		if seen[row.ID] || row.Event != "uncertain" {
			t.Fatal("K4 journal interleaved or erased row")
		}
		seen[row.ID] = true
	}
	if err = j.Append(plan); err == nil {
		t.Fatal("K4 closed journal ignored persistence error")
	}
}

type failingK4Sync struct{ short bool }

func (f failingK4Sync) Write(b []byte) (int, error) {
	if f.short {
		return len(b) - 1, nil
	}
	return len(b), nil
}
func (f failingK4Sync) Sync() error { return fmt.Errorf("fsync unavailable") }
func TestK4JournalRejectsSyncFailure(t *testing.T) {
	j := K4Journal{File: failingK4Sync{}}
	if err := j.Append(map[string]string{"outcome": "uncertain"}); err == nil {
		t.Fatal("K4 journal accepted missing fsync")
	}
	j.File = failingK4Sync{short: true}
	if err := j.Append(map[string]string{"outcome": "uncertain"}); err == nil || !strings.Contains(err.Error(), "short") {
		t.Fatal("K4 journal accepted short write")
	}
}

type memoryK4Journal struct {
	mu     sync.Mutex
	data   []byte
	synced map[string]string
	fail   bool
}

func (m *memoryK4Journal) Write(b []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = append(m.data, b...)
	return len(b), nil
}
func (m *memoryK4Journal) Sync() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail {
		return fmt.Errorf("sync failed")
	}
	m.synced = map[string]string{}
	for _, line := range bytes.Split(m.data, []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var row map[string]any
		_ = json.Unmarshal(line, &row)
		if row["event"] == "arrival" {
			m.synced[row["key"].(string)] = row["outcome"].(string)
		}
	}
	return nil
}
func TestK4PlanPersistsBeforeHTTPAndDoesNotWaitForCompletions(t *testing.T) {
	c := testConfig()
	c.Rate = 20
	c.Count = 4
	c.Concurrency = 4
	plan, _ := Plan(c)
	mem := &memoryK4Journal{}
	started := make(chan string, 4)
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- runK4Plan(context.Background(), c, plan, time.Now().Add(20*time.Millisecond), &K4Journal{File: mem}, realClock{}, func(_ context.Context, body []byte) (int, []byte, error) {
			var req struct {
				ID string `json:"workflow_id"`
			}
			_ = json.Unmarshal(body, &req)
			mem.mu.Lock()
			outcome := mem.synced[req.ID]
			mem.mu.Unlock()
			if outcome != "uncertain" {
				t.Error("K4 sent HTTP without durable uncertain journal")
			}
			started <- req.ID
			<-release
			return 201, []byte(fmt.Sprintf(`{"workflow":{"workflow_id":%q}}`, req.ID)), nil
		}, func() (time.Time, error) { return time.Time{}, nil })
	}()
	for i := 0; i < 4; i++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			close(release)
			<-done
			t.Fatal("K4 completion blocked absolute arrivals")
		}
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
func TestK4PlanStopsBeforeAnyHTTPOnJournalFailure(t *testing.T) {
	c := testConfig()
	plan, _ := Plan(c)
	sent := false
	err := runK4Plan(context.Background(), c, plan, time.Now(), &K4Journal{File: &memoryK4Journal{fail: true}}, realClock{}, func(context.Context, []byte) (int, []byte, error) { sent = true; return 0, nil, nil }, func() (time.Time, error) { return time.Time{}, nil })
	if err == nil || sent {
		t.Fatal("K4 journal failure allowed HTTP")
	}
}
func TestK4PlanRetainsRejectedAndUncertainOutcomes(t *testing.T) {
	for _, status := range []int{400, 503} {
		c := testConfig()
		c.Count = 1
		plan, _ := Plan(c)
		mem := &memoryK4Journal{}
		err := runK4Plan(context.Background(), c, plan, time.Now(), &K4Journal{File: mem}, realClock{}, func(context.Context, []byte) (int, []byte, error) { return status, []byte(`{}`), nil }, func() (time.Time, error) { return time.Time{}, nil })
		if err != nil {
			t.Fatal(err)
		}
		expected := "uncertain"
		if status == 400 {
			expected = "rejected"
		}
		if mem.synced[plan.Rows[0].Key] != expected {
			t.Fatal("K4 adverse outcome erased or treated as harness failure")
		}
	}
}
