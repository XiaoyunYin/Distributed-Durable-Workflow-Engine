// Package dur053 implements the registered absolute-schedule campaign workload.
package dur053

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"durable-agent-execution-engine/internal/invariants"
	"durable-agent-execution-engine/internal/state"
)

var idPattern = regexp.MustCompile(`^[a-zA-Z0-9-]{1,64}$`)

type Config struct {
	BearerTokenFile string
	URL             string
	Namespace       string
	RunID           string
	Arm             string
	Seed            int64
	Rate            float64
	Count           int
	Concurrency     int
	CPUUnits        int
	Timeout         time.Duration
}

func (c Config) Validate() error {
	if c.Rate <= 0 || math.IsNaN(c.Rate) || math.IsInf(c.Rate, 0) || c.Rate > 100000 || c.Count < 1 || c.Count > 1000000 || c.Concurrency < 1 || c.Concurrency > 1024 || c.Timeout <= 0 || c.CPUUnits < 1 || c.CPUUnits > 1000000 || !idPattern.MatchString(c.RunID) || c.Namespace == "" {
		return fmt.Errorf("invalid bounded campaign configuration")
	}
	parsed, err := url.ParseRequestURI(c.URL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || float64(c.Count)/c.Rate > 72*3600 {
		return fmt.Errorf("invalid URL or schedule exceeds 72 hours")
	}
	switch c.Arm {
	case "short", "medium", "long", "cpu":
	default:
		return fmt.Errorf("invalid arm")
	}
	return nil
}

// Offset is computed from the index, never by adding rounded sleep intervals.
func Offset(i int, rate float64) time.Duration {
	return time.Duration(float64(i) * float64(time.Second) / rate)
}
func Activity(i int, arm string) string {
	if arm == "cpu" {
		return "dur053.cpu"
	}
	if i%10 == 8 {
		return "pure.echo"
	}
	if i%10 == 9 {
		return "pure.add"
	}
	return "dur048.sleep"
}
func DefinitionID(run, activity string) string { return "dur053-" + run + "-" + activity }

func Prepare(ctx context.Context, store *state.Store, c Config) error {
	if err := c.Validate(); err != nil {
		return err
	}
	names := []string{"dur048.sleep", "pure.echo", "pure.add"}
	if c.Arm == "cpu" {
		names = []string{"dur053.cpu"}
	}
	for _, name := range names {
		graph, _ := json.Marshal(map[string]any{"entry": name, "nodes": []map[string]string{{"id": name, "kind": "activity", "next": "done"}, {"id": "done", "kind": "success"}}})
		versions, _ := json.Marshal(map[string]string{name: "v1"})
		effects, _ := json.Marshal(map[string]string{name: "PURE_ACTIVITY"})
		sum := sha256.Sum256(append(append(graph, versions...), effects...))
		if err := store.CreateDefinition(ctx, state.DefinitionInput{DefinitionID: DefinitionID(c.RunID, name), Version: 1, DefinitionHash: hex.EncodeToString(sum[:]), Graph: graph, ActivityVersions: versions, EffectClasses: effects}); err != nil {
			return err
		}
	}
	return nil
}
func Plan(c Config) (invariants.Arrivals, error) {
	if err := c.Validate(); err != nil {
		return invariants.Arrivals{}, err
	}
	result := invariants.Arrivals{Schema: "dur053.arrivals.v1", Offered: c.Count, Status: "IN_PROGRESS", Namespace: c.Namespace, RunID: c.RunID, Arm: c.Arm, Seed: c.Seed, Rate: c.Rate, Concurrency: c.Concurrency, CPUUnits: c.CPUUnits, Rows: make([]invariants.Arrival, c.Count)}
	for i := range result.Rows {
		activity := Activity(i, c.Arm)
		key := fmt.Sprintf("dur053-%s-%d-%06d", c.RunID, c.Seed, i)
		var input any = map[string]any{"run_id": c.RunID, "sequence": i}
		var expected any = input
		if activity == "pure.add" {
			input = []int{2, 3}
			expected = 5
		}
		if activity == "dur053.cpu" {
			digest := make([]byte, 32)
			for j := 0; j < c.CPUUnits; j++ {
				sum := sha256.Sum256(digest)
				digest = sum[:]
			}
			expected = map[string]any{"digest": hex.EncodeToString(digest), "work_units": c.CPUUnits}
		}
		body, _ := json.Marshal(map[string]any{"workflow_id": key, "namespace": c.Namespace, "submission_key": key, "payload": input, "definition_id": DefinitionID(c.RunID, activity), "definition_version": 1, "initial_node_id": activity, "initial_input": input, "actor_id": "dur053-loadgen"})
		want, _ := json.Marshal(expected)
		result.Rows[i] = invariants.Arrival{Sequence: i, Key: key, WorkflowID: key, Activity: activity, ScheduledNS: int64(Offset(i, c.Rate)), Outcome: "NOT_SENT", Response: json.RawMessage("null"), Request: body, Expected: want}
	}
	return result, nil
}

// Clock preserves monotonic scheduling and allows exact virtual-clock tests.
type Clock interface {
	Now() time.Time
	Wait(context.Context, time.Time) error
}
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }
func (realClock) Wait(ctx context.Context, at time.Time) error {
	timer := time.NewTimer(time.Until(at))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type Sender func(context.Context, []byte) (int, []byte, error)

func HTTP(client *http.Client, url string) Sender { return HTTPWithToken(client, url, "") }
func HTTPWithToken(client *http.Client, url, tokenFile string) Sender {
	return func(ctx context.Context, body []byte) (int, []byte, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url+"/v1/workflows", bytes.NewReader(body))
		if err != nil {
			return 0, nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		if tokenFile != "" {
			data, e := os.ReadFile(tokenFile)
			if e != nil || strings.TrimSpace(string(data)) == "" {
				return 0, nil, fmt.Errorf("campaign token unavailable")
			}
			req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(data)))
		}
		response, err := client.Do(req)
		if err != nil {
			return 0, nil, err
		}
		defer response.Body.Close()
		data, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
		if len(data) > 1<<20 {
			return response.StatusCode, nil, fmt.Errorf("response exceeds bound")
		}
		return response.StatusCode, data, err
	}
}
func Run(ctx context.Context, c Config, planned invariants.Arrivals) invariants.Arrivals {
	return RunWithClock(ctx, c, planned, realClock{}, HTTPWithToken(&http.Client{Timeout: c.Timeout, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}, c.URL, c.BearerTokenFile))
}

// RunWithClock never waits for a response or a free slot in its schedule loop.
// An overdue interval is explicitly missed, never replayed in a catch-up burst.
func RunWithClock(ctx context.Context, c Config, result invariants.Arrivals, clock Clock, send Sender) invariants.Arrivals {
	t0 := clock.Now()
	slots := make(chan struct{}, c.Concurrency)
	var requests sync.WaitGroup
	for i := range result.Rows {
		result.Rows[i].ScheduledAt = t0.Add(Offset(i, c.Rate)).UTC()
	}
	for i := range result.Rows {
		row := &result.Rows[i]
		at := t0.Add(Offset(i, c.Rate))
		if err := clock.Wait(ctx, at); err != nil {
			row.Outcome = "CANCELED"
			row.Error = err.Error()
			continue
		}
		now := clock.Now()
		if i+1 < len(result.Rows) && !now.Before(t0.Add(Offset(i+1, c.Rate))) {
			row.Outcome = "MISSED_DEADLINE"
			continue
		}
		select {
		case slots <- struct{}{}:
		default:
			row.Outcome = "CAPACITY_MISSED"
			continue
		}
		requests.Add(1)
		go func(row *invariants.Arrival) {
			defer requests.Done()
			defer func() { <-slots }()
			start := clock.Now()
			ns := start.Sub(t0).Nanoseconds()
			utc := start.UTC()
			// A delayed goroutine also cannot back-fill an expired arrival.
			// It has not sent HTTP: keep the actual send timestamps absent.
			if row.Sequence+1 < len(result.Rows) && !start.Before(t0.Add(Offset(row.Sequence+1, c.Rate))) {
				row.Outcome = "MISSED_SEND_DEADLINE"
				return
			}
			row.SendNS = &ns
			row.LatenessNS = ns - row.ScheduledNS
			row.SentAt = &utc
			requestCtx, cancel := context.WithTimeout(ctx, c.Timeout)
			defer cancel()
			status, data, err := send(requestCtx, row.Request)
			finish := clock.Now().Sub(t0).Nanoseconds()
			row.FinishedNS = &finish
			row.HTTPStatus = status
			row.Response = data
			if !json.Valid(data) {
				row.Response = json.RawMessage("null")
			}
			if err != nil {
				row.Outcome = "UNCERTAIN"
				row.Error = err.Error()
				return
			}
			if status >= 200 && status < 300 {
				var response struct {
					Workflow struct {
						ID string `json:"workflow_id"`
					} `json:"workflow"`
				}
				if json.Unmarshal(data, &response) != nil || response.Workflow.ID != row.WorkflowID {
					row.Outcome = "UNCERTAIN"
					row.Error = "unexpected acceptance response"
				} else {
					row.Outcome = "ACCEPTED"
				}
			} else if status >= 400 && status < 500 {
				row.Outcome = "REJECTED"
			} else {
				row.Outcome = "UNCERTAIN"
			}
		}(row)
	}
	requests.Wait()
	result.Status = "PASS"
	for _, row := range result.Rows {
		if row.Outcome != "ACCEPTED" {
			result.Status = "FAIL"
		}
	}
	return result
}
