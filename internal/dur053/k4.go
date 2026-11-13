package dur053

// K4 has a separate scheduler. K3 Run and its all-accepted rule remain unchanged.
import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"durable-agent-execution-engine/internal/invariants"
	"durable-agent-execution-engine/internal/state"
)

type K4Journal struct {
	mu   sync.Mutex
	File interface {
		Write([]byte) (int, error)
		Sync() error
	}
}

func (j *K4Journal) Append(row any) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	b, err := json.Marshal(row)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	n, err := j.File.Write(b)
	if err != nil {
		return err
	}
	if n != len(b) {
		return fmt.Errorf("K4 short journal write")
	}
	return j.File.Sync()
}

// The plan precedes any HTTP send. A fsynced uncertain row precedes each send;
// after process loss it reconciles by the original immutable submission key.
// All updates append; a torn final line cannot erase the durable plan prefix.
func RunK4(ctx context.Context, c Config, database, output, startText, stopFile string) error {
	if err := c.Validate(); err != nil {
		return err
	}
	start, err := time.Parse(time.RFC3339Nano, startText)
	if err != nil || stopFile == "" {
		return fmt.Errorf("K4 absolute start and stop file required")
	}
	f, err := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	j := &K4Journal{File: f}
	store, err := state.NewCampaignFromURL(ctx, database)
	if err != nil {
		return err
	}
	defer store.Close()
	if err = Prepare(ctx, store, c); err != nil {
		return err
	}
	plan, err := Plan(c)
	if err != nil {
		return err
	}

	send := HTTPWithToken(&http.Client{Timeout: c.Timeout, Transport: k4Transport{base: http.DefaultTransport}, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}, c.URL, c.BearerTokenFile)
	stop := func() (time.Time, error) {
		raw, e := os.ReadFile(stopFile)
		if os.IsNotExist(e) {
			return time.Time{}, nil
		}
		if e != nil {
			return time.Time{}, e
		}
		return time.Parse(time.RFC3339Nano, strings.TrimSpace(string(raw)))
	}
	return runK4Plan(ctx, c, plan, start, j, realClock{}, send, stop)
}

func runK4Plan(ctx context.Context, c Config, plan invariants.Arrivals, start time.Time, j *K4Journal, clock Clock, send Sender, stop func() (time.Time, error)) error {
	for i := range plan.Rows {
		plan.Rows[i].ScheduledAt = start.Add(Offset(i, c.Rate)).UTC()
	}
	if err := j.Append(map[string]any{"event": "plan", "plan": plan}); err != nil {
		return err
	}
	slots := make(chan struct{}, c.Concurrency)
	var wg sync.WaitGroup
	failures := make(chan error, c.Concurrency+1)
	appendRow := func(row any) {
		if e := j.Append(row); e != nil {
			select {
			case failures <- e:
			default:
			}
		}
	}
	for i, row := range plan.Rows {
		at := start.Add(Offset(i, c.Rate))
		if e := clock.Wait(ctx, at); e != nil {
			break
		}
		end, e := stop()
		if e != nil {
			wg.Wait()
			return e
		}
		if !end.IsZero() && !at.Before(end) {
			break
		}

		select {
		case e := <-failures:
			wg.Wait()
			return e
		default:
		}
		sum := sha256.Sum256(row.Request)
		base := map[string]any{"event": "arrival", "id": row.Key, "key": row.Key, "workflow": row.WorkflowID, "scheduled": at.UTC(), "sent": nil, "outcome": "unsent", "status": 0, "error": "", "request_hash": hex.EncodeToString(sum[:]), "attempts": 0}
		if !clock.Now().Before(start.Add(Offset(i+1, c.Rate))) {
			base["error"] = "missed-deadline"
			appendRow(base)
			continue
		}
		select {
		case slots <- struct{}{}:
		default:
			base["error"] = "capacity-missed"
			appendRow(base)
			continue
		}
		wg.Add(1)
		go func(body []byte, entry map[string]any, index int) {
			defer wg.Done()
			defer func() { <-slots }()
			if !clock.Now().Before(start.Add(Offset(index+1, c.Rate))) {
				entry["error"] = "missed-send-deadline"
				appendRow(entry)
				return
			}
			entry["sent"] = clock.Now().UTC()
			entry["outcome"] = "uncertain"
			entry["attempts"] = 1
			if e := j.Append(entry); e != nil {
				select {
				case failures <- e:
				default:
				}
				return
			}
			requestCtx, cancel := context.WithTimeout(ctx, c.Timeout)
			defer cancel()
			status, data, e := send(requestCtx, body)
			entry["status"] = status
			if e != nil {
				entry["error"] = "transport-error"
			} else if status >= 200 && status < 300 {
				var r struct {
					Workflow struct {
						ID string `json:"workflow_id"`
					} `json:"workflow"`
				}
				if json.Unmarshal(data, &r) == nil && r.Workflow.ID == entry["workflow"] {
					entry["outcome"] = "accepted"
				} else {
					entry["error"] = "acceptance-identity"
				}
			} else if status >= 400 && status < 500 {
				entry["outcome"] = "rejected"
			}
			appendRow(entry)
		}(row.Request, base, i)
	}
	wg.Wait()
	select {
	case e := <-failures:
		return e
	default:
	}
	return j.Append(map[string]any{"event": "closed"})
}

// K4 request IDs bind target-local API acknowledgments to the registered stream.
type k4Transport struct{ base http.RoundTripper }

func (t k4Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.GetBody == nil {
		return nil, fmt.Errorf("K4 submission lacks replayable body")
	}
	body, err := req.GetBody()
	if err != nil {
		return nil, err
	}
	defer body.Close()
	var id struct {
		Workflow string `json:"workflow_id"`
	}
	if err = json.NewDecoder(body).Decode(&id); err != nil || id.Workflow == "" {
		return nil, fmt.Errorf("K4 submission identity missing")
	}
	req.Header.Set("X-Dur053-K4-Request", id.Workflow)
	return t.base.RoundTrip(req)
}
