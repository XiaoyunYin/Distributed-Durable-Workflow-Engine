package dur053

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptrace"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"durable-agent-execution-engine/internal/invariants"
)

type j2Attempt struct {
	entered, written, refused time.Time
	callbacks, status         int
	ack, reason               string
}

func (row *j2Outcome) record(result j2Attempt, start, completed time.Time) {
	row.Transport = offsetNS(start, result.entered)
	row.Written = offsetNS(start, result.written)
	row.Refusal = offsetNS(start, result.refused)
	row.Completed = offsetNS(start, completed)
	row.WriteCallbacks, row.Status = result.callbacks, result.status
	row.Acknowledged, row.Error = result.ack, result.reason
	if result.entered.IsZero() {
		return
	}
	row.Outcome = "uncertain"
	switch {
	case result.ack == row.Key && result.reason == "":
		row.Outcome = "accepted"
	case 400 <= result.status && result.status < 500 && result.reason == "":
		row.Outcome = "rejected"
	}
}

const (
	j2AwaitingWrite = iota
	j2WrittenInTime
	j2WriteExpired
)

type j2WriteGuard struct {
	state     atomic.Int32
	first     atomic.Pointer[time.Time]
	callbacks atomic.Int32
	cancel    context.CancelFunc
	cutoff    time.Time
}

func (g *j2WriteGuard) expire() {
	if g.state.CompareAndSwap(j2AwaitingWrite, j2WriteExpired) {
		g.cancel()
	}
}

func (g *j2WriteGuard) observe(at time.Time, err error) {
	g.callbacks.Add(1)
	if err != nil {
		return
	}
	g.first.CompareAndSwap(nil, &at)
	if at.Before(g.cutoff) {
		g.state.CompareAndSwap(j2AwaitingWrite, j2WrittenInTime)
	} else {
		g.expire()
	}
}

func newJ2HTTPSender(c Config) j2SlotSender {
	return j2HTTPSender(c, http.DefaultTransport)
}

type j2TransportFunc func(*http.Request) (*http.Response, error)

func (f j2TransportFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func j2HTTPSender(c Config, transport http.RoundTripper) j2SlotSender {
	return func(ctx context.Context, row invariants.Arrival, at, cutoff time.Time) (result j2Attempt) {
		deadline, cancel := context.WithTimeout(ctx, c.Timeout)
		defer cancel()
		guard := &j2WriteGuard{cancel: cancel, cutoff: cutoff}
		request, err := http.NewRequestWithContext(httptrace.WithClientTrace(deadline, &httptrace.ClientTrace{
			WroteRequest: func(info httptrace.WroteRequestInfo) { guard.observe(time.Now(), info.Err) },
		}), http.MethodPost, c.URL+"/v1/workflows", bytes.NewReader(row.Request))
		if err != nil {
			return j2Attempt{refused: time.Now(), reason: "request-setup-error"}
		}
		request.GetBody = nil // A nonempty POST body cannot be rewound for transport retries.
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-Dur053-K4-Request", row.WorkflowID)
		if c.BearerTokenFile != "" {
			data, err := os.ReadFile(c.BearerTokenFile)
			token := strings.TrimSpace(string(data))
			if err != nil || token == "" {
				return j2Attempt{refused: time.Now(), reason: "token-unavailable"}
			}
			request.Header.Set("Authorization", "Bearer "+token)
		}
		timer := time.AfterFunc(time.Until(cutoff), guard.expire)
		defer timer.Stop()
		client := http.Client{
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
			Transport: j2TransportFunc(func(req *http.Request) (*http.Response, error) {
				entered := time.Now()
				if entered.Before(at) {
					result.refused, result.reason = entered, "early-wake"
					return nil, errJ2Cutoff
				}
				if !entered.Before(cutoff) || guard.state.Load() == j2WriteExpired {
					result.refused, result.reason = entered, "missed-send-deadline"
					return nil, errJ2Cutoff
				}
				if err := req.Context().Err(); err != nil {
					result.refused, result.reason = entered, "request-cancelled-before-transport"
					return nil, err
				}
				result.entered = entered
				return transport.RoundTrip(req)
			}),
		}
		defer func() {
			if written := guard.first.Load(); written != nil {
				result.written = *written
			}
			result.callbacks = int(guard.callbacks.Load())
		}()
		response, err := client.Do(request)
		if err != nil {
			if result.reason == "" {
				result.reason = j2TransportError(guard, err)
				if result.entered.IsZero() {
					result.refused = time.Now()
				}
			}
			return result
		}
		defer response.Body.Close()
		result.status = response.StatusCode
		const maxResponse = 1 << 20
		data, err := io.ReadAll(io.LimitReader(response.Body, maxResponse+1))
		if err != nil {
			result.reason = j2TransportError(guard, err)
			return result
		}
		if len(data) > maxResponse {
			result.reason = "response-exceeds-bound"
			return result
		}
		if result.status >= 200 && result.status < 300 {
			var receipt struct {
				Workflow struct {
					ID string `json:"workflow_id"`
				} `json:"workflow"`
			}
			if json.Unmarshal(data, &receipt) == nil && receipt.Workflow.ID == row.Key {
				result.ack = receipt.Workflow.ID
			} else {
				result.reason = "acceptance-identity"
			}
		}
		return result
	}
}

func j2TransportError(guard *j2WriteGuard, err error) string {
	if guard.state.Load() == j2WriteExpired {
		return "write-cutoff"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "request-timeout"
	}
	return "transport-error"
}
