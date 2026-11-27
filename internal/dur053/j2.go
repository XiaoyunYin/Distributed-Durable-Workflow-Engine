package dur053

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"time"

	"durable-agent-execution-engine/internal/invariants"
	"durable-agent-execution-engine/internal/state"
)

const (
	j2Cap            = 16
	j2RequestTimeout = 10 * time.Second
	maxJ2Duration    = 72 * time.Hour
	maxJ2Arrivals    = 1_000_000
)

// J2Segment occupies a half-open interval. Adjacent segments share an origin,
// sequence counter and in-flight request cap.
type J2Segment struct {
	Rate     float64
	Duration time.Duration
}

type J2Result struct {
	Offered  int    `json:"offered"`
	Accepted int    `json:"accepted"`
	Status   string `json:"status"`
}

type j2Slot struct {
	offset time.Duration
	cutoff time.Duration
}

type j2CutoffKey struct{}

var errJ2Cutoff = errors.New("J2 dispatch cutoff expired")

// The cutoff applies to starting the request. An already dispatched request
// may finish after a rate transition using its original response deadline.
type j2CutoffTransport struct {
	base http.RoundTripper
}

func (t j2CutoffTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	cutoff, ok := req.Context().Value(j2CutoffKey{}).(time.Time)
	if !ok || !time.Now().Before(cutoff) {
		return nil, errJ2Cutoff
	}
	return t.base.RoundTrip(req)
}

func recordJ2Cutoff(entry map[string]any) {
	entry["sent"] = nil
	entry["outcome"] = "unsent"
	entry["status"] = 0
	entry["error"] = "cutoff-after-intent"
	entry["attempts"] = 0
}

func j2Plan(c Config, segments []J2Segment) (Config, invariants.Arrivals, []j2Slot, error) {
	if len(segments) < 1 || len(segments) > 2 {
		return c, invariants.Arrivals{}, nil, errors.New("J2 needs one or two scheduled periods")
	}
	var start time.Duration
	var slots []j2Slot
	for _, period := range segments {
		if period.Duration <= 0 || period.Duration > maxJ2Duration-start ||
			period.Rate <= 0 || period.Rate > 100000 || math.IsNaN(period.Rate) {
			return c, invariants.Arrivals{}, nil, errors.New("invalid J2 period duration or rate")
		}
		offered := math.Ceil(period.Duration.Seconds() * period.Rate)
		if offered < 1 || offered > float64(maxJ2Arrivals-len(slots)) {
			return c, invariants.Arrivals{}, nil, errors.New("J2 schedule exceeds arrival bounds")
		}
		count := int(offered)
		// Offset uses the stock float64-to-duration conversion. Check the
		// half-open endpoints with that same nanosecond representation.
		if Offset(count-1, period.Rate) >= period.Duration ||
			Offset(count, period.Rate) < period.Duration {
			return c, invariants.Arrivals{}, nil, errors.New("J2 period does not match half-open slots")
		}
		for i := range count {
			next := Offset(i+1, period.Rate)
			if len(segments) == 2 {
				next = min(next, period.Duration)
			}
			slots = append(slots, j2Slot{
				offset: start + Offset(i, period.Rate),
				cutoff: start + next,
			})
		}
		start += period.Duration
	}
	c.Rate, c.Count = segments[0].Rate, len(slots)
	plan, err := Plan(c)
	if err != nil {
		return c, invariants.Arrivals{}, nil, err
	}
	for i, slot := range slots {
		plan.Rows[i].ScheduledNS = int64(slot.offset)
	}
	return c, plan, slots, nil
}

func runJ2Plan(ctx context.Context, c Config, plan invariants.Arrivals, slots []j2Slot, start time.Time, j *K4Journal, clock Clock, send Sender) (J2Result, error) {
	accepted, err := runK4Schedule(ctx, c, plan, start, j, clock, send, k4Timing{
		slot: func(i int) (time.Duration, time.Duration) {
			return slots[i].offset, slots[i].cutoff
		},
		dispatchCutoffs: true,
	})
	result := J2Result{Offered: len(slots), Accepted: accepted, Status: "FAIL"}
	if err == nil && accepted == len(slots) {
		result.Status = "PASS"
	}
	return result, err
}

// RunJ2Continuous is the separate J2 sender for the continuous hold-to-step
// episode. Single-rate runs use the unchanged stock loadgen CLI.
func RunJ2Continuous(ctx context.Context, c Config, periods [2]J2Segment, database, output, startText string) (J2Result, error) {
	if c.Arm != "short" || c.Concurrency != j2Cap || c.Timeout != j2RequestTimeout {
		return J2Result{}, errors.New("J2 sender requires the short arm, cap 16 and 10s request timeout")
	}
	parsed, err := time.Parse(time.RFC3339Nano, startText)
	if err != nil {
		return J2Result{}, fmt.Errorf("J2 origin: %w", err)
	}
	// Anchor the supplied wall timestamp to this process's monotonic clock.
	now := time.Now()
	start := now.Add(parsed.Sub(now))
	c, plan, slots, err := j2Plan(c, periods[:])
	if err != nil {
		return J2Result{}, err
	}

	f, err := os.OpenFile(output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return J2Result{}, err
	}
	defer f.Close()
	ctx, cancel := context.WithDeadline(ctx, start.Add(periods[0].Duration+periods[1].Duration+c.Timeout+30*time.Second))
	defer cancel()
	store, err := state.NewCampaignFromURL(ctx, database)
	if err != nil {
		return J2Result{}, err
	}
	defer store.Close()
	if err := Prepare(ctx, store, c); err != nil {
		return J2Result{}, err
	}
	return runJ2Plan(ctx, c, plan, slots, start, &K4Journal{File: f}, realClock{},
		k4HTTPSender(c, j2CutoffTransport{base: http.DefaultTransport}))
}
