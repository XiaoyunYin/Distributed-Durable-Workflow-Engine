package dur053

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"durable-agent-execution-engine/internal/invariants"
	"durable-agent-execution-engine/internal/state"
)

const (
	j2DispatchSchema = "j2.dispatch-journal.v1"
	j2DispatchMax    = 26678
	j2DispatchLead   = 2 * time.Second
)

type j2DispatchSchedule struct {
	Journal   string `json:"dispatch_journal"`
	Namespace string `json:"namespace"`
	RunID     string `json:"run_id"`
	Seed      int64  `json:"seed"`
	Rate      string `json:"rate"`
	Count     int    `json:"count"`
	Cap       int    `json:"cap"`
	Timeout   string `json:"request_timeout"`
	Origin    int64  `json:"origin_ns"`
	WarmupEnd int64  `json:"warmup_end_ns"`
	HoldEnd   int64  `json:"hold_end_ns"`
	Stop      int64  `json:"stop_ns"`
	Periods   []struct {
		Rate    string `json:"rate"`
		Seconds int64  `json:"seconds"`
		Count   int    `json:"count"`
	} `json:"periods"`
}

func (s j2DispatchSchedule) plan(c Config) (Config, invariants.Arrivals, []j2Slot, error) {
	bad := errors.New("invalid bound J2 dispatch schedule")
	if s.Journal != j2DispatchSchema || s.Cap != j2Cap || s.Timeout != j2RequestTimeout.String() ||
		s.Count <= 0 || s.Count > j2DispatchMax || s.Origin <= 0 || s.WarmupEnd < s.Origin ||
		s.Stop <= s.WarmupEnd || s.Stop-s.Origin > int64(maxJ2Duration) {
		return c, invariants.Arrivals{}, nil, bad
	}
	var periods []J2Segment
	switch len(s.Periods) {
	case 0:
		rate, err := strconv.ParseFloat(s.Rate, 64)
		if err != nil || s.HoldEnd != 0 {
			return c, invariants.Arrivals{}, nil, bad
		}
		periods = []J2Segment{{Rate: rate, Duration: time.Duration(s.Stop - s.Origin)}}
	case 2:
		if s.Periods[0].Rate != s.Rate || s.HoldEnd <= s.WarmupEnd || s.HoldEnd >= s.Stop {
			return c, invariants.Arrivals{}, nil, bad
		}
		bounds := [2]int64{s.HoldEnd - s.Origin, s.Stop - s.HoldEnd}
		for i, period := range s.Periods {
			rate, err := strconv.ParseFloat(period.Rate, 64)
			if err != nil || !(rate > 0 && rate <= 100000) || bounds[i]%int64(time.Second) != 0 || bounds[i]/int64(time.Second) != period.Seconds ||
				period.Count <= 0 || period.Count > s.Count || Offset(period.Count-1, rate) >= time.Duration(bounds[i]) ||
				Offset(period.Count, rate) < time.Duration(bounds[i]) {
				return c, invariants.Arrivals{}, nil, bad
			}
			periods = append(periods, J2Segment{Rate: rate, Duration: time.Duration(bounds[i])})
		}
		if s.Periods[0].Count+s.Periods[1].Count != s.Count {
			return c, invariants.Arrivals{}, nil, bad
		}
	default:
		return c, invariants.Arrivals{}, nil, bad
	}
	c.Namespace, c.RunID, c.Seed = s.Namespace, s.RunID, s.Seed
	c.Arm, c.Concurrency, c.Timeout = "short", j2Cap, j2RequestTimeout
	// Check the computed count before allocating the plan, including an invalid
	// schedule that advertises a smaller population than its rate would offer.
	total := 0.0
	for _, segment := range periods {
		total += math.Ceil(segment.Duration.Seconds() * segment.Rate)
	}
	if total != float64(s.Count) {
		return c, invariants.Arrivals{}, nil, bad
	}
	return buildJ2Plan(c, periods, true)
}

// J2DispatchResult binds negative preflight evidence to the exact schedule.
type J2DispatchResult struct {
	J2Result
	Schema          string `json:"schema"`
	RunID           string `json:"run_id"`
	ScheduleSHA256  string `json:"schedule_sha256"`
	DispatchEnabled bool   `json:"dispatch_enabled"`
	AllAccepted     string `json:"all_accepted_verdict"`
}

// RunJ2Dispatch is an opt-in sender. The legacy K4 and J2 senders never enter it.
func RunJ2Dispatch(ctx context.Context, c Config, database, output, schedulePath string) (result J2DispatchResult, err error) {
	result.Schema, result.Status, result.AllAccepted = j2DispatchSchema, "FAIL", "FAIL"
	source, err := os.Open(schedulePath)
	if err != nil {
		return result, err
	}
	raw, err := io.ReadAll(io.LimitReader(source, (64<<10)+1))
	closeErr := source.Close()
	if err = errors.Join(err, closeErr); err != nil {
		return result, err
	}
	var schedule j2DispatchSchedule
	if len(raw) > 64<<10 || !json.Valid(raw) || json.Unmarshal(raw, &schedule) != nil {
		return result, errors.New("invalid J2 schedule file")
	}
	result.ScheduleSHA256, result.RunID = sumHex(raw), schedule.RunID
	now := time.Now()
	start := now.Add(time.Unix(0, schedule.Origin).Sub(now))
	ctx, cancel := context.WithDeadline(ctx, start.Add(time.Duration(schedule.Stop-schedule.Origin)+j2RequestTimeout+30*time.Second))
	defer cancel()
	c, plan, slots, err := schedule.plan(c)
	if err != nil {
		return result, err
	}
	result.Offered = len(slots)
	if err := prepareJ2BeforeSlots(ctx, database, c); err != nil {
		return result, err
	}
	f, err := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	return dispatchJ2(ctx, result, plan, slots, start, realClock{}, f, func() error {
		dir, err := os.Open(filepath.Dir(output))
		if err != nil {
			return err
		}
		return errors.Join(dir.Sync(), dir.Close())
	}, newJ2HTTPSender(c))
}

func prepareJ2BeforeSlots(ctx context.Context, database string, c Config) error {
	store, err := state.NewCampaignFromURL(ctx, database)
	if err != nil {
		return err
	}
	defer store.Close()
	return Prepare(ctx, store, c)
}

type j2SlotSender func(context.Context, invariants.Arrival, time.Time, time.Time) j2Attempt

func dispatchJ2(ctx context.Context, result J2DispatchResult, plan invariants.Arrivals, slots []j2Slot,
	start time.Time, clock Clock, file j2SyncWriter, syncDir func() error, send j2SlotSender,
) (J2DispatchResult, error) {
	precommit, err := commitJ2Plan(file, syncDir, plan, slots, result.ScheduleSHA256, start, clock)
	if err != nil {
		return result, fmt.Errorf("J2 precommit: %w", err)
	}
	if ctx.Err() != nil || clock.Now().After(start.Add(-j2DispatchLead)) {
		return result, errors.New("J2 precommit missed its preflight deadline")
	}
	result.DispatchEnabled = true
	outcomes := make([]j2Outcome, len(slots))
	ready := make(chan int, len(slots)) // One completion per planned key, even if the writer stops.
	writer := make(chan error, 1)
	var failed atomic.Bool
	go func() {
		err := writeJ2Outcomes(file, precommit, outcomes, ready, start, clock)
		failed.Store(err != nil)
		writer <- err
	}()
	var inFlight atomic.Int32
	var requests sync.WaitGroup
	for i, slot := range slots {
		at, cutoff := start.Add(slot.offset), start.Add(slot.cutoff)
		if failed.Load() || clock.Wait(ctx, at) != nil || failed.Load() {
			break
		}
		intent := clock.Now()
		row := j2Outcome{Event: "outcome", Sequence: i, Key: plan.Rows[i].Key, Outcome: "unsent", Intent: offsetNS(start, intent)}
		reason := ""
		active := int(inFlight.Load())
		if intent.Before(at) {
			reason = "early-wake"
		} else if !intent.Before(cutoff) {
			reason = "missed-deadline"
		} else if active == j2Cap {
			reason = "capacity-missed"
		}
		if reason != "" {
			row.Refusal, row.Error, row.InFlight = row.Intent, reason, active
			outcomes[i] = row
			ready <- i
			continue
		}
		row.InFlight = int(inFlight.Add(1))
		requests.Go(func() {
			attempt := send(ctx, plan.Rows[i], at, cutoff)
			inFlight.Add(-1)
			row.record(attempt, start, clock.Now())
			outcomes[i] = row
			ready <- i
		})
	}
	requests.Wait()
	close(ready)
	err = <-writer
	if err != nil {
		return result, err
	}
	valid := 0
	for i, row := range outcomes {
		if row.Outcome == "accepted" {
			result.Accepted++
		}
		if row.Written != nil && row.WriteCallbacks == 1 && *row.Written >= int64(slots[i].offset) && *row.Written < int64(slots[i].cutoff) {
			valid++
		}
	}
	if result.Accepted == len(slots) {
		result.AllAccepted = "PASS"
		if valid == len(slots) {
			result.Status = "PASS"
		}
	}
	return result, nil
}

func sumHex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func offsetNS(start, at time.Time) *int64 {
	if at.IsZero() {
		return nil
	}
	ns := int64(at.Sub(start))
	return &ns
}
