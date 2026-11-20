//go:build dur053_k6

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/engine"
	"durable-agent-execution-engine/internal/k4observe"
	"durable-agent-execution-engine/internal/k6hook"
	"durable-agent-execution-engine/internal/state"
)

const k6RenewalIdleBound = 12 * time.Second

func configureK6Partition(options *engine.ServeOptions) error {
	cell := os.Getenv("DUR053_K6_PARTITION_CELL")
	workflow := os.Getenv("DUR053_K6_PARTITION_WORKFLOW")
	if (cell != "medium-2" && cell != "medium-3") || workflow == "" ||
		os.Getenv("DUR053_K4_CAMPAIGN") != "1" || options.AfterLeaseAcquired != nil ||
		os.Getenv("DUR053_K6_LEASE_TAKEOVER_ROLE") != "" {
		return errors.New("F1B_HOOK_CONFIGURATION")
	}
	client, err := k6hook.FromEnvironment("scheduler", strings.ReplaceAll(state.K4Session, "-", ""))
	if err != nil {
		return err
	}
	if client == nil {
		return errors.New("F1B_HOOK_RECEIVER_REQUIRED")
	}
	process, err := k4observe.ProcessStartID()
	if err != nil {
		return err
	}
	emit := func(event string, fields k6hook.Fields, data map[string]any) {
		data["event"], data["workflow"], data["invocation"] = event, fields.Key, fields.Invocation
		data["pid"], data["process_start_id"] = os.Getpid(), process
		data["session"], data["pod_uid"] = state.K4Session, os.Getenv("POD_UID")
		data["at_ns"] = time.Now().UnixNano()
		line, _ := json.Marshal(data)
		fmt.Fprintf(os.Stderr, "DUR053_K6_OBSERVATION f1b %s\n", line)
	}
	prepare := func(scheduler context.Context, store *state.Store, boundary string, fields k6hook.Fields) error {
		sampleCtx, stop := context.WithTimeout(scheduler, 60*time.Second)
		stopped := make(chan struct{})
		go func() {
			defer close(stopped)
			ticker := time.NewTicker(350 * time.Millisecond)
			defer ticker.Stop()
			for {
				started := time.Now().UnixNano()
				probeCtx, cancel := context.WithTimeout(sampleCtx, 700*time.Millisecond)
				var backend int
				err := store.Pool().QueryRow(probeCtx, "SELECT pg_backend_pid()").Scan(&backend)
				cancel()
				outcome := "connected"
				if err != nil {
					outcome = "unreachable"
				}
				emit("pool_probe", fields, map[string]any{"started_ns": started, "outcome": outcome, "backend_pid": backend})
				select {
				case <-sampleCtx.Done():
					return
				case <-ticker.C:
				}
			}
		}()
		released, err := finishK6Takeover(client, boundary, fields)
		stop()
		<-stopped
		if err == nil && !released {
			return errors.New("F1B_HOLD_UNACKNOWLEDGED")
		}
		return err
	}
	call := func(fields k6hook.Fields, operation string, err error) {
		outcome := "OTHER_ERROR"
		switch {
		case errors.Is(err, state.ErrLeaseNotOwned):
			outcome = "ErrLeaseNotOwned"
		case errors.Is(err, state.ErrK6RenewalSessionLost):
			outcome = "SESSION_LOST"
		case err == nil:
			outcome = "COMMITTED"
		}
		emit("fenced_call", fields, map[string]any{"operation": operation, "outcome": outcome, "objects": fields.Objects})
	}
	used := false // The Serve loop invokes these callbacks serially.
	if cell == "medium-3" {
		options.LeaseTakeover.CreateAttempt = func(ctx, scheduler context.Context, store *state.Store, input state.AttemptInput) (state.Attempt, error) {
			if used || input.WorkflowID != workflow {
				return store.CreateAttempt(ctx, input)
			}
			used = true
			var fields k6hook.Fields
			var cancel context.CancelFunc = func() {}
			defer func() { cancel() }()
			attempt, err := store.CreateAttemptForTakeover(ctx, input, func(backend state.LeaseTakeoverBackend) (context.Context, error) {
				fields = k6LeaseFields(input.Lease, workflow, input.ExpectedRevision, backend)
				fields.Objects["cell"] = cell
				fields.Objects["node"] = input.NodeID
				fields.Objects["iteration"] = strconv.Itoa(input.Iteration)
				if err := prepare(scheduler, store, k6PredecessorBoundary, fields); err != nil {
					return ctx, err
				}
				resumed, stop := context.WithTimeout(scheduler, 5*time.Second)
				cancel = stop
				return resumed, nil
			})
			if fields.Invocation != "" {
				call(fields, "CreateAttempt", err)
			}
			return attempt, err
		}
		return nil
	}
	var renewalRef state.LeaseRef
	var fields k6hook.Fields
	options.LeaseTakeover.Renew = func(ctx, scheduler context.Context, store *state.Store, wf state.Workflow, ref state.LeaseRef, ttl time.Duration) (state.Lease, error) {
		if used || wf.WorkflowID != workflow {
			return store.RenewLease(ctx, ref, ttl)
		}
		used = true
		var cancel context.CancelFunc = func() {}
		defer func() { cancel() }()
		ctx = state.WithK6RenewalHold(ctx, k6RenewalIdleBound, func(point state.K6RenewalPoint) (context.Context, error) {
			fields = k6LeaseFields(ref, workflow, wf.Revision, point.Backend)
			fields.Objects["cell"] = cell
			fields.Objects["transaction"] = point.XID
			fields.Objects["db_time_us"] = strconv.FormatInt(point.DBTime.UnixMicro(), 10)
			fields.Objects["expires_us"] = strconv.FormatInt(point.Lease.LeaseExpiresAt.UnixMicro(), 10)
			fields.Objects["idle_timeout_ms"] = strconv.FormatInt(k6RenewalIdleBound.Milliseconds(), 10)
			renewalRef = ref
			if err := prepare(scheduler, store, "renew_lease_locked", fields); err != nil {
				return ctx, err
			}
			resumed, stop := context.WithTimeout(scheduler, 5*time.Second)
			cancel = stop
			return resumed, nil
		})
		lease, err := store.RenewLease(ctx, ref, ttl)
		if fields.Invocation != "" {
			call(fields, "RenewLease", err)
		}
		return lease, err
	}
	options.LeaseTakeover.AfterRelease = func(ref state.LeaseRef, err error) {
		if ref == renewalRef && fields.Invocation != "" {
			call(fields, "ReleaseLease", err)
			fields = k6hook.Fields{}
		}
	}
	return nil
}
