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

const (
	k6PredecessorBoundary = "lease_takeover"
	k6SuccessorBoundary   = "lease_takeover_acquired"
)

func configureK6LeaseTakeover(options *engine.ServeOptions) error {
	role := os.Getenv("DUR053_K6_LEASE_TAKEOVER_ROLE")
	workflow := os.Getenv("DUR053_K6_LEASE_TAKEOVER_WORKFLOW")
	if (role != "predecessor" && role != "successor") || workflow == "" ||
		os.Getenv("DUR053_K4_CAMPAIGN") != "1" || options.AfterLeaseAcquired != nil {
		return errors.New("K6_LEASE_TAKEOVER_CONFIGURATION")
	}
	client, err := k6hook.FromEnvironment(role, strings.ReplaceAll(state.K4Session, "-", ""))
	if err != nil {
		return err
	}
	if client == nil {
		return errors.New("K6_LEASE_TAKEOVER_RECEIVER_REQUIRED")
	}
	process, err := k4observe.ProcessStartID()
	if err != nil {
		return err
	}
	switch role {
	case "predecessor":
		options.LeaseTakeover.CreateAttempt = func(ctx, scheduler context.Context, store *state.Store, input state.AttemptInput) (state.Attempt, error) {
			if input.WorkflowID != workflow {
				return store.CreateAttempt(ctx, input)
			}
			return k6TakeoverAttempt(ctx, scheduler, store, input, client, process)
		}
	case "successor":
		finished := false
		options.LeaseTakeover.Acquire = func(ctx context.Context, store *state.Store, wf state.Workflow, owner string, ttl time.Duration) (state.Lease, bool, func() (bool, error), error) {
			if wf.WorkflowID != workflow || finished {
				lease, acquired, err := store.AcquireLease(ctx, wf.PartitionID, owner, ttl)
				return lease, acquired, nil, err
			}
			lease, acquired, backend, xid, err := store.AcquireLeaseForTakeover(ctx, wf.PartitionID, owner, ttl)
			if err != nil || !acquired {
				return lease, acquired, nil, err
			}
			ref := state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch}
			fields := k6LeaseFields(ref, wf.WorkflowID, wf.Revision, backend)
			fields.Objects["transaction"] = xid
			return lease, true, func() (bool, error) {
				released, err := finishK6Takeover(client, k6SuccessorBoundary, fields)
				if err == nil && !released {
					err = errors.New("K6_LEASE_TAKEOVER_SUCCESSOR_UNACKNOWLEDGED")
				}
				finished = released
				return released, err
			}, nil
		}
	}
	return nil
}

func k6LeaseFields(ref state.LeaseRef, workflow string, revision int64, backend state.LeaseTakeoverBackend) k6hook.Fields {
	return k6hook.Fields{
		Key: workflow, Invocation: state.NewID(),
		Objects: map[string]string{
			"cell": "hook-5", "partition": strconv.Itoa(int(ref.PartitionID)),
			"owner": ref.OwnerID, "epoch": strconv.FormatInt(ref.Epoch, 10),
			"revision": strconv.FormatInt(revision, 10), "backend_pid": strconv.Itoa(backend.PID),
			"backend_start_us": strconv.FormatInt(backend.StartUS, 10),
		},
	}
}

func finishK6Takeover(client k6hook.Client, boundary string, fields k6hook.Fields) (bool, error) {
	hold, err := client.Hit(boundary, fields)
	if err != nil || hold == nil {
		return false, err
	}
	if hold.Action() != "release" {
		return false, errors.New("K6_LEASE_TAKEOVER_COMMAND")
	}
	if err := hold.Finish(); err != nil {
		return false, err
	}
	return true, nil
}

func k6TakeoverAttempt(ctx, scheduler context.Context, store *state.Store, input state.AttemptInput,
	client k6hook.Client, process string) (state.Attempt, error) {
	var fields k6hook.Fields
	var released bool
	var cancel context.CancelFunc = func() {}
	defer func() { cancel() }()
	attempt, err := store.CreateAttemptForTakeover(ctx, input, func(backend state.LeaseTakeoverBackend) (context.Context, error) {
		fields = k6LeaseFields(input.Lease, input.WorkflowID, input.ExpectedRevision, backend)
		fields.Objects["node"] = input.NodeID
		fields.Objects["iteration"] = strconv.Itoa(input.Iteration)
		var err error
		released, err = finishK6Takeover(client, k6PredecessorBoundary, fields)
		if !released {
			return ctx, err
		}
		// The pre-hold deadline was for the original scheduler pass.
		// A released operation gets a fresh deadline bounded by role shutdown.
		resumed, stop := context.WithTimeout(scheduler, 5*time.Second)
		cancel = stop
		return resumed, nil
	})
	if released {
		outcome := "OTHER_ERROR"
		switch {
		case errors.Is(err, state.ErrLeaseNotOwned):
			outcome = "ErrLeaseNotOwned"
		case err == nil:
			outcome = "COMMITTED"
		}
		line, _ := json.Marshal(map[string]any{
			"event": "k6_lease_takeover_call", "operation": "CreateAttempt",
			"workflow": fields.Key, "invocation": fields.Invocation, "objects": fields.Objects,
			"pid": os.Getpid(), "process_start_id": process, "session": state.K4Session,
			"pod_uid": os.Getenv("POD_UID"), "outcome": outcome, "at_ns": time.Now().UnixNano(),
		})
		fmt.Fprintf(os.Stderr, "DUR053_K6_OBSERVATION k6_lease_takeover_call %s\n", line)
	}
	return attempt, err
}
