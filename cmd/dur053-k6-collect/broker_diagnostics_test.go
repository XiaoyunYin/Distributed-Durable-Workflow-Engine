package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/invariants/k6v1"
	"github.com/segmentio/kafka-go"
)

type drainedBroker struct{}

func (drainedBroker) DescribeGroups(context.Context, *kafka.DescribeGroupsRequest) (*kafka.DescribeGroupsResponse, error) {
	return &kafka.DescribeGroupsResponse{Groups: []kafka.DescribeGroupsResponseGroup{{GroupID: "group", GroupState: "Empty"}}}, nil
}

func (drainedBroker) Metadata(context.Context, *kafka.MetadataRequest) (*kafka.MetadataResponse, error) {
	return &kafka.MetadataResponse{ClusterID: "cluster", Topics: []kafka.Topic{{Name: "topic", Partitions: []kafka.Partition{{ID: 0}}}}}, nil
}

func (drainedBroker) ListOffsets(context.Context, *kafka.ListOffsetsRequest) (*kafka.ListOffsetsResponse, error) {
	return &kafka.ListOffsetsResponse{Topics: map[string][]kafka.PartitionOffsets{"topic": {{Partition: 0, LastOffset: 7}}}}, nil
}

func (drainedBroker) OffsetFetch(context.Context, *kafka.OffsetFetchRequest) (*kafka.OffsetFetchResponse, error) {
	return &kafka.OffsetFetchResponse{Topics: map[string][]kafka.OffsetFetchPartition{"topic": {{Partition: 0, CommittedOffset: 7}}}}, nil
}

type changingMembersBroker struct {
	drainedBroker
	afterFirstDescribe func()
}

func (b *changingMembersBroker) DescribeGroups(ctx context.Context, req *kafka.DescribeGroupsRequest) (*kafka.DescribeGroupsResponse, error) {
	if b.afterFirstDescribe != nil {
		b.afterFirstDescribe()
		b.afterFirstDescribe = nil
	}
	return b.drainedBroker.DescribeGroups(ctx, req)
}

func put(t *testing.T, path, data string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
}

func diagnostics(t *testing.T, dir string) brokerHistory {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, brokerDiagnosticFile))
	if err != nil {
		t.Fatal(err)
	}
	var history brokerHistory
	if err := json.Unmarshal(raw, &history); err != nil {
		t.Fatal(err)
	}
	return history
}

func brokerTestConfig(dir string) config {
	return config{
		Manifest: k6v1.Manifest{Namespace: "episode", Episode: "medium-3-0"},
		Group:    "group", Topics: map[string]string{"topic": "topic-id"},
		Members: filepath.Join(dir, "members.json"), Fence: filepath.Join(dir, "fence.json"),
		Output: filepath.Join(dir, "frames"),
	}
}

func writeTestFence(t *testing.T, c config) {
	t.Helper()
	raw, err := k6v1.Canonical(k6v1.Binding(c.Manifest))
	if err != nil {
		t.Fatal(err)
	}
	put(t, c.Fence, string(raw))
}

func TestPostDrainBrokerConvergence(t *testing.T) {
	for _, scenario := range []string{"converges", "expires", "before_fence"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			c := brokerTestConfig(dir)
			observer := collector{config: c}
			const lagging = `{"generation":4,"members":{"leaving-member":4},"coordinator":{"generation":4,"members":[{"id":"leaving-member","client_id":"worker","generation":4,"assignments":[]}]}}` + "\n"
			put(t, c.Members, lagging)
			if scenario != "before_fence" {
				writeTestFence(t, c)
			}
			const code = "K6_BROKER_MEMBERS_UNJOINED"
			readLagging := func() error {
				t.Helper()
				row, converging, err := observer.snapshotBroker(t.Context(), drainedBroker{}, c.Topics)
				if err == nil || err.Error() != code || row.Group != "" || row.Partitions != nil || converging != (scenario != "before_fence") {
					t.Fatalf("lagging file yielded row %+v, convergence %t, error %v", row, converging, err)
				}
				if converging {
					return drainMembershipPending{err}
				}
				return err
			}
			var failures collectionFailures
			start := time.Now()
			if failures.shouldExit(start, true, false, errors.New("K6_POD_WATCH_STALE")) {
				t.Fatal("pre-drain failure changed collector lifetime")
			}
			firstDrainedFailure := start.Add(6 * time.Second)
			if failures.shouldExit(firstDrainedFailure, true, observer.drained(), readLagging()) {
				t.Fatal("pre-fence failure exhausted post-drain convergence")
			}
			if scenario == "before_fence" {
				if !failures.retryUntil.IsZero() {
					t.Fatal("non-drained bracket started a convergence budget")
				}
				if _, err := os.Stat(filepath.Join(dir, brokerDiagnosticFile)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("pre-drain broker input was retained: %v", err)
				}
				return
			}
			deadline := firstDrainedFailure.Add(drainConvergenceWait)
			if failures.retryUntil != deadline || failures.shouldExit(deadline.Add(-time.Second), true, true, readLagging()) {
				t.Fatal("recoverable departure did not receive one full bounded window")
			}
			if scenario == "converges" {
				put(t, c.Members, `{"generation":4,"members":{}}`)
				row, converging, err := observer.snapshotBroker(t.Context(), drainedBroker{}, c.Topics)
				if err != nil || converging || row.State != "empty" || len(row.Members) != 0 || len(row.Partitions) != 1 {
					t.Fatalf("converged file did not close the bracket: %+v, %t, %v", row, converging, err)
				}
				if failures.shouldExit(deadline.Add(-time.Millisecond), true, true, nil) || !failures.retryUntil.IsZero() {
					t.Fatal("successful bracket did not clear convergence")
				}
			} else if !failures.shouldExit(deadline, true, true, readLagging()) {
				t.Fatal("persistent disagreement exceeded the convergence bound")
			}
			history := diagnostics(t, dir)[code]
			if history == nil || len(history.First) != 2 || (scenario == "expires" && history.Count != 3) {
				t.Fatalf("missing failure diagnostics: %+v", history)
			}
			attempt := history.First[0]
			if !bytes.Contains(attempt.Before, []byte(`"state":"Empty"`)) || !bytes.Equal(attempt.Before, attempt.After) || len(attempt.MemberReads) != 2 {
				t.Fatalf("missing bracket observations: %+v", attempt)
			}
			for _, read := range attempt.MemberReads {
				if string(read.Bytes) != lagging || read.SHA256 != k6v1.Hash([]byte(lagging)) ||
					!bytes.Contains(read.Coordinator, []byte(`"id":"leaving-member"`)) {
					t.Fatalf("missing exact callback input: %+v", read)
				}
			}
		})
	}
}

func TestBrokerDiagnosticsCaptureFenceDuringBracket(t *testing.T) {
	dir := t.TempDir()
	c := brokerTestConfig(dir)
	observer := collector{config: c}
	const old = `{"generation":4,"members":{"leaving-member":4}}`
	const cleared = `{"generation":4,"members":{}}`
	put(t, c.Members, old)
	client := &changingMembersBroker{afterFirstDescribe: func() {
		put(t, c.Members, cleared)
		writeTestFence(t, c)
	}}
	row, converging, err := observer.snapshotBroker(t.Context(), client, c.Topics)
	const code = "K6_BROKER_GENERATION_RACED"
	if err == nil || err.Error() != code || !converging || row.Group != "" {
		t.Fatalf("fence-straddling bracket was not rejected and retained: %+v %t %v", row, converging, err)
	}
	reads := diagnostics(t, dir)[code].First[0].MemberReads
	if len(reads) != 2 || string(reads[0].Bytes) != old || string(reads[1].Bytes) != cleared {
		t.Fatalf("callback reads were replaced with a later observation: %+v", reads)
	}
}

func TestPostDrainNonMembershipFailureKeepsOriginalLimit(t *testing.T) {
	var failures collectionFailures
	at := time.Now()
	err := errors.New("K6_BROKER_DESCRIBE")
	if failures.shouldExit(at, true, true, drainMembershipPending{errors.New("K6_BROKER_GENERATION_RACED")}) ||
		!failures.shouldExit(at.Add(k6v1.MaxSampleGap+time.Millisecond), true, true, err) {
		t.Fatal("ordinary errors inherited the membership convergence window")
	}
}

func TestBrokerDiagnosticsBounded(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, brokerDiagnosticFile)
	h := brokerHistory{}
	for i := 1; i <= 7; i++ {
		attempt := brokerAttempt{AtUTC: fmt.Sprint(i)}
		if err := h.retain(path, "describe", attempt); err != nil {
			t.Fatal(err)
		}
		if i == 3 {
			if err := h.retain(path, "other", attempt); err != nil {
				t.Fatal(err)
			}
		}
	}
	got := diagnostics(t, dir)
	main := got["describe"]
	if main.Count != 7 || len(main.First) != 2 || len(main.Last) != 2 ||
		main.First[0].AtUTC != "1" || main.First[1].AtUTC != "2" || main.Last[0].AtUTC != "6" || main.Last[1].AtUTC != "7" ||
		got["other"].Count != 1 || got["other"].First[0].AtUTC != "3" {
		t.Fatalf("incorrect first/last retention: %+v", main)
	}
	members := filepath.Join(dir, "members.json")
	data := strings.Repeat(" ", brokerDiagnosticBytes+1) + `{"generation":4,"members":{}}`
	put(t, members, data)
	attempt := brokerAttempt{}
	if _, _, _, err := readMembers(members, &attempt)(t.Context()); err != nil {
		t.Fatal(err)
	}
	read := attempt.MemberReads[0]
	if len(read.Bytes) != brokerDiagnosticBytes || read.Size != len(data) || read.SHA256 != k6v1.Hash([]byte(data)) ||
		string(boundedView(data)) != `{"truncated":true}` {
		t.Fatal("oversized diagnostic lost its bound or full-file digest")
	}
}
