package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/restoreledger"
)

type bundleEndpointJournal struct{ f *drillFixture }

func (j bundleEndpointJournal) Header() restoreledger.Header { return j.f.header }
func (j bundleEndpointJournal) RecordWitness(w restoreledger.Witness) error {
	j.f.ledger.append(w.CompletedNS+1, fixtureLedgerRow{Probe: &w})
	return nil
}

func (f *drillFixture) exerciseControllerEndpoint(r restoreReceipt, restoreSHA, freshSHA string) {
	at := []int64{f.ns(92), f.ns(93), f.ns(95)}
	var order []string
	o, err := observeReopenedEndpoint(f.t.Context(), func() (int64, error) {
		v := at[0]
		at = at[1:]
		return v, nil
	}, endpointProbes{
		oldDenied:     func(context.Context) error { order = append(order, "old"); return nil },
		foreignDenied: func(context.Context) error { order = append(order, "foreign"); return nil },
		observer: func(context.Context) (uint64, uint32, error) {
			order = append(order, "observer")
			return r.State.SystemID, r.State.Timeline, nil
		},
	})
	if err != nil || !slices.Equal(order, []string{"old", "observer", "foreign", "observer"}) {
		f.t.Fatalf("target endpoint observation order %v: %v", order, err)
	}
	dir := filepath.Clean(f.t.TempDir())
	for _, file := range []string{"restore.json", "kafka-fresh.json"} {
		f.write(dir, file, f.files[file])
	}
	err = bindControllerEndpoint(dir, bundleEndpointJournal{f}, restoreSHA, freshSHA, o, func(path string, p readyEndpoint) (string, error) {
		return f.put(filepath.Base(path), p), nil
	})
	if err != nil {
		f.t.Fatal(err)
	}
}

func TestControllerRunStopsBeforePublishingOnPhaseFailure(t *testing.T) {
	for _, end := range []drillStep{stepProvision, stepLoss, stepEndpoint, stepReader, stepWait, stepResources, stepSeal} {
		t.Run(string(end), func(t *testing.T) {
			fault := errors.New("fixture failure")
			var steps []drillStep
			err := driveDrill(t.Context(), func(_ context.Context, s drillStep) error {
				steps = append(steps, s)
				if s == end {
					return fault
				}
				return nil
			})
			if !errors.Is(err, fault) || len(steps) == 0 || steps[len(steps)-1] != end {
				t.Fatalf("unexpected run %v: %v", steps, err)
			}
			if end == stepReader && slices.Contains(steps, stepRecover) {
				t.Fatal("writers opened before reader authority was verified")
			}
		})
	}
}

func TestControllerFixtureWorkloadAndReservedLease(t *testing.T) {
	for _, id := range []string{"q1", "r1", "r2", "r3"} {
		t.Run(id, func(t *testing.T) {
			w, err := buildFixtureWorkload("rehearsal", id, kubeNamespace(id), orphanClock{})
			if err != nil {
				t.Fatal(err)
			}
			var roles []string
			l1, _ := partition.ID(w.submission("l1").ID)
			for _, s := range w.work.NonMarkers {
				roles = append(roles, s.Role)
				if s.RequestSHA != restoreledger.SHA256(w.requests[s.Role]) {
					t.Fatal("unbound fixture submission")
				}
				part, _ := partition.ID(s.ID)
				if s.Role != "l1" && part == l1 {
					t.Fatalf("held lease starves %s", s.Role)
				}
			}
			if !slices.Equal(roles, []string{"d1", "d2", "f1", "l1", "e1", "n1", "n2", "n3", "n4"}) {
				t.Fatalf("wrong workload %v", roles)
			}
			for _, m := range w.markers.Markers {
				if p, _ := partition.ID(m.WorkflowID); p == l1 {
					t.Fatal("held lease starves a marker")
				}
			}
		})
	}
}

func TestControllerCgroupCounters(t *testing.T) {
	dir := t.TempDir()
	for file, text := range map[string]string{
		"cpu.stat": "usage_usec 17\nthrottled_usec 2\n", "memory.current": "4096\n",
		"memory.stat": "anon 1024\ninactive_file 512\n", "memory.events": "oom 0\noom_kill 0\n",
		"io.stat": "8:0 rbytes=100 wbytes=300 rios=1 wios=1\n",
	} {
		if err := os.WriteFile(filepath.Join(dir, file), []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var sample resourceSample
	if err := addCgroupStats(dir, &sample); err != nil || sample.CPUUsageNS != 17000 || sample.CPUThrottledNS != 2000 ||
		sample.WorkingSetBytes != 3584 || sample.IOReadBytes != 100 || sample.IOWrittenBytes != 300 {
		raw, _ := json.Marshal(sample)
		t.Fatalf("resource counters: %s: %v", raw, err)
	}
}

func TestControllerEndpointDoesNotOpenWithoutTypedDenials(t *testing.T) {
	for _, failure := range []string{"old", "foreign", "drift"} {
		t.Run(failure, func(t *testing.T) {
			calls := 0
			_, err := observeReopenedEndpoint(t.Context(), func() (int64, error) { return 100, nil }, endpointProbes{
				oldDenied: func(context.Context) error {
					if failure == "old" {
						return errors.New("old port accepted")
					}
					return nil
				},
				foreignDenied: func(context.Context) error {
					if failure == "foreign" {
						return errors.New("foreign role authenticated")
					}
					return nil
				},
				observer: func(context.Context) (uint64, uint32, error) {
					calls++
					if failure == "drift" {
						return uint64(calls), 2, nil
					}
					return 12, 2, nil
				},
			})
			if err == nil {
				t.Fatalf("opened target despite %s", failure)
			}
		})
	}
	roles := hba(engineDatabase, []string{targetWriterRole, observerRole}, "", false)
	if !strings.Contains(roles, "host all all all reject") || strings.Contains(roles, sourceWriterRole) || strings.Contains(roles, "local") {
		t.Fatalf("reopened target has an unexpected authority route: %s", roles)
	}
}

type fixtureSubmissionJournal struct {
	operation restoreledger.Operation
	outcome   restoreledger.Outcome
	stamp     int64
}

func (j *fixtureSubmissionJournal) Header() restoreledger.Header {
	return restoreledger.Header{Source: "source", Target: "target"}
}
func (j *fixtureSubmissionJournal) NowNS() (int64, error) { j.stamp++; return j.stamp, nil }
func (j *fixtureSubmissionJournal) RecordSend(o restoreledger.Operation) (int64, error) {
	j.operation = o
	return j.NowNS()
}
func (j *fixtureSubmissionJournal) RecordOutcome(o restoreledger.Outcome) (int64, error) {
	j.outcome = o
	return j.NowNS()
}
func (j *fixtureSubmissionJournal) RecordWitness(restoreledger.Witness) error { return nil }

type fixtureSubmissionTransport func(*http.Request) (*http.Response, error)

func (f fixtureSubmissionTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestControllerSubmissionAcknowledgmentBindsRequestAndAuthority(t *testing.T) {
	w, err := buildFixtureWorkload("rehearsal", "q1", "j7-q1", orphanClock{})
	if err != nil {
		t.Fatal(err)
	}
	s := w.submission("n1")
	part, _ := partition.ID(s.ID)
	for _, tc := range []string{"valid", "foreign-ack", "tampered-request"} {
		t.Run(tc, func(t *testing.T) {
			j := &fixtureSubmissionJournal{stamp: 100}
			calls := 0
			c := fixtureHTTP{origin: "http://127.0.0.1", token: "fixture-token", client: &http.Client{
				Transport: fixtureSubmissionTransport(func(r *http.Request) (*http.Response, error) {
					calls++
					if j.operation.RequestID != s.RequestID || j.operation.Authority != "target" || r.Header.Get("Authorization") != "Bearer fixture-token" {
						t.Fatal("request reached the transport before its authority and identity were written")
					}
					id := s.ID
					if tc == "foreign-ack" {
						id = "unrelated"
					}
					body := fmt.Sprintf(`{"created":true,"workflow":{"workflow_id":%q,"namespace":%q,
						"submission_key":%q,"definition_id":%q,"definition_version":1,"partition_id":%d,
						"revision":1,"state":"RUNNABLE","payload_hash":"sub-v1:%s"}}`,
						id, w.work.Fixture.Namespace, s.Key, s.Definition, part, testDigest)
					return &http.Response{StatusCode: 201, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
				}),
			}}
			body := w.requests["n1"]
			if tc == "tampered-request" {
				body = []byte("{}")
			}
			got, err := submitFixture(t.Context(), j, c, "target", w.work.Fixture.Namespace, s, body)
			switch tc {
			case "valid":
				if err != nil || !got.Acked() || got.OutcomeSync <= got.Outcome.CompletedNS || got.Outcome.SentNS <= got.SendSynced {
					t.Fatalf("unbound durable acknowledgment %+v: %v", got, err)
				}
			case "foreign-ack":
				if err == nil || j.outcome.Disposition != restoreledger.Ambiguous || calls != 1 {
					t.Fatalf("accepted foreign response: %+v %v", j.outcome, err)
				}
			case "tampered-request":
				if err == nil || calls != 0 || j.operation.RequestID != "" {
					t.Fatalf("sent altered sealed request: %v", err)
				}
			}
		})
	}
}
