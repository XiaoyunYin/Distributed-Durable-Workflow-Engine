package j7markers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/restoreledger"
)

type fixtureJournal struct {
	report       restoreledger.Report
	ns           int64
	sendDelay    int64
	outcomeDelay int64
}

func (j *fixtureJournal) Header() restoreledger.Header { return j.report.Header }
func (j *fixtureJournal) NowNS() (int64, error)        { return j.ns, nil }

func (j *fixtureJournal) RecordSend(op restoreledger.Operation) (int64, error) {
	j.ns += j.sendDelay
	j.report.Exchanges = append(j.report.Exchanges, restoreledger.Exchange{Operation: op, SendSynced: j.ns})
	return j.ns, nil
}

func (j *fixtureJournal) RecordOutcome(out restoreledger.Outcome) (int64, error) {
	j.ns += j.outcomeDelay
	x := &j.report.Exchanges[len(j.report.Exchanges)-1]
	x.Outcome, x.OutcomeSync = &out, j.ns
	return j.ns, nil
}

func (j *fixtureJournal) RecordWitness(w restoreledger.Witness) error {
	j.ns++
	j.report.Witnesses = append(j.report.Witnesses, restoreledger.RecordedWitness{Witness: w, RecordedNS: j.ns})
	return nil
}

func markerFixture(t *testing.T) (Plan, *fixtureJournal) {
	t.Helper()
	f := Fixture{Campaign: "campaign", Drill: "R1", Namespace: "fixture", DefinitionID: "echo",
		DefinitionSHA256: restoreledger.SHA256([]byte("definition"))}
	plan, err := NewPlan(f)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := plan.Digest()
	if err != nil {
		t.Fatal(err)
	}
	return plan, &fixtureJournal{ns: 10, sendDelay: 10, outcomeDelay: 10,
		report: restoreledger.Report{Header: restoreledger.Header{
			Campaign: f.Campaign, Drill: f.Drill, Source: "primary", MarkersSHA256: digest,
		}}}
}

func withVirtualWait(_ context.Context, ledger Journal, at int64) error {
	j := ledger.(*fixtureJournal)
	j.ns = max(j.ns, at)
	return nil
}

func createdBody(f Fixture, m Marker) string {
	p, _ := partition.ID(m.WorkflowID)
	return fmt.Sprintf(`{"created":true,"workflow":{"workflow_id":%q,"namespace":%q,"submission_key":%q,
"definition_id":%q,"definition_version":1,"partition_id":%d,"revision":1,"state":"RUNNABLE","payload_hash":%q}}`,
		m.WorkflowID, f.Namespace, m.SubmissionKey, f.DefinitionID, p, "sub-v1:"+strings.Repeat("a", 64))
}

func cutFixture(request string, ack int64) restoreledger.Witness {
	return restoreledger.Witness{
		Kind: restoreledger.ArchiveCut, Phase: "markers", Authority: "primary", RequestID: request,
		StartedNS: ack, CompletedNS: ack + 50, EvidenceSHA: restoreledger.SHA256([]byte("archive inventory")),
		Timeline: 1, LSN: "0/1000028", Segment: "000000010000000000000001", EndLSN: "0/2000000",
	}
}

func TestRunMarkersAndReplay(t *testing.T) {
	plan, ledger := markerFixture(t)
	const origin = 1_000_000_000
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/workflows" ||
			r.Header.Get("Authorization") != "Bearer fixture" {
			t.Errorf("unexpected marker route or authorization: %s %s", r.Method, r.URL.Path)
		}
		raw, err := io.ReadAll(r.Body)
		var packet struct {
			InitialInput struct {
				Marker int `json:"marker"`
			} `json:"initial_input"`
		}
		if err != nil || json.Unmarshal(raw, &packet) != nil ||
			packet.InitialInput.Marker < 1 || packet.InitialInput.Marker > markerCount {
			t.Error("unable to decode marker body")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		marker := plan.Markers[packet.InitialInput.Marker-1]
		if !bytes.Equal(raw, marker.Request) || r.Header.Get("X-Request-ID") != marker.RequestID {
			t.Error("wire request diverged from the sealed marker")
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, createdBody(plan.Fixture, marker))
	}))
	defer server.Close()
	var actions []int
	sender := Sender{
		AfterMarker: func(ctx context.Context, sequence int, ack int64) error {
			if ctx.Err() != nil || len(ledger.report.Exchanges) != sequence ||
				ledger.report.Exchanges[sequence-1].OutcomeSync != ack {
				t.Fatal("marker hook ran before the fsynced acknowledgment")
			}
			if sequence == archivedMarkers && len(ledger.report.Witnesses) != 1 {
				t.Fatal("M12 hook preceded the durable cut")
			}
			actions = append(actions, sequence)
			return nil
		},
		Client: server.Client(), SourceURL: server.URL,
		BearerToken: func(context.Context) (string, error) { return "fixture", nil },
		PlaceCut: func(ctx context.Context, ack int64) (restoreledger.Witness, error) {
			if _, ok := ctx.Deadline(); !ok || len(ledger.report.Exchanges) != archivedMarkers ||
				!ledger.report.Exchanges[archivedMarkers-1].Acked() {
				t.Fatal("cut was requested before a durable M12 ack or without a deadline")
			}
			witness := cutFixture(plan.Markers[archivedMarkers-1].RequestID, ack)
			ledger.ns = witness.CompletedNS
			return witness, nil
		},
	}
	lastAck, err := sender.run(t.Context(), ledger, plan, origin, withVirtualWait)
	if err != nil {
		t.Fatal(err)
	}
	if len(actions) != markerCount || actions[11] != 12 || actions[14] != 15 {
		t.Fatal("marker companion stages changed the schedule")
	}
	if lastAck != ledger.report.Exchanges[markerCount-1].OutcomeSync {
		t.Fatalf("reported outage origin %d differs from the last durable ack", lastAck)
	}
	ledger.report.Sealed = true
	if err := Verify(plan, origin, ledger.report); err != nil {
		t.Fatal(err)
	}
	if len(ledger.report.Exchanges) != 15 {
		t.Fatalf("marker denominator %d", len(ledger.report.Exchanges))
	}
	ledger.report.Witnesses[0].RecordedNS = ledger.report.Exchanges[archivedMarkers].Outcome.SentNS
	if err := Verify(plan, origin, ledger.report); err == nil {
		t.Fatal("accepted a cut recorded at the first post-cut send")
	}
	ledger.report.Witnesses[0].RecordedNS--
	ledger.report.Exchanges[6].OutcomeSync = 0
	if err := Verify(plan, origin, ledger.report); err == nil {
		t.Fatal("accepted a pre-cut acknowledgment without fsync proof")
	}
}

type roundTrip func(*http.Request) (*http.Response, error)

func (r roundTrip) RoundTrip(req *http.Request) (*http.Response, error) { return r(req) }

func TestMarkerFailuresStopWithoutCatchingUp(t *testing.T) {
	for _, tc := range []struct {
		name     string
		calls    int
		last     string
		wantText string
	}{
		{"missed send", 0, restoreledger.NotSent, "MISSED_SEND_WINDOW"},
		{"read deadline", 1, restoreledger.Acknowledged, "exceeded its response deadline"},
		{"malformed create", 1, restoreledger.Ambiguous, "CREATION_IDENTITY_MISMATCH"},
		{"replayed create", 1, restoreledger.Ambiguous, "HTTP_UNEXPECTED_STATUS"},
		{"redirect", 1, restoreledger.Ambiguous, "HTTP_UNEXPECTED_STATUS"},
		{"fsync deadline", 1, restoreledger.Acknowledged, "exceeded its fsync window"},
		{"late cut", 12, restoreledger.Acknowledged, "archive cut witness is unbound or late"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan, ledger := markerFixture(t)
			calls := 0
			if tc.name == "missed send" {
				ledger.sendDelay = int64(sendDeviation) + 1
			}
			if tc.name == "fsync deadline" {
				ledger.outcomeDelay = int64(ackSyncLimit) + 1
			}
			client := &http.Client{Transport: roundTrip(func(req *http.Request) (*http.Response, error) {
				m := plan.Markers[calls]
				calls++
				rawBody := createdBody(plan.Fixture, m)
				status := http.StatusCreated
				switch tc.name {
				case "read deadline":
					ledger.ns += int64(responseLimit) + 1
				case "malformed create":
					rawBody = strings.Replace(rawBody, m.WorkflowID, "wrong-workflow", 1)
				case "replayed create":
					status = http.StatusOK
				case "redirect":
					status = http.StatusTemporaryRedirect
				}
				return &http.Response{
					StatusCode: status, Header: http.Header{"Location": []string{"/replacement"}},
					Body: io.NopCloser(strings.NewReader(rawBody)), Request: req,
				}, nil
			})}
			sender := Sender{Client: client, SourceURL: "http://fixture.example",
				PlaceCut: func(_ context.Context, ack int64) (restoreledger.Witness, error) {
					if tc.name != "late cut" {
						t.Fatal("cut requested after a failed marker")
					}
					w := cutFixture(plan.Markers[archivedMarkers-1].RequestID, ack)
					w.CompletedNS = ack + int64(archiveCutLimit) + 1
					return w, nil
				}}
			lastAck, err := sender.run(t.Context(), ledger, plan, 1_000_000_000, withVirtualWait)
			if lastAck != 0 || err == nil || !strings.Contains(err.Error(), tc.wantText) {
				t.Fatalf("run = %v, want %s", err, tc.wantText)
			}
			if calls != tc.calls {
				t.Fatalf("HTTP calls %d, want %d", calls, tc.calls)
			}
			x := ledger.report.Exchanges[len(ledger.report.Exchanges)-1]
			if x.Disposition() != tc.last || len(ledger.report.Witnesses) != 0 {
				t.Fatalf("last exchange = %+v, witnesses = %v", x, ledger.report.Witnesses)
			}
		})
	}
}

func TestMarkerPlanIsStableAndRejectsPayloadDrift(t *testing.T) {
	p, _ := markerFixture(t)
	first, err := p.Digest()
	if err != nil {
		t.Fatal(err)
	}
	another, err := NewPlan(p.Fixture)
	if err != nil {
		t.Fatal(err)
	}
	second, err := another.Digest()
	if err != nil || first != second || another.Markers[14].OffsetNS != int64(70*time.Second) {
		t.Fatalf("unstable planned IDs or schedule: %s, %s, %v", first, second, err)
	}
	another.Markers[7].Request = json.RawMessage(`{"workflow_id":"another"}`)
	if _, err := another.Digest(); err == nil {
		t.Fatal("mutation escaped the plan binding")
	}
}
