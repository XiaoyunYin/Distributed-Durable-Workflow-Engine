package k6v1

import (
	"slices"
	"strconv"
	"testing"
	"time"
)

func TestK6LowerOffsetsDisposed(t *testing.T) {
	for _, tc := range []struct {
		name, cell       string
		durable, restart int64
		violates         bool
	}{
		{"MissingProof", "hook-3", 12, 12, false},
		{"LowerGap", "hook-3", 11, 12, true},
		{"LowerGapOutside", "hook-0", 11, 12, true},
		{"OutsideCeiling", "hook-0", 12, 12, false},
		{"NoAdvance", "hook-0", 10, 11, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hook := tc.cell == "hook-3"
			rows := map[string][]Row{offsets: {{"consumer_id": workerGroup, "topic": taskTopic,
				"kafka_partition": testNumber("3"), "next_offset": testNumber(strconv.FormatInt(tc.durable, 10))}}}
			baseline, hold, restart := int64(10), int64(11), tc.restart
			if tc.name == "NoAdvance" {
				baseline = hold
			}
			stream := Stream[BrokerRow]{}
			for i, next := range []int64{baseline, hold, restart} {
				stream.Rows = append(stream.Rows, Record[BrokerRow]{
					Position: Position{Sequence: int64(i + 1)},
					Data: BrokerRow{Group: workerGroup, Partitions: []BrokerPartition{
						{Topic: taskTopic, TopicID: "topic-id", Partition: 3, Low: 10, End: 14, Committed: &next},
					}},
				})
			}
			r := transportOrder(t, Manifest{Cell: tc.cell}, rows, func(e engineOrder) { e.checkLowerOffsets(&stream) })
			label := r.Labels[6]
			if tc.violates {
				requireTransportFinding(t, r, lowerOffsetsPredicate, "OFFSET_BEYOND_DURABLE_DISPOSITION", Violation,
					BrokerPath, []any{int64(3), "topic-id", int64(3), int64(11)})
				if r.Safety != "FAIL" || r.ExitCode() != Fail {
					t.Fatalf("offset violation lost in roll-up: %+v", r)
				}
			} else if r.Safety == "FAIL" {
				t.Fatalf("legitimate offsets failed safety: %+v", r.Findings)
			}
			if !hook {
				assertNoPromotion(t, label)
				if !tc.violates && (len(r.Findings) != 0 || r.ExitCode() != Incomplete) {
					t.Fatalf("out-of-ceiling offsets required proof: %+v", r)
				}
				return
			}
			if !tc.violates {
				for _, source := range []string{"broker_reset_hold_restart_offsets", "broker_event_copies", "offset_disposition_transactions"} {
					requireTransportFinding(t, r, lowerOffsetsPredicate, "required_proof_missing:"+source, MissingProof,
						BrokerPath, []any{int64(1), "topic-id", int64(3), int64(10)})
				}
				if r.Completeness != "FAIL" || label.Outcome != "NOT COVERED" {
					t.Fatalf("offset proof gap lost in roll-up: %+v", r)
				}
			}
		})
	}
}

func TestK6OffsetBracket(t *testing.T) {
	for _, tc := range []struct{ name, code string }{
		{"Clean", ""}, {"LowerMissing", "OFFSET_BRACKET_UNDISPOSED_COPY"},
		{"LowerLate", "OFFSET_BRACKET_UNDISPOSED_COPY"}, {"BaselineMismatch", "OFFSET_ENGINE_BASELINE_MISMATCH"},
		{"TopicIDChange", "OFFSET_BRACKET_TOPIC_CHANGED"}, {"WrongGroup", "required_proof_missing:broker_reset_hold_restart_offsets"},
		{"InterruptedTopicChange", "OFFSET_BRACKET_TOPIC_CHANGED"},
		{"MissingPhase", "required_proof_missing:broker_reset_hold_restart_offsets"},
		{"WrongBoundary", "required_proof_missing:broker_reset_hold_restart_offsets"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := Manifest{Cell: "hook-3"}
			key := PartitionIdentity{"id", 0}
			stream := Stream[BrokerRow]{}
			for i, name := range []string{"reset", "hold", "restart"} {
				at := fixed.Add(time.Duration(i) * time.Second)
				end, next, journal := int64(i), int64(i/2), int64(min(i, 1)*3)
				if name == "restart" {
					journal = 4
				}
				row := BrokerRow{Group: workerGroup, Cluster: "cluster", BeforeUTC: UTC(at), AfterUTC: UTC(at),
					Partitions:  []BrokerPartition{{Topic: taskTopic, TopicID: key.TopicID, End: end, Committed: &next}},
					OffsetPhase: &OffsetPhase{Name: name, Binding: Binding(m), RequestedUTC: UTC(at.Add(-time.Millisecond)), CaptureThrough: journal}}
				stream.Rows = append(stream.Rows, Record[BrokerRow]{Position: Position{int64(i + 1), UTC(at)}, Data: row})
			}
			base, disposition := testNumber("0"), int64(2)
			switch tc.name {
			case "LowerMissing":
				disposition = 0
			case "LowerLate":
				disposition = 4
			case "BaselineMismatch":
				base = testNumber("1")
			case "TopicIDChange":
				stream.Rows[1].Data.TopicIDsAfter = map[string]string{taskTopic: "other"}
			case "InterruptedTopicChange":
				stream.Rows[1].Data.TopicIDsAfter = map[string]string{taskTopic: "other"}
				stream.Rows = stream.Rows[:2]
			case "WrongGroup":
				stream.Rows[1].Data.Group = "other"
			case "MissingPhase":
				stream.Rows[1].Data.OffsetPhase = nil
			case "WrongBoundary":
				stream.Rows[1].Data.OffsetPhase.CaptureThrough = 2
			}
			rows := map[string][]Row{WriteCapture: {
				capturedRow(offsets, "insert", "1", "1", []any{workerGroup, taskTopic, testNumber("0")}, nil, Row{"next_offset": base}),
				{"transaction_id": "1", "event_order": testNumber("3")}, {"transaction_id": "2", "event_order": testNumber("4")},
			}}
			r := transportOrder(t, m, rows, func(e engineOrder) {
				e.captureReady = true
				e.dispositionOrders[deliveryPosition{consumerPartition{workerGroup, taskTopic, testNumber("0")}, testNumber("0")}] = disposition
				e.checkLowerOffsets(&stream, brokerCopies{complete: true, byPosition: map[brokerPosition]BrokerCopy{{key, 0}: {}}})
			})
			if tc.code == "" {
				if len(r.Findings) != 0 || r.Labels[6].Outcome != "covered/pass" {
					t.Fatalf("missing I7 bracket award: %+v", r.Findings)
				}
				return
			}
			if !slices.ContainsFunc(r.Findings, func(f Finding) bool { return f.Code == tc.code }) {
				t.Fatalf("expected bracket finding %s: %+v", tc.code, r.Findings)
			}
		})
	}
}
