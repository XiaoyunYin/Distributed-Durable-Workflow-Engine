package k6v1

import (
	"strconv"
	"testing"
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
