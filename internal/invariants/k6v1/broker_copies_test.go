package k6v1

import (
	"testing"
	"testing/fstest"
)

func TestK6BrokerCopyIndex(t *testing.T) {
	m, _ := quietFixture(t, false)
	bundle := fstest.MapFS{BrokerCopiesPath: {Data: []byte("malformed\n")}}
	writeFixture(t, bundle, ManifestPath, m)
	indexFixture(t, bundle)
	r := Verify(bundle)
	for _, f := range r.Findings {
		if f.Source == BrokerCopiesPath && f.Code == "EXTERNAL_SCHEMA" {
			return
		}
	}
	t.Fatalf("indexed broker source did not reach its decoder: %+v", r.Findings)
}

func TestK6BrokerCopies(t *testing.T) {
	const event = "d7dc6717-1b22-459a-b474-dd345bac8b74"
	payload := Hash([]byte("payload"))
	for _, tc := range []struct {
		name, code string
		kind       FindingKind
	}{
		{"Clean", "", ""},
		{"MissingCopy", "BROKER_COPY_MISSING", Violation},
		{"ExtraCopy", "BROKER_COPY_WITHOUT_ATTEMPT", Violation},
		{"KeyMismatch", "BROKER_COPY_KEY_MISMATCH", Violation},
		{"PayloadMismatch", "BROKER_COPY_PAYLOAD_MISMATCH", Violation},
		{"DeliveryWithoutCopy", "BROKER_DELIVERY_WITHOUT_COPY", Violation},
		{"CollectionGap", "required_proof_missing:broker_event_copy_range", MissingProof},
		{"GapAndKeyMismatch", "required_proof_missing:broker_event_copy_range", MissingProof},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := Manifest{Cell: "hook-0"}
			key := PartitionIdentity{"task-id", 0}
			tasks := BrokerPartition{Topic: taskTopic, TopicID: key.TopicID, End: 1}
			events := BrokerPartition{Topic: eventTopic, TopicID: "event-id"}
			wire := BrokerCopy{EventID: event, Key: []byte(event), PayloadSHA256: payload}
			copy := BrokerCopyRow{PartitionIdentity: key, Copy: &wire}
			rows := map[string][]Row{
				WriteCapture: {
					{"subject": outbox, "operation": "insert", "row_key": []any{event},
						"after_image": map[string]any{"topic": taskTopic, "key": event, "payload_hash": payload}},
					{"subject": publicationStart, "row_key": []any{event, testNumber("1")}},
				},
				publications: {{"event_id": event, "broker_acknowledged": true}},
			}
			copies := []BrokerCopyRow{copy}
			switch tc.name {
			case "MissingCopy":
				tasks.End, copies = 0, nil
			case "ExtraCopy":
				tasks.End = 2
				second := wire
				second.Offset = 1
				copies = append(copies, BrokerCopyRow{PartitionIdentity: key, Copy: &second})
			case "KeyMismatch", "GapAndKeyMismatch":
				wire.Key = []byte("different")
			case "PayloadMismatch":
				wire.PayloadSHA256 = Hash([]byte("different"))
			case "DeliveryWithoutCopy":
				rows[WriteCapture] = append(rows[WriteCapture], Row{"subject": physicalDelivery,
					"row_key": []any{workerGroup, taskTopic, testNumber("0"), testNumber("1")}, "after_image": map[string]any{"event_id": event}})
			}
			copies = append(copies, BrokerCopyRow{PartitionIdentity: key, Range: &tasks, Complete: tc.kind != MissingProof},
				BrokerCopyRow{PartitionIdentity: PartitionIdentity{events.TopicID, 0}, Range: &events, Complete: true})
			stream := fixtureStream(m, "broker-copies", "broker-reader", copies...)
			broker := fixtureStream(m, "broker", "broker-reader", BrokerRow{Partitions: []BrokerPartition{events, tasks}})
			r := transportOrder(t, m, rows, func(e engineOrder) {
				e.captureReady = true
				e.result.Labels[5].PopulationCount = 1
				e.checkBrokerCopies(&broker, &stream)
			})
			if tc.code == "" {
				if len(r.Findings) != 0 || r.Labels[5].Outcome != "covered/pass" {
					t.Fatalf("missing I6 broker award: %+v", r.Findings)
				}
				return
			}
			if tc.name == "GapAndKeyMismatch" {
				if len(r.Findings) != 2 || r.Findings[1].Code != "BROKER_COPY_KEY_MISMATCH" || r.Safety != "FAIL" {
					t.Fatalf("collection gap concealed a witnessed key mismatch: %+v", r.Findings)
				}
				r.Findings = r.Findings[:1]
			}
			if len(r.Findings) != 1 || r.Findings[0].Code != tc.code || r.Findings[0].Kind != tc.kind {
				t.Fatalf("expected broker finding %s: %+v", tc.code, r.Findings)
			}
		})
	}
}
