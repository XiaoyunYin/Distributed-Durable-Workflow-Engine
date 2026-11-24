package k6v1

import (
	"encoding/json"
	"strconv"
)

const lowerOffsetsPredicate = "LowerOffsetsDisposed"

func (e engineOrder) checkLowerOffsets(broker *Stream[BrokerRow], copied ...brokerCopies) {
	label := &e.result.Labels[6]
	label.PopulationUnit = "advanced_topic_partition"
	_, haveOffsets := e.rows[offsets]
	durable := map[int64]int64{}
	for _, row := range e.rows[offsets] {
		if row["consumer_id"] == workerGroup && row["topic"] == taskTopic {
			durable[number(row["kafka_partition"])] = number(row["next_offset"])
		}
	}
	var first *Witness
	if broker != nil {
		previous := map[PartitionIdentity]int64{}
		for _, record := range broker.Rows {
			if record.Data.Group != workerGroup {
				continue
			}
			for _, p := range record.Data.Partitions {
				if p.Topic != taskTopic {
					continue
				}
				key := PartitionIdentity{p.TopicID, p.Partition}
				prior, seen := previous[key]
				next := effectiveOffset(p)
				previous[key] = next
				if first == nil {
					ref := offsetWitness(record, key, next)
					first = &ref
				}
				if !seen || p.Committed == nil || next <= prior {
					continue
				}
				label.PopulationCount++
				// The durable consumer watermark is the next contiguous
				// disposition. Even the closing cut cannot justify an observed
				// broker advancement beyond it. A smaller broker watermark
				// leaves the per-record and earlier-commit checks unproved.
				covered := durable[p.Partition]
				if haveOffsets && next > covered {
					ref := offsetWitness(record, key, max(prior, covered))
					e.result.record(Violation, lowerOffsetsPredicate, "OFFSET_BEYOND_DURABLE_DISPOSITION", BrokerPath, &ref)
					break
				}
			}
		}
	}
	var copies brokerCopies
	if len(copied) != 0 {
		copies = copied[0]
	}
	e.requireOffsetProof(first, broker, copies)
}

func effectiveOffset(p BrokerPartition) int64 {
	if p.Committed != nil {
		return *p.Committed
	}
	return p.Low
}

func (e engineOrder) requireOffsetProof(first *Witness, broker *Stream[BrokerRow], copies brokerCopies) {
	if e.result.Labels[6].Registered {
		if e.capture && len(e.workflows) == 0 && len(e.rows[offsets]) == 0 && e.result.Labels[6].PopulationCount == 0 {
			e.result.Labels[6].Reason = "no_positive_population"
			return
		}
		bracket := e.checkOffsetBracket(broker, copies)
		if !bracket {
			e.requireWitness(lowerOffsetsPredicate, "broker_reset_hold_restart_offsets", first)
		}
		if !copies.complete {
			e.requireWitness(lowerOffsetsPredicate, "broker_event_copies", first)
		}
		if !e.captureReady {
			e.requireWitness(lowerOffsetsPredicate, "offset_disposition_transactions", first)
		}
	}
}

func (e engineOrder) checkOffsetBracket(broker *Stream[BrokerRow], copies brokerCopies) bool {
	if broker == nil {
		return false
	}
	var phases []Record[BrokerRow]
	for _, row := range broker.Rows {
		if row.Data.OffsetPhase != nil {
			phases = append(phases, row)
		}
	}
	if len(phases) == 0 || phases[0].Data.OffsetPhase.Name != "reset" {
		return false
	}
	reset := phases[0]
	baseline := workerPartitions(reset.Data)
	if len(baseline) == 0 {
		return false
	}
	bracketEnd := broker.Rows[len(broker.Rows)-1].Position.Sequence
	if len(phases) == 3 && phases[2].Data.OffsetPhase.Name == "restart" {
		bracketEnd = phases[2].Position.Sequence
	}
	for _, phase := range broker.Rows {
		if phase.Position.Sequence < reset.Position.Sequence || phase.Position.Sequence > bracketEnd {
			continue
		}
		if phase.Data.Cluster != reset.Data.Cluster || phase.Data.Group != workerGroup {
			return false
		}
		for _, p := range phase.Data.Partitions {
			if p.Topic != taskTopic {
				continue
			}
			if baseline[p.Partition].TopicID != p.TopicID || (phase.Data.TopicIDsAfter[p.Topic] != "" && phase.Data.TopicIDsAfter[p.Topic] != p.TopicID) {
				key := PartitionIdentity{p.TopicID, p.Partition}
				ref := offsetWitness(phase, key, effectiveOffset(p))
				e.result.record(Violation, lowerOffsetsPredicate, "OFFSET_BRACKET_TOPIC_CHANGED", BrokerPath, &ref)
				return false
			}
		}
		if len(workerPartitions(phase.Data)) != len(baseline) {
			return false
		}
	}
	if len(phases) != 3 {
		return false
	}
	boundaries := map[int64]bool{0: true}
	writes := e.rows[WriteCapture]
	for i, row := range writes {
		if i+1 == len(writes) || row["transaction_id"] != writes[i+1]["transaction_id"] {
			boundaries[number(row["event_order"])] = true
		}
	}
	for i, phase := range []string{"reset", "hold", "restart"} {
		p := phases[i].Data.OffsetPhase
		if phases[i].Data.Group != workerGroup || p.Name != phase || !boundaries[p.CaptureThrough] ||
			i > 0 && (p.CaptureThrough < phases[i-1].Data.OffsetPhase.CaptureThrough || p.RequestedUTC <= phases[i-1].Data.AfterUTC) {
			return false
		}
	}
	hold, restart := phases[1], phases[2]
	for _, row := range writes {
		if row["subject"] != offsets || row["operation"] != "insert" {
			continue
		}
		key := row["row_key"].([]any)
		if key[0] != workerGroup || key[1] != taskTopic {
			continue
		}
		p, ok := baseline[number(key[2])]
		if !ok || number(captureImage(row, "after_image")["next_offset"]) != effectiveOffset(p) {
			e.violate(lowerOffsetsPredicate, "OFFSET_ENGINE_BASELINE_MISMATCH", WriteCapture, row)
		}
	}
	if !copies.complete || !e.captureReady {
		return true
	}
	label := &e.result.Labels[6]
	label.PopulationCount = 0
	held := workerPartitions(hold.Data)
	for _, p := range restart.Data.Partitions {
		if p.Topic != taskTopic {
			continue
		}
		from := effectiveOffset(baseline[p.Partition])
		mid := effectiveOffset(held[p.Partition])
		end := effectiveOffset(p)
		if end > mid {
			label.PopulationCount++
		}
		key := PartitionIdentity{p.TopicID, p.Partition}
		// This is the held-commit trial. Lower records must already be
		// disposed at the independently fenced hold, before releasing it.
		for n := from; n < end; n++ {
			pos := deliveryPosition{consumerPartition{workerGroup, taskTopic, json.Number(strconv.FormatInt(p.Partition, 10))}, json.Number(strconv.FormatInt(n, 10))}
			order := e.dispositionOrders[pos]
			if _, ok := copies.byPosition[brokerPosition{key, n}]; !ok || order == 0 || order > hold.Data.OffsetPhase.CaptureThrough {
				ref := offsetWitness(restart, key, n)
				e.result.record(Violation, lowerOffsetsPredicate, "OFFSET_BRACKET_UNDISPOSED_COPY", BrokerPath, &ref)
				break
			}
		}
	}
	if label.PopulationCount > 0 {
		label.Outcome, label.Reason = "covered/pass", "predicate_satisfied"
	} else {
		label.Reason = "no_positive_population"
	}
	return true
}

func workerPartitions(b BrokerRow) map[int64]BrokerPartition {
	out := map[int64]BrokerPartition{}
	for _, p := range b.Partitions {
		if p.Topic == taskTopic {
			out[p.Partition] = p
		}
	}
	return out
}

func offsetWitness(record Record[BrokerRow], key PartitionIdentity, offset int64) Witness {
	raw, _ := Canonical(record)
	return Witness{Path: BrokerPath, RowKey: []any{record.Position.Sequence, key.TopicID, key.Partition, offset}, SHA256: Hash(raw)}
}
