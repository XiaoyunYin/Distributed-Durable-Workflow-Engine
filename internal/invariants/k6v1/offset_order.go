package k6v1

const lowerOffsetsPredicate = "LowerOffsetsDisposed"

func (e engineOrder) checkLowerOffsets(broker *Stream[BrokerRow]) {
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
				next := p.Low
				if p.Committed != nil {
					next = *p.Committed
				}
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
					e.requireOffsetProof(&ref)
					return
				}
			}
		}
	}
	e.requireOffsetProof(first)
}

func (e engineOrder) requireOffsetProof(first *Witness) {
	if e.result.Labels[6].Registered {
		if e.capture && len(e.workflows) == 0 && len(e.rows[offsets]) == 0 && e.result.Labels[6].PopulationCount == 0 {
			e.result.Labels[6].Reason = "no_positive_population"
			return
		}
		e.requireWitness(lowerOffsetsPredicate, "broker_reset_hold_restart_offsets", first)
		e.requireWitness(lowerOffsetsPredicate, "broker_event_copies", first)
		if !e.captureReady {
			e.requireWitness(lowerOffsetsPredicate, "offset_disposition_transactions", first)
		}
	}
}

func offsetWitness(record Record[BrokerRow], key PartitionIdentity, offset int64) Witness {
	raw, _ := Canonical(record)
	return Witness{Path: BrokerPath, RowKey: []any{record.Position.Sequence, key.TopicID, key.Partition, offset}, SHA256: Hash(raw)}
}
