package k6v1

type BrokerCopy struct {
	Offset        int64  `json:"offset"`
	EventID       string `json:"event_id"`
	Key           []byte `json:"key"`
	PayloadSHA256 string `json:"payload_sha256"`
}

// A scan closes each reset-to-cut partition range with its observed bounds.
// An unreadable range is kept with complete=false, including retention loss.
type BrokerCopyRow struct {
	PartitionIdentity
	Copy     *BrokerCopy      `json:"copy,omitempty"`
	Range    *BrokerPartition `json:"range,omitempty"`
	Complete bool             `json:"complete,omitempty"`
}

func (r BrokerCopyRow) valid() bool {
	if !nonempty(r.TopicID) || r.Partition < 0 {
		return false
	}
	if r.Copy != nil {
		return r.Range == nil && !r.Complete && r.Copy.Offset >= 0 &&
			(r.Copy.EventID == "" || uuid.MatchString(r.Copy.EventID)) && hashes(r.Copy.PayloadSHA256)
	}
	p := r.Range
	return p != nil && EngineTopic(p.Topic) && p.TopicID == r.TopicID && p.Partition == r.Partition &&
		p.Low >= 0 && p.End >= p.Low && p.Committed == nil
}

func EngineTopic(name string) bool { return name == taskTopic || name == eventTopic }

func copyWitness(record Record[BrokerCopyRow]) Witness {
	b := record.Data
	key := []any{b.TopicID, b.Partition}
	if b.Copy != nil {
		key = append(key, b.Copy.Offset)
	}
	raw, _ := Canonical(record)
	return Witness{BrokerCopiesPath, key, Hash(raw)}
}

type brokerPosition struct {
	PartitionIdentity
	offset int64
}

type brokerCopies struct {
	complete   bool
	topics     map[string]map[int64]PartitionIdentity
	byPosition map[brokerPosition]BrokerCopy
}

func (e engineOrder) checkBrokerCopies(broker *Stream[BrokerRow], stream *Stream[BrokerCopyRow]) brokerCopies {
	label := &e.result.Labels[5]
	out := brokerCopies{topics: map[string]map[int64]PartitionIdentity{}, byPosition: map[brokerPosition]BrokerCopy{}}
	if stream == nil || broker == nil || len(broker.Rows) == 0 {
		return out
	}
	closing := map[PartitionIdentity]BrokerPartition{}
	for _, p := range broker.Rows[len(broker.Rows)-1].Data.Partitions {
		if EngineTopic(p.Topic) {
			key := PartitionIdentity{p.TopicID, p.Partition}
			closing[key] = p
			if out.topics[p.Topic] == nil {
				out.topics[p.Topic] = map[int64]PartitionIdentity{}
			}
			out.topics[p.Topic][p.Partition] = key
		}
	}
	seen, counts := map[PartitionIdentity]bool{}, map[PartitionIdentity]int64{}
	var gap *Witness
	for _, row := range broker.Rows {
		for _, p := range row.Data.Partitions {
			key := PartitionIdentity{p.TopicID, p.Partition}
			if EngineTopic(p.Topic) && (out.topics[p.Topic][p.Partition] != key ||
				row.Data.TopicIDsAfter[p.Topic] != "" && row.Data.TopicIDsAfter[p.Topic] != p.TopicID) {
				ref := offsetWitness(row, key, p.Low)
				gap = &ref
				break
			}
		}
		if gap != nil {
			break
		}
	}
	for _, row := range stream.Rows {
		b := row.Data
		key := b.PartitionIdentity
		p, exists := closing[key]
		valid := exists && !seen[key]
		if b.Copy != nil {
			valid = valid && b.Copy.Offset == counts[key] && b.Copy.Offset < p.End
			counts[key]++
			position := brokerPosition{key, b.Copy.Offset}
			if _, found := out.byPosition[position]; exists && !seen[key] && !found && b.Copy.Offset >= p.Low && b.Copy.Offset < p.End {
				out.byPosition[position] = *b.Copy
			}
		} else {
			valid = valid && b.Complete && p.Low == 0 && b.Range.Low == 0 && b.Range.End == p.End &&
				b.Range.Topic == p.Topic && counts[key] == p.End
			seen[key] = true
		}
		if !valid && gap == nil {
			ref := copyWitness(row)
			gap = &ref
		}
	}
	if gap != nil || stream.Seal.Mode != "terminal" || len(seen) != len(closing) ||
		len(out.topics[taskTopic]) == 0 || len(out.topics[eventTopic]) == 0 {
		if gap == nil {
			raw, _ := Canonical(stream.Seal)
			gap = &Witness{Path: BrokerCopiesPath, RowKey: []any{"seal"}, SHA256: Hash(raw)}
		}
		e.requireWitness(dispatchPredicate, "broker_event_copy_range", gap)
	} else {
		out.complete = true
	}
	if e.captureReady || (len(e.rows[outbox]) == 0 && len(e.workflows) == 0) {
		e.joinBrokerCopies(stream, out)
	}
	if e.captureReady && out.complete && label.PopulationCount > 0 && len(out.byPosition) > 0 {
		label.Outcome, label.Reason = "covered/pass", "predicate_satisfied"
	}
	return out
}

func (e engineOrder) joinBrokerCopies(stream *Stream[BrokerCopyRow], copies brokerCopies) {
	attempts, found := map[any]int{}, map[any]int{}
	created := map[any]Row{}
	for _, row := range e.rows[WriteCapture] {
		switch {
		case row["subject"] == outbox && row["operation"] == "insert":
			created[row["row_key"].([]any)[0]] = captureImage(row, "after_image")
		case row["subject"] == publicationStart:
			attempts[row["row_key"].([]any)[0]]++
		}
	}
	joined := map[brokerPosition]bool{}
	for _, row := range stream.Rows {
		b := row.Data
		if b.Copy == nil {
			continue
		}
		position := brokerPosition{b.PartitionIdentity, b.Copy.Offset}
		if _, known := copies.byPosition[position]; !known || joined[position] {
			continue
		}
		joined[position] = true
		copy := b.Copy
		id := copy.EventID
		base := created[id]
		found[id]++
		flag := func(code string) {
			ref := copyWitness(row)
			e.result.record(Violation, dispatchPredicate, code, BrokerCopiesPath, &ref)
		}
		topic, _ := base["topic"].(string)
		switch {
		case found[id] > attempts[id] || copies.topics[topic][b.Partition] != b.PartitionIdentity:
			flag("BROKER_COPY_WITHOUT_ATTEMPT")
		case string(copy.Key) != base["key"]:
			flag("BROKER_COPY_KEY_MISMATCH")
		case base["payload_hash"] != copy.PayloadSHA256:
			flag("BROKER_COPY_PAYLOAD_MISMATCH")
		}
	}
	if !copies.complete {
		return // An unreadable range cannot prove a record's absence.
	}
	for _, row := range e.rows[publications] {
		if row["broker_acknowledged"] == true && found[row["event_id"]] == 0 {
			e.violate(dispatchPredicate, "BROKER_COPY_MISSING", publications, row)
		}
	}
	for _, row := range e.rows[WriteCapture] {
		if row["subject"] != physicalDelivery {
			continue
		}
		pos := capturedPosition(row["row_key"].([]any))
		topic, _ := pos.topic.(string)
		key := brokerPosition{copies.topics[topic][number(pos.partition)], number(pos.offset)}
		copy, ok := copies.byPosition[key]
		id, _ := captureImage(row, "after_image")["event_id"].(string)
		if !ok || copy.EventID != id {
			e.violate(dispatchPredicate, "BROKER_DELIVERY_WITHOUT_COPY", WriteCapture, row)
		}
	}
}

// BrokerEventID only retains the wire's public event identity. Absent or
// malformed headers leave an unattempted physical copy in the population.
func BrokerEventID(ids []string) string {
	if len(ids) == 1 && uuid.MatchString(ids[0]) {
		return ids[0]
	}
	return ""
}
