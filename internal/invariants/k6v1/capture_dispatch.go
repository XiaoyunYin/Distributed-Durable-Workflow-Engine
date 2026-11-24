package k6v1

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
)

func (e engineOrder) checkCapturedPublications() {
	type publicationKey struct{ event, attempt any }
	created := map[any]Row{}
	claims, starts := map[publicationKey]Row{}, map[publicationKey]Row{}
	marks, published := map[publicationKey]Row{}, map[publicationKey]Row{}
	for _, row := range e.rows[WriteCapture] {
		before, after := captureImage(row, "before_image"), captureImage(row, "after_image")
		key := row["row_key"].([]any)
		switch row["subject"] {
		case outbox:
			id := key[0]
			if row["operation"] == "insert" {
				created[id] = row
			}
			base := captureImage(created[id], "after_image")
			if after != nil && (base["key"] != after["key"] || base["payload_hash"] != after["payload_hash"] ||
				base["topic"] != after["topic"] || base["event_type"] != after["event_type"] ||
				base["aggregate_revision"] != after["aggregate_revision"] || created[id]["workflow_id"] != row["workflow_id"]) {
				e.violate(dispatchPredicate, "CAPTURE_PUBLICATION_IDENTITY", WriteCapture, row)
			}
			if after["publish_state"] == "CLAIMED" && before["relay_attempts"] != after["relay_attempts"] {
				attempt := publicationKey{id, after["relay_attempts"]}
				if before == nil || number(after["relay_attempts"]) != number(before["relay_attempts"])+1 || claims[attempt] != nil {
					e.violate(dispatchPredicate, "CAPTURE_PUBLICATION_CLAIM", WriteCapture, row)
				}
				claims[attempt] = row
			}
			if before["publish_state"] == "CLAIMED" && after != nil && after["publish_state"] != "CLAIMED" {
				published[publicationKey{id, before["relay_attempts"]}] = row
			}
		case publicationStart:
			attempt := publicationKey{key[0], key[1]}
			base := captureImage(created[attempt.event], "after_image")
			claim := captureImage(claims[attempt], "after_image")
			if row["operation"] != "start" || before != nil || after == nil || starts[attempt] != nil ||
				claim == nil || claims[attempt]["transaction_id"] == row["transaction_id"] ||
				claim["claim_owner"] != after["relay_owner"] || base["topic"] != after["topic"] ||
				created[attempt.event]["workflow_id"] != row["workflow_id"] {
				e.violate(dispatchPredicate, "CAPTURE_PUBLICATION_START", WriteCapture, row)
			} else if after["key"] == nil || after["payload_hash"] == nil {
				e.require(dispatchPredicate, "publication_attempt_identities", WriteCapture, row)
			} else if after["key"] != base["key"] || after["payload_hash"] != base["payload_hash"] {
				e.violate(dispatchPredicate, "CAPTURE_PUBLICATION_IDENTITY", WriteCapture, row)
			}
			starts[attempt] = row
		case publications:
			if after != nil {
				attempt := publicationKey{after["event_id"], after["relay_attempt"]}
				marks[attempt] = row
			}
		}
	}
	// Replay marks in row order after indexing the transaction's update.
	for _, row := range e.rows[WriteCapture] {
		before, after := captureImage(row, "before_image"), captureImage(row, "after_image")
		switch {
		case row["subject"] == outbox && before["publish_state"] == "CLAIMED" && after != nil && after["publish_state"] != "CLAIMED":
			id := row["row_key"].([]any)[0]
			mark := marks[publicationKey{id, before["relay_attempts"]}]
			if mark["transaction_id"] != row["transaction_id"] {
				e.violate(dispatchPredicate, "CAPTURE_PUBLICATION_MARK", WriteCapture, row)
			}
		case row["subject"] == publications && after != nil:
			attempt := publicationKey{after["event_id"], after["relay_attempt"]}
			start, claim := starts[attempt], claims[attempt]
			end := published[attempt]
			state := captureImage(end, "after_image")["publish_state"]
			localQuarantine := state == "QUARANTINED" && dispatchTopic(captureImage(end, "after_image")["event_type"]) == ""
			if end["transaction_id"] != row["transaction_id"] || captureImage(claim, "after_image")["claim_owner"] != after["relay_owner"] ||
				(state == "PUBLISHED") != (after["broker_acknowledged"] == true) ||
				!localQuarantine && (start == nil || start["transaction_id"] == row["transaction_id"] || number(start["event_order"]) >= number(row["event_order"])) {
				e.violate(dispatchPredicate, "CAPTURE_PUBLICATION_MARK", WriteCapture, row)
			}
		}
	}
}

func capturedPosition(key []any) deliveryPosition {
	return deliveryPosition{consumerPartition{key[0], key[1], key[2]}, key[3]}
}

func transportReference(pos deliveryPosition) string {
	return fmt.Sprintf("transport/%s/%s/%d/%d", pos.consumer, pos.topic, number(pos.partition), number(pos.offset))
}

type eventConsumer struct{ consumer, event any }

type capturedDelivery struct {
	position deliveryPosition
	event    any
}

func (e engineOrder) checkCapturedDispositions(events map[any]Row) {
	rows := e.rows[WriteCapture]
	accepted := map[eventConsumer]bool{}
	disposed := map[deliveryPosition]capturedDelivery{}
	watermarks := map[consumerPartition]int64{}
	for len(rows) > 0 {
		end := 1
		for end < len(rows) && rows[end]["transaction_id"] == rows[0]["transaction_id"] {
			end++
		}
		deliveries := e.checkDispositionTransaction(rows[:end], events, accepted, disposed)
		e.checkCapturedOffsetAdvance(rows[:end], deliveries, disposed, watermarks)
		rows = rows[end:]
	}
	e.checkCapturedReconciliations()
}

func (e engineOrder) checkCapturedReconciliations() {
	obligations := map[string]Row{}
	for _, row := range e.rows[WriteCapture] {
		key, after := row["row_key"].([]any), captureImage(row, "after_image")
		switch row["subject"] {
		case outbox:
			obligations["outbox/"+key[0].(string)] = row
		case wakeups:
			obligations["wakeup/"+key[0].(string)] = row
		case quarantine:
			if after != nil {
				obligations[transportReference(delivery(after))] = row
			}
		case reconciliation:
			switch after["kind"] {
			case "PENDING_OUTBOX", "LOST_WAKEUP", "POISON_RECORD":
				linked := obligations[after["reference"].(string)]
				image := captureImage(linked, "after_image")
				if image == nil || linked["workflow_id"] != row["workflow_id"] ||
					(after["kind"] == "POISON_RECORD" && linked["subject"] == outbox && image["publish_state"] != "QUARANTINED") {
					e.violate(dispatchPredicate, "CAPTURE_RECONCILIATION_BINDING", WriteCapture, row)
				}
			}
		}
	}
}

func (e engineOrder) checkDispositionTransaction(rows []Row, events map[any]Row, accepted map[eventConsumer]bool, disposed map[deliveryPosition]capturedDelivery) map[capturedDelivery]Row {
	deliveries := map[capturedDelivery]Row{}
	inboxes, poisons := map[capturedDelivery]Row{}, map[capturedDelivery]Row{}
	woken, reconciled := map[any]bool{}, map[any]bool{}
	for _, row := range rows {
		key, after := row["row_key"].([]any), captureImage(row, "after_image")
		switch row["subject"] {
		case physicalDelivery:
			id := capturedDelivery{capturedPosition(key), after["event_id"]}
			if after == nil || row["before_image"] != nil || number(id.position.offset) < 0 || deliveries[id] != nil {
				e.violate(dispatchPredicate, "CAPTURE_DELIVERY_BINDING", WriteCapture, row)
			}
			deliveries[id] = row
		case inbox:
			if row["operation"] == "insert" && after["kafka_offset"] != nil {
				pos := deliveryPosition{consumerPartition{key[0], after["topic"], after["kafka_partition"]}, after["kafka_offset"]}
				inboxes[capturedDelivery{pos, key[1]}] = row
			}
		case quarantine:
			if row["operation"] == "insert" {
				poisons[capturedDelivery{delivery(after), after["event_id"]}] = row
			}
		case wakeups:
			if row["operation"] == "insert" && after["event_id"] != nil {
				woken[after["event_id"]] = true
			}
		case reconciliation:
			if after["kind"] == "POISON_RECORD" {
				reconciled[after["reference"]] = true
			}
		}
	}
	for _, row := range rows {
		after, key := captureImage(row, "after_image"), row["row_key"].([]any)
		switch row["subject"] {
		case physicalDelivery:
			id := capturedDelivery{capturedPosition(key), after["event_id"]}
			consumer := eventConsumer{id.position.consumer, id.event}
			prior, known := disposed[id.position]
			event := events[id.event]
			matchesEvent := event != nil && event["topic"] == id.position.topic && dispatchConsumer(id.position.topic) == consumer.consumer
			valid := false
			switch row["operation"] {
			case "ACCEPTED":
				valid = matchesEvent && inboxes[id] != nil && captureImage(inboxes[id], "after_image")["disposition"] == "ACCEPTED" &&
					(id.position.topic != eventTopic || woken[id.event])
				accepted[consumer] = valid
			case "ALREADY_HANDLED":
				valid = matchesEvent && accepted[consumer]
			case "QUARANTINED":
				valid = (poisons[id] != nil || known && prior == id) && reconciled[transportReference(id.position)]
			}
			if !valid || known && prior != id {
				e.violate(dispatchPredicate, "CAPTURE_DELIVERY_BINDING", WriteCapture, row)
			} else {
				disposed[id.position] = id
				if e.dispositionOrders[id.position] == 0 {
					e.dispositionOrders[id.position] = number(row["event_order"])
				}
			}
		case inbox, quarantine:
			if row["operation"] != "insert" {
				continue
			}
			id := capturedDelivery{delivery(after), after["event_id"]}
			if row["subject"] == inbox {
				id.position.consumer, id.event = key[0], key[1]
			}
			if id.position.offset != nil && deliveries[id] == nil {
				e.violate(dispatchPredicate, "CAPTURE_DISPOSITION_TRANSACTION", WriteCapture, row)
			}
		case wakeups:
			if row["operation"] == "insert" && after["event_id"] != nil && !slices.ContainsFunc(rows, func(write Row) bool {
				return write["subject"] == physicalDelivery && write["operation"] == "ACCEPTED" &&
					captureImage(write, "after_image")["event_id"] == after["event_id"]
			}) {
				e.violate(dispatchPredicate, "CAPTURE_DISPOSITION_TRANSACTION", WriteCapture, row)
			}
		case outbox:
			if after["publish_state"] == "QUARANTINED" && captureImage(row, "before_image")["publish_state"] != "QUARANTINED" &&
				!reconciled["outbox/"+key[0].(string)] {
				e.violate(dispatchPredicate, "CAPTURE_DISPOSITION_TRANSACTION", WriteCapture, row)
			}
		}
	}
	return deliveries
}

func (e engineOrder) checkCapturedOffsetAdvance(rows []Row, deliveries map[capturedDelivery]Row, disposed map[deliveryPosition]capturedDelivery, watermarks map[consumerPartition]int64) {
	initial := map[consumerPartition]int64{}
	observed := map[consumerPartition]bool{}
	for id := range deliveries {
		observed[id.position.consumerPartition] = true
	}
	for _, row := range rows {
		if row["subject"] != offsets {
			continue
		}
		key := row["row_key"].([]any)
		partition := consumerPartition{key[0], key[1], key[2]}
		after := captureImage(row, "after_image")
		if after == nil {
			e.violate(dispatchPredicate, "CAPTURE_OFFSET_REGRESSION", WriteCapture, row)
			continue
		}
		next := number(after["next_offset"])
		prior, exists := watermarks[partition]
		if !exists {
			prior = next // The first insert initializes the engine's baseline.
		}
		if _, seen := initial[partition]; !seen {
			initial[partition] = prior
		}
		if next < prior {
			e.violate(dispatchPredicate, "CAPTURE_OFFSET_REGRESSION", WriteCapture, row)
		}
		if next > prior && !observed[partition] {
			e.violate(dispatchPredicate, "CAPTURE_OFFSET_TRANSACTION", WriteCapture, row)
		}
		for offset := prior; offset < next; offset++ {
			pos := deliveryPosition{partition, json.Number(strconv.FormatInt(offset, 10))}
			if _, ok := disposed[pos]; !ok {
				predicate := dispatchPredicate
				if partition.consumer == workerGroup && partition.topic == taskTopic {
					predicate = lowerOffsetsPredicate
				}
				e.violate(predicate, "CAPTURE_OFFSET_DISPOSITION_GAP", WriteCapture, row)
				break
			}
		}
		watermarks[partition] = next
	}
	for _, row := range rows {
		if row["subject"] != physicalDelivery {
			continue
		}
		pos := capturedPosition(row["row_key"].([]any))
		id := capturedDelivery{pos, captureImage(row, "after_image")["event_id"]}
		if deliveries[id]["event_order"] != row["event_order"] {
			continue
		}
		next, exists := watermarks[pos.consumerPartition]
		if start, changed := initial[pos.consumerPartition]; changed {
			if number(pos.offset) != start {
				continue // A prior duplicate or a delivery beyond a gap.
			}
		} else if exists && number(pos.offset) != next {
			continue
		}
		if !exists || next <= number(pos.offset) {
			e.violate(dispatchPredicate, "CAPTURE_OFFSET_TRANSACTION", WriteCapture, row)
		}
	}
}
