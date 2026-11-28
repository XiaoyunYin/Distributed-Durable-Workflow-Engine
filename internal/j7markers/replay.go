package j7markers

import (
	"errors"
	"fmt"

	"durable-agent-execution-engine/internal/restoreledger"
)

// Verify checks marker acknowledgments and times against the sealed schedule
// and cut witness. Recovered database identity joins belong to the restore
// verifier. An incomplete journal can still be retained as failed evidence.
func Verify(plan Plan, originNS int64, report restoreledger.Report) error {
	if !report.Sealed {
		return errors.New("marker replay requires a sealed ledger")
	}
	_, err := verify(plan, originNS, report, markerCount)
	return err
}

// BoundaryAck verifies a live marker checkpoint. The cut requires exactly M01
// through M12 and the outage requires all 15 offers plus the witnessed cut.
func BoundaryAck(plan Plan, originNS int64, report restoreledger.Report, outage bool) (int64, error) {
	if report.Sealed {
		return 0, errors.New("physical source actions require the live ledger")
	}
	count := archivedMarkers
	if outage {
		count = markerCount
	}
	return verify(plan, originNS, report, count)
}

func verify(plan Plan, originNS int64, report restoreledger.Report, count int) (int64, error) {
	if err := checkBinding(plan, report.Header, originNS); err != nil {
		return 0, err
	}
	exchanges := make([]restoreledger.Exchange, 0, markerCount)
	for _, x := range report.Exchanges {
		if x.Operation.Phase == "markers" {
			exchanges = append(exchanges, x)
		}
	}
	if len(exchanges) != count {
		return 0, fmt.Errorf("expected %d marker sends, found %d", count, len(exchanges))
	}
	for i, marker := range plan.Markers[:count] {
		x := exchanges[i]
		scheduled := originNS + marker.OffsetNS
		if x.Operation != markerOperation(marker, plan.Fixture, report.Header.Source, scheduled) ||
			!x.Acked() || !withinWindow(x.Outcome.SentNS, scheduled) ||
			x.Outcome.CompletedNS-x.Outcome.SentNS > int64(responseLimit) ||
			x.OutcomeSync-x.Outcome.CompletedNS > int64(ackSyncLimit) ||
			!validPayloadHash(x.Outcome.PayloadHash) {
			return 0, fmt.Errorf("marker M%02d is missing its scheduled durable creation ack", marker.Sequence)
		}
	}
	before := exchanges[archivedMarkers-1]
	var cuts int
	for _, w := range report.Witnesses {
		if w.Kind != restoreledger.ArchiveCut {
			continue
		}
		cuts++
		if count == archivedMarkers {
			return 0, errors.New("archive cut has already been recorded")
		}
		after := exchanges[archivedMarkers]
		if w.RequestID != before.Operation.RequestID || w.Authority != report.Header.Source ||
			!cutWithin(w.Witness, before.OutcomeSync, after.Operation.ScheduledNS) ||
			w.RecordedNS > before.OutcomeSync+int64(archiveCutLimit) ||
			w.RecordedNS >= after.Outcome.SentNS {
			return 0, errors.New("archive cut does not precede the post-cut marker")
		}
	}
	if count == markerCount && cuts != 1 {
		return 0, fmt.Errorf("expected one cut witness, found %d", cuts)
	}
	return exchanges[count-1].OutcomeSync, nil
}
