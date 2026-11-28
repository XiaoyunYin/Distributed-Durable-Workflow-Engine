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
	if err := checkBinding(plan, report.Header, originNS); err != nil {
		return err
	}
	exchanges := make([]restoreledger.Exchange, 0, markerCount)
	for _, x := range report.Exchanges {
		if x.Operation.Phase == "markers" {
			exchanges = append(exchanges, x)
		}
	}
	if len(exchanges) != markerCount {
		return fmt.Errorf("expected %d marker sends, found %d", markerCount, len(exchanges))
	}
	for i, marker := range plan.Markers {
		x := exchanges[i]
		scheduled := originNS + marker.OffsetNS
		if x.Operation != markerOperation(marker, plan.Fixture, report.Header.Source, scheduled) ||
			!x.Acked() || !withinWindow(x.Outcome.SentNS, scheduled) ||
			x.Outcome.CompletedNS-x.Outcome.SentNS > int64(responseLimit) ||
			x.OutcomeSync-x.Outcome.CompletedNS > int64(ackSyncLimit) ||
			!validPayloadHash(x.Outcome.PayloadHash) {
			return fmt.Errorf("marker M%02d is missing its scheduled durable creation ack", marker.Sequence)
		}
	}
	before, after := exchanges[archivedMarkers-1], exchanges[archivedMarkers]
	var cuts int
	for _, w := range report.Witnesses {
		if w.Kind != restoreledger.ArchiveCut {
			continue
		}
		cuts++
		if w.RequestID != before.Operation.RequestID || w.Authority != report.Header.Source ||
			!cutWithin(w.Witness, before.OutcomeSync, after.Operation.ScheduledNS) ||
			w.RecordedNS > before.OutcomeSync+int64(archiveCutLimit) ||
			w.RecordedNS >= after.Outcome.SentNS {
			return errors.New("archive cut does not precede the post-cut marker")
		}
	}
	if cuts != 1 {
		return fmt.Errorf("expected one cut witness, found %d", cuts)
	}
	return nil
}
