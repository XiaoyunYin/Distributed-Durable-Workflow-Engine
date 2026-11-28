package j7markers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/restoreledger"
)

const maxResponseBytes = 1 << 20

// Journal is implemented by restoreledger.Writer.
type Journal interface {
	Header() restoreledger.Header
	NowNS() (int64, error)
	RecordSend(restoreledger.Operation) (int64, error)
	RecordOutcome(restoreledger.Outcome) (int64, error)
	RecordWitness(restoreledger.Witness) error
}

// PlaceCut runs after M12's fsynced ack. The next slice will supply the
// independent archive witness after creating the restore point and switching WAL.
type PlaceCut func(ctx context.Context, syncedAckNS int64) (restoreledger.Witness, error)

type Sender struct {
	Client      *http.Client
	SourceURL   string
	BearerToken func(context.Context) (string, error)
	PlaceCut    PlaceCut
}

// Run keeps every offer on the original open-loop schedule and returns M15's
// fsynced ack time for the outage deadline. The journal stays open.
func (s Sender) Run(ctx context.Context, ledger Journal, plan Plan, originNS int64) (int64, error) {
	return s.run(ctx, ledger, plan, originNS, waitUntil)
}

func (s Sender) run(ctx context.Context, ledger Journal, plan Plan, originNS int64,
	wait func(context.Context, Journal, int64) error) (int64, error) {
	if err := checkBinding(plan, ledger.Header(), originNS); err != nil {
		return 0, err
	}
	endpoint, err := submissionURL(s.SourceURL)
	if err != nil {
		return 0, err
	}
	if s.Client == nil || s.PlaceCut == nil {
		return 0, errors.New("marker sender requires the fixture HTTP client and archive-cut callback")
	}
	client := *s.Client
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	var lastAck int64
	for _, marker := range plan.Markers {
		scheduled := originNS + marker.OffsetNS
		if err := wait(ctx, ledger, scheduled); err != nil {
			return 0, err
		}
		syncedAck, err := s.send(ctx, &client, ledger, endpoint, plan.Fixture, marker, scheduled)
		if err != nil {
			return 0, fmt.Errorf("marker M%02d: %w", marker.Sequence, err)
		}
		lastAck = syncedAck
		if marker.Sequence == archivedMarkers {
			next := scheduled + int64(markerCadence)
			if err := s.archiveCut(ctx, ledger, marker, syncedAck, next); err != nil {
				return 0, fmt.Errorf("archive cut after M12: %w", err)
			}
		}
	}
	return lastAck, nil
}

func checkBinding(p Plan, header restoreledger.Header, originNS int64) error {
	digest, err := p.Digest()
	if err != nil {
		return err
	}
	if header.Campaign != p.Fixture.Campaign || header.Drill != p.Fixture.Drill ||
		header.MarkersSHA256 != digest || originNS <= 0 ||
		originNS > math.MaxInt64-int64((markerCount+1)*markerCadence) {
		return errors.New("marker plan or schedule does not match the ledger binding")
	}
	return nil
}

func submissionURL(source string) (string, error) {
	u, err := url.Parse(source)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") ||
		u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" ||
		(u.Path != "" && u.Path != "/") {
		return "", errors.New("marker source must be a bare HTTP or HTTPS origin")
	}
	u.Path = "/v1/workflows"
	return u.String(), nil
}

func waitUntil(ctx context.Context, ledger Journal, scheduled int64) error {
	for {
		now, err := ledger.NowNS()
		if err != nil {
			return err
		}
		if now >= scheduled {
			return ctx.Err()
		}
		timer := time.NewTimer(time.Duration(scheduled - now))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func markerOperation(marker Marker, f Fixture, authority string, scheduled int64) restoreledger.Operation {
	return restoreledger.Operation{
		RequestID: marker.RequestID, Kind: restoreledger.Submit, Phase: "markers", Authority: authority,
		Sequence: marker.Sequence, WorkflowID: marker.WorkflowID, Namespace: f.Namespace,
		SubmissionKey: marker.SubmissionKey, RequestSHA256: marker.RequestSHA256, ScheduledNS: scheduled,
	}
}

func (s Sender) send(ctx context.Context, client *http.Client, ledger Journal, endpoint string,
	f Fixture, marker Marker, scheduled int64) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(marker.Request))
	if err != nil {
		return 0, err
	}
	req.GetBody = nil
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Request-ID", marker.RequestID)
	if s.BearerToken != nil {
		token, err := s.BearerToken(ctx)
		if err != nil || token == "" || strings.ContainsAny(token, " \t\r\n") {
			return 0, errors.New("marker fixture authorization failed")
		}
		req.Header.Set("Authorization", "Bearer "+token)
	}
	op := markerOperation(marker, f, ledger.Header().Source, scheduled)
	if _, err := ledger.RecordSend(op); err != nil {
		return 0, err
	}
	out := restoreledger.Outcome{RequestID: op.RequestID}
	reqCtx, cancel := context.WithTimeout(ctx, responseLimit)
	defer cancel()
	sent, err := ledger.NowNS()
	if err != nil {
		return 0, err
	}
	switch {
	case !withinWindow(sent, scheduled), ctx.Err() != nil:
		out.Disposition, out.Code = restoreledger.NotSent, "MISSED_SEND_WINDOW"
	default:
		out.SentNS = sent
		err = receive(client, req.WithContext(reqCtx), ledger, f, marker, &out)
		if err != nil {
			return 0, err
		}
	}
	synced, err := ledger.RecordOutcome(out)
	if err != nil {
		return 0, err
	}
	if out.Disposition != restoreledger.Acknowledged {
		return 0, fmt.Errorf("marker did not receive a valid creation ack (%s)", out.Code)
	}
	if out.CompletedNS-out.SentNS > int64(responseLimit) {
		return 0, errors.New("marker creation acknowledgment exceeded its response deadline")
	}
	if synced < out.CompletedNS || synced-out.CompletedNS > int64(ackSyncLimit) {
		return 0, errors.New("marker acknowledgment exceeded its fsync window")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return synced, nil
}

func receive(client *http.Client, req *http.Request, ledger Journal,
	f Fixture, marker Marker, out *restoreledger.Outcome) error {
	resp, err := client.Do(req)
	if err != nil {
		out.Disposition, out.Code = restoreledger.Ambiguous, "HTTP_INCOMPLETE"
		return nil
	}
	defer resp.Body.Close()
	out.HTTPStatus = resp.StatusCode
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil || len(body) > maxResponseBytes {
		out.Disposition, out.Code = restoreledger.Ambiguous, "HTTP_INCOMPLETE"
		return nil
	}
	completed, err := ledger.NowNS()
	if err != nil {
		return err
	}
	out.CompletedNS, out.ResponseSHA256 = completed, restoreledger.SHA256(body)
	switch {
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		out.Disposition, out.Code = restoreledger.Rejected, "HTTP_REJECTED"
	case resp.StatusCode == http.StatusCreated:
		if hash, ok := creation(body, f, marker); ok {
			out.Disposition, out.DurableID, out.PayloadHash = restoreledger.Acknowledged, marker.WorkflowID, hash
			break
		}
		out.Disposition, out.Code = restoreledger.Ambiguous, "CREATION_IDENTITY_MISMATCH"
	default:
		out.Disposition, out.Code = restoreledger.Ambiguous, "HTTP_UNEXPECTED_STATUS"
	}
	return nil
}

func creation(body []byte, f Fixture, marker Marker) (string, bool) {
	var res struct {
		Created  bool `json:"created"`
		Workflow struct {
			ID                string `json:"workflow_id"`
			Namespace         string `json:"namespace"`
			SubmissionKey     string `json:"submission_key"`
			DefinitionID      string `json:"definition_id"`
			DefinitionVersion int    `json:"definition_version"`
			Partition         *int16 `json:"partition_id"`
			Revision          int    `json:"revision"`
			State             string `json:"state"`
			PayloadHash       string `json:"payload_hash"`
		} `json:"workflow"`
	}
	if json.Unmarshal(body, &res) != nil {
		return "", false
	}
	w := res.Workflow
	part, _ := partition.ID(marker.WorkflowID)
	return w.PayloadHash, res.Created && w.ID == marker.WorkflowID && w.Namespace == f.Namespace &&
		w.SubmissionKey == marker.SubmissionKey && w.DefinitionID == f.DefinitionID &&
		w.DefinitionVersion == definitionVersion && w.Partition != nil && int16(part) == *w.Partition &&
		w.Revision == 1 && w.State == "RUNNABLE" && validPayloadHash(w.PayloadHash)
}

func validPayloadHash(value string) bool {
	suffix, found := strings.CutPrefix(value, "sub-v1:")
	return found && restoreledger.IsSHA256(suffix)
}

func withinWindow(at, scheduled int64) bool {
	return at >= scheduled-int64(sendDeviation) && at <= scheduled+int64(sendDeviation)
}

func (s Sender) archiveCut(ctx context.Context, ledger Journal, marker Marker, ackAt, next int64) error {
	now, err := ledger.NowNS()
	if err != nil {
		return err
	}
	deadline := min(ackAt+int64(archiveCutLimit), next-1)
	if now >= deadline {
		return errors.New("archive cut window elapsed")
	}
	cutCtx, cancel := context.WithTimeout(ctx, time.Duration(deadline-now))
	defer cancel()
	w, err := s.PlaceCut(cutCtx, ackAt)
	if err != nil {
		return err
	}
	if w.RequestID != marker.RequestID || w.Authority != ledger.Header().Source ||
		!cutWithin(w, ackAt, next) || cutCtx.Err() != nil {
		return errors.New("archive cut witness is unbound or late")
	}
	if err := ledger.RecordWitness(w); err != nil {
		return err
	}
	now, err = ledger.NowNS()
	if err != nil {
		return err
	}
	if now > deadline {
		return errors.New("archive cut proof was recorded too late")
	}
	return nil
}

func cutWithin(w restoreledger.Witness, ackAt, next int64) bool {
	return w.Kind == restoreledger.ArchiveCut && w.Phase == "markers" && w.StartedNS >= ackAt &&
		w.CompletedNS >= w.StartedNS && w.CompletedNS <= ackAt+int64(archiveCutLimit) && w.CompletedNS < next
}
