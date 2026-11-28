// Package j7markers sends the registered J7 pure marker workflows.
package j7markers

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"durable-agent-execution-engine/internal/restoreledger"
)

const (
	markerCount       = 15
	archivedMarkers   = 12
	markerCadence     = 5 * time.Second
	sendDeviation     = 100 * time.Millisecond
	responseLimit     = time.Second
	ackSyncLimit      = 250 * time.Millisecond
	archiveCutLimit   = 3 * time.Second
	definitionVersion = 1
	nodeID            = "pure.echo"
)

var fixtureID = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,62}$`)

type Fixture struct {
	Campaign         string `json:"campaign"`
	Drill            string `json:"drill"`
	Namespace        string `json:"namespace"`
	DefinitionID     string `json:"definition_id"`
	DefinitionSHA256 string `json:"definition_sha256"`
}

type Marker struct {
	Sequence       int             `json:"sequence"`
	RequestID      string          `json:"request_id"`
	WorkflowID     string          `json:"workflow_id"`
	SubmissionKey  string          `json:"submission_key"`
	OffsetNS       int64           `json:"offset_ns"`
	Request        json.RawMessage `json:"request"`
	RequestSHA256  string          `json:"request_sha256"`
	ExpectedOutput json.RawMessage `json:"expected_output"`
}

type Plan struct {
	Schema          string   `json:"schema"`
	Fixture         Fixture  `json:"fixture"`
	Activity        string   `json:"activity"`
	ActivityVersion string   `json:"activity_version"`
	Markers         []Marker `json:"markers"`
}

// NewPlan materializes the exact request bodies and expected pure.echo
// outputs. The controller binds its digest into the ledger before the drill.
func NewPlan(f Fixture) (Plan, error) {
	if !fixtureID.MatchString(f.Campaign) || !fixtureID.MatchString(f.Drill) ||
		!fixtureID.MatchString(f.Namespace) || !fixtureID.MatchString(f.DefinitionID) ||
		!restoreledger.IsSHA256(f.DefinitionSHA256) {
		return Plan{}, errors.New("invalid marker fixture identity or definition digest")
	}
	p := Plan{Schema: "j7.marker-plan.v1", Fixture: f, Activity: nodeID, ActivityVersion: "v1"}
	for n := 1; n <= markerCount; n++ {
		key := fmt.Sprintf("j7-%s-m%02d", restoreledger.SHA256([]byte(f.Campaign+"\x00"+f.Drill)), n)
		payload, err := json.Marshal(struct {
			Campaign string `json:"campaign"`
			Drill    string `json:"drill"`
			Sequence int    `json:"marker"`
		}{f.Campaign, f.Drill, n})
		if err != nil {
			return Plan{}, err
		}
		request, err := json.Marshal(struct {
			WorkflowID        string          `json:"workflow_id"`
			Namespace         string          `json:"namespace"`
			SubmissionKey     string          `json:"submission_key"`
			Payload           json.RawMessage `json:"payload"`
			DefinitionID      string          `json:"definition_id"`
			DefinitionVersion int             `json:"definition_version"`
			InitialNodeID     string          `json:"initial_node_id"`
			InitialInput      json.RawMessage `json:"initial_input"`
		}{
			WorkflowID: key, Namespace: f.Namespace, SubmissionKey: key, Payload: payload,
			DefinitionID: f.DefinitionID, DefinitionVersion: definitionVersion, InitialNodeID: nodeID, InitialInput: payload,
		})
		if err != nil {
			return Plan{}, err
		}
		p.Markers = append(p.Markers, Marker{
			Sequence: n, RequestID: key + "-submit", WorkflowID: key, SubmissionKey: key,
			OffsetNS: int64(time.Duration(n-1) * markerCadence), Request: request,
			RequestSHA256: restoreledger.SHA256(request), ExpectedOutput: payload,
		})
	}
	return p, nil
}

func (p Plan) Digest() (string, error) {
	expected, err := NewPlan(p.Fixture)
	if err != nil {
		return "", err
	}
	want, err := json.Marshal(expected)
	if err != nil {
		return "", err
	}
	got, err := json.Marshal(p)
	if err != nil || !bytes.Equal(got, want) {
		return "", errors.New("marker plan differs from its registered fixture")
	}
	return restoreledger.SHA256(want), nil
}
