package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"durable-agent-execution-engine/internal/invariants"
)

func TestCheckerStreamsReportAndSnapshot(t *testing.T) {
	row := json.RawMessage(`{ "<tag>": ["a&b", "\u003c", 123456789012345678901234567890] }`)
	trace := invariants.Trace{
		Snapshot: map[string]invariants.SnapshotTable{
			"z-rows":  {Rows: make([]json.RawMessage, 3000)},
			"b-empty": {Rows: []json.RawMessage{}},
			"a-null":  {},
		},
		History: make([]invariants.HistoryRecord, 3000),
		Results: []invariants.AcceptedResult{},
	}
	for i := range trace.History {
		trace.History[i].WorkflowID = "wf<&>"
		trace.Snapshot["z-rows"].Rows[i] = row
	}
	trace.Snapshot["z-rows"].Rows[0] = nil
	result := report{Status: "FAIL", Trace: &trace, Violations: []string{"missing"}, NonTerminal: []string{},
		Campaign: &invariants.CampaignReport{Executions: []invariants.ExecutionMetric{}, BusySeconds: 0.125}}
	var legacy bytes.Buffer
	if err := json.NewEncoder(&legacy).Encode(result); err != nil {
		t.Fatal(err)
	}
	stream := &boundedWrite{limit: 64 * 1024}
	if err := writeReport(stream, result); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stream.Bytes(), legacy.Bytes()) || stream.writes < 2 {
		t.Fatalf("stream differs from legacy encoding or wrote only %d chunks", stream.writes)
	}

	var raw struct {
		Trace json.RawMessage `json:"trace"`
	}
	if err := json.Unmarshal(stream.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	var want invariants.Trace
	if err := json.Unmarshal(raw.Trace, &want); err != nil {
		t.Fatal(err)
	}
	got, err := decodeSnapshot(boundedRead{Reader: bytes.NewReader(raw.Trace), limit: 4096})
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("row-streamed snapshot differs from json.Unmarshal: %v", err)
	}
}

func TestSnapshotDecodePreservesLegacyFieldSemantics(t *testing.T) {
	for _, input := range []string{
		`null`,
		`{"HISTORY":[],"history":[{"WorkflowID":"first","Revision":4}]}`,
		`{"History":[{"WorkflowID":"first","Revision":4}, {"WorkflowID":"abandoned"}],"history":[{"Revision":5}]}`,
		`{"History":[{"WorkflowID":"first"}],"history":null,"sNaPsHoT":{"t":{"rows":[]}, "z": null}, "snapshot":{"u":{"COUNT":2,"rows":[null,1e10000]}}, "unknown":{"big":[1e10000,{"nested":[true]}]}}`,
		`{"Snapshot":{"t":{"count":1,"sha256":"first","rows":[]},"t":{"rows":[{}]}}}`,
	} {
		var want invariants.Trace
		if err := json.Unmarshal([]byte(input), &want); err != nil {
			t.Fatal(err)
		}
		got, err := decodeSnapshot(strings.NewReader(input))
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("input %s: got %+v, err %v, want %+v", input, got, err, want)
		}
	}
	for _, input := range []string{``, `{`, `{} {}`, `{} !`, `{"History":[{}`, `{"Snapshot":{"t":{"rows":[1,]}}}`, `{"Snapshot":{"extra":1}}`, `{"unknown":[}`} {
		if _, err := decodeSnapshot(strings.NewReader(input)); err == nil {
			t.Fatalf("accepted malformed snapshot %q", input)
		}
	}
}

type boundedWrite struct {
	bytes.Buffer
	limit  int
	writes int
}

func (b *boundedWrite) Write(p []byte) (int, error) {
	if len(p) > b.limit {
		return 0, errors.New("write exceeded stream buffer")
	}
	b.writes++
	return b.Buffer.Write(p)
}

type boundedRead struct {
	io.Reader
	limit int
}

func (b boundedRead) Read(p []byte) (int, error) {
	if len(p) > b.limit {
		return 0, errors.New("decoder grew beyond row scratch space")
	}
	return b.Reader.Read(p)
}
