package k6ingress

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"durable-agent-execution-engine/internal/k6hook"
)

const MaxBody = 64 << 10
const maxHeader = 24 << 10

type Relay struct {
	SinkAddress string
	SinkPeer    string
	SinkTLS     *tls.Config
	Witness     interface{ Record(Record) error }
	Hook        k6hook.Client
	admission   sync.Mutex
	ordinal     uint64
}

type exchange struct {
	request *http.Request
	wire    []byte
	body    []byte
}

// Read exactly one fixed-length HTTP/1.1 request on each TLS connection.
// The parser is used for framing and the route allowlist. The original header
// lines and entity bytes, including Authorization, are written to the sink.
func readRequest(input *bufio.Reader) (exchange, error) {
	var header bytes.Buffer
	for {
		line, err := input.ReadSlice('\n')
		if err != nil || header.Len()+len(line) > maxHeader || !bytes.HasSuffix(line, []byte("\r\n")) {
			return exchange{}, errors.New("K6_RELAY_HTTP_HEADER")
		}
		header.Write(line)
		if bytes.Equal(line, []byte("\r\n")) {
			break
		}
	}
	request, err := http.ReadRequest(bufio.NewReader(bytes.NewReader(header.Bytes())))
	if err != nil || request.Proto != "HTTP/1.1" || len(request.TransferEncoding) != 0 ||
		request.ContentLength < 0 || request.ContentLength > MaxBody || request.Host == "" ||
		len(request.Header.Values("Content-Length")) != 1 || request.Header.Get("Expect") != "" ||
		request.Header.Get("Upgrade") != "" || input.Buffered() > int(request.ContentLength) {
		return exchange{}, errors.New("K6_RELAY_HTTP_FRAMING")
	}
	body := make([]byte, request.ContentLength)
	if _, err := io.ReadFull(input, body); err != nil {
		return exchange{}, errors.New("K6_RELAY_HTTP_BODY")
	}
	return exchange{request, append(header.Bytes(), body...), body}, nil
}

func readResponse(conn net.Conn, request *http.Request) (wire, body []byte, status int, err error) {
	var captured bytes.Buffer
	input := bufio.NewReader(io.TeeReader(io.LimitReader(conn, MaxBody+maxHeader), &captured))
	response, err := http.ReadResponse(input, request)
	if err != nil {
		return nil, nil, 0, err
	}
	defer response.Body.Close()
	body, err = io.ReadAll(io.LimitReader(response.Body, MaxBody+1))
	if err != nil || len(body) > MaxBody || input.Buffered() != 0 || response.StatusCode < 200 ||
		(response.ContentLength < 0 && len(response.TransferEncoding) == 0) {
		return nil, nil, 0, errors.New("K6_RELAY_HTTP_RESPONSE")
	}
	return captured.Bytes(), body, response.StatusCode, nil
}

func localResponseBytes(status int) []byte {
	return fmt.Appendf(nil, "HTTP/1.1 %d %s\r\nContent-Length: 0\r\nConnection: close\r\n\r\n", status, http.StatusText(status))
}

func localResponse(conn net.Conn, status int) {
	_, _ = conn.Write(localResponseBytes(status))
}

func allowedRoute(request *http.Request) bool {
	return request.Method == http.MethodPost &&
		(request.RequestURI == "/v1/apply" || request.RequestURI == "/v1/lookup")
}

func (r *Relay) Handle(worker *tls.Conn) {
	if r.handleCampaign(worker) {
		return
	}
	defer worker.Close()
	if worker.SetDeadline(time.Now().Add(18*time.Second)) != nil {
		return
	}
	request, err := readRequest(bufio.NewReaderSize(worker, maxHeader))
	if err != nil {
		localResponse(worker, http.StatusBadRequest)
		return
	}
	record := inventory(request, 0)
	state := worker.ConnectionState()
	binding, err := state.ExportKeyingMaterial(BindingLabel, nil, 32)
	if err != nil {
		localResponse(worker, http.StatusServiceUnavailable)
		return
	}
	record.WorkerBinding = Hash(binding)
	if !allowedRoute(request.request) {
		r.reject(worker, record)
		return
	}
	sink, err := tls.DialWithDialer(&net.Dialer{Timeout: 2 * time.Second}, "tcp", r.SinkAddress, r.SinkTLS)
	if err == nil {
		defer sink.Close()
		state := sink.ConnectionState()
		binding, bindingErr := state.ExportKeyingMaterial(BindingLabel, nil, 32)
		if ValidPeer(state, r.SinkPeer) && bindingErr == nil && sink.SetDeadline(time.Now().Add(12*time.Second)) == nil {
			record.ConnectionHash = Hash(binding)
		}
	}
	if err := r.admit(&record); err != nil {
		localResponse(worker, http.StatusServiceUnavailable)
		return
	}
	defer func() {
		if sink != nil {
			_ = sink.Close()
		}
		_ = worker.Close()
		_ = r.Witness.Record(record.outcome("close"))
	}()
	if record.ConnectionHash == "" {
		_ = r.Witness.Record(record.outcome("no_response"))
		_ = r.Witness.Record(record.outcome("worker_drop"))
		return
	}
	outbound := request.wire
	if _, err := sink.Write(outbound); err != nil {
		_ = r.Witness.Record(record.outcome("sink_send_failed"))
		_ = r.Witness.Record(record.outcome("no_response"))
		_ = r.Witness.Record(record.outcome("worker_drop"))
		return
	}
	record.Event = "sink_send"
	record.WireHash, record.WireLength = Hash(outbound), len(outbound)
	if r.Witness.Record(record) != nil {
		return
	}
	wire, body, status, err := readResponse(sink, request.request)
	if err != nil {
		_ = r.Witness.Record(record.outcome("no_response"))
		_ = r.Witness.Record(record.outcome("worker_drop"))
		return
	}
	record.Event, record.Status, record.BodyHash, record.Length = "sink_response", status, Hash(body), len(body)
	record.WireHash, record.WireLength = Hash(wire), len(wire)
	if r.Witness.Record(record) != nil {
		return
	}
	record.Event = "worker_delivery"
	if _, err := worker.Write(wire); err != nil {
		record = record.outcome("worker_drop")
	}
	_ = r.Witness.Record(record)
}

func (r *Relay) reject(worker *tls.Conn, record Record) {
	record.Operation = "rejected"
	if record.Method != "POST" {
		record.Method = "OTHER"
	}
	if err := r.admit(&record); err != nil {
		localResponse(worker, http.StatusServiceUnavailable)
		return
	}
	defer func() {
		_ = worker.Close()
		_ = r.Witness.Record(record.outcome("close"))
	}()
	wire := localResponseBytes(http.StatusNotFound)
	record.Event, record.Status, record.BodyHash, record.Length = "local_response", http.StatusNotFound, Hash(nil), 0
	record.WireHash, record.WireLength = Hash(wire), len(wire)
	if r.Witness.Record(record) != nil {
		return
	}
	record.Event = "worker_delivery"
	if _, err := worker.Write(wire); err != nil {
		record = record.outcome("worker_drop")
	}
	_ = r.Witness.Record(record)
}

func (r *Relay) admit(record *Record) error {
	r.admission.Lock()
	defer r.admission.Unlock()
	record.Ordinal = r.ordinal + 1
	if err := r.Witness.Record(*record); err != nil {
		return err
	}
	r.ordinal++
	return nil
}

func (r Record) outcome(event string) Record {
	r.Event, r.Status, r.BodyHash, r.Length = event, 0, "", 0
	r.WireHash, r.WireLength = "", 0
	return r
}

func inventory(request exchange, ordinal uint64) Record {
	var labels struct {
		Key       string `json:"logical_key"`
		RequestID string `json:"request_id"`
		Intent    string `json:"intent_id"`
		Resource  string `json:"resource_id"`
	}
	// Untrusted observation metadata is hashed and never controls forwarding.
	_ = json.Unmarshal(request.body, &labels)
	return Record{
		Event: "inbound", Ordinal: ordinal, Method: request.request.Method,
		Operation:      strings.TrimPrefix(request.request.RequestURI, "/v1/"),
		InvocationHash: Hash([]byte(request.request.Header.Get("X-Dur053-Invocation"))),
		KeyHash:        Hash([]byte(labels.Key)), RequestIDHash: Hash([]byte(labels.RequestID)),
		IntentHash: Hash([]byte(labels.Intent)), ResourceHash: Hash([]byte(labels.Resource)),
		Length: len(request.body), BodyHash: Hash(request.body),
		WireLength: len(request.wire), WireHash: Hash(request.wire),
	}
}
