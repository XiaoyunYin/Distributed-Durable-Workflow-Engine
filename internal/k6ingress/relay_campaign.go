//go:build dur053_k6

package k6ingress

import (
	"bufio"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"syscall"
	"time"

	"durable-agent-execution-engine/internal/k6hook"
)

func validCampaignRecord(r Record) bool {
	return r.Copy == 0 || r.Copy == 2 && r.Operation == "apply" && r.ConnectionHash != "" &&
		r.Event != "inbound" && r.Event != "worker_delivery"
}

func validCampaignEvent(r Record) bool {
	switch r.Event {
	case "copy_open":
		return r.Copy == 2 && r.Status == 0 && r.validBytes()
	case "fault_drop", "extra_reply":
		return r.Operation == "apply" && r.Status == 200 && r.validBytes() &&
			(r.Event == "extra_reply") == (r.Copy == 2)
	case "caller_closed":
		return r.Copy == 0 && r.Status == 0 && r.Length == 0 && r.BodyHash == "" && r.WireLength == 0 && r.WireHash == ""
	default:
		return false
	}
}

func (r *Relay) dialCampaign(record *Record, until time.Time) *tls.Conn {
	sink, err := tls.DialWithDialer(&net.Dialer{Timeout: 2 * time.Second}, "tcp", r.SinkAddress, r.SinkTLS)
	if err != nil {
		return nil
	}
	state := sink.ConnectionState()
	binding, err := state.ExportKeyingMaterial(BindingLabel, nil, 32)
	if !ValidPeer(state, r.SinkPeer) || err != nil || sink.SetDeadline(until) != nil {
		_ = sink.Close()
		return nil
	}
	record.ConnectionHash = Hash(binding)
	return sink
}

func (r *Relay) deliverCampaign(sink *tls.Conn, request exchange, record Record) ([]byte, []byte, Record) {
	if sink == nil {
		_ = r.Witness.Record(record.outcome("no_response"))
		return nil, nil, record
	}
	defer sink.Close()
	if _, err := sink.Write(request.wire); err != nil {
		_ = r.Witness.Record(record.outcome("sink_send_failed"))
		_ = r.Witness.Record(record.outcome("no_response"))
		return nil, nil, record
	}
	record.Event = "sink_send"
	if r.Witness.Record(record) != nil {
		return nil, nil, record
	}
	wire, body, status, err := readResponse(sink, request.request)
	if err != nil {
		_ = r.Witness.Record(record.outcome("no_response"))
		return nil, nil, record
	}
	record.Event, record.Status, record.BodyHash, record.Length = "sink_response", status, Hash(body), len(body)
	record.ApplicationID = responseApplication(body)
	record.WireHash, record.WireLength = Hash(wire), len(wire)
	if r.Witness.Record(record) != nil {
		return nil, nil, record
	}
	return wire, body, record
}

type caller struct {
	mu     sync.Mutex
	closed chan struct{}
	sent   bool
}

func (r *Relay) watchCaller(worker *tls.Conn, input *bufio.Reader, record Record) (*caller, <-chan struct{}) {
	c := &caller{closed: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := input.ReadByte()
		c.mu.Lock()
		defer c.mu.Unlock()
		if !c.sent && (errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNRESET)) {
			if r.Witness.Record(record.outcome("caller_closed")) == nil {
				close(c.closed)
			}
		}
	}()
	return c, done
}

func (c *caller) reply(worker *tls.Conn, wire []byte) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.closed:
		return false
	default:
		c.sent = true
		n, err := worker.Write(wire)
		return err == nil && n == len(wire)
	}
}

func (r *Relay) hit(boundary, action string, fields k6hook.Fields) k6hook.Hold {
	hold, err := r.Hook.Hit(boundary, fields)
	if err != nil || hold != nil && (hold.Action() != action || !time.Now().Before(hold.Deadline())) {
		log.Print("K6_RELAY_HOOK_FAILED")
		return nil
	}
	return hold
}

func finish(hold k6hook.Hold) {
	if hold.Finish() != nil {
		log.Print("K6_RELAY_HOOK_FINISH_FAILED")
	}
}

func (r *Relay) handleCampaign(worker *tls.Conn) bool {
	if r.Hook == nil {
		return false
	}
	defer worker.Close()
	if worker.SetDeadline(time.Now().Add(4*time.Second)) != nil {
		return true
	}
	input := bufio.NewReaderSize(worker, maxHeader)
	request, err := readRequest(input)
	if err != nil {
		localResponse(worker, http.StatusBadRequest)
		return true
	}
	state := worker.ConnectionState()
	binding, err := state.ExportKeyingMaterial(BindingLabel, nil, 32)
	until := time.Now().Add(62 * time.Second)
	if err != nil || worker.SetDeadline(until) != nil {
		localResponse(worker, http.StatusServiceUnavailable)
		return true
	}
	record := inventory(request, 0)
	record.WorkerBinding = Hash(binding)
	if !allowedRoute(request.request) {
		r.reject(worker, record)
		return true
	}
	sink := r.dialCampaign(&record, until)
	if sink != nil {
		defer sink.Close()
	}
	if r.admit(&record) != nil {
		localResponse(worker, http.StatusServiceUnavailable)
		return true
	}
	c, done := r.watchCaller(worker, input, record)
	defer func() {
		_ = worker.Close()
		<-done
		_ = r.Witness.Record(record.outcome("close"))
	}()

	var duplicate k6hook.Hold
	var fields k6hook.Fields
	if record.Operation == "apply" && sink != nil {
		var labels struct {
			Key      string `json:"logical_key"`
			Request  string `json:"request_id"`
			Intent   string `json:"intent_id"`
			Scope    string `json:"scope_hash"`
			Message  string `json:"message_id"`
			Resource string `json:"resource_id"`
		}
		invocation := request.request.Header.Get("X-Dur053-Invocation")
		if json.Unmarshal(request.body, &labels) == nil && invocation != "" && invocation == labels.Message {
			fields = k6hook.Fields{Key: labels.Key, Request: labels.Request, Invocation: invocation,
				Intent: labels.Intent, Resource: labels.Resource, Scope: labels.Scope, Binding: record.ConnectionHash}
			duplicate = r.hit("relay-pre-forward", "duplicate", fields)
		}
	}
	wire, body, response := r.deliverCampaign(sink, request, record)
	if wire == nil {
		_ = r.Witness.Record(record.outcome("worker_drop"))
		return true
	}
	if duplicate != nil && response.Status == 200 {
		r.sendExtra(request, record, duplicate)
	} else if fields.Invocation != "" && response.Status == 200 {
		var receipt struct {
			App string `json:"application_id"`
		}
		if json.Unmarshal(body, &receipt) == nil && receipt.App != "" {
			fields.App, fields.Reply = receipt.App, response.BodyHash
			if drop := r.hit("relay-post-commit", "drop", fields); drop != nil && r.drop(c, drop, response) {
				return true
			}
		}
	}
	response.Event = "worker_delivery"
	if !c.reply(worker, wire) {
		response = response.outcome("worker_drop")
	}
	_ = r.Witness.Record(response)
	return true
}

func (r *Relay) sendExtra(request exchange, original Record, hold k6hook.Hold) {
	if !time.Now().Before(hold.Deadline()) {
		log.Print("K6_RELAY_HOOK_EXPIRED")
		return
	}
	extra := original
	extra.Copy, extra.Event, extra.ConnectionHash = 2, "copy_open", ""
	sink := r.dialCampaign(&extra, hold.Deadline())
	if sink == nil {
		log.Print("K6_RELAY_COPY_CONNECT_FAILED")
		return
	}
	defer sink.Close()
	if r.Witness.Record(extra) != nil {
		return
	}
	defer func() { _ = r.Witness.Record(extra.outcome("close")) }()
	wire, _, response := r.deliverCampaign(sink, request, extra)
	if wire != nil && response.Status == 200 {
		response.Event = "extra_reply"
		if r.Witness.Record(response) == nil {
			finish(hold)
		}
	}
}

func (r *Relay) drop(c *caller, hold k6hook.Hold, response Record) bool {
	timer := time.NewTimer(time.Until(hold.Deadline()))
	defer timer.Stop()
	select {
	case <-timer.C:
		log.Print("K6_RELAY_CALLER_TIMEOUT_NOT_OBSERVED")
		return false
	case <-c.closed:
		if !time.Now().Before(hold.Deadline()) {
			return false
		}
	}
	response.Event = "fault_drop"
	if r.Witness.Record(response) != nil || r.Witness.Record(response.outcome("worker_drop")) != nil {
		return false
	}
	finish(hold)
	return true
}
