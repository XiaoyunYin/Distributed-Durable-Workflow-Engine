// Package k6ingress implements the forward-only relay and its independent witness.
package k6ingress

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sync"
	"time"
)

const BindingLabel = "EXPORTER-dur053-k6-sink"
const MaxRecord = 4096

var hexHash = regexp.MustCompile("^[0-9a-f]{64}$")
var sessionID = regexp.MustCompile("^[0-9a-f]{32}$")
var publicUUID = regexp.MustCompile("^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$")
var peerURI = regexp.MustCompile("^spiffe://[a-z0-9.-]+/[a-z0-9/-]+$")

func Hash(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

type Record struct {
	Session        string    `json:"session"`
	Sequence       uint64    `json:"sequence"`
	Time           time.Time `json:"time"`
	Event          string    `json:"event"`
	HighWater      uint64    `json:"high_water,omitempty"`
	Ordinal        uint64    `json:"ordinal,omitempty"`
	Copy           int       `json:"copy,omitempty"`
	Method         string    `json:"method,omitempty"`
	Operation      string    `json:"operation,omitempty"`
	InvocationHash string    `json:"invocation_hash,omitempty"`
	KeyHash        string    `json:"key_hash,omitempty"`
	RequestIDHash  string    `json:"request_id_hash,omitempty"`
	IntentHash     string    `json:"intent_hash,omitempty"`
	ResourceHash   string    `json:"resource_hash,omitempty"`
	Length         int       `json:"length"`
	BodyHash       string    `json:"body_hash,omitempty"`
	WireLength     int       `json:"wire_length,omitempty"`
	WireHash       string    `json:"wire_hash,omitempty"`
	ConnectionHash string    `json:"connection_hash,omitempty"`
	WorkerBinding  string    `json:"worker_binding,omitempty"`
	IntentID       string    `json:"intent_id,omitempty"`
	GrantHash      string    `json:"grant_hash,omitempty"`
	Attempt        int64     `json:"attempt,omitempty"`
	ApplicationID  string    `json:"application_id,omitempty"`
	RelayPeer      string    `json:"relay_peer"`
	SinkPeer       string    `json:"sink_peer"`
	Status         int       `json:"status,omitempty"`
}

type ack struct {
	Session  string `json:"session"`
	Sequence uint64 `json:"sequence"`
	Hash     string `json:"sha256"`
}

func ValidPeer(state tls.ConnectionState, expected string) bool {
	if !peerURI.MatchString(expected) || len(state.VerifiedChains) == 0 || len(state.VerifiedChains[0]) == 0 {
		return false
	}
	uris := state.VerifiedChains[0][0].URIs
	return len(uris) == 1 && uris[0].String() == expected
}

func (r Record) valid(relay, sink string) bool {
	if !sessionID.MatchString(r.Session) || r.Sequence == 0 || r.Time.IsZero() ||
		r.RelayPeer != relay || r.SinkPeer != sink {
		return false
	}
	if r.Event == "seal" {
		expected := Record{Session: r.Session, Sequence: r.Sequence, Time: r.Time, Event: "seal",
			HighWater: r.Sequence - 1, RelayPeer: relay, SinkPeer: sink}
		return r == expected
	}
	allowed := r.Method == "POST" && (r.Operation == "apply" || r.Operation == "lookup")
	rejected := r.Operation == "rejected" && (r.Method == "POST" || r.Method == "OTHER") && r.ConnectionHash == ""
	if r.Ordinal == 0 || (!allowed && !rejected) ||
		r.HighWater != 0 || r.Length < 0 || r.Length > MaxBody ||
		!validCampaignRecord(r) ||
		(r.ConnectionHash != "" && !hexHash.MatchString(r.ConnectionHash)) || !hexHash.MatchString(r.WorkerBinding) {
		return false
	}
	for _, field := range []string{r.InvocationHash, r.KeyHash, r.RequestIDHash, r.IntentHash, r.ResourceHash} {
		if !hexHash.MatchString(field) {
			return false
		}
	}
	if (r.IntentID != "" || r.GrantHash != "" || r.Attempt != 0) &&
		(!publicUUID.MatchString(r.IntentID) || Hash([]byte(r.IntentID)) != r.IntentHash ||
			!hexHash.MatchString(r.GrantHash) || r.Attempt < 1) ||
		r.ApplicationID != "" && !publicUUID.MatchString(r.ApplicationID) {
		return false
	}
	switch r.Event {
	case "inbound", "sink_send":
		return r.Status == 0 && r.validBytes()
	case "worker_delivery", "sink_response":
		return r.Status >= 200 && r.Status <= 599 && r.validBytes()
	case "local_response":
		return rejected && r.Status == 404 && r.validBytes()
	case "sink_send_failed", "no_response", "worker_drop", "close":
		return r.Status == 0 && r.Length == 0 && r.BodyHash == "" && r.WireLength == 0 && r.WireHash == ""
	default:
		return validCampaignEvent(r)
	}
}

func (r Record) validBytes() bool {
	return r.WireLength > r.Length && r.WireLength <= MaxBody+maxHeader &&
		hexHash.MatchString(r.BodyHash) && hexHash.MatchString(r.WireHash)
}

type watermark struct {
	sequence uint64
	sealed   bool
}

// Receiver owns the append-only file. Restart replays its durable prefix.
// A malformed or truncated file cannot be reopened for further writes.
type Receiver struct {
	file     *os.File
	mu       sync.Mutex
	sessions map[string]watermark
	failed   bool
	relay    string
	sink     string
}

func OpenReceiver(path, relay, sink string) (*Receiver, error) {
	if !peerURI.MatchString(relay) || !peerURI.MatchString(sink) || relay == sink {
		return nil, errors.New("K6_WITNESS_PEERS")
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return nil, errors.New("K6_WITNESS_OPEN")
	}
	r := &Receiver{file: f, sessions: make(map[string]watermark), relay: relay, sink: sink}
	if err := r.replay(); err != nil {
		_ = f.Close()
		return nil, err
	}
	if runtime.GOOS != "windows" {
		dir, err := os.Open(filepath.Dir(path))
		if err != nil {
			_ = f.Close()
			return nil, errors.New("K6_WITNESS_DIRECTORY")
		}
		err = dir.Sync()
		_ = dir.Close()
		if err != nil {
			_ = f.Close()
			return nil, errors.New("K6_WITNESS_DIRECTORY_SYNC")
		}
	}
	return r, nil
}

func (r *Receiver) Close() error { return r.file.Close() }

func readFrame(input *bufio.Reader) ([]byte, error) {
	line, err := input.ReadSlice('\n')
	if err != nil {
		return line, err
	}
	return line[:len(line)-1], nil
}

// ReadJournal verifies canonical records and fsynced receiver sequence without
// opening a producer connection or replaying untrusted payloads.
func ReadJournal(input io.Reader, relay, sink string) ([]Record, error) {
	reader := bufio.NewReaderSize(input, MaxRecord)
	r := &Receiver{relay: relay, sink: sink, sessions: make(map[string]watermark)}
	rows := []Record{}
	for {
		line, err := readFrame(reader)
		if errors.Is(err, io.EOF) && len(line) == 0 {
			return rows, nil
		}
		record, valid := r.parse(line)
		if err != nil || !valid {
			return nil, errors.New("K6_INGRESS_JOURNAL")
		}
		r.sessions[record.Session] = watermark{record.Sequence, record.Event == "seal"}
		rows = append(rows, record)
	}
}

func (r *Receiver) parse(line []byte) (Record, bool) {
	var record Record
	if json.Unmarshal(line, &record) != nil || !record.valid(r.relay, r.sink) {
		return Record{}, false
	}
	canonical, err := json.Marshal(record)
	mark := r.sessions[record.Session]
	return record, err == nil && bytes.Equal(canonical, line) && !mark.sealed &&
		record.Sequence == mark.sequence+1
}

func (r *Receiver) replay() error {
	input := bufio.NewReaderSize(r.file, MaxRecord)
	for {
		line, err := readFrame(input)
		if errors.Is(err, io.EOF) && len(line) == 0 {
			return nil
		}
		record, valid := r.parse(line)
		if err != nil || !valid {
			return errors.New("K6_WITNESS_REPLAY")
		}
		r.sessions[record.Session] = watermark{record.Sequence, record.Event == "seal"}
	}
}

func (r *Receiver) append(line []byte) (ack, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	record, valid := r.parse(line)
	if r.failed || !valid {
		return ack{}, errors.New("K6_WITNESS_RECORD")
	}
	if _, err := r.file.Write(append(line, '\n')); err != nil {
		r.failed = true
		return ack{}, errors.New("K6_WITNESS_WRITE")
	}
	if err := r.file.Sync(); err != nil {
		r.failed = true
		return ack{}, errors.New("K6_WITNESS_SYNC")
	}
	r.sessions[record.Session] = watermark{record.Sequence, record.Event == "seal"}
	return ack{record.Session, record.Sequence, Hash(line)}, nil
}

func (r *Receiver) Handle(conn *tls.Conn) {
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil || conn.Handshake() != nil ||
		!ValidPeer(conn.ConnectionState(), r.relay) {
		return
	}
	line, err := readFrame(bufio.NewReaderSize(conn, MaxRecord))
	if err != nil {
		return
	}
	reply, err := r.append(line)
	if err == nil {
		_ = json.NewEncoder(conn).Encode(reply)
	}
}

// Client serializes all records in one relay process session. A failed dial
// has sent no record. A failure after dialing leaves the sequence ambiguous and
// poisons the session. It must never be retried or silently sealed.
type Client struct {
	Address        string
	TLS            *tls.Config
	Session        string
	RelayPeer      string
	SinkPeer       string
	ObserveTimings bool
	mu             sync.Mutex
	sequence       uint64
	poisoned       bool
	sealed         bool
}

func (c *Client) Record(record Record) error {
	started := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.poisoned || c.sealed {
		return errors.New("K6_WITNESS_SESSION_CLOSED")
	}
	record.Session, record.Sequence, record.Time = c.Session, c.sequence+1, time.Now().UTC()
	record.RelayPeer, record.SinkPeer = c.RelayPeer, c.SinkPeer
	if record.Event == "seal" {
		record.HighWater = c.sequence
	}
	data, err := json.Marshal(record)
	if err != nil || len(data)+1 >= MaxRecord {
		return errors.New("K6_WITNESS_ENCODE")
	}
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 2 * time.Second}, "tcp", c.Address, c.TLS)
	if err != nil {
		return errors.New("K6_WITNESS_CONNECT")
	}
	defer conn.Close()
	if conn.SetDeadline(time.Now().Add(2*time.Second)) != nil {
		return errors.New("K6_WITNESS_DEADLINE")
	}
	c.poisoned = true
	if _, err := conn.Write(append(data, '\n')); err != nil {
		return errors.New("K6_WITNESS_SEND")
	}
	line, err := readFrame(bufio.NewReaderSize(conn, MaxRecord))
	var reply ack
	if err != nil || json.Unmarshal(line, &reply) != nil ||
		reply != (ack{record.Session, record.Sequence, Hash(data)}) {
		return errors.New("K6_WITNESS_ACK")
	}
	c.poisoned = false
	c.sequence++
	c.sealed = record.Event == "seal"
	if c.ObserveTimings && record.Event != "seal" {
		log.Printf("k6_witness %s %s %d", record.RequestIDHash, record.Event, time.Since(started).Nanoseconds())
	}
	return nil
}

func (c *Client) Seal() error {
	if err := c.Record(Record{Event: "seal"}); err != nil {
		return fmt.Errorf("K6_WITNESS_SEAL: %w", err)
	}
	return nil
}
