package k6ingress

import (
	"bufio"
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const relayPeer = "spiffe://episode/relay"
const sinkPeer = "spiffe://episode/sink"

func credentials(t *testing.T) *tls.Config {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	relay, _ := url.Parse(relayPeer)
	sink, _ := url.Parse(sinkPeer)
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), IsCA: true, BasicConstraintsValid: true,
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err = x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	issue := func(serial int64, name string, peer *url.URL) tls.Certificate {
		leaf := &x509.Certificate{SerialNumber: big.NewInt(serial), DNSNames: []string{name},
			URIs: []*url.URL{peer}, NotBefore: ca.NotBefore, NotAfter: ca.NotAfter,
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}}
		der, err := x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		return tls.Certificate{Certificate: [][]byte{der, caDER}, PrivateKey: key}
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	return &tls.Config{
		MinVersion: tls.VersionTLS13, ClientCAs: roots, RootCAs: roots, ServerName: "relay.test",
		Certificates: []tls.Certificate{issue(2, "relay.test", relay), issue(3, "sink.test", sink)},
	}
}

func listen(t *testing.T, cfg *tls.Config, handle func(*tls.Conn)) string {
	t.Helper()
	l, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			_ = conn.SetDeadline(time.Now().Add(4 * time.Second))
			handle(conn.(*tls.Conn))
			_ = conn.Close()
		}
	}()
	t.Cleanup(func() {
		_ = l.Close()
		<-done
	})
	return l.Addr().String()
}

type recorderFunc func(Record) error

func (f recorderFunc) Record(r Record) error { return f(r) }

type upstream struct {
	request    []byte
	connection string
}

func fixtureRelay(t *testing.T, witness recorderFunc) (string, *tls.Config, <-chan upstream) {
	t.Helper()
	config := credentials(t)
	sinkConfig := config.Clone()
	sinkConfig.Certificates = config.Certificates[1:]
	sinkConfig.ClientAuth = tls.RequireAndVerifyClientCert
	atSink := make(chan upstream, 3)
	address := listen(t, sinkConfig, func(conn *tls.Conn) {
		message, err := readRequest(bufio.NewReader(conn))
		if err != nil {
			return
		}
		state := conn.ConnectionState()
		if !ValidPeer(state, relayPeer) {
			return
		}
		binding, err := state.ExportKeyingMaterial(BindingLabel, nil, 32)
		if err != nil {
			return
		}
		atSink <- upstream{message.wire, Hash(binding)}
		_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 7\r\nConnection: close\r\n\r\nreceipt")
	})
	workerConfig := config.Clone()
	workerConfig.Certificates = config.Certificates[:1]
	upstreamConfig := workerConfig.Clone()
	upstreamConfig.ServerName = "sink.test"
	relay := &Relay{SinkAddress: address, SinkPeer: sinkPeer, SinkTLS: upstreamConfig, Witness: witness}
	return listen(t, workerConfig, relay.Handle), workerConfig, atSink
}

func requestBytes(route string) []byte {
	body := `{"logical_key":"stable","request_id":"request-a","grant_token":"private-grant"}`
	return []byte(fmt.Sprintf("POST %s HTTP/1.1\r\nHost: worker\r\naUtHoRiZaTiOn:  Bearer projected.secret.token\r\n"+
		"Content-Length: %d\r\nX-Dur053-Invocation: test-invocation\r\nConnection: close\r\n\r\n%s", route, len(body), body))
}

func exchangeRequest(t *testing.T, address string, config *tls.Config, route string) (int, string) {
	t.Helper()
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", address, config)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(4 * time.Second))
	if _, err := conn.Write(requestBytes(route)); err != nil {
		t.Fatal(err)
	}
	res, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "POST"})
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return res.StatusCode, string(body)
}

func TestK6RelayRequiresJournalACK(t *testing.T) {
	address, config, upstream := fixtureRelay(t, func(Record) error {
		return errors.New("withheld")
	})
	if status, _ := exchangeRequest(t, address, config, "/v1/apply"); status != 503 {
		t.Fatalf("missing ACK response = %d", status)
	}
	select {
	case <-upstream:
		t.Fatal("relay sent the request without a durable journal ACK")
	case <-time.After(150 * time.Millisecond):
	}
}

func TestK6RelayPreservesAuthorizationAndWire(t *testing.T) {
	events := make(chan Record, 8)
	address, config, upstream := fixtureRelay(t, func(record Record) error {
		events <- record
		return nil
	})
	status, body := exchangeRequest(t, address, config, "/v1/apply")
	if status != 200 || body != "receipt" {
		t.Fatalf("reply = %d %q", status, body)
	}
	got := <-upstream
	if !bytes.Equal(got.request, requestBytes("/v1/apply")) {
		t.Fatal("relay changed original wire bytes or Authorization")
	}
	for _, kind := range []string{"inbound", "sink_send", "sink_response", "worker_delivery", "close"} {
		select {
		case record := <-events:
			if record.Event != kind || record.Ordinal != 1 || record.ConnectionHash != got.connection {
				t.Fatalf("missing or misbound journal event %s", kind)
			}
			data, _ := json.Marshal(record)
			for _, secret := range []string{"private-grant", "projected.secret.token", "test-invocation", "request-a"} {
				if strings.Contains(string(data), secret) {
					t.Fatal("journal contains raw request material")
				}
			}
		case <-time.After(time.Second):
			t.Fatal("journal event missing")
		}
	}
}

func TestK6RelayOrdinaryRoutes(t *testing.T) {
	address, config, upstream := fixtureRelay(t, func(Record) error { return nil })
	for _, route := range []string{"/v1/allocate", "/v1/arm", "/v1/hold", "/v1/fault", "/arm", "/v1/apply?arm=1"} {
		if status, _ := exchangeRequest(t, address, config, route); status != 404 {
			t.Fatalf("ordinary relay exposed a forbidden route %s (%d)", route, status)
		}
	}
	select {
	case <-upstream:
		t.Fatal("forbidden route reached upstream")
	default:
	}
}

func recordForTest() Record {
	sum := Hash([]byte("fixture"))
	return Record{Ordinal: 1, Method: "POST", Operation: "apply", Event: "inbound",
		InvocationHash: sum, KeyHash: sum, RequestIDHash: sum, IntentHash: sum, ResourceHash: sum,
		Length: 9, BodyHash: sum, WireLength: 90, WireHash: sum, WorkerBinding: sum, ConnectionHash: sum}
}

func TestK6WitnessSyncAndRestart(t *testing.T) {
	name := filepath.Join(t.TempDir(), "journal.jsonl")
	witness, err := OpenReceiver(name, relayPeer, sinkPeer)
	if err != nil {
		t.Fatal(err)
	}
	cfg := credentials(t)
	cfg.Certificates = cfg.Certificates[:1]
	server := cfg.Clone()
	server.ClientAuth = tls.RequireAndVerifyClientCert
	client := &Client{Address: listen(t, server, witness.Handle), TLS: cfg,
		Session: "123456789abcdef0123456789abcdef0", RelayPeer: relayPeer, SinkPeer: sinkPeer}
	if err := client.Record(recordForTest()); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(name)
	if err != nil || bytes.Count(contents, []byte("\n")) != 1 {
		t.Fatal("ACK returned before record was readable on the host")
	}
	if err := witness.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenReceiver(name, relayPeer, sinkPeer)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	client.Address = listen(t, server, reopened.Handle)
	if err := client.Seal(); err != nil {
		t.Fatal(err)
	}
	contents, err = os.ReadFile(name)
	if err != nil || !bytes.Contains(contents, []byte(`"high_water":1`)) {
		t.Fatal("restart did not preserve the session high-water mark")
	}
	if err := client.Record(recordForTest()); err == nil {
		t.Fatal("sealed session accepted another record")
	}
	truncated := filepath.Join(t.TempDir(), "truncated.jsonl")
	if err := os.WriteFile(truncated, bytes.TrimSuffix(contents, []byte{'\n'}), 0600); err != nil {
		t.Fatal(err)
	}
	if opened, err := OpenReceiver(truncated, relayPeer, sinkPeer); err == nil {
		_ = opened.Close()
		t.Fatal("receiver reopened a truncated journal")
	}
}

func TestK6WitnessRejectsAmbiguousACK(t *testing.T) {
	for _, kind := range []string{"missing", "late", "replayed", "wrong-key"} {
		t.Run(kind, func(t *testing.T) {
			cfg := credentials(t)
			address := listen(t, cfg, func(conn *tls.Conn) {
				data, err := readFrame(bufio.NewReaderSize(conn, MaxRecord))
				if err != nil || kind == "missing" {
					return
				}
				var record Record
				_ = json.Unmarshal(data, &record)
				reply := ack{record.Session, record.Sequence, Hash(data)}
				switch kind {
				case "late":
					time.Sleep(2100 * time.Millisecond)
				case "replayed":
					reply.Sequence--
				case "wrong-key":
					reply.Hash = Hash([]byte("another request"))
				}
				_ = json.NewEncoder(conn).Encode(reply)
			})
			witness := &Client{Address: address, TLS: cfg, Session: "123456789abcdef0123456789abcdef0",
				RelayPeer: relayPeer, SinkPeer: sinkPeer}
			if err := witness.Record(recordForTest()); err == nil {
				t.Fatal("invalid ACK admitted the request")
			}
			if !witness.poisoned || witness.Seal() == nil {
				t.Fatal("ambiguous sequence was silently sealed")
			}
		})
	}
}
