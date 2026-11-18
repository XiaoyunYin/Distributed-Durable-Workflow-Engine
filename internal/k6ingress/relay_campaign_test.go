//go:build dur053_k6

package k6ingress

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/k6hook"
)

type campaignHook func(string, k6hook.Fields) (k6hook.Hold, error)

func (f campaignHook) Hit(boundary string, fields k6hook.Fields) (k6hook.Hold, error) {
	return f(boundary, fields)
}

type campaignHold struct {
	action string
	until  time.Time
}

func (h campaignHold) Action() string      { return h.action }
func (h campaignHold) Deadline() time.Time { return h.until }
func (h campaignHold) Finish() error       { return nil }

func TestTaggedRelayActionsAndControl(t *testing.T) {
	for _, name := range []string{"duplicate", "drop", "unarmed", "ack-rejected"} {
		t.Run(name, func(t *testing.T) {
			records := make(chan Record, 20)
			hook := campaignHook(func(boundary string, fields k6hook.Fields) (k6hook.Hold, error) {
				if fields.Key != "effect" || fields.Invocation != "invocation" || len(fields.Binding) != 64 {
					t.Error("relay hook omitted exchange identities")
				}
				if name == "duplicate" && boundary == "relay-pre-forward" || name == "drop" && boundary == "relay-post-commit" {
					if name == "drop" && (fields.App != "app" || len(fields.Reply) != 64) {
						t.Error("drop was not tied to the committed reply")
					}
					return campaignHold{name, time.Now().Add(3 * time.Second)}, nil
				}
				if name == "ack-rejected" && boundary == "relay-post-commit" {
					return nil, errors.New("K6_HOOK_ACK")
				}
				return nil, nil
			})
			address, cfg, arrivals := fixtureRelay(t, func(record Record) error { records <- record; return nil }, hook)
			conn, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", address, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			body := `{"logical_key":"effect","message_id":"invocation","request_id":"request","scope_hash":"scope"}`
			wire := fmt.Appendf(nil, "POST /v1/apply HTTP/1.1\r\nHost: worker\r\nContent-Length: %d\r\nX-Dur053-Invocation: invocation\r\nConnection: close\r\n\r\n%s", len(body), body)
			if _, err := conn.Write(wire); err != nil {
				t.Fatal(err)
			}
			_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			if name == "drop" {
				_ = conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
				var b [1]byte
				n, err := conn.Read(b[:])
				var timeout net.Error
				if n != 0 || !errors.As(err, &timeout) || !timeout.Timeout() {
					t.Fatal("worker received bytes or disconnect before timeout")
				}
				_ = conn.Close()
			} else {
				resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "POST"})
				if err != nil || resp.StatusCode != 200 {
					t.Fatalf("forwarded reply missing: %v", err)
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
			seen := map[string]int{}
			var response, drop Record
			timer := time.NewTimer(3 * time.Second)
			defer timer.Stop()
			for seen["close"] < 1 || name == "duplicate" && seen["close"] < 2 {
				select {
				case record := <-records:
					seen[record.Event]++
					if record.Event == "sink_response" {
						response = record
					}
					if record.Event == "fault_drop" {
						drop = record
					}
				case <-timer.C:
					t.Fatal("relay did not close journal exchange")
				}
			}
			if name == "duplicate" {
				if len(arrivals) != 2 || seen["sink_send"] != 2 || seen["extra_reply"] != 1 || seen["worker_delivery"] != 1 {
					t.Fatal("duplicate did not record both deliveries and one worker reply")
				}
				first, second := <-arrivals, <-arrivals
				if first.connection == second.connection || !bytes.Equal(first.request, second.request) || !bytes.Equal(first.request, wire) {
					t.Fatal("duplicate did not send the exact buffered request on separate connections")
				}
			} else if name == "drop" {
				if seen["worker_delivery"] != 0 || seen["worker_drop"] != 1 || seen["caller_closed"] != 1 ||
					drop.WireHash != response.WireHash || drop.WireLength == 0 {
					t.Fatal("withheld reply was not joined to its timeout and original bytes")
				}
			} else if len(arrivals) != 1 || seen["worker_delivery"] != 1 || seen["fault_drop"] != 0 {
				t.Fatal("unarmed or unacknowledged request did not stay forward-only")
			}
		})
	}
}
