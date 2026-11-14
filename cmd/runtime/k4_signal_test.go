package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"
)

type blockingSignalHandler struct {
	sync.Mutex
	records []map[string]any
	entered chan struct{}
}

func (h *blockingSignalHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *blockingSignalHandler) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *blockingSignalHandler) WithGroup(string) slog.Handler            { return h }
func (h *blockingSignalHandler) Handle(_ context.Context, r slog.Record) error {
	if h.entered != nil {
		close(h.entered)
	}
	time.Sleep(100 * time.Millisecond)
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "data" {
			var data map[string]any
			_ = json.Unmarshal([]byte(a.Value.String()), &data)
			h.Lock()
			h.records = append(h.records, data)
			h.Unlock()
		}
		return true
	})
	return nil
}

func TestK4StopWaitsForSignalRecord(t *testing.T) {
	t.Setenv("DUR053_K4_CAMPAIGN", "1")
	h := &blockingSignalHandler{entered: make(chan struct{})}
	old := slog.Default()
	slog.SetDefault(slog.New(h))
	defer slog.SetDefault(old)
	var channel chan<- os.Signal
	ctx, stop := k4SignalContext(context.Background(), func(ch chan<- os.Signal, _ ...os.Signal) {
		channel = ch
	}, func(chan<- os.Signal) {})
	defer stop()
	channel <- syscall.SIGTERM
	select {
	case <-h.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("signal handler did not start")
	}
	stopped := make(chan struct{})
	go func() { stop(); close(stopped) }()
	<-ctx.Done()
	h.Lock()
	present := len(h.records) == 1
	h.Unlock()
	if !present {
		t.Fatal("observer stop overtook signal record")
	}
	<-stopped
}

func TestK4SignalLoggedBeforeCancellation(t *testing.T) {
	t.Setenv("DUR053_K4_CAMPAIGN", "1")
	t.Setenv("POD_UID", "pod-uid")
	t.Setenv("CONTAINER_ID", "container-id")
	h := &blockingSignalHandler{}
	old := slog.Default()
	slog.SetDefault(slog.New(h))
	defer slog.SetDefault(old)
	var channels []chan<- os.Signal
	ctx, stop := k4SignalContext(context.Background(), func(ch chan<- os.Signal, signals ...os.Signal) {
		channels = append(channels, ch)
	}, func(chan<- os.Signal) {})
	defer stop()
	before := time.Now()
	for _, channel := range channels {
		channel <- syscall.SIGTERM
	}
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("signal did not cancel lifecycle")
	}
	h.Lock()
	defer h.Unlock()
	if len(h.records) != 1 {
		t.Fatal("k4_signal missing at lifecycle cancellation")
	}
	r := h.records[0]
	if r["event"] != "k4_signal" || r["signal"] != float64(15) || r["pod_uid"] != "pod-uid" || r["container_id"] != "container-id" || r["session"] == "" {
		t.Fatalf("incorrect signal record: %v", r)
	}
	at, err := time.Parse(time.RFC3339Nano, r["timestamp"].(string))
	if err != nil || at.Before(before) || time.Since(at) < 100*time.Millisecond {
		t.Fatalf("timestamp is not signal receipt time: %v", r)
	}
}

func TestK4NonCampaignSignalContext(t *testing.T) {
	t.Setenv("DUR053_K4_CAMPAIGN", "")
	h := &blockingSignalHandler{}
	old := slog.Default()
	slog.SetDefault(slog.New(h))
	defer slog.SetDefault(old)
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx, stop := k4SignalContext(parent, func(chan<- os.Signal, ...os.Signal) {
		t.Fatal("non-campaign observer subscribed")
	}, func(chan<- os.Signal) { t.Fatal("non-campaign observer stopped") })
	defer stop()
	select {
	case <-ctx.Done():
		t.Fatal("context cancelled before parent")
	default:
	}
	cancel()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("NotifyContext did not follow parent")
	}
	stop()
	h.Lock()
	defer h.Unlock()
	if len(h.records) != 0 {
		t.Fatal("non-campaign observer logged")
	}
}
