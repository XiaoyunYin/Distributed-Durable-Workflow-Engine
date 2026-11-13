package k4observe

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestK4BoundaryOptInAndAcknowledgments(t *testing.T) {
	old := slog.Default()
	defer slog.SetDefault(old)
	for _, flag := range []string{"0", "1"} {
		t.Run(flag, func(t *testing.T) {
			t.Setenv("DUR053_K4_CAMPAIGN", flag)
			t.Setenv("DUR053_K4_HOLD_FILE", "")
			var out bytes.Buffer
			slog.SetDefault(slog.New(slog.NewJSONHandler(&out, nil)))
			done := Begin(context.Background(), "relay", "publication", "session", []string{"w"})
			done()
			if flag == "0" && out.Len() != 0 {
				t.Fatal("K4 observation escaped opt-in")
			}
			if flag == "1" && (!strings.Contains(out.String(), "publication") || strings.Count(out.String(), "acknowledgment") != 2) {
				t.Fatal("K4 entry or exit acknowledgment missing")
			}
		})
	}
}
func TestK4BoundaryHoldEndsOnReleaseOrCancellation(t *testing.T) {
	previous := slog.Default()
	defer slog.SetDefault(previous)
	entered := make(chan struct{})
	slog.SetDefault(slog.New(slog.NewJSONHandler(&boundaryEntryWriter{entered: entered}, nil)))
	t.Setenv("DUR053_K4_CAMPAIGN", "1")
	path := filepath.Join(t.TempDir(), "hold")
	t.Setenv("DUR053_K4_HOLD_FILE", path)
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	finished := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		done := Begin(context.Background(), "api", "request", "session", nil)
		close(finished)
		done()
		close(exited)
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("K4 boundary entry was not acknowledged")
	}
	runtime.Gosched()
	select {
	case <-finished:
		t.Fatal("K4 hold did not await release")
	case <-time.After(100 * time.Millisecond):
		runtime.Gosched()
		select {
		case <-finished:
			t.Fatal("K4 hold did not await release")
		default:
		}
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("K4 hold ignored release")
	}
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("K4 exit acknowledgment did not finish")
	}
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	began := time.Now()
	done := Begin(ctx, "api", "request", "session", nil)
	done()
	if time.Since(began) > time.Second {
		t.Fatal("K4 hold ignored context cancellation")
	}
}

type boundaryEntryWriter struct {
	once    sync.Once
	entered chan struct{}
}

func (w *boundaryEntryWriter) Write(b []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	return len(b), nil
}
