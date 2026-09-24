// Command dur050-sink is a loopback-only HTTP acknowledgement target used to
// calibrate the DUR-050 load generator without touching PostgreSQL or Kafka.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const maxRequestBytes = 2 << 20

type submission struct {
	WorkflowID string `json:"workflow_id"`
}

func main() {
	address := flag.String("addr", "127.0.0.1:8787", "loopback listen address")
	flag.Parse()
	if err := serve(*address); err != nil {
		log.Fatal(err)
	}
}

func serve(address string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("parse sink address: %w", err)
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	if ip == nil || !ip.IsLoopback() {
		return errors.New("DUR-050 calibration sink must bind to a loopback address")
	}
	server := &http.Server{Addr: address, Handler: newHandler(), ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, MaxHeaderBytes: 16 << 10}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("listen for loopback calibration sink: %w", err)
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stop)
	go func() {
		<-stop
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()
	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve loopback calibration sink: %w", err)
	}
	return nil
}

func newHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /v1/workflows", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)
		var request submission
		decoder := json.NewDecoder(r.Body)
		if err := decoder.Decode(&request); err != nil || request.WorkflowID == "" {
			http.Error(w, "invalid sink request", http.StatusBadRequest)
			return
		}
		var trailing any
		if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
			http.Error(w, "invalid trailing request body", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"created": true,
			"workflow": map[string]string{"workflow_id": request.WorkflowID}})
	})
	return mux
}
