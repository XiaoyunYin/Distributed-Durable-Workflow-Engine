package main

import (
	"errors"
	"fmt"
	"slices"
	"time"
)

type resourceWindow struct {
	Schema         string           `json:"schema"`
	CPUs           int              `json:"logical_cpu_count"`
	InstalledKiB   int64            `json:"installed_memory_kib"`
	StartupFreeKiB int64            `json:"before_drill_free_kib"`
	EngineLimit    int              `json:"engine_connection_limit"`
	SinkLimit      int              `json:"sink_connection_limit"`
	RoleLimits     map[string]quota `json:"role_limits"`
	Samples        []resourceSample `json:"samples"`
}

type quota struct {
	MilliCPU int   `json:"millicpu"`
	Bytes    int64 `json:"memory_bytes"`
}

type resourceSample struct {
	AtNS            int64  `json:"monotonic_ns"`
	Phase           string `json:"phase,omitempty"`
	FreeKiB         int64  `json:"free_kib"`
	AvailableKiB    int64  `json:"available_memory_kib"`
	SwapKiB         int64  `json:"swap_used_kib"`
	EngineSessions  int    `json:"engine_sessions"`
	SinkSessions    int    `json:"sink_sessions"`
	UnexpectedKills int    `json:"unexpected_terminations"`
	OOM             int    `json:"oom_events"`
	CPUThrottledNS  int64  `json:"cpu_throttled_ns"`
	CPUUsageNS      int64  `json:"cpu_usage_ns"`
	WorkingSetBytes int64  `json:"working_set_bytes"`
	IOReadBytes     int64  `json:"io_read_bytes"`
	IOWrittenBytes  int64  `json:"io_written_bytes"`
}

var j7RoleLimits = map[string]quota{
	"engine-db": {1000, 1 << 30}, "sink-db": {500, 512 << 20}, "kafka": {1000, 1536 << 20},
	"api": {500, 256 << 20}, "scheduler": {500, 256 << 20}, "relay": {500, 256 << 20},
	"worker-1": {500, 512 << 20}, "worker-2": {500, 512 << 20}, "sink": {500, 512 << 20},
}

func checkResources(r resourceWindow, drillStart, final int64) error {
	if r.Schema != "j7.resource-samples.v1" || r.CPUs < 4 || r.InstalledKiB < 12<<20 ||
		r.StartupFreeKiB < 8<<20 || r.EngineLimit <= 0 || r.EngineLimit > 100 || r.SinkLimit <= 0 ||
		r.SinkLimit > 16 || len(r.Samples) < 2 {
		return errors.New("J7_CAPACITY: runner resources or connection ceilings could not be proved")
	}
	want := j7RoleLimits
	for role, q := range want {
		if r.RoleLimits[role] != q {
			return fmt.Errorf("J7_CAPACITY: effective limits differ for %s", role)
		}
	}
	if len(r.RoleLimits) != len(want) {
		return errors.New("J7_CAPACITY: unaccounted resource roles")
	}
	seen := []string{}
	first, last := r.Samples[0], r.Samples[len(r.Samples)-1]
	if first.AtNS > drillStart || last.AtNS < final {
		return errors.New("J7_CAPACITY: sampling does not bracket the complete drill")
	}
	for i, s := range r.Samples {
		prefix := r
		prefix.Samples = r.Samples[:i]
		if err := checkResourceSample(prefix, s); err != nil {
			return err
		}
		if s.Phase != "" {
			seen = append(seen, s.Phase)
		}
	}
	for _, phase := range []string{"backup", "outage", "replay", "export"} {
		if !slices.Contains(seen, phase) {
			return fmt.Errorf("J7_CAPACITY: resource observation missing at %s", phase)
		}
	}
	return nil
}

func checkResourceSample(r resourceWindow, s resourceSample) error {
	if s.AtNS <= 0 || s.FreeKiB < 4<<20 || s.AvailableKiB < 2<<20 || s.SwapKiB < 0 ||
		s.EngineSessions < 0 || 4*s.EngineSessions > 3*r.EngineLimit || s.SinkSessions < 0 ||
		4*s.SinkSessions > 3*r.SinkLimit || s.UnexpectedKills != 0 || s.OOM != 0 ||
		min(s.CPUThrottledNS, s.CPUUsageNS, s.WorkingSetBytes, s.IOReadBytes, s.IOWrittenBytes) < 0 {
		return errors.New("J7_CAPACITY: pressure, unexpected termination or less than 25 percent connection reserve")
	}
	if n := len(r.Samples); n > 0 {
		last := r.Samples[n-1]
		if s.AtNS < last.AtNS || s.AtNS-last.AtNS > int64(5*time.Second) || s.SwapKiB > r.Samples[0].SwapKiB {
			return errors.New("J7_CAPACITY: sample gap or swap growth")
		}
	}
	return nil
}
