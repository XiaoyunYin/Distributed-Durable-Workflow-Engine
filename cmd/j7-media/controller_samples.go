package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"durable-agent-execution-engine/internal/restoreledger"
)

const sampleCadence = 2 * time.Second

type controllerSampler struct {
	window   resourceWindow
	now      restoreledger.Clock
	collect  func(context.Context) (resourceSample, error)
	lock     sync.Mutex
	firstErr error
	halt     context.CancelFunc
	done     chan struct{}
}

func (s *controllerSampler) take(ctx context.Context, phase string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.firstErr != nil {
		return s.firstErr
	}
	begin, err := s.now()
	if err != nil {
		return err
	}
	scope, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	sample, err := s.collect(scope)
	if err != nil {
		return err
	}
	sample.AtNS, sample.Phase = begin, phase
	w := &s.window
	if err := checkResourceSample(*w, sample); err != nil {
		return err
	}
	w.Samples = append(w.Samples, sample)
	return nil
}

func (s *controllerSampler) start(ctx context.Context, stopDrill context.CancelCauseFunc) error {
	if err := s.take(ctx, "start"); err != nil {
		return err
	}
	run, cancel := context.WithCancel(ctx)
	s.halt, s.done = cancel, make(chan struct{})
	go func() {
		defer close(s.done)
		t := time.NewTicker(sampleCadence)
		defer t.Stop()
		for {
			select {
			case <-run.Done():
				return
			case <-t.C:
				if err := s.take(run, ""); err != nil && run.Err() == nil {
					s.lock.Lock()
					s.firstErr = err
					s.lock.Unlock()
					stopDrill(fmt.Errorf("J7_CAPACITY: %w", err))
					return
				}
			}
		}
	}()
	return nil
}

func (s *controllerSampler) stop() {
	if s.halt != nil {
		s.halt()
		<-s.done
		s.halt = nil
	}
}

func (s *controllerSampler) finish(ctx context.Context, start, end int64) (resourceWindow, error) {
	s.stop()
	if err := s.take(ctx, "export"); err != nil {
		return s.window, err
	}
	return s.window, checkResources(s.window, start, end)
}

func hostResourceSample(ctx context.Context, h fixtureHost, cgroups []string, engineSessions, sinkSessions int) (resourceSample, error) {
	r := resourceSample{EngineSessions: engineSessions, SinkSessions: sinkSessions}
	cap, err := h.hostCapacity(ctx, true)
	if err != nil {
		return r, err
	}
	r.FreeKiB = min(cap.Filesystems["workspace"], cap.Filesystems["docker"])
	r.AvailableKiB, r.SwapKiB = cap.AvailableKiB, cap.SwapKiB
	for _, path := range cgroups {
		if err := addCgroupStats(path, &r); err != nil {
			return r, err
		}
	}
	return r, nil
}

func addCgroupStats(dir string, r *resourceSample) error {
	read := func(name string) (string, error) {
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(raw)), nil
	}
	cpu, err := read("cpu.stat")
	if err != nil {
		return err
	}
	mem, err := read("memory.current")
	if err != nil {
		return err
	}
	events, err := read("memory.events")
	if err != nil {
		return err
	}
	disk, err := read("io.stat")
	if err != nil {
		return err
	}
	counters, err := parseFlatCounters(cpu)
	if err != nil {
		return err
	}
	for _, key := range []string{"usage_usec", "throttled_usec"} {
		if _, ok := counters[key]; !ok {
			return errors.New("missing CPU cgroup counter")
		}
	}
	r.CPUUsageNS += counters["usage_usec"] * 1000
	r.CPUThrottledNS += counters["throttled_usec"] * 1000
	working, err := strconv.ParseInt(mem, 10, 64)
	if err != nil || working < 0 {
		return errors.New("invalid cgroup memory fixture counter")
	}
	stats, err := read("memory.stat")
	if err != nil {
		return err
	}
	memory, err := parseFlatCounters(stats)
	if err != nil {
		return err
	}
	inactive, ok := memory["inactive_file"]
	if !ok {
		return errors.New("missing cgroup inactive file memory")
	}
	r.WorkingSetBytes += max(0, working-inactive)
	counters, err = parseFlatCounters(events)
	if err != nil {
		return err
	}
	kills, present := counters["oom_kill"]
	if !present {
		return errors.New("missing cgroup OOM kill counter")
	}
	r.OOM += int(kills)
	for _, line := range strings.Split(disk, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		for _, f := range fields[1:] {
			key, value, _ := strings.Cut(f, "=")
			if key != "rbytes" && key != "wbytes" {
				continue
			}
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil || n < 0 {
				return errors.New("invalid cgroup I/O counter")
			}
			if key == "rbytes" {
				r.IOReadBytes += n
			} else {
				r.IOWrittenBytes += n
			}
		}
	}
	return nil
}

func parseFlatCounters(s string) (map[string]int64, error) {
	values := map[string]int64{}
	for _, line := range strings.Split(s, "\n") {
		pair := strings.Fields(line)
		if len(pair) != 2 {
			return nil, errors.New("invalid cgroup counter line")
		}
		n, err := strconv.ParseInt(pair[1], 10, 64)
		if err != nil || n < 0 {
			return nil, errors.New("invalid cgroup counter value")
		}
		if _, present := values[pair[0]]; present {
			return nil, errors.New("duplicate cgroup counter")
		}
		values[pair[0]] = n
	}
	return values, nil
}

func findControllerCgroups(root string, ids map[string]string) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	var out []string
	found := map[string]bool{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		for id := range ids {
			if d.Name() == id || d.Name() == "docker-"+id+".scope" || d.Name() == "cri-containerd-"+id+".scope" {
				if found[id] {
					return errors.New("container appeared in multiple cgroups")
				}
				found[id] = true
				out = append(out, path)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(found) != len(ids) {
		return nil, errors.New("missing owned container cgroup accounting")
	}
	return out, nil
}
