package main

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"strings"
	"sync"
	"time"
)

func availableCPUCores() int { return runtime.NumCPU() }

const (
	generatorCPUSamplingInterval = time.Second
	generatorCPUThresholdPercent = 80.0
	allowedCPUOverageFraction    = 0.01
	maximumCPUSampleGap          = 2 * time.Second
)

type cpuIntervalSample struct {
	StartElapsedSeconds float64 `json:"start_elapsed_seconds"`
	EndElapsedSeconds   float64 `json:"end_elapsed_seconds"`
	CPUPercent          float64 `json:"cpu_percent_normalized_per_core"`
}

type cpuValidationSummary struct {
	Status               string              `json:"status"`
	SamplingIntervalMS   int64               `json:"sampling_interval_ms"`
	ThresholdPercent     float64             `json:"threshold_percent"`
	AllowedOverageRatio  float64             `json:"allowed_overage_ratio"`
	WindowSeconds        float64             `json:"window_seconds"`
	OverThresholdSeconds float64             `json:"over_threshold_seconds"`
	OverThresholdRatio   float64             `json:"over_threshold_ratio"`
	MaximumCPUPercent    float64             `json:"maximum_cpu_percent_normalized_per_core"`
	NormalizationCores   int                 `json:"normalization_cores"`
	MaximumSampleGapMS   float64             `json:"maximum_sample_gap_ms"`
	Samples              []cpuIntervalSample `json:"samples"`
	InvalidReason        string              `json:"invalid_reason,omitempty"`
}

type processCPUSampler struct {
	interval time.Duration
	started  time.Time
	stop     chan struct{}
	done     chan struct{}
	mu       sync.Mutex
	samples  []cpuIntervalSample
	err      error
}

func startProcessCPUSampler(interval time.Duration) (*processCPUSampler, error) {
	if interval <= 0 {
		return nil, errors.New("CPU sample interval must be positive")
	}
	baseline, err := readProcessCPUTime()
	if err != nil {
		return nil, err
	}
	sampler := &processCPUSampler{interval: interval, started: time.Now(), stop: make(chan struct{}), done: make(chan struct{})}
	go sampler.sampleLoop(baseline)
	return sampler, nil
}

func (sampler *processCPUSampler) sampleLoop(previousCPU time.Duration) {
	defer close(sampler.done)
	previousWall := sampler.started
	ticker := time.NewTicker(sampler.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if !sampler.capture(&previousWall, &previousCPU) {
				return
			}
		case <-sampler.stop:
			_ = sampler.capture(&previousWall, &previousCPU)
			return
		}
	}
}

func (sampler *processCPUSampler) capture(previousWall *time.Time, previousCPU *time.Duration) bool {
	now := time.Now()
	cpuNow, err := readProcessCPUTime()
	if err != nil {
		sampler.mu.Lock()
		sampler.err = fmt.Errorf("read process CPU time: %w", err)
		sampler.mu.Unlock()
		return false
	}
	wallDelta := now.Sub(*previousWall)
	cpuDelta := cpuNow - *previousCPU
	cores := availableCPUCores()
	if wallDelta <= 0 || cpuDelta < 0 || cores <= 0 {
		sampler.mu.Lock()
		sampler.err = fmt.Errorf("invalid process CPU sample (wall=%s cpu=%s cores=%d)", wallDelta, cpuDelta, cores)
		sampler.mu.Unlock()
		return false
	}
	sample := cpuIntervalSample{
		StartElapsedSeconds: previousWall.Sub(sampler.started).Seconds(),
		EndElapsedSeconds:   now.Sub(sampler.started).Seconds(),
		CPUPercent:          cpuDelta.Seconds() / wallDelta.Seconds() / float64(cores) * 100,
	}
	sampler.mu.Lock()
	sampler.samples = append(sampler.samples, sample)
	sampler.mu.Unlock()
	*previousWall = now
	*previousCPU = cpuNow
	return true
}

func (sampler *processCPUSampler) Stop() ([]cpuIntervalSample, error) {
	close(sampler.stop)
	<-sampler.done
	sampler.mu.Lock()
	defer sampler.mu.Unlock()
	return append([]cpuIntervalSample(nil), sampler.samples...), sampler.err
}

func applyCPUValidation(summary *runSummary, samples []cpuIntervalSample, window time.Duration, cores int, sampleErr error) {
	baseStatus := summary.Status
	validation := cpuValidationSummary{
		Status: "PASS", SamplingIntervalMS: generatorCPUSamplingInterval.Milliseconds(), ThresholdPercent: generatorCPUThresholdPercent,
		AllowedOverageRatio: allowedCPUOverageFraction, WindowSeconds: window.Seconds(),
		NormalizationCores: cores, Samples: append([]cpuIntervalSample(nil), samples...),
	}
	var reasons []string
	if sampleErr != nil {
		reasons = append(reasons, "process CPU sampling failed: "+sampleErr.Error())
	}
	if window <= 0 || cores <= 0 || len(samples) == 0 {
		reasons = append(reasons, "CPU validation has no valid measurement window or samples")
	}
	coveredSeconds := 0.0
	for index, sample := range samples {
		interval := sample.EndElapsedSeconds - sample.StartElapsedSeconds
		if interval <= 0 || sample.CPUPercent < 0 || math.IsNaN(sample.CPUPercent) || math.IsInf(sample.CPUPercent, 0) {
			reasons = append(reasons, "CPU series contains an invalid sample interval")
			continue
		}
		coveredSeconds += interval
		if interval*1000 > validation.MaximumSampleGapMS {
			validation.MaximumSampleGapMS = interval * 1000
		}
		if sample.CPUPercent > validation.MaximumCPUPercent {
			validation.MaximumCPUPercent = sample.CPUPercent
		}
		if sample.CPUPercent > generatorCPUThresholdPercent {
			validation.OverThresholdSeconds += interval
		}
		if index > 0 && sample.StartElapsedSeconds-samples[index-1].EndElapsedSeconds > 0.001 {
			reasons = append(reasons, "CPU series contains an uncovered sampling gap")
		}
	}
	if validation.MaximumSampleGapMS > float64(maximumCPUSampleGap.Milliseconds()) {
		reasons = append(reasons, "CPU sampling interval exceeded the 2-second validity bound")
	}
	if window > 0 {
		validation.OverThresholdRatio = validation.OverThresholdSeconds / window.Seconds()
		if validation.OverThresholdRatio > allowedCPUOverageFraction {
			reasons = append(reasons, "generator CPU exceeded 80% for more than 1% of the measurement window")
		}
		if coveredSeconds < window.Seconds()*0.99 {
			reasons = append(reasons, "CPU samples cover less than 99% of the measurement window")
		}
	}
	if len(reasons) > 0 {
		validation.Status = "FAIL"
		validation.InvalidReason = strings.Join(reasons, "; ")
	}
	summary.GeneratorCPU = validation
	if validation.Status == "FAIL" {
		appendSummaryFailure(summary, validation.InvalidReason)
		return
	}
	switch baseStatus {
	case "PENDING_CPU_VALIDATION":
		if summary.InvalidReason == "" {
			summary.Status = "PASS"
		}
	case "FAIL":
		// Preserve an existing open-loop validity failure.
	default:
		appendSummaryFailure(summary, "load run has no pending base-validity summary")
	}
}

func appendSummaryFailure(summary *runSummary, reason string) {
	if summary.InvalidReason == "" {
		summary.InvalidReason = reason
	} else {
		summary.InvalidReason += "; " + reason
	}
	summary.Status = "FAIL"
}
