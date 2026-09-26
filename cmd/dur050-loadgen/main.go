// Command dur050-loadgen emits a deterministic open-loop arrival stream for
// the deployed DUR-050 HTTP API. It does not wait for workflow completion.
package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"durable-agent-execution-engine/internal/dur050"
)

const (
	requestConcurrency = 64
	maxRequestAttempts = 3
	maxResponseBytes   = 1 << 20
)

var runIDPattern = regexp.MustCompile(`^[A-Za-z0-9-]{1,48}$`)

var csvColumns = []string{
	"record_type", "sequence", "family", "workflow_id", "submission_key",
	"scheduled_at_utc", "scheduled_at_monotonic_ns", "request_started_at_utc",
	"request_started_at_monotonic_ns", "request_finished_at_utc",
	"request_finished_at_monotonic_ns", "schedule_delay_ms", "attempt_count",
	"http_status", "outcome", "created", "error",
}

type campaignConfig struct {
	APIURL    string         `json:"api_url"`
	Namespace string         `json:"namespace"`
	RunID     string         `json:"run_id"`
	Seed      int64          `json:"seed"`
	Families  []familyConfig `json:"families"`
}

type familyConfig struct {
	Name              string          `json:"name"`
	DefinitionID      string          `json:"definition_id"`
	DefinitionVersion int             `json:"definition_version"`
	InitialNodeID     string          `json:"initial_node_id"`
	Payload           json.RawMessage `json:"payload"`
	InitialInput      json.RawMessage `json:"initial_input"`
}

type submissionRequest struct {
	WorkflowID        string          `json:"workflow_id"`
	Namespace         string          `json:"namespace"`
	SubmissionKey     string          `json:"submission_key"`
	Payload           json.RawMessage `json:"payload"`
	DefinitionID      string          `json:"definition_id"`
	DefinitionVersion int             `json:"definition_version"`
	InitialNodeID     string          `json:"initial_node_id"`
	InitialInput      json.RawMessage `json:"initial_input"`
	ActorID           string          `json:"actor_id"`
}

type workflowResponse struct {
	Workflow struct {
		WorkflowID string `json:"workflow_id"`
	} `json:"workflow"`
	Created bool `json:"created"`
}

type apiError struct {
	Error struct {
		Code string `json:"code"`
	} `json:"error"`
}

type requestRecord struct {
	Sequence      int
	Family        string
	WorkflowID    string
	SubmissionKey string
	ScheduledUTC  time.Time
	ScheduledMono int64
	StartedUTC    time.Time
	StartedMono   int64
	FinishedUTC   time.Time
	FinishedMono  int64
	ScheduleDelay time.Duration
	Attempts      int
	HTTPStatus    int
	Outcome       string
	Created       bool
	Error         string
}

type runSummary struct {
	Schema                string               `json:"schema"`
	Status                string               `json:"status"`
	ClockModel            string               `json:"clock_model"`
	Namespace             string               `json:"namespace"`
	RunID                 string               `json:"run_id"`
	Seed                  int64                `json:"seed"`
	OfferedRatePerSecond  float64              `json:"offered_rate_per_second"`
	Scheduled             int                  `json:"scheduled"`
	Submitted             int                  `json:"submitted"`
	Accepted              int                  `json:"accepted"`
	Rejected              int                  `json:"rejected"`
	Ambiguous             int                  `json:"ambiguous"`
	GeneratorCapacityMiss int                  `json:"generator_capacity_missed"`
	P99ScheduleDelayMS    float64              `json:"p99_schedule_to_submit_ms"`
	MaxScheduleDelayMS    float64              `json:"max_schedule_to_submit_ms"`
	MaxInFlight           int                  `json:"max_in_flight"`
	RetryLimit            int                  `json:"retry_limit"`
	GeneratorCPU          cpuValidationSummary `json:"generator_cpu"`
	InvalidReason         string               `json:"invalid_reason,omitempty"`
}

func main() {
	configPath := flag.String("config", "", "campaign JSON configuration")
	rate := flag.Float64("rate", 0, "open-loop workflow arrival rate per second")
	count := flag.Int("count", 0, "even number of arrivals; mutually exclusive with -duration")
	duration := flag.Duration("duration", 0, "fixed open-loop arrival window; mutually exclusive with -count")
	outputPath := flag.String("output", "", "new immutable CSV output path")
	workflowIDsPath := flag.String("workflow-ids-output", "", "optional new newline-delimited file of accepted workflow IDs")
	runID := flag.String("run-id", "", "optional per-run ID override from the campaign config")
	singleFamily := flag.String("single-family", "", "pilot-only single-workflow family selector (seq-8 or fanout-8); requires -count 1")
	requestTimeout := flag.Duration("request-timeout", 10*time.Second, "timeout for each HTTP submission attempt")
	flag.Parse()
	if !dur050.HasSharedMonotonicClock() {
		log.Fatal("DUR-050 load generator requires Linux shared CLOCK_MONOTONIC")
	}
	if err := run(*configPath, *rate, *count, *duration, *outputPath, *workflowIDsPath, *runID, *singleFamily, *requestTimeout); err != nil {
		log.Fatal(err)
	}
}

func run(configPath string, rate float64, count int, duration time.Duration, outputPath, workflowIDsPath, runID, singleFamily string, requestTimeout time.Duration) error {
	if !dur050.HasSharedMonotonicClock() {
		return errors.New("DUR-050 load generator requires Linux shared CLOCK_MONOTONIC")
	}
	if configPath == "" || outputPath == "" {
		return errors.New("-config and -output are required")
	}
	if rate <= 0 || requestTimeout <= 0 {
		return errors.New("-rate and -request-timeout must be positive")
	}
	if (count > 0) == (duration > 0) {
		return errors.New("specify exactly one of -count or -duration")
	}
	if count < 0 || duration < 0 {
		return errors.New("-count and -duration cannot be negative")
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("read campaign config: %w", err)
	}
	var config campaignConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return fmt.Errorf("decode campaign config: %w", err)
	}
	if runID != "" {
		config.RunID = runID
	}
	if err := validateConfig(config); err != nil {
		return err
	}
	if duration > 0 {
		count = int(math.Floor(duration.Seconds()*rate + 1e-9))
	}
	if singleFamily != "" {
		if singleFamily != "seq-8" && singleFamily != "fanout-8" {
			return errors.New("-single-family must be seq-8 or fanout-8")
		}
		if duration > 0 || count != 1 {
			return errors.New("-single-family is pilot-only and requires -count 1, not -duration")
		}
	} else if count < 2 || (duration == 0 && count%2 != 0) {
		return errors.New("-count requires an even integer of at least 2 for the balanced two-family workload; -duration may produce one extra family arrival")
	}
	if err := ensureOutputPathsAvailable(outputPath, workflowIDsPath); err != nil {
		return err
	}
	ctx := context.Background()
	client := newHTTPClient()
	defer client.CloseIdleConnections()
	endpoint := strings.TrimRight(config.APIURL, "/") + "/v1/workflows"
	cpuSampler, err := startProcessCPUSampler(generatorCPUSamplingInterval)
	if err != nil {
		return fmt.Errorf("start required generator CPU sampler: %w", err)
	}
	records, summary, err := runCampaignWithFamily(ctx, client, endpoint, config, rate, count, requestTimeout, singleFamily)
	cpuSamples, cpuSampleErr := cpuSampler.Stop()
	applyCPUValidation(&summary, cpuSamples, cpuSamplesWindow(cpuSamples), runtime.NumCPU(), cpuSampleErr)
	if writeErr := writeArtifacts(outputPath, workflowIDsPath, records, summary); writeErr != nil {
		if err != nil {
			return errors.Join(err, writeErr)
		}
		return writeErr
	}
	encoded, _ := json.Marshal(summary)
	_, _ = fmt.Fprintln(os.Stdout, string(encoded))
	if err != nil {
		return err
	}
	if summary.InvalidReason != "" {
		return fmt.Errorf("DUR-050 load run invalid: %s", summary.InvalidReason)
	}
	return nil
}

func validateConfig(config campaignConfig) error {
	parsed, err := url.ParseRequestURI(config.APIURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return errors.New("campaign api_url must be an absolute http(s) URL")
	}
	if !strings.HasPrefix(config.Namespace, "dur050-") || config.RunID == "" || !runIDPattern.MatchString(config.RunID) {
		return errors.New("campaign namespace must start with dur050- and run_id must contain only letters, digits, or hyphens")
	}
	if len(config.Families) != 2 {
		return errors.New("campaign config must define exactly two workload families")
	}
	seen := map[string]bool{}
	for _, family := range config.Families {
		if family.Name != "seq-8" && family.Name != "fanout-8" {
			return fmt.Errorf("unsupported workload family %q", family.Name)
		}
		if seen[family.Name] || family.DefinitionID == "" || family.DefinitionVersion <= 0 || family.InitialNodeID == "" {
			return fmt.Errorf("family %q has duplicate or incomplete definition metadata", family.Name)
		}
		seen[family.Name] = true
		if len(family.Payload) == 0 {
			family.Payload = json.RawMessage(`{}`)
		}
		if len(family.InitialInput) == 0 {
			family.InitialInput = json.RawMessage(`{}`)
		}
		if !json.Valid(family.Payload) || !json.Valid(family.InitialInput) {
			return fmt.Errorf("family %q payload and initial_input must be valid JSON", family.Name)
		}
	}
	if !seen["seq-8"] || !seen["fanout-8"] {
		return errors.New("campaign config must contain seq-8 and fanout-8")
	}
	return nil
}

func newHTTPClient() *http.Client {
	transport := &http.Transport{
		MaxConnsPerHost:     requestConcurrency,
		MaxIdleConns:        requestConcurrency,
		MaxIdleConnsPerHost: requestConcurrency,
		IdleConnTimeout:     90 * time.Second,
	}
	return &http.Client{Transport: transport, CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}}
}

func runCampaign(ctx context.Context, client *http.Client, endpoint string, config campaignConfig,
	rate float64, count int, requestTimeout time.Duration) ([]requestRecord, runSummary, error) {
	return runCampaignWithFamily(ctx, client, endpoint, config, rate, count, requestTimeout, "")
}

func runCampaignWithFamily(ctx context.Context, client *http.Client, endpoint string, config campaignConfig,
	rate float64, count int, requestTimeout time.Duration, singleFamily string) ([]requestRecord, runSummary, error) {
	families, err := buildFamilySchedule(config, count, singleFamily)
	if err != nil {
		return nil, runSummary{}, err
	}

	startUTC := time.Now()
	startMono, err := dur050.SharedMonotonicNanoseconds()
	if err != nil {
		return nil, runSummary{}, fmt.Errorf("read shared monotonic clock at run start: %w", err)
	}
	records := make([]requestRecord, count)
	semaphore := make(chan struct{}, requestConcurrency)
	var wait sync.WaitGroup
	for index := 0; index < count; index++ {
		offset := time.Duration(float64(index) * float64(time.Second) / rate)
		scheduledUTC := startUTC.Add(offset)
		if delay := time.Until(scheduledUTC); delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return records[:index], runSummary{}, ctx.Err()
			case <-timer.C:
			}
		}
		sequence := index + 1
		workflowID := fmt.Sprintf("%s-%s-%06d", config.Namespace, config.RunID, sequence)
		submissionKey := fmt.Sprintf("%s/%06d", config.RunID, sequence)
		records[index] = requestRecord{Sequence: sequence, Family: families[index].Name,
			WorkflowID: workflowID, SubmissionKey: submissionKey, ScheduledUTC: scheduledUTC,
			ScheduledMono: startMono + int64(offset)}
		select {
		case semaphore <- struct{}{}:
			wait.Add(1)
			go func(index int, family familyConfig, request submissionRequest) {
				defer wait.Done()
				defer func() { <-semaphore }()
				records[index] = submitOne(ctx, client, endpoint, request, records[index], requestTimeout)
			}(index, families[index], makeSubmission(config, families[index], workflowID, submissionKey))
		default:
			records[index].Outcome = "generator_capacity_missed"
			records[index].Error = "64-request in-flight limit was full at scheduled arrival"
			records[index] = finishRecord(records[index])
		}
	}
	wait.Wait()
	summary := summarize(config, rate, records)
	return records, summary, nil
}

func buildFamilySchedule(config campaignConfig, count int, singleFamily string) ([]familyConfig, error) {
	families := make([]familyConfig, 0, count)
	if singleFamily != "" {
		for _, family := range config.Families {
			if family.Name == singleFamily {
				for index := 0; index < count; index++ {
					families = append(families, family)
				}
				break
			}
		}
		if len(families) != count {
			return nil, fmt.Errorf("single-family selector %q is absent from campaign config", singleFamily)
		}
	} else {
		if len(config.Families) != 2 || count < 2 {
			return nil, errors.New("balanced schedule requires exactly two families and at least two arrivals")
		}
		for index := 0; index < count/2; index++ {
			families = append(families, config.Families...)
		}
		if count%2 != 0 {
			extraFamily := int(config.Seed & 1)
			families = append(families, config.Families[extraFamily])
		}
		random := rand.New(rand.NewSource(config.Seed))
		random.Shuffle(len(families), func(i, j int) { families[i], families[j] = families[j], families[i] })
	}
	return families, nil
}

func makeSubmission(config campaignConfig, family familyConfig, workflowID, submissionKey string) submissionRequest {
	payload := family.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	initialInput := family.InitialInput
	if len(initialInput) == 0 {
		initialInput = json.RawMessage(`{}`)
	}
	return submissionRequest{WorkflowID: workflowID, Namespace: config.Namespace,
		SubmissionKey: submissionKey, Payload: payload, DefinitionID: family.DefinitionID,
		DefinitionVersion: family.DefinitionVersion, InitialNodeID: family.InitialNodeID,
		InitialInput: initialInput, ActorID: "dur050-loadgen"}
}

func submitOne(parent context.Context, client *http.Client, endpoint string, request submissionRequest,
	record requestRecord, requestTimeout time.Duration) requestRecord {
	requestBytes, err := json.Marshal(request)
	if err != nil {
		record.Outcome = "rejected"
		record.Error = "encode request: " + err.Error()
		return finishRecord(record)
	}
	for attempt := 1; attempt <= maxRequestAttempts; attempt++ {
		attemptCtx, cancel := context.WithTimeout(parent, requestTimeout)
		httpRequest, requestErr := http.NewRequestWithContext(attemptCtx, http.MethodPost, endpoint, strings.NewReader(string(requestBytes)))
		if requestErr != nil {
			cancel()
			record.Outcome = "rejected"
			record.Error = "create HTTP request: " + requestErr.Error()
			return finishRecord(record)
		}
		httpRequest.Header.Set("Content-Type", "application/json")
		if record.StartedUTC.IsZero() {
			record.StartedUTC = time.Now().UTC()
			if mono, clockErr := dur050.SharedMonotonicNanoseconds(); clockErr == nil {
				record.StartedMono = mono
				record.ScheduleDelay = time.Duration(mono - record.ScheduledMono)
			} else {
				record.Error = "read monotonic clock before submit: " + clockErr.Error()
			}
		}
		record.Attempts = attempt
		response, doErr := client.Do(httpRequest)
		if doErr != nil {
			cancel()
			record.Error = doErr.Error()
			if attempt < maxRequestAttempts && retryDelay(parent, attempt) {
				continue
			}
			record.Outcome = "ambiguous"
			return finishRecord(record)
		}
		record.HTTPStatus = response.StatusCode
		body, readErr := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes))
		closeErr := response.Body.Close()
		cancel()
		if readErr != nil || closeErr != nil {
			record.Error = errors.Join(readErr, closeErr).Error()
			if attempt < maxRequestAttempts && retryDelay(parent, attempt) {
				continue
			}
			record.Outcome = "ambiguous"
			return finishRecord(record)
		}
		if response.StatusCode == http.StatusOK || response.StatusCode == http.StatusCreated {
			var result workflowResponse
			if err := json.Unmarshal(body, &result); err == nil && result.Workflow.WorkflowID == request.WorkflowID {
				record.Outcome = "accepted"
				record.Created = response.StatusCode == http.StatusCreated && result.Created
				record.Error = ""
				return finishRecord(record)
			}
			record.Error = "success response did not identify the submitted workflow"
			if attempt < maxRequestAttempts && retryDelay(parent, attempt) {
				continue
			}
			record.Outcome = "ambiguous"
			return finishRecord(record)
		}
		var apiErr apiError
		_ = json.Unmarshal(body, &apiErr)
		if response.StatusCode == http.StatusTooManyRequests ||
			(response.StatusCode == http.StatusServiceUnavailable && apiErr.Error.Code == "BACKPRESSURE") ||
			(response.StatusCode >= 400 && response.StatusCode < 500) {
			record.Outcome = "rejected"
			record.Error = apiErr.Error.Code
			return finishRecord(record)
		}
		record.Error = fmt.Sprintf("HTTP %d", response.StatusCode)
		if attempt < maxRequestAttempts && retryDelay(parent, attempt) {
			continue
		}
		record.Outcome = "ambiguous"
		return finishRecord(record)
	}
	record.Outcome = "ambiguous"
	record.Error = "submission attempts exhausted without a definitive response"
	return finishRecord(record)
}

func retryDelay(ctx context.Context, attempt int) bool {
	delay := time.Duration(attempt*100) * time.Millisecond
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func finishRecord(record requestRecord) requestRecord {
	record.FinishedUTC = time.Now().UTC()
	if mono, err := dur050.SharedMonotonicNanoseconds(); err == nil {
		record.FinishedMono = mono
	} else if record.Error == "" {
		record.Error = "read monotonic clock after submit: " + err.Error()
	}
	return record
}

func summarize(config campaignConfig, rate float64, records []requestRecord) runSummary {
	values := make([]float64, 0, len(records))
	result := runSummary{Schema: "dur050-loadgen.v1", Status: "PENDING_CPU_VALIDATION", ClockModel: "Linux CLOCK_MONOTONIC shared by generator and observer on one host/boot",
		Namespace: config.Namespace, RunID: config.RunID, Seed: config.Seed, OfferedRatePerSecond: rate,
		Scheduled: len(records), MaxInFlight: requestConcurrency, RetryLimit: maxRequestAttempts}
	for _, record := range records {
		switch record.Outcome {
		case "accepted":
			result.Accepted++
			result.Submitted++
		case "rejected":
			result.Rejected++
			result.Submitted++
		case "ambiguous":
			result.Ambiguous++
			result.Submitted++
		case "generator_capacity_missed":
			result.GeneratorCapacityMiss++
		default:
			result.GeneratorCapacityMiss++
		}
		if !record.StartedUTC.IsZero() {
			values = append(values, float64(record.ScheduleDelay)/float64(time.Millisecond))
		}
	}
	if len(values) > 0 {
		sort.Float64s(values)
		p99Rank := int(math.Ceil(0.99 * float64(len(values))))
		result.P99ScheduleDelayMS = values[p99Rank-1]
		result.MaxScheduleDelayMS = values[len(values)-1]
	}
	var reasons []string
	if result.GeneratorCapacityMiss > 0 {
		reasons = append(reasons, "scheduled arrivals were not submitted")
	}
	if result.Ambiguous > 0 {
		reasons = append(reasons, "uncertain submissions were not resolved by same-key retries")
	}
	if len(values) == 0 || result.P99ScheduleDelayMS > 50 {
		reasons = append(reasons, "p99 scheduled-to-submit delay exceeds 50 ms or has no samples")
	}
	if reasons != nil {
		result.Status = "FAIL"
		result.InvalidReason = strings.Join(reasons, "; ")
	}
	return result
}

func ensureOutputPathsAvailable(outputPath, workflowIDsPath string) error {
	if filepath.Clean(outputPath) == "." {
		return errors.New("-output must name a file")
	}
	paths := []string{outputPath, outputPath + ".summary.json"}
	if workflowIDsPath != "" {
		if filepath.Clean(workflowIDsPath) == filepath.Clean(outputPath) ||
			filepath.Clean(workflowIDsPath) == filepath.Clean(outputPath+".summary.json") {
			return errors.New("workflow ID output path must differ from CSV and summary paths")
		}
		paths = append(paths, workflowIDsPath)
	}
	for _, path := range paths {
		if _, err := os.Stat(path); err == nil {
			return fmt.Errorf("refusing to overwrite existing load-generator artifact %s", path)
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("check load-generator output path %s: %w", path, err)
		}
	}
	return nil
}

func writeArtifacts(outputPath, workflowIDsPath string, records []requestRecord, summary runSummary) error {
	file, err := os.OpenFile(outputPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create immutable load-generator CSV: %w", err)
	}
	writer := csv.NewWriter(file)
	if err := writer.Write(csvColumns); err != nil {
		_ = file.Close()
		return err
	}
	for _, record := range records {
		row := []string{"submission", fmt.Sprint(record.Sequence), record.Family, record.WorkflowID,
			record.SubmissionKey, record.ScheduledUTC.UTC().Format(time.RFC3339Nano), fmt.Sprint(record.ScheduledMono),
			formatUTC(record.StartedUTC), formatInt64(record.StartedMono), formatUTC(record.FinishedUTC),
			formatInt64(record.FinishedMono), formatFloat(float64(record.ScheduleDelay) / float64(time.Millisecond)),
			fmt.Sprint(record.Attempts), fmt.Sprint(record.HTTPStatus), record.Outcome, fmt.Sprint(record.Created), record.Error}
		if err := writer.Write(row); err != nil {
			_ = file.Close()
			return err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}
	summaryFile, err := os.OpenFile(outputPath+".summary.json", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create immutable load-generator summary: %w", err)
	}
	if _, err := summaryFile.Write(append(encoded, '\n')); err != nil {
		_ = summaryFile.Close()
		return fmt.Errorf("write load-generator summary: %w", err)
	}
	if err := summaryFile.Close(); err != nil {
		return fmt.Errorf("write load-generator summary: %w", err)
	}
	if workflowIDsPath != "" {
		idsFile, err := os.OpenFile(workflowIDsPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return fmt.Errorf("create immutable accepted workflow ID list: %w", err)
		}
		for _, record := range records {
			if record.Outcome == "accepted" {
				if _, err := fmt.Fprintln(idsFile, record.WorkflowID); err != nil {
					_ = idsFile.Close()
					return fmt.Errorf("write accepted workflow ID: %w", err)
				}
			}
		}
		if err := idsFile.Close(); err != nil {
			return fmt.Errorf("close accepted workflow ID list: %w", err)
		}
	}
	return nil
}

func formatUTC(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func formatInt64(value int64) string {
	if value == 0 {
		return ""
	}
	return fmt.Sprint(value)
}

func formatFloat(value float64) string {
	if value == 0 {
		return "0"
	}
	return fmt.Sprintf("%.6f", value)
}
