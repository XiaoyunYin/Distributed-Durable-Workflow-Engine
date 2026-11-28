package telemetry

import (
	"bytes"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestKModeScrapeMatchesBaselineBytes(t *testing.T) {
	want, err := os.ReadFile("testdata/k_scrape_a0e065a.prom")
	if err != nil {
		t.Fatal(err)
	}
	m := NewWithStageTimings("relay", false)
	m.startedUnix.Store(1760000000)
	m.durableReadyUnix.Store(1760000001)
	m.RecordClaimAccepted()
	m.lastAcceptedClaimUnix.Store(1760000002)
	m.RecordLeaseAcquired()
	m.RecordLeaseRenewed()
	m.RecordFencedWrite()
	m.RecordResultAccepted()
	m.RecordReconciliationItem()
	m.RecordRelayPublication()
	m.RecordRelayFailure()
	m.RecordDBTransaction()
	m.RecordDBQuery(2 * time.Millisecond)
	m.RecordLockWait(time.Millisecond)
	m.WorkerStarted()
	m.WorkerFinished()
	if !m.StartStage().IsZero() {
		t.Fatal("disabled timing read the clock")
	}
	m.RecordStage(StageRelayClaim, time.Second)
	recorder := httptest.NewRecorder()
	m.ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	if recorder.Header().Get("Content-Type") != "text/plain; version=0.0.4" ||
		!bytes.Equal(recorder.Body.Bytes(), want) {
		t.Fatalf("K scrape no longer matches the a0e065a renderer:\n%s", recorder.Body)
	}
}

func TestRelayHistogramBucketsMatchWitnessedDurations(t *testing.T) {
	m := NewWithStageTimings("relay", true)
	for _, delta := range []time.Duration{time.Millisecond, 2 * time.Millisecond, 10 * time.Millisecond, 700 * time.Second} {
		m.RecordStage(StageRelayClaim, delta)
	}
	m.RecordStage(StageRelayClaim, -time.Nanosecond)
	m.RecordStage(stageCount, time.Second)
	metric := `durable_j9a_relay_stage_seconds`
	rendered := m.Render()
	for _, want := range []string{
		"# TYPE " + metric + " histogram",
		metric + `_bucket{role="relay",stage="claim",le="0.0005"} 0`,
		metric + `_bucket{role="relay",stage="claim",le="0.001"} 1`,
		metric + `_bucket{role="relay",stage="claim",le="0.005"} 2`,
		metric + `_bucket{role="relay",stage="claim",le="0.01"} 3`,
		metric + `_bucket{role="relay",stage="claim",le="600"} 3`,
		metric + `_bucket{role="relay",stage="claim",le="+Inf"} 4`,
		metric + `_count{role="relay",stage="claim"} 4`,
		metric + `_sum{role="relay",stage="claim"} 700.013`,
		metric + `_count{role="relay",stage="wakeup_wait_notify"} 0`,
	} {
		if !strings.Contains(rendered, want+"\n") {
			t.Errorf("missing sample %q", want)
		}
	}
	if strings.Contains(rendered, "durable_j9a_workflow") {
		t.Fatal("offline workflow histograms appeared in the runtime scrape")
	}
}

func TestStageHistogramConcurrentScrape(t *testing.T) {
	m := NewWithStageTimings("relay", true)
	var writers sync.WaitGroup
	for range 4 {
		writers.Go(func() {
			for range 200 {
				m.RecordStage(StageRelayFinalize, time.Millisecond)
			}
		})
	}
	for range 10 {
		scrape := m.Render()
		_, bucketTail, bucketOK := strings.Cut(scrape, `_bucket{role="relay",stage="finalize",le="+Inf"} `)
		_, countTail, countOK := strings.Cut(scrape, `_count{role="relay",stage="finalize"} `)
		bucket, _, _ := strings.Cut(bucketTail, "\n")
		count, _, _ := strings.Cut(countTail, "\n")
		if !bucketOK || !countOK || bucket != count {
			t.Fatalf("inconsistent finalize scrape: +Inf=%q count=%q", bucket, count)
		}
	}
	writers.Wait()
	if !strings.Contains(m.Render(), `durable_j9a_relay_stage_seconds_count{role="relay",stage="finalize"} 800`+"\n") {
		t.Fatal("concurrent observations were lost")
	}
}
