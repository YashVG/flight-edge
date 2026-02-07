package benchmarks

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yash/flightedge/internal/ontology"
	"github.com/yash/flightedge/internal/query"
)

// ---------------------------------------------------------------------------
// Integration Tests for Performance Validation
// ---------------------------------------------------------------------------

// TestLoadGenerator100EPS verifies we can sustain 100 events/sec.
func TestLoadGenerator100EPS(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in short mode")
	}
	
	lg := NewLoadGenerator(100, 3*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	stats := lg.Run(ctx)
	
	t.Logf("Load test results:")
	t.Logf("  Duration: %v", stats.Duration)
	t.Logf("  Total events: %d", stats.TotalEvents)
	t.Logf("  Events/sec: %.2f", stats.EventsPerSec)
	t.Logf("  Total nodes: %d", stats.TotalNodes)
	
	// Verify we achieved at least 80% of target rate
	assert.GreaterOrEqual(t, stats.EventsPerSec, 80.0, "Should achieve at least 80 events/sec")
	assert.Equal(t, int64(0), stats.Errors, "Should have no errors")
}

// TestLoadGenerator200EPS verifies we can sustain 200 events/sec.
func TestLoadGenerator200EPS(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in short mode")
	}
	
	lg := NewLoadGenerator(200, 3*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	stats := lg.Run(ctx)
	
	t.Logf("Load test results:")
	t.Logf("  Duration: %v", stats.Duration)
	t.Logf("  Total events: %d", stats.TotalEvents)
	t.Logf("  Events/sec: %.2f", stats.EventsPerSec)
	
	// Verify we achieved at least 80% of target rate
	assert.GreaterOrEqual(t, stats.EventsPerSec, 160.0, "Should achieve at least 160 events/sec")
}

// TestConcurrentQueries10Workers validates 10 parallel workers.
func TestConcurrentQueries10Workers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrent test in short mode")
	}
	
	cqb := NewConcurrentQueryBench(10000)
	stats := cqb.RunConcurrent(10, 100)
	
	t.Logf("Concurrent query results (10 workers):")
	t.Logf("  Total queries: %d", stats.TotalQueries)
	t.Logf("  Total time: %v", stats.TotalTime)
	t.Logf("  Queries/sec: %.2f", stats.QueriesPerSec)
	t.Logf("  P50: %v", stats.P50)
	t.Logf("  P95: %v", stats.P95)
	t.Logf("  P99: %v", stats.P99)
	
	// P99 should be under 50ms target
	assert.Less(t, stats.P99, 50*time.Millisecond, "P99 latency should be under 50ms")
}

// TestConcurrentQueries50Workers validates 50 parallel workers.
func TestConcurrentQueries50Workers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrent test in short mode")
	}
	
	cqb := NewConcurrentQueryBench(10000)
	stats := cqb.RunConcurrent(50, 100)
	
	t.Logf("Concurrent query results (50 workers):")
	t.Logf("  Total queries: %d", stats.TotalQueries)
	t.Logf("  Total time: %v", stats.TotalTime)
	t.Logf("  Queries/sec: %.2f", stats.QueriesPerSec)
	t.Logf("  P50: %v", stats.P50)
	t.Logf("  P95: %v", stats.P95)
	t.Logf("  P99: %v", stats.P99)
	
	// P99 should be under 50ms target even under heavy load
	assert.Less(t, stats.P99, 50*time.Millisecond, "P99 latency should be under 50ms")
}

// TestMemoryConstraint512MB verifies memory stays under 512MB with realistic load.
func TestMemoryConstraint512MB(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping memory test in short mode")
	}
	
	const maxMemoryMB = 512.0
	const targetNodes = 100000
	
	ont := ontology.New()
	qe := query.New(ont)
	
	// Force GC before test
	runtime.GC()
	before := CaptureMemoryProfile()
	
	// Populate with many nodes (simulating heavy load)
	airlines := []string{"UAL", "ACA", "DAL", "AAL", "BAW", "AFR", "DLH"}
	airports := []string{"YVR", "JFK", "LAX", "ORD", "DFW", "SFO", "SEA", "LHR"}
	
	for i := 0; i < targetNodes; i++ {
		n := ontology.Node{ID: fmt.Sprintf("fl-%d", i), Type: ontology.TypeFlight}
		n.SetString("flight_id", fmt.Sprintf("FL%06d", i))
		n.SetString("callsign", fmt.Sprintf("%s%04d", airlines[i%len(airlines)], i%10000))
		n.SetString("departure_code", airports[i%len(airports)])
		n.SetString("arrival_code", airports[(i+1)%len(airports)])
		n.SetFloat("altitude", float64(i%45000))
		n.SetFloat("velocity", float64(200+i%300))
		n.SetLocation("position", 30.0+float64(i%20), -100.0+float64(i%60))
		n.SetTimestamp("last_contact", time.Now())
		ont.AddNode(n)
		
		// Index periodically
		if i%1000 == 0 {
			qe.IndexFlight(n.ID, n.ID, airports[i%len(airports)], airports[(i+1)%len(airports)])
		}
	}
	
	// Force GC and measure
	runtime.GC()
	after := CaptureMemoryProfile()
	
	t.Logf("Memory profile after %d nodes:", targetNodes)
	t.Logf("  Heap allocated: %.2f MB", after.HeapMB())
	t.Logf("  Total sys: %.2f MB", after.SysMB())
	t.Logf("  Heap objects: %d", after.HeapObjects)
	t.Logf("  Delta heap: %.2f MB", after.HeapMB()-before.HeapMB())
	t.Logf("  Nodes per MB: %.2f", float64(targetNodes)/after.HeapMB())
	
	assert.Less(t, after.HeapMB(), maxMemoryMB, 
		"Heap memory (%.2f MB) should be under %v MB limit", after.HeapMB(), maxMemoryMB)
}

// TestLatencyP99Under50ms validates P99 latency target.
// Uses moderate concurrency (10 workers) which is realistic for edge deployment.
func TestLatencyP99Under50ms(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping latency test in short mode")
	}
	
	// Use 10K nodes and 10 workers for realistic edge scenario
	cqb := NewConcurrentQueryBench(10000)
	stats := cqb.RunConcurrent(10, 500)
	
	t.Logf("Latency distribution (10 workers, 500 queries each):")
	t.Logf("  Total queries: %d", stats.TotalQueries)
	t.Logf("  P50: %v", stats.P50)
	t.Logf("  P95: %v", stats.P95)
	t.Logf("  P99: %v", stats.P99)
	t.Logf("  Min: %v", stats.Min)
	t.Logf("  Max: %v", stats.Max)
	t.Logf("  Avg: %v", stats.Avg)
	
	// P99 under 50ms for realistic edge load
	assert.Less(t, stats.P99, 50*time.Millisecond, "P99 should be under 50ms")
}

// TestSustainedLoad validates system under sustained load.
func TestSustainedLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping sustained load test in short mode")
	}
	
	const duration = 10 * time.Second
	const eventsPerSec = 150
	
	lg := NewLoadGenerator(eventsPerSec, duration)
	ctx, cancel := context.WithTimeout(context.Background(), duration+5*time.Second)
	defer cancel()
	
	// Start load generation
	go lg.Run(ctx)
	
	// Periodically measure memory during load
	var maxMemMB float64
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	
	deadline := time.After(duration)
	for {
		select {
		case <-deadline:
			goto done
		case <-ticker.C:
			profile := CaptureMemoryProfile()
			if profile.HeapMB() > maxMemMB {
				maxMemMB = profile.HeapMB()
			}
		}
	}
	
done:
	t.Logf("Sustained load test (%.0f events/sec for %v):", float64(eventsPerSec), duration)
	t.Logf("  Peak memory: %.2f MB", maxMemMB)
	t.Logf("  Final nodes: %d", lg.ont.Size())
	
	assert.Less(t, maxMemMB, 512.0, "Peak memory should be under 512MB")
}

// ---------------------------------------------------------------------------
// CPU and Memory Profiling Tests
// ---------------------------------------------------------------------------

// TestCPUProfile generates CPU profile for analysis.
func TestCPUProfile(t *testing.T) {
	if os.Getenv("ENABLE_PROFILING") != "true" {
		t.Skip("Set ENABLE_PROFILING=true to run profiling tests")
	}
	
	f, err := os.Create("cpu.prof")
	require.NoError(t, err)
	defer f.Close()
	
	err = pprof.StartCPUProfile(f)
	require.NoError(t, err)
	defer pprof.StopCPUProfile()
	
	// Run workload
	cqb := NewConcurrentQueryBench(50000)
	cqb.RunConcurrent(20, 1000)
	
	t.Log("CPU profile written to cpu.prof")
	t.Log("Analyze with: go tool pprof cpu.prof")
}

// TestMemoryProfile generates memory profile for analysis.
func TestMemoryProfile(t *testing.T) {
	if os.Getenv("ENABLE_PROFILING") != "true" {
		t.Skip("Set ENABLE_PROFILING=true to run profiling tests")
	}
	
	// Run workload
	ont := ontology.New()
	for i := 0; i < 100000; i++ {
		n := ontology.Node{ID: fmt.Sprintf("fl-%d", i), Type: ontology.TypeFlight}
		n.SetString("callsign", fmt.Sprintf("UAL%04d", i%10000))
		ont.AddNode(n)
	}
	
	runtime.GC()
	
	f, err := os.Create("mem.prof")
	require.NoError(t, err)
	defer f.Close()
	
	err = pprof.WriteHeapProfile(f)
	require.NoError(t, err)
	
	t.Log("Memory profile written to mem.prof")
	t.Log("Analyze with: go tool pprof mem.prof")
}

// ---------------------------------------------------------------------------
// Regression Tests
// ---------------------------------------------------------------------------

// TestPerformanceRegression validates performance hasn't regressed.
func TestPerformanceRegression(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping regression test in short mode")
	}
	
	// Baseline thresholds (adjust based on CI hardware)
	const (
		maxP99Latency    = 50 * time.Millisecond
		minQPS           = 1000.0
		maxMemoryMB      = 512.0
		minEventsPerSec  = 100.0
	)
	
	t.Run("QueryLatency", func(t *testing.T) {
		cqb := NewConcurrentQueryBench(10000)
		stats := cqb.RunConcurrent(20, 500)
		
		assert.Less(t, stats.P99, maxP99Latency, "P99 regression: %v > %v", stats.P99, maxP99Latency)
		assert.Greater(t, stats.QueriesPerSec, minQPS, "QPS regression: %.2f < %.2f", stats.QueriesPerSec, minQPS)
	})
	
	t.Run("MemoryUsage", func(t *testing.T) {
		ont := ontology.New()
		for i := 0; i < 50000; i++ {
			n := ontology.Node{ID: fmt.Sprintf("fl-%d", i), Type: ontology.TypeFlight}
			n.SetString("callsign", fmt.Sprintf("UAL%04d", i%10000))
			ont.AddNode(n)
		}
		runtime.GC()
		
		profile := CaptureMemoryProfile()
		assert.Less(t, profile.HeapMB(), maxMemoryMB, "Memory regression: %.2f MB > %.2f MB", profile.HeapMB(), maxMemoryMB)
	})
	
	t.Run("IngestionRate", func(t *testing.T) {
		lg := NewLoadGenerator(150, 2*time.Second)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		
		stats := lg.Run(ctx)
		assert.Greater(t, stats.EventsPerSec, minEventsPerSec, "Ingestion regression: %.2f < %.2f events/sec", stats.EventsPerSec, minEventsPerSec)
	})
}
