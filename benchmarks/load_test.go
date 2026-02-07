package benchmarks

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yash/flightedge/internal/ontology"
	"github.com/yash/flightedge/internal/query"
	"github.com/yash/flightedge/pkg/models"
)

// ---------------------------------------------------------------------------
// Load Generator - Simulates 100-200 events/sec ingestion
// ---------------------------------------------------------------------------

// LoadGenerator simulates realistic flight event ingestion.
type LoadGenerator struct {
	ont        *ontology.Engine
	qe         *query.Engine
	eventsPerSec int
	duration   time.Duration

	// Stats
	totalEvents  atomic.Int64
	totalQueries atomic.Int64
	errors       atomic.Int64
}

// NewLoadGenerator creates a load generator with the specified event rate.
func NewLoadGenerator(eventsPerSec int, duration time.Duration) *LoadGenerator {
	ont := ontology.New()
	qe := query.New(ont)
	
	// Seed airports
	airports := []string{"YVR", "JFK", "LAX", "ORD", "DFW", "SFO", "SEA", "LHR", "CDG", "NRT"}
	for _, code := range airports {
		n := ontology.Node{ID: "apt-" + code, Type: ontology.TypeAirport}
		n.SetString("code", code)
		ont.AddNode(n)
	}
	
	return &LoadGenerator{
		ont:          ont,
		qe:           qe,
		eventsPerSec: eventsPerSec,
		duration:     duration,
	}
}

// generateFlight creates a realistic flight event.
func (lg *LoadGenerator) generateFlight(i int) models.Flight {
	airlines := []string{"UAL", "ACA", "DAL", "AAL", "BAW", "AFR", "DLH", "SWA", "JBU", "WJA"}
	
	return models.Flight{
		ICAO24:      fmt.Sprintf("%06x", rand.Intn(0xFFFFFF)),
		Callsign:    fmt.Sprintf("%s%04d", airlines[rand.Intn(len(airlines))], rand.Intn(9999)),
		Origin:      "US",
		Latitude:    30.0 + rand.Float64()*20,
		Longitude:   -130.0 + rand.Float64()*60,
		Altitude:    float64(rand.Intn(45000)),
		Velocity:    float64(200 + rand.Intn(300)),
		Heading:     float64(rand.Intn(360)),
		OnGround:    rand.Float64() < 0.1,
		LastContact: time.Now(),
	}
}

// ingestFlight adds a flight to the ontology.
func (lg *LoadGenerator) ingestFlight(f models.Flight) {
	n := ontology.Node{ID: f.ICAO24, Type: ontology.TypeFlight}
	n.SetString("flight_id", f.ICAO24)
	n.SetString("callsign", f.Callsign)
	n.SetString("origin_country", f.Origin)
	n.SetLocation("position", f.Latitude, f.Longitude)
	n.SetFloat("altitude", f.Altitude)
	n.SetFloat("velocity", f.Velocity)
	n.SetFloat("heading", f.Heading)
	n.SetBool("on_ground", f.OnGround)
	n.SetTimestamp("last_contact", f.LastContact)
	lg.ont.AddNode(n)
	
	// Index for queries
	if len(f.Callsign) >= 3 {
		lg.qe.IndexFlight(f.ICAO24, f.Callsign, "", "")
	}
	
	lg.totalEvents.Add(1)
}

// Run executes the load test.
func (lg *LoadGenerator) Run(ctx context.Context) LoadStats {
	startTime := time.Now()
	interval := time.Second / time.Duration(lg.eventsPerSec)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	deadline := time.After(lg.duration)
	i := 0

	for {
		select {
		case <-ctx.Done():
			return lg.stats(startTime)
		case <-deadline:
			return lg.stats(startTime)
		case <-ticker.C:
			flight := lg.generateFlight(i)
			lg.ingestFlight(flight)
			i++
		}
	}
}

// stats returns the load test statistics.
func (lg *LoadGenerator) stats(startTime time.Time) LoadStats {
	elapsed := time.Since(startTime)
	return LoadStats{
		Duration:       elapsed,
		TotalEvents:   lg.totalEvents.Load(),
		EventsPerSec:  float64(lg.totalEvents.Load()) / elapsed.Seconds(),
		TotalNodes:    lg.ont.Size(),
		Errors:        lg.errors.Load(),
	}
}

// LoadStats holds load test results.
type LoadStats struct {
	Duration      time.Duration
	TotalEvents   int64
	EventsPerSec  float64
	TotalNodes    int
	Errors        int64
}

// ---------------------------------------------------------------------------
// Concurrent Query Benchmark
// ---------------------------------------------------------------------------

// ConcurrentQueryBench tests parallel query performance.
type ConcurrentQueryBench struct {
	ont        *ontology.Engine
	qe         *query.Engine
	
	// Results
	latencies  []time.Duration
	latencyMu  sync.Mutex
}

// NewConcurrentQueryBench creates a benchmark with pre-populated data.
func NewConcurrentQueryBench(numNodes int) *ConcurrentQueryBench {
	ont := ontology.New()
	
	// Seed airports
	airports := []string{"YVR", "JFK", "LAX", "ORD", "DFW", "SFO", "SEA", "LHR", "CDG", "NRT"}
	for _, code := range airports {
		n := ontology.Node{ID: "apt-" + code, Type: ontology.TypeAirport}
		n.SetString("code", code)
		ont.AddNode(n)
	}
	
	// Populate flights
	airlines := []string{"UAL", "ACA", "DAL", "AAL", "BAW"}
	for i := 0; i < numNodes; i++ {
		n := ontology.Node{ID: fmt.Sprintf("fl-%d", i), Type: ontology.TypeFlight}
		n.SetString("flight_id", fmt.Sprintf("FL%06d", i))
		n.SetString("callsign", fmt.Sprintf("%s%04d", airlines[i%len(airlines)], i%10000))
		n.SetString("departure_code", airports[i%len(airports)])
		n.SetString("arrival_code", airports[(i+1)%len(airports)])
		n.SetTimestamp("scheduled_departure", time.Now().Add(time.Duration(i%24)*time.Hour))
		n.SetFloat("delay_minutes", float64(i%60))
		n.SetLocation("position", 30.0+float64(i%20), -130.0+float64(i%60))
		ont.AddNode(n)
	}
	
	qe := query.New(ont)
	qe.RebuildIndexes()
	
	return &ConcurrentQueryBench{
		ont:       ont,
		qe:        qe,
		latencies: make([]time.Duration, 0, 10000),
	}
}

// RunConcurrent executes parallel queries.
func (cqb *ConcurrentQueryBench) RunConcurrent(numWorkers, queriesPerWorker int) ConcurrentStats {
	var wg sync.WaitGroup
	startTime := time.Now()
	
	// Different query types to simulate realistic load
	queryFuncs := []func(){
		func() {
			cqb.qe.Execute(query.Request{
				NodeType:   ontology.TypeFlight,
				MaxResults: 10,
			})
		},
		func() {
			cqb.qe.GetFlightsByAirport("JFK", query.TimeRange{}, 10)
		},
		func() {
			cqb.qe.GetDelayedFlights(15*time.Minute, "", 10)
		},
		func() {
			cqb.qe.GetFlightPath("fl-0")
		},
		func() {
			cqb.qe.PredictDelay("fl-100")
		},
	}
	
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			for q := 0; q < queriesPerWorker; q++ {
				queryFunc := queryFuncs[(workerID+q)%len(queryFuncs)]
				
				start := time.Now()
				queryFunc()
				latency := time.Since(start)
				
				cqb.latencyMu.Lock()
				cqb.latencies = append(cqb.latencies, latency)
				cqb.latencyMu.Unlock()
			}
		}(w)
	}
	
	wg.Wait()
	totalTime := time.Since(startTime)
	
	return cqb.calculateStats(totalTime, numWorkers, queriesPerWorker)
}

func (cqb *ConcurrentQueryBench) calculateStats(totalTime time.Duration, workers, qpw int) ConcurrentStats {
	cqb.latencyMu.Lock()
	defer cqb.latencyMu.Unlock()
	
	if len(cqb.latencies) == 0 {
		return ConcurrentStats{}
	}
	
	// Sort for percentile calculation
	sorted := make([]time.Duration, len(cqb.latencies))
	copy(sorted, cqb.latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	
	totalQueries := workers * qpw
	
	return ConcurrentStats{
		TotalQueries:  totalQueries,
		TotalTime:     totalTime,
		QueriesPerSec: float64(totalQueries) / totalTime.Seconds(),
		P50:           sorted[len(sorted)*50/100],
		P95:           sorted[len(sorted)*95/100],
		P99:           sorted[len(sorted)*99/100],
		Min:           sorted[0],
		Max:           sorted[len(sorted)-1],
		Avg:           cqb.avgLatency(),
	}
}

func (cqb *ConcurrentQueryBench) avgLatency() time.Duration {
	if len(cqb.latencies) == 0 {
		return 0
	}
	var total time.Duration
	for _, l := range cqb.latencies {
		total += l
	}
	return total / time.Duration(len(cqb.latencies))
}

// ConcurrentStats holds concurrent query benchmark results.
type ConcurrentStats struct {
	TotalQueries  int
	TotalTime     time.Duration
	QueriesPerSec float64
	P50           time.Duration
	P95           time.Duration
	P99           time.Duration
	Min           time.Duration
	Max           time.Duration
	Avg           time.Duration
}

// ---------------------------------------------------------------------------
// Memory Profile
// ---------------------------------------------------------------------------

// MemoryProfile captures memory usage at a point in time.
type MemoryProfile struct {
	Alloc        uint64
	TotalAlloc   uint64
	Sys          uint64
	HeapAlloc    uint64
	HeapSys      uint64
	HeapInuse    uint64
	HeapObjects  uint64
	StackInuse   uint64
	NumGC        uint32
}

// CaptureMemoryProfile returns current memory statistics.
func CaptureMemoryProfile() MemoryProfile {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	
	return MemoryProfile{
		Alloc:       m.Alloc,
		TotalAlloc:  m.TotalAlloc,
		Sys:         m.Sys,
		HeapAlloc:   m.HeapAlloc,
		HeapSys:     m.HeapSys,
		HeapInuse:   m.HeapInuse,
		HeapObjects: m.HeapObjects,
		StackInuse:  m.StackInuse,
		NumGC:       m.NumGC,
	}
}

// AllocMB returns allocated memory in megabytes.
func (mp MemoryProfile) AllocMB() float64 {
	return float64(mp.Alloc) / 1024 / 1024
}

// HeapMB returns heap memory in megabytes.
func (mp MemoryProfile) HeapMB() float64 {
	return float64(mp.HeapAlloc) / 1024 / 1024
}

// SysMB returns total system memory in megabytes.
func (mp MemoryProfile) SysMB() float64 {
	return float64(mp.Sys) / 1024 / 1024
}

// ---------------------------------------------------------------------------
// Go Benchmarks
// ---------------------------------------------------------------------------

// BenchmarkLoadGenerator100EPS benchmarks 100 events/sec ingestion.
func BenchmarkLoadGenerator100EPS(b *testing.B) {
	for i := 0; i < b.N; i++ {
		lg := NewLoadGenerator(100, 1*time.Second)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		lg.Run(ctx)
		cancel()
	}
}

// BenchmarkLoadGenerator200EPS benchmarks 200 events/sec ingestion.
func BenchmarkLoadGenerator200EPS(b *testing.B) {
	for i := 0; i < b.N; i++ {
		lg := NewLoadGenerator(200, 1*time.Second)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		lg.Run(ctx)
		cancel()
	}
}

// BenchmarkConcurrentQueries10Workers benchmarks 10 parallel query workers.
func BenchmarkConcurrentQueries10Workers(b *testing.B) {
	cqb := NewConcurrentQueryBench(10000)
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		cqb.latencies = cqb.latencies[:0]
		cqb.RunConcurrent(10, 100)
	}
}

// BenchmarkConcurrentQueries25Workers benchmarks 25 parallel query workers.
func BenchmarkConcurrentQueries25Workers(b *testing.B) {
	cqb := NewConcurrentQueryBench(10000)
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		cqb.latencies = cqb.latencies[:0]
		cqb.RunConcurrent(25, 100)
	}
}

// BenchmarkConcurrentQueries50Workers benchmarks 50 parallel query workers.
func BenchmarkConcurrentQueries50Workers(b *testing.B) {
	cqb := NewConcurrentQueryBench(10000)
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		cqb.latencies = cqb.latencies[:0]
		cqb.RunConcurrent(50, 100)
	}
}

// BenchmarkMemoryUnder512MB verifies memory stays under 512MB constraint.
func BenchmarkMemoryUnder512MB(b *testing.B) {
	const maxMemoryMB = 512.0
	const targetNodes = 100000
	
	for i := 0; i < b.N; i++ {
		ont := ontology.New()
		
		// Force GC before measurement
		runtime.GC()
		before := CaptureMemoryProfile()
		
		// Populate with many nodes
		for j := 0; j < targetNodes; j++ {
			n := ontology.Node{ID: fmt.Sprintf("fl-%d", j), Type: ontology.TypeFlight}
			n.SetString("callsign", fmt.Sprintf("UAL%04d", j%10000))
			n.SetString("flight_id", fmt.Sprintf("FL%06d", j))
			n.SetFloat("altitude", float64(j%45000))
			n.SetLocation("position", 30.0+float64(j%20), -100.0+float64(j%60))
			ont.AddNode(n)
		}
		
		// Force GC and measure
		runtime.GC()
		after := CaptureMemoryProfile()
		
		usedMB := after.HeapMB()
		if usedMB > maxMemoryMB {
			b.Fatalf("Memory exceeded %vMB limit: %.2fMB for %d nodes", maxMemoryMB, usedMB, targetNodes)
		}
		
		b.ReportMetric(usedMB, "heap_MB")
		b.ReportMetric(float64(targetNodes)/usedMB, "nodes_per_MB")
		b.ReportMetric(after.HeapMB()-before.HeapMB(), "delta_MB")
	}
}

// BenchmarkLatencyDistribution measures latency percentiles.
func BenchmarkLatencyDistribution(b *testing.B) {
	cqb := NewConcurrentQueryBench(50000)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cqb.latencies = cqb.latencies[:0]
		stats := cqb.RunConcurrent(20, 500)
		
		b.ReportMetric(float64(stats.P50.Microseconds()), "p50_us")
		b.ReportMetric(float64(stats.P95.Microseconds()), "p95_us")
		b.ReportMetric(float64(stats.P99.Microseconds()), "p99_us")
		b.ReportMetric(stats.QueriesPerSec, "qps")
	}
}
