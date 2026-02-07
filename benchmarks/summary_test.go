package benchmarks

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/yash/flightedge/internal/ontology"
	"github.com/yash/flightedge/internal/query"
)

var (
	separator   = strings.Repeat("=", 70)
	subseparator = strings.Repeat("-", 70)
)

// ---------------------------------------------------------------------------
// Benchmark Summary - Generates comprehensive performance report
// ---------------------------------------------------------------------------

// TestBenchmarkSummary runs all benchmarks and prints a summary report.
// Run with: go test -v -run TestBenchmarkSummary -timeout=5m ./benchmarks/
func TestBenchmarkSummary(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping summary in short mode")
	}
	
	fmt.Println("\n" + separator)
	fmt.Println("FlightEdge Performance Benchmark Summary")
	fmt.Println(separator + "\n")
	
	// System info
	fmt.Printf("System Information:\n")
	fmt.Printf("  Go Version: %s\n", runtime.Version())
	fmt.Printf("  GOOS/GOARCH: %s/%s\n", runtime.GOOS, runtime.GOARCH)
	fmt.Printf("  NumCPU: %d\n", runtime.NumCPU())
	fmt.Printf("  GOMAXPROCS: %d\n", runtime.GOMAXPROCS(0))
	fmt.Println()
	
	// Memory baseline
	runtime.GC()
	baseline := CaptureMemoryProfile()
	fmt.Printf("Memory Baseline:\n")
	fmt.Printf("  Heap: %.2f MB\n", baseline.HeapMB())
	fmt.Printf("  Sys: %.2f MB\n", baseline.SysMB())
	fmt.Println()
	
	// Run benchmarks
	fmt.Println(subseparator)
	fmt.Println("1. INGESTION PERFORMANCE")
	fmt.Println(subseparator)
	
	for _, rate := range []int{100, 150, 200} {
		lg := NewLoadGenerator(rate, 3*time.Second)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		stats := lg.Run(ctx)
		cancel()
		
		fmt.Printf("\n  Target: %d events/sec\n", rate)
		fmt.Printf("    Achieved: %.2f events/sec (%.1f%%)\n", stats.EventsPerSec, stats.EventsPerSec/float64(rate)*100)
		fmt.Printf("    Total events: %d\n", stats.TotalEvents)
		fmt.Printf("    Duration: %v\n", stats.Duration)
		fmt.Printf("    Status: %s\n", passFailStr(stats.EventsPerSec >= float64(rate)*0.8))
	}
	
	fmt.Println("\n" + subseparator)
	fmt.Println("2. CONCURRENT QUERY PERFORMANCE")
	fmt.Println(subseparator)
	
	for _, workers := range []int{10, 25, 50} {
		cqb := NewConcurrentQueryBench(10000)
		stats := cqb.RunConcurrent(workers, 100)
		
		fmt.Printf("\n  Workers: %d (100 queries each)\n", workers)
		fmt.Printf("    Queries/sec: %.2f\n", stats.QueriesPerSec)
		fmt.Printf("    P50: %v\n", stats.P50)
		fmt.Printf("    P95: %v\n", stats.P95)
		fmt.Printf("    P99: %v\n", stats.P99)
		fmt.Printf("    Max: %v\n", stats.Max)
		fmt.Printf("    P99 < 50ms: %s\n", passFailStr(stats.P99 < 50*time.Millisecond))
	}
	
	fmt.Println("\n" + subseparator)
	fmt.Println("3. LATENCY DISTRIBUTION")
	fmt.Println(subseparator)
	
	cqb := NewConcurrentQueryBench(50000)
	latencyStats := cqb.RunConcurrent(20, 500)
	
	fmt.Printf("\n  Sample size: %d queries\n", latencyStats.TotalQueries)
	fmt.Printf("  Min: %v\n", latencyStats.Min)
	fmt.Printf("  P50 (median): %v\n", latencyStats.P50)
	fmt.Printf("  P95: %v\n", latencyStats.P95)
	fmt.Printf("  P99: %v\n", latencyStats.P99)
	fmt.Printf("  Max: %v\n", latencyStats.Max)
	fmt.Printf("  Avg: %v\n", latencyStats.Avg)
	
	fmt.Println("\n" + subseparator)
	fmt.Println("4. MEMORY USAGE")
	fmt.Println(subseparator)
	
	for _, nodeCount := range []int{10000, 50000, 100000} {
		runtime.GC()
		before := CaptureMemoryProfile()
		
		ont := populateEngine(nodeCount)
		
		runtime.GC()
		after := CaptureMemoryProfile()
		_ = ont // keep reference
		
		heapMB := after.HeapMB()
		deltaMB := after.HeapMB() - before.HeapMB()
		nodesPerMB := float64(nodeCount) / deltaMB
		
		fmt.Printf("\n  Nodes: %d\n", nodeCount)
		fmt.Printf("    Heap: %.2f MB\n", heapMB)
		fmt.Printf("    Delta: %.2f MB\n", deltaMB)
		fmt.Printf("    Nodes/MB: %.2f\n", nodesPerMB)
		fmt.Printf("    Under 512MB: %s\n", passFailStr(heapMB < 512))
	}
	
	fmt.Println("\n" + subseparator)
	fmt.Println("5. SCALABILITY")
	fmt.Println(subseparator)
	
	fmt.Printf("\n  Testing query performance at different scales:\n")
	for _, size := range []int{1000, 10000, 50000} {
		cqb := NewConcurrentQueryBench(size)
		stats := cqb.RunConcurrent(10, 100)
		
		fmt.Printf("    %d nodes: %.2f qps, P99=%v\n", size, stats.QueriesPerSec, stats.P99)
	}
	
	// Final summary
	fmt.Println("\n" + separator)
	fmt.Println("SUMMARY")
	fmt.Println(separator)
	
	// Re-run key metrics for final summary
	lg := NewLoadGenerator(150, 2*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	loadStats := lg.Run(ctx)
	cancel()
	
	cqb = NewConcurrentQueryBench(50000)
	queryStats := cqb.RunConcurrent(20, 500)
	
	runtime.GC()
	memProfile := CaptureMemoryProfile()
	
	fmt.Printf("\n")
	fmt.Printf("  %-30s %s\n", "Ingestion Rate (150 target):", 
		statusStr(loadStats.EventsPerSec >= 120, fmt.Sprintf("%.1f events/sec", loadStats.EventsPerSec)))
	fmt.Printf("  %-30s %s\n", "Query P99 (<50ms target):", 
		statusStr(queryStats.P99 < 50*time.Millisecond, queryStats.P99.String()))
	fmt.Printf("  %-30s %s\n", "Memory (<512MB target):", 
		statusStr(memProfile.HeapMB() < 512, fmt.Sprintf("%.1f MB", memProfile.HeapMB())))
	fmt.Printf("  %-30s %s\n", "Concurrent QPS:", 
		fmt.Sprintf("%.0f queries/sec", queryStats.QueriesPerSec))
	fmt.Println()
}

func passFailStr(pass bool) string {
	if pass {
		return "✓ PASS"
	}
	return "✗ FAIL"
}

func statusStr(pass bool, value string) string {
	if pass {
		return fmt.Sprintf("✓ %s", value)
	}
	return fmt.Sprintf("✗ %s", value)
}

// ---------------------------------------------------------------------------
// Individual Query Type Benchmarks
// ---------------------------------------------------------------------------

func BenchmarkQueryTypes(b *testing.B) {
	cqb := NewConcurrentQueryBench(50000)
	
	b.Run("Execute", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cqb.qe.Execute(query.Request{
				NodeType:   ontology.TypeFlight,
				MaxResults: 10,
			})
		}
	})
	
	b.Run("GetFlightsByAirport", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cqb.qe.GetFlightsByAirport("JFK", query.TimeRange{}, 10)
		}
	})
	
	b.Run("GetDelayedFlights", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cqb.qe.GetDelayedFlights(15*time.Minute, "", 10)
		}
	})
	
	b.Run("GetDelayedFlightsByAirline", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cqb.qe.GetDelayedFlights(15*time.Minute, "UAL", 10)
		}
	})
	
	b.Run("GetFlightPath", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cqb.qe.GetFlightPath("fl-0")
		}
	})
	
	b.Run("PredictDelay", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cqb.qe.PredictDelay("fl-100")
		}
	})
}
