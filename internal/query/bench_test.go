package query

import (
	"fmt"
	"testing"
	"time"

	"github.com/yash/flightedge/internal/ontology"
)

// ---------------------------------------------------------------------------
// Benchmark Helpers
// ---------------------------------------------------------------------------

func populateBenchOntology(numFlights, numAirports int) (*ontology.Engine, *Engine) {
	ont := ontology.New()
	
	// Create airports
	airports := make([]string, numAirports)
	for i := 0; i < numAirports; i++ {
		code := fmt.Sprintf("AP%03d", i)
		airports[i] = code
		
		apt := ontology.Node{ID: "apt-" + code, Type: ontology.TypeAirport}
		apt.SetString("code", code)
		apt.SetString("name", "Airport "+code)
		apt.SetLocation("position", 40.0+float64(i)*0.1, -100.0+float64(i)*0.1)
		ont.AddNode(apt)
	}
	
	// Create flights
	airlines := []string{"UAL", "ACA", "DAL", "AAL", "BAW", "AFR", "DLH"}
	baseTime := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	
	qe := New(ont)
	
	for i := 0; i < numFlights; i++ {
		airline := airlines[i%len(airlines)]
		depIdx := i % numAirports
		arrIdx := (i + 1) % numAirports
		depCode := airports[depIdx]
		arrCode := airports[arrIdx]
		
		schedDep := baseTime.Add(time.Duration(i%24) * time.Hour)
		delay := time.Duration((i % 10) * 5) * time.Minute // 0, 5, 10, ... 45 min delays
		actualDep := schedDep.Add(delay)
		
		f := ontology.Node{ID: fmt.Sprintf("fl-%d", i), Type: ontology.TypeFlight}
		f.SetString("flight_id", fmt.Sprintf("%s%04d", airline, i))
		f.SetString("callsign", fmt.Sprintf("%s%04d", airline, i))
		f.SetString("departure_code", depCode)
		f.SetString("arrival_code", arrCode)
		f.SetTimestamp("scheduled_departure", schedDep)
		f.SetTimestamp("actual_departure", actualDep)
		f.SetFloat("altitude", 35000+float64(i%5000))
		f.SetLocation("position", 40.0+float64(i%100)*0.01, -100.0+float64(i%100)*0.01)
		ont.AddNode(f)
		
		// Add edges
		ont.AddEdge(f.ID, "apt-"+depCode, ontology.RelDepartsFrom)
		ont.AddEdge(f.ID, "apt-"+arrCode, ontology.RelArrivesAt)
		
		// Index
		qe.IndexFlight(f.ID, f.ID, depCode, arrCode)
	}
	
	return ont, qe
}

// ---------------------------------------------------------------------------
// GetFlightsByAirport Benchmarks
// ---------------------------------------------------------------------------

func BenchmarkGetFlightsByAirport(b *testing.B) {
	sizes := []int{100, 1000, 10000}
	
	for _, size := range sizes {
		b.Run(fmt.Sprintf("flights=%d", size), func(b *testing.B) {
			_, qe := populateBenchOntology(size, 50)
			b.ResetTimer()
			
			for i := 0; i < b.N; i++ {
				qe.GetFlightsByAirport("AP000", TimeRange{}, 10)
			}
		})
	}
}

func BenchmarkGetFlightsByAirportWithTimeRange(b *testing.B) {
	_, qe := populateBenchOntology(10000, 50)
	
	baseTime := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	tr := TimeRange{
		Start: baseTime,
		End:   baseTime.Add(6 * time.Hour),
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qe.GetFlightsByAirport("AP000", tr, 10)
	}
}

func BenchmarkGetFlightsByAirportNoIndex(b *testing.B) {
	// Compare: full scan without index
	ont, _ := populateBenchOntology(10000, 50)
	qe := New(ont) // Fresh engine without indexes
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// This will return empty since indexes aren't built
		// Just measuring the overhead
		qe.GetFlightsByAirport("AP000", TimeRange{}, 10)
	}
}

// ---------------------------------------------------------------------------
// GetDelayedFlights Benchmarks
// ---------------------------------------------------------------------------

func BenchmarkGetDelayedFlights(b *testing.B) {
	sizes := []int{100, 1000, 10000}
	
	for _, size := range sizes {
		b.Run(fmt.Sprintf("flights=%d", size), func(b *testing.B) {
			_, qe := populateBenchOntology(size, 50)
			b.ResetTimer()
			
			for i := 0; i < b.N; i++ {
				qe.GetDelayedFlights(15*time.Minute, "", 10)
			}
		})
	}
}

func BenchmarkGetDelayedFlightsByAirline(b *testing.B) {
	sizes := []int{100, 1000, 10000}
	
	for _, size := range sizes {
		b.Run(fmt.Sprintf("flights=%d", size), func(b *testing.B) {
			_, qe := populateBenchOntology(size, 50)
			b.ResetTimer()
			
			for i := 0; i < b.N; i++ {
				qe.GetDelayedFlights(15*time.Minute, "UAL", 10)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// GetFlightPath Benchmarks
// ---------------------------------------------------------------------------

func BenchmarkGetFlightPath(b *testing.B) {
	_, qe := populateBenchOntology(10000, 50)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qe.GetFlightPath(fmt.Sprintf("UAL%04d", i%1000))
	}
}

func BenchmarkGetFlightPathByNodeID(b *testing.B) {
	_, qe := populateBenchOntology(10000, 50)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qe.GetFlightPath(fmt.Sprintf("fl-%d", i%10000))
	}
}

// ---------------------------------------------------------------------------
// PredictDelay Benchmarks
// ---------------------------------------------------------------------------

func BenchmarkPredictDelay(b *testing.B) {
	_, qe := populateBenchOntology(10000, 50)
	
	// Populate history
	baseTime := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 1000; i++ {
		qe.RecordDelay(
			fmt.Sprintf("AP%03d", i%50),
			fmt.Sprintf("AP%03d", (i+1)%50),
			"UAL",
			baseTime.Add(time.Duration(i%24)*time.Hour),
			time.Duration(i%60)*time.Minute,
		)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qe.PredictDelay(fmt.Sprintf("fl-%d", i%10000))
	}
}

func BenchmarkPredictDelayNoHistory(b *testing.B) {
	_, qe := populateBenchOntology(10000, 50)
	// No history data
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qe.PredictDelay(fmt.Sprintf("fl-%d", i%10000))
	}
}

// ---------------------------------------------------------------------------
// Index Comparison Benchmarks
// ---------------------------------------------------------------------------

func BenchmarkIndexLookupVsFullScan(b *testing.B) {
	ont, qe := populateBenchOntology(10000, 50)
	
	b.Run("with_index", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Uses airport index
			qe.GetFlightsByAirport("AP000", TimeRange{}, 10)
		}
	})
	
	b.Run("full_scan", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Full type scan
			var count int
			ont.ForEachNodeOfType(ontology.TypeFlight, func(n *ontology.Node) bool {
				if dep, ok := n.GetString("departure_code"); ok && dep == "AP000" {
					count++
					if count >= 10 {
						return false
					}
				}
				return true
			})
		}
	})
}

func BenchmarkAirlineIndexVsFullScan(b *testing.B) {
	ont, qe := populateBenchOntology(10000, 50)
	
	b.Run("with_index", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			flightIDs := qe.airlineIdx.Get("UAL")
			_ = len(flightIDs)
		}
	})
	
	b.Run("full_scan", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var count int
			ont.ForEachNodeOfType(ontology.TypeFlight, func(n *ontology.Node) bool {
				if cs, ok := n.GetString("callsign"); ok && len(cs) >= 3 && cs[:3] == "UAL" {
					count++
				}
				return true
			})
		}
	})
}

// ---------------------------------------------------------------------------
// Memory Pool Benchmarks
// ---------------------------------------------------------------------------

func BenchmarkWithPool(b *testing.B) {
	for i := 0; i < b.N; i++ {
		slice := acquireNodeSlice()
		for j := 0; j < 100; j++ {
			*slice = append(*slice, ontology.Node{ID: fmt.Sprintf("n%d", j)})
		}
		releaseNodeSlice(slice)
	}
}

func BenchmarkWithoutPool(b *testing.B) {
	for i := 0; i < b.N; i++ {
		slice := make([]ontology.Node, 0, 100)
		for j := 0; j < 100; j++ {
			slice = append(slice, ontology.Node{ID: fmt.Sprintf("n%d", j)})
		}
		_ = slice
	}
}

// ---------------------------------------------------------------------------
// History Recording Benchmarks
// ---------------------------------------------------------------------------

func BenchmarkRecordDelay(b *testing.B) {
	h := NewDelayHistory()
	now := time.Now()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Record(
			fmt.Sprintf("AP%03d", i%50),
			fmt.Sprintf("AP%03d", (i+1)%50),
			"UAL",
			now,
			time.Duration(i%60)*time.Minute,
		)
	}
}

func BenchmarkGetRouteAvgDelay(b *testing.B) {
	h := NewDelayHistory()
	now := time.Now()
	
	// Populate
	for i := 0; i < 10000; i++ {
		h.Record(
			fmt.Sprintf("AP%03d", i%50),
			fmt.Sprintf("AP%03d", (i+1)%50),
			"UAL",
			now,
			time.Duration(i%60)*time.Minute,
		)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.GetRouteAvgDelay("AP000", "AP001")
	}
}

// ---------------------------------------------------------------------------
// Concurrent Access Benchmarks
// ---------------------------------------------------------------------------

func BenchmarkConcurrentReads(b *testing.B) {
	_, qe := populateBenchOntology(10000, 50)
	
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			qe.GetFlightsByAirport(fmt.Sprintf("AP%03d", i%50), TimeRange{}, 10)
			i++
		}
	})
}

func BenchmarkConcurrentDelayedFlights(b *testing.B) {
	_, qe := populateBenchOntology(10000, 50)
	
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			qe.GetDelayedFlights(15*time.Minute, "", 10)
		}
	})
}

// ---------------------------------------------------------------------------
// P99 Latency Verification
// ---------------------------------------------------------------------------

func BenchmarkP99Latency(b *testing.B) {
	_, qe := populateBenchOntology(10000, 50)
	
	// Populate history
	baseTime := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 1000; i++ {
		qe.RecordDelay("AP000", "AP001", "UAL", baseTime, time.Duration(i%30)*time.Minute)
	}
	
	operations := []struct {
		name string
		fn   func()
	}{
		{"GetFlightsByAirport", func() { qe.GetFlightsByAirport("AP000", TimeRange{}, 10) }},
		{"GetDelayedFlights", func() { qe.GetDelayedFlights(15*time.Minute, "", 10) }},
		{"GetFlightPath", func() { qe.GetFlightPath("fl-0") }},
		{"PredictDelay", func() { qe.PredictDelay("fl-0") }},
	}
	
	for _, op := range operations {
		b.Run(op.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				op.fn()
			}
		})
	}
}
