package benchmarks

import (
	"fmt"
	"testing"

	"github.com/yash/flightedge/internal/ontology"
	"github.com/yash/flightedge/internal/query"
)

func populateEngine(n int) *ontology.Engine {
	eng := ontology.New()
	for i := 0; i < n; i++ {
		node := ontology.Node{
			ID:   fmt.Sprintf("flight-%d", i),
			Type: ontology.TypeFlight,
		}
		node.SetString("callsign", fmt.Sprintf("FL%04d", i))
		node.SetString("origin_country", "US")
		node.SetString("flight_id", fmt.Sprintf("FID-%d", i))
		eng.AddNode(node)
	}
	return eng
}

func BenchmarkAddNode(b *testing.B) {
	eng := ontology.New()
	for i := 0; i < b.N; i++ {
		node := ontology.Node{
			ID:   fmt.Sprintf("n-%d", i),
			Type: ontology.TypeFlight,
		}
		node.SetString("callsign", "TEST")
		eng.AddNode(node)
	}
}

func BenchmarkGetNode(b *testing.B) {
	eng := populateEngine(10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eng.GetNode(fmt.Sprintf("flight-%d", i%10000))
	}
}

func BenchmarkGetFlightByID(b *testing.B) {
	eng := populateEngine(10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eng.GetFlightByID(fmt.Sprintf("FID-%d", i%10000))
	}
}

func BenchmarkPropLookup(b *testing.B) {
	eng := populateEngine(10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eng.Prop(fmt.Sprintf("flight-%d", i%10000), "callsign")
	}
}

func BenchmarkFindByType(b *testing.B) {
	for _, size := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("nodes=%d", size), func(b *testing.B) {
			eng := populateEngine(size)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				eng.FindByType(ontology.TypeFlight)
			}
		})
	}
}

func BenchmarkForEachNodeOfType(b *testing.B) {
	for _, size := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("nodes=%d", size), func(b *testing.B) {
			eng := populateEngine(size)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				eng.ForEachNodeOfType(ontology.TypeFlight, func(n *ontology.Node) bool {
					return true
				})
			}
		})
	}
}

func BenchmarkQueryExecute(b *testing.B) {
	for _, size := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("nodes=%d", size), func(b *testing.B) {
			ont := populateEngine(size)
			qe := query.New(ont)
			req := query.Request{
				NodeType:   ontology.TypeFlight,
				Filters:    map[string]string{"origin_country": "US"},
				MaxResults: 10,
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				qe.Execute(req)
			}
		})
	}
}
