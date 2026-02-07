package metrics

import (
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// Prometheus-compatible Metrics Registry
// ---------------------------------------------------------------------------

// Registry holds all application metrics.
type Registry struct {
	mu       sync.RWMutex
	counters map[string]*Counter
	gauges   map[string]*Gauge
	histos   map[string]*Histogram

	// Built-in runtime metrics
	startTime time.Time
}

// NewRegistry creates a new metrics registry.
func NewRegistry() *Registry {
	return &Registry{
		counters:  make(map[string]*Counter),
		gauges:    make(map[string]*Gauge),
		histos:    make(map[string]*Histogram),
		startTime: time.Now(),
	}
}

// Counter returns or creates a counter metric.
func (r *Registry) Counter(name, help string) *Counter {
	r.mu.Lock()
	defer r.mu.Unlock()

	if c, ok := r.counters[name]; ok {
		return c
	}
	c := &Counter{name: name, help: help}
	r.counters[name] = c
	return c
}

// Gauge returns or creates a gauge metric.
func (r *Registry) Gauge(name, help string) *Gauge {
	r.mu.Lock()
	defer r.mu.Unlock()

	if g, ok := r.gauges[name]; ok {
		return g
	}
	g := &Gauge{name: name, help: help}
	r.gauges[name] = g
	return g
}

// Histogram returns or creates a histogram metric.
func (r *Registry) Histogram(name, help string, buckets []float64) *Histogram {
	r.mu.Lock()
	defer r.mu.Unlock()

	if h, ok := r.histos[name]; ok {
		return h
	}
	h := NewHistogram(name, help, buckets)
	r.histos[name] = h
	return h
}

// Export returns all metrics in Prometheus text format.
func (r *Registry) Export() string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var out string

	// Runtime metrics
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	out += "# HELP go_memstats_alloc_bytes Number of bytes allocated and still in use.\n"
	out += "# TYPE go_memstats_alloc_bytes gauge\n"
	out += fmt.Sprintf("go_memstats_alloc_bytes %d\n", memStats.Alloc)

	out += "# HELP go_memstats_heap_alloc_bytes Number of heap bytes allocated and still in use.\n"
	out += "# TYPE go_memstats_heap_alloc_bytes gauge\n"
	out += fmt.Sprintf("go_memstats_heap_alloc_bytes %d\n", memStats.HeapAlloc)

	out += "# HELP go_memstats_heap_sys_bytes Number of heap bytes obtained from system.\n"
	out += "# TYPE go_memstats_heap_sys_bytes gauge\n"
	out += fmt.Sprintf("go_memstats_heap_sys_bytes %d\n", memStats.HeapSys)

	out += "# HELP go_memstats_heap_inuse_bytes Number of heap bytes in use.\n"
	out += "# TYPE go_memstats_heap_inuse_bytes gauge\n"
	out += fmt.Sprintf("go_memstats_heap_inuse_bytes %d\n", memStats.HeapInuse)

	out += "# HELP go_memstats_stack_inuse_bytes Number of stack bytes in use.\n"
	out += "# TYPE go_memstats_stack_inuse_bytes gauge\n"
	out += fmt.Sprintf("go_memstats_stack_inuse_bytes %d\n", memStats.StackInuse)

	out += "# HELP go_memstats_sys_bytes Number of bytes obtained from system.\n"
	out += "# TYPE go_memstats_sys_bytes gauge\n"
	out += fmt.Sprintf("go_memstats_sys_bytes %d\n", memStats.Sys)

	out += "# HELP go_gc_duration_seconds Summary of GC pause durations.\n"
	out += "# TYPE go_gc_duration_seconds gauge\n"
	out += fmt.Sprintf("go_gc_duration_seconds %f\n", float64(memStats.PauseTotalNs)/1e9)

	out += "# HELP go_goroutines Number of goroutines.\n"
	out += "# TYPE go_goroutines gauge\n"
	out += fmt.Sprintf("go_goroutines %d\n", runtime.NumGoroutine())

	out += "# HELP go_threads Number of OS threads.\n"
	out += "# TYPE go_threads gauge\n"
	out += fmt.Sprintf("go_threads %d\n", runtime.GOMAXPROCS(0))

	out += "# HELP process_uptime_seconds Time since process start.\n"
	out += "# TYPE process_uptime_seconds gauge\n"
	out += fmt.Sprintf("process_uptime_seconds %f\n", time.Since(r.startTime).Seconds())

	// Application counters
	for _, c := range r.counters {
		out += fmt.Sprintf("# HELP %s %s\n", c.name, c.help)
		out += fmt.Sprintf("# TYPE %s counter\n", c.name)
		out += fmt.Sprintf("%s %d\n", c.name, c.value.Load())
	}

	// Application gauges
	for _, g := range r.gauges {
		out += fmt.Sprintf("# HELP %s %s\n", g.name, g.help)
		out += fmt.Sprintf("# TYPE %s gauge\n", g.name)
		out += fmt.Sprintf("%s %f\n", g.name, g.Get())
	}

	// Application histograms
	for _, h := range r.histos {
		out += h.Export()
	}

	return out
}

// ---------------------------------------------------------------------------
// Counter
// ---------------------------------------------------------------------------

// Counter is a monotonically increasing metric.
type Counter struct {
	name  string
	help  string
	value atomic.Int64
}

// Inc increments the counter by 1.
func (c *Counter) Inc() {
	c.value.Add(1)
}

// Add adds the given value to the counter.
func (c *Counter) Add(v int64) {
	c.value.Add(v)
}

// Value returns the current counter value.
func (c *Counter) Value() int64 {
	return c.value.Load()
}

// ---------------------------------------------------------------------------
// Gauge
// ---------------------------------------------------------------------------

// Gauge is a metric that can go up and down.
type Gauge struct {
	name  string
	help  string
	bits  atomic.Uint64
}

// Set sets the gauge to the given value.
func (g *Gauge) Set(v float64) {
	g.bits.Store(float64ToBits(v))
}

// Inc increments the gauge by 1.
func (g *Gauge) Inc() {
	g.Add(1)
}

// Dec decrements the gauge by 1.
func (g *Gauge) Dec() {
	g.Add(-1)
}

// Add adds the given value to the gauge.
func (g *Gauge) Add(v float64) {
	for {
		old := g.bits.Load()
		newVal := bitsToFloat64(old) + v
		if g.bits.CompareAndSwap(old, float64ToBits(newVal)) {
			return
		}
	}
}

// Get returns the current gauge value.
func (g *Gauge) Get() float64 {
	return bitsToFloat64(g.bits.Load())
}

// ---------------------------------------------------------------------------
// Histogram
// ---------------------------------------------------------------------------

// Histogram tracks value distributions.
type Histogram struct {
	name    string
	help    string
	buckets []float64
	counts  []atomic.Int64
	sum     atomic.Int64
	count   atomic.Int64
}

// NewHistogram creates a histogram with the given buckets.
func NewHistogram(name, help string, buckets []float64) *Histogram {
	return &Histogram{
		name:    name,
		help:    help,
		buckets: buckets,
		counts:  make([]atomic.Int64, len(buckets)),
	}
}

// Observe records a value.
func (h *Histogram) Observe(v float64) {
	// Update bucket counts
	for i, bound := range h.buckets {
		if v <= bound {
			h.counts[i].Add(1)
		}
	}

	// Update sum and count
	h.sum.Add(int64(v * 1e6)) // Store as microseconds for precision
	h.count.Add(1)
}

// Export returns the histogram in Prometheus format.
func (h *Histogram) Export() string {
	var out string

	out += fmt.Sprintf("# HELP %s %s\n", h.name, h.help)
	out += fmt.Sprintf("# TYPE %s histogram\n", h.name)

	for i, bound := range h.buckets {
		out += fmt.Sprintf("%s_bucket{le=\"%g\"} %d\n", h.name, bound, h.counts[i].Load())
	}
	out += fmt.Sprintf("%s_bucket{le=\"+Inf\"} %d\n", h.name, h.count.Load())
	out += fmt.Sprintf("%s_sum %f\n", h.name, float64(h.sum.Load())/1e6)
	out += fmt.Sprintf("%s_count %d\n", h.name, h.count.Load())

	return out
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func float64ToBits(f float64) uint64 {
	return math.Float64bits(f)
}

func bitsToFloat64(b uint64) float64 {
	return math.Float64frombits(b)
}

// ---------------------------------------------------------------------------
// Default Registry
// ---------------------------------------------------------------------------

var defaultRegistry = NewRegistry()

// Default returns the default metrics registry.
func Default() *Registry {
	return defaultRegistry
}

// ---------------------------------------------------------------------------
// Pre-defined Application Metrics
// ---------------------------------------------------------------------------

var (
	// Ingestion metrics
	IngestionRequests     = defaultRegistry.Counter("flightedge_ingestion_requests_total", "Total number of ingestion requests")
	IngestionErrors       = defaultRegistry.Counter("flightedge_ingestion_errors_total", "Total number of ingestion errors")
	IngestionFlights      = defaultRegistry.Counter("flightedge_ingestion_flights_total", "Total flights ingested")
	IngestionLatency      = defaultRegistry.Histogram("flightedge_ingestion_latency_seconds", "Ingestion request latency", []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10})

	// Ontology metrics
	OntologyNodes         = defaultRegistry.Gauge("flightedge_ontology_nodes", "Number of nodes in ontology")
	OntologyEdges         = defaultRegistry.Gauge("flightedge_ontology_edges", "Number of edges in ontology")
	OntologyFlights       = defaultRegistry.Gauge("flightedge_ontology_flights", "Number of flight nodes")
	OntologyAirports      = defaultRegistry.Gauge("flightedge_ontology_airports", "Number of airport nodes")

	// Query metrics
	QueryRequests         = defaultRegistry.Counter("flightedge_query_requests_total", "Total number of query requests")
	QueryErrors           = defaultRegistry.Counter("flightedge_query_errors_total", "Total number of query errors")
	QueryLatency          = defaultRegistry.Histogram("flightedge_query_latency_seconds", "Query latency", []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25})

	// HTTP metrics
	HTTPRequests          = defaultRegistry.Counter("flightedge_http_requests_total", "Total HTTP requests")
	HTTPLatency           = defaultRegistry.Histogram("flightedge_http_latency_seconds", "HTTP request latency", []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1})

	// System metrics
	ActiveConnections     = defaultRegistry.Gauge("flightedge_active_connections", "Number of active connections")
)
