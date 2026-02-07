// Package edge provides configuration and utilities for edge deployment scenarios
// with constrained resources (limited memory, CPU, storage).
package edge

import (
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"time"
)

// ---------------------------------------------------------------------------
// Memory Mode Configuration
// ---------------------------------------------------------------------------

// MemoryMode defines the memory usage strategy.
type MemoryMode int

const (
	// MemoryModeNormal uses default settings for best performance.
	// Suitable for systems with 1GB+ RAM.
	MemoryModeNormal MemoryMode = iota

	// MemoryModeReduced enables memory-saving optimizations with moderate
	// performance impact. Suitable for 512MB RAM.
	MemoryModeReduced

	// MemoryModeAggressive enables maximum memory savings with significant
	// performance trade-offs. Suitable for 256MB RAM or less.
	MemoryModeAggressive
)

func (m MemoryMode) String() string {
	switch m {
	case MemoryModeNormal:
		return "normal"
	case MemoryModeReduced:
		return "reduced"
	case MemoryModeAggressive:
		return "aggressive"
	default:
		return "unknown"
	}
}

// ParseMemoryMode parses a memory mode string.
func ParseMemoryMode(s string) MemoryMode {
	switch s {
	case "reduced":
		return MemoryModeReduced
	case "aggressive":
		return MemoryModeAggressive
	default:
		return MemoryModeNormal
	}
}

// ---------------------------------------------------------------------------
// Edge Configuration
// ---------------------------------------------------------------------------

// Config holds all edge deployment configuration.
type Config struct {
	// Memory management
	MemoryMode      MemoryMode
	MemoryLimitMB   int
	GCPercent       int
	SoftLimitMB     int // Trigger degradation at this threshold

	// Data retention
	DataRetentionHours int  // Keep data for N hours (0 = unlimited)
	MaxNodes           int  // Maximum nodes to retain (0 = unlimited)
	EnableCompression  bool // Compress historical data

	// Performance
	MaxProcs          int
	SmallBuffers      bool // Use smaller pre-allocated buffers
	LazyIndexing      bool // Defer index building for faster startup

	// Degradation
	EnableDegradation bool // Enable graceful degradation
	DegradationAction DegradationAction
}

// DegradationAction defines what to do when approaching limits.
type DegradationAction int

const (
	// DegradationDropOldest drops oldest data first.
	DegradationDropOldest DegradationAction = iota

	// DegradationRejectNew rejects new data when full.
	DegradationRejectNew

	// DegradationCompact triggers compaction/compression.
	DegradationCompact
)

// DefaultConfig returns default edge configuration.
func DefaultConfig() Config {
	return Config{
		MemoryMode:        MemoryModeNormal,
		MemoryLimitMB:     512,
		GCPercent:         100,
		SoftLimitMB:       450,
		DataRetentionHours: 0,
		MaxNodes:          0,
		EnableCompression: false,
		MaxProcs:          0,
		SmallBuffers:      false,
		LazyIndexing:      false,
		EnableDegradation: false,
		DegradationAction: DegradationDropOldest,
	}
}

// ReducedMemoryConfig returns configuration optimized for 512MB environments.
func ReducedMemoryConfig() Config {
	return Config{
		MemoryMode:         MemoryModeReduced,
		MemoryLimitMB:      512,
		GCPercent:          50,     // More frequent GC
		SoftLimitMB:        400,    // Earlier degradation trigger
		DataRetentionHours: 6,      // Keep 6 hours
		MaxNodes:           50000,  // Cap at 50K nodes
		EnableCompression:  true,
		MaxProcs:           1,
		SmallBuffers:       true,
		LazyIndexing:       false,
		EnableDegradation:  true,
		DegradationAction:  DegradationDropOldest,
	}
}

// AggressiveMemoryConfig returns configuration for severely constrained environments.
func AggressiveMemoryConfig() Config {
	return Config{
		MemoryMode:         MemoryModeAggressive,
		MemoryLimitMB:      256,
		GCPercent:          20,     // Very frequent GC
		SoftLimitMB:        200,    // Early degradation
		DataRetentionHours: 2,      // Keep only 2 hours
		MaxNodes:           20000,  // Cap at 20K nodes
		EnableCompression:  true,
		MaxProcs:           1,
		SmallBuffers:       true,
		LazyIndexing:       true,   // Faster startup
		EnableDegradation:  true,
		DegradationAction:  DegradationDropOldest,
	}
}

// LoadFromEnv loads configuration from environment variables.
func LoadFromEnv() Config {
	cfg := DefaultConfig()

	// Memory mode
	if v := os.Getenv("MEMORY_MODE"); v != "" {
		cfg.MemoryMode = ParseMemoryMode(v)
		// Apply preset
		switch cfg.MemoryMode {
		case MemoryModeReduced:
			cfg = ReducedMemoryConfig()
		case MemoryModeAggressive:
			cfg = AggressiveMemoryConfig()
		}
	}

	// Override individual settings
	if v := os.Getenv("MEMORY_LIMIT_MB"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.MemoryLimitMB = n
		}
	}
	if v := os.Getenv("GC_PERCENT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.GCPercent = n
		}
	}
	if v := os.Getenv("SOFT_LIMIT_MB"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.SoftLimitMB = n
		}
	}
	if v := os.Getenv("DATA_RETENTION_HOURS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.DataRetentionHours = n
		}
	}
	if v := os.Getenv("MAX_NODES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.MaxNodes = n
		}
	}
	if v := os.Getenv("ENABLE_COMPRESSION"); v == "true" {
		cfg.EnableCompression = true
	}
	if v := os.Getenv("SMALL_BUFFERS"); v == "true" {
		cfg.SmallBuffers = true
	}
	if v := os.Getenv("LAZY_INDEXING"); v == "true" {
		cfg.LazyIndexing = true
	}
	if v := os.Getenv("ENABLE_DEGRADATION"); v == "true" {
		cfg.EnableDegradation = true
	}
	if v := os.Getenv("GOMAXPROCS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.MaxProcs = n
		}
	}

	return cfg
}

// Apply applies the configuration to the runtime.
func (c Config) Apply() {
	// Set GOMAXPROCS
	if c.MaxProcs > 0 {
		runtime.GOMAXPROCS(c.MaxProcs)
	}

	// Set GC percent
	if c.GCPercent > 0 {
		debug.SetGCPercent(c.GCPercent)
	}

	// Set memory limit (Go 1.19+)
	if c.MemoryLimitMB > 0 {
		limit := int64(c.MemoryLimitMB) * 1024 * 1024
		debug.SetMemoryLimit(limit)
	}
}

// BufferSizes returns appropriate buffer sizes for this configuration.
func (c Config) BufferSizes() BufferSizes {
	var b BufferSizes

	switch c.MemoryMode {
	case MemoryModeAggressive:
		b = BufferSizes{
			SlabCapacity:   512,
			IndexCapacity:  256,
			ResultCapacity: 16,
			BatchSize:      50,
			ChannelBuffer:  10,
		}
	case MemoryModeReduced:
		b = BufferSizes{
			SlabCapacity:   1024,
			IndexCapacity:  512,
			ResultCapacity: 32,
			BatchSize:      100,
			ChannelBuffer:  50,
		}
	default:
		b = BufferSizes{
			SlabCapacity:   4096,
			IndexCapacity:  1024,
			ResultCapacity: 64,
			BatchSize:      100,
			ChannelBuffer:  100,
		}
	}

	// Set convenience aliases
	b.NodeSlab = b.SlabCapacity
	b.FlightIndex = b.IndexCapacity
	b.AirportIndex = b.IndexCapacity / 2 // Typically fewer airports than flights

	return b
}

// BufferSizes holds pre-allocation sizes.
type BufferSizes struct {
	SlabCapacity   int // Initial slot slice capacity
	IndexCapacity  int // Initial index map capacity
	ResultCapacity int // Query result slice capacity
	BatchSize      int // Batch processing size
	ChannelBuffer  int // Channel buffer sizes

	// Convenience aliases for ontology engine
	NodeSlab     int // Alias for SlabCapacity
	FlightIndex  int // Flight index capacity
	AirportIndex int // Airport index capacity
}

// RetentionDuration returns the data retention as a duration.
func (c Config) RetentionDuration() time.Duration {
	if c.DataRetentionHours <= 0 {
		return 0
	}
	return time.Duration(c.DataRetentionHours) * time.Hour
}

// ---------------------------------------------------------------------------
// Trade-off Documentation
// ---------------------------------------------------------------------------

/*
EDGE DEPLOYMENT TRADE-OFFS

This package provides configuration options for deploying FlightEdge in
resource-constrained edge environments. Understanding the trade-offs helps
choose the right configuration.

┌────────────────────┬────────────────────────────────────────────────────────┐
│ SETTING            │ TRADE-OFF                                              │
├────────────────────┼────────────────────────────────────────────────────────┤
│ GCPercent (low)    │ + Lower memory usage                                   │
│                    │ - Higher CPU usage, more GC pauses                     │
│                    │ - Slightly higher query latency (P99)                  │
├────────────────────┼────────────────────────────────────────────────────────┤
│ SmallBuffers       │ + Faster startup, lower initial memory                 │
│                    │ - More allocations as data grows                       │
│                    │ - Slightly higher sustained memory churn               │
├────────────────────┼────────────────────────────────────────────────────────┤
│ DataRetention      │ + Bounded memory usage                                 │
│ (limited)          │ - Historical queries limited to retention window       │
│                    │ - Background cleanup adds CPU overhead                 │
├────────────────────┼────────────────────────────────────────────────────────┤
│ MaxNodes (capped)  │ + Guaranteed memory ceiling                            │
│                    │ - Data loss when limit reached                         │
│                    │ - Requires degradation strategy                        │
├────────────────────┼────────────────────────────────────────────────────────┤
│ EnableCompression  │ + ~40% reduction in historical data size               │
│                    │ - CPU overhead for compression/decompression           │
│                    │ - Slightly higher query latency for compressed data    │
├────────────────────┼────────────────────────────────────────────────────────┤
│ LazyIndexing       │ + Faster cold start (<5s)                              │
│                    │ - First queries slower until indexes built             │
│                    │ - Temporary increase in query latency                  │
├────────────────────┼────────────────────────────────────────────────────────┤
│ EnableDegradation  │ + System remains responsive under pressure             │
│                    │ - May drop data or reject writes                       │
│                    │ - Query completeness not guaranteed                    │
└────────────────────┴────────────────────────────────────────────────────────┘

RECOMMENDED CONFIGURATIONS BY ENVIRONMENT:

┌─────────────────┬─────────┬─────────┬───────────┬──────────┬──────────────┐
│ Environment     │ RAM     │ Mode    │ Retention │ MaxNodes │ GCPercent    │
├─────────────────┼─────────┼─────────┼───────────┼──────────┼──────────────┤
│ Cloud/Server    │ 2GB+    │ normal  │ unlimited │ unlimited│ 100 (default)│
│ Edge Gateway    │ 512MB   │ reduced │ 6 hours   │ 50,000   │ 50           │
│ IoT Device      │ 256MB   │ aggress.│ 2 hours   │ 20,000   │ 20           │
│ Raspberry Pi    │ 1GB     │ reduced │ 12 hours  │ 100,000  │ 75           │
└─────────────────┴─────────┴─────────┴───────────┴──────────┴──────────────┘

PERFORMANCE IMPACT (approximate):

┌─────────────────┬───────────────┬───────────────┬───────────────┐
│ Metric          │ Normal Mode   │ Reduced Mode  │ Aggressive    │
├─────────────────┼───────────────┼───────────────┼───────────────┤
│ Cold Start      │ 3-5s          │ 2-3s          │ <2s           │
│ P50 Latency     │ 5-10µs        │ 10-20µs       │ 20-50µs       │
│ P99 Latency     │ <5ms          │ <10ms         │ <25ms         │
│ Memory (50K)    │ ~200MB        │ ~150MB        │ ~100MB        │
│ Throughput      │ 200+ evt/s    │ 150 evt/s     │ 100 evt/s     │
└─────────────────┴───────────────┴───────────────┴───────────────┘
*/
