// Package edge provides the bounded-memory controls used by FlightEdge.
package edge

import (
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"time"
)

// MemoryMode selects conservative initial capacities for the in-memory core.
// It is a local resource-control setting, not a performance claim.
type MemoryMode int

const (
	MemoryModeNormal MemoryMode = iota
	MemoryModeReduced
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

func parseMemoryMode(s string) MemoryMode {
	switch s {
	case "reduced":
		return MemoryModeReduced
	case "aggressive":
		return MemoryModeAggressive
	default:
		return MemoryModeNormal
	}
}

// DegradationAction defines the response when the soft memory limit is hit.
type DegradationAction int

const (
	DegradationDropOldest DegradationAction = iota
	DegradationRejectNew
	DegradationCompact
)

// Config contains only controls wired into the running core.
type Config struct {
	MemoryMode         MemoryMode
	MemoryLimitMB      int
	GCPercent          int
	SoftLimitMB        int
	DataRetentionHours int
	MaxNodes           int
	MaxProcs           int
	EnableDegradation  bool
	DegradationAction  DegradationAction
}

func DefaultConfig() Config {
	return Config{
		MemoryMode:        MemoryModeNormal,
		MemoryLimitMB:     512,
		GCPercent:         100,
		SoftLimitMB:       450,
		DegradationAction: DegradationDropOldest,
	}
}

func ReducedMemoryConfig() Config {
	return Config{
		MemoryMode:         MemoryModeReduced,
		MemoryLimitMB:      512,
		GCPercent:          50,
		SoftLimitMB:        400,
		DataRetentionHours: 6,
		MaxNodes:           50000,
		MaxProcs:           1,
		EnableDegradation:  true,
		DegradationAction:  DegradationDropOldest,
	}
}

func AggressiveMemoryConfig() Config {
	return Config{
		MemoryMode:         MemoryModeAggressive,
		MemoryLimitMB:      256,
		GCPercent:          20,
		SoftLimitMB:        200,
		DataRetentionHours: 2,
		MaxNodes:           20000,
		MaxProcs:           1,
		EnableDegradation:  true,
		DegradationAction:  DegradationDropOldest,
	}
}

func LoadFromEnv() Config {
	cfg := DefaultConfig()
	if mode := os.Getenv("MEMORY_MODE"); mode != "" {
		switch parseMemoryMode(mode) {
		case MemoryModeReduced:
			cfg = ReducedMemoryConfig()
		case MemoryModeAggressive:
			cfg = AggressiveMemoryConfig()
		}
	}

	overrideInt := func(key string, target *int) {
		if value := os.Getenv(key); value != "" {
			if parsed, err := strconv.Atoi(value); err == nil {
				*target = parsed
			}
		}
	}
	overrideInt("MEMORY_LIMIT_MB", &cfg.MemoryLimitMB)
	overrideInt("GC_PERCENT", &cfg.GCPercent)
	overrideInt("SOFT_LIMIT_MB", &cfg.SoftLimitMB)
	overrideInt("DATA_RETENTION_HOURS", &cfg.DataRetentionHours)
	overrideInt("MAX_NODES", &cfg.MaxNodes)
	overrideInt("GOMAXPROCS", &cfg.MaxProcs)
	if os.Getenv("ENABLE_DEGRADATION") == "true" {
		cfg.EnableDegradation = true
	}
	return cfg
}

func (c Config) Apply() {
	if c.MaxProcs > 0 {
		runtime.GOMAXPROCS(c.MaxProcs)
	}
	if c.GCPercent > 0 {
		debug.SetGCPercent(c.GCPercent)
	}
	if c.MemoryLimitMB > 0 {
		debug.SetMemoryLimit(int64(c.MemoryLimitMB) * 1024 * 1024)
	}
}

// BufferSizes determines only initial allocation capacities. Growth remains
// dynamic and must be measured under the deployment workload.
func (c Config) BufferSizes() BufferSizes {
	var b BufferSizes
	switch c.MemoryMode {
	case MemoryModeAggressive:
		b = BufferSizes{SlabCapacity: 512, IndexCapacity: 256, ResultCapacity: 16, BatchSize: 50, ChannelBuffer: 10}
	case MemoryModeReduced:
		b = BufferSizes{SlabCapacity: 1024, IndexCapacity: 512, ResultCapacity: 32, BatchSize: 100, ChannelBuffer: 50}
	default:
		b = BufferSizes{SlabCapacity: 4096, IndexCapacity: 1024, ResultCapacity: 64, BatchSize: 100, ChannelBuffer: 100}
	}
	b.NodeSlab = b.SlabCapacity
	b.FlightIndex = b.IndexCapacity
	b.AirportIndex = b.IndexCapacity / 2
	return b
}

type BufferSizes struct {
	SlabCapacity   int
	IndexCapacity  int
	ResultCapacity int
	BatchSize      int
	ChannelBuffer  int
	NodeSlab       int
	FlightIndex    int
	AirportIndex   int
}

func (c Config) RetentionDuration() time.Duration {
	if c.DataRetentionHours <= 0 {
		return 0
	}
	return time.Duration(c.DataRetentionHours) * time.Hour
}
