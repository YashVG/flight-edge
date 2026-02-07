package edge

import (
	"context"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// Memory Monitor
// ---------------------------------------------------------------------------

// MemoryState represents the current memory pressure level.
type MemoryState int

const (
	// MemoryStateNormal - operating normally
	MemoryStateNormal MemoryState = iota

	// MemoryStateWarning - approaching soft limit
	MemoryStateWarning

	// MemoryStateCritical - at or above soft limit
	MemoryStateCritical

	// MemoryStateEmergency - approaching hard limit
	MemoryStateEmergency
)

func (s MemoryState) String() string {
	switch s {
	case MemoryStateNormal:
		return "normal"
	case MemoryStateWarning:
		return "warning"
	case MemoryStateCritical:
		return "critical"
	case MemoryStateEmergency:
		return "emergency"
	default:
		return "unknown"
	}
}

// MemoryStats holds current memory statistics.
type MemoryStats struct {
	AllocMB     float64
	HeapMB      float64
	SysMB       float64
	NumGC       uint32
	State       MemoryState
	UsageRatio  float64 // 0.0 - 1.0 of soft limit
}

// MemoryMonitor tracks memory usage and triggers actions when thresholds are reached.
type MemoryMonitor struct {
	config Config

	mu           sync.RWMutex
	currentState MemoryState
	stats        MemoryStats
	listeners    []MemoryListener

	// Atomic for fast path checks
	isWarning   atomic.Bool
	isCritical  atomic.Bool

	running     atomic.Bool
	cancel      context.CancelFunc
}

// MemoryListener is called when memory state changes.
type MemoryListener func(oldState, newState MemoryState, stats MemoryStats)

// NewMemoryMonitor creates a memory monitor with the given configuration.
func NewMemoryMonitor(cfg Config) *MemoryMonitor {
	return &MemoryMonitor{
		config:    cfg,
		listeners: make([]MemoryListener, 0),
	}
}

// AddListener adds a callback for memory state changes.
func (m *MemoryMonitor) AddListener(l MemoryListener) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.listeners = append(m.listeners, l)
}

// Start begins monitoring memory usage.
func (m *MemoryMonitor) Start(ctx context.Context) {
	if m.running.Swap(true) {
		return // Already running
	}

	ctx, m.cancel = context.WithCancel(ctx)
	go m.monitorLoop(ctx)
}

// Stop halts memory monitoring.
func (m *MemoryMonitor) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
	m.running.Store(false)
}

// Stats returns current memory statistics.
func (m *MemoryMonitor) Stats() MemoryStats {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.stats
}

// State returns the current memory state.
func (m *MemoryMonitor) State() MemoryState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.currentState
}

// IsWarning returns true if memory usage is elevated (fast path).
func (m *MemoryMonitor) IsWarning() bool {
	return m.isWarning.Load()
}

// IsCritical returns true if memory usage is critical (fast path).
func (m *MemoryMonitor) IsCritical() bool {
	return m.isCritical.Load()
}

// ShouldRejectWrites returns true if new data should be rejected.
func (m *MemoryMonitor) ShouldRejectWrites() bool {
	if !m.config.EnableDegradation {
		return false
	}
	if m.config.DegradationAction != DegradationRejectNew {
		return false
	}
	return m.isCritical.Load()
}

// ForceGC triggers a garbage collection if in warning state or above.
func (m *MemoryMonitor) ForceGC() {
	if m.IsWarning() {
		runtime.GC()
	}
}

func (m *MemoryMonitor) monitorLoop(ctx context.Context) {
	// Check interval based on memory mode
	interval := 5 * time.Second
	if m.config.MemoryMode == MemoryModeAggressive {
		interval = 2 * time.Second
	} else if m.config.MemoryMode == MemoryModeReduced {
		interval = 3 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.checkMemory()
		}
	}
}

func (m *MemoryMonitor) checkMemory() {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	softLimitBytes := float64(m.config.SoftLimitMB) * 1024 * 1024
	hardLimitBytes := float64(m.config.MemoryLimitMB) * 1024 * 1024

	heapMB := float64(memStats.HeapAlloc) / 1024 / 1024
	usageRatio := float64(memStats.HeapAlloc) / softLimitBytes

	// Determine state
	var newState MemoryState
	if float64(memStats.HeapAlloc) >= hardLimitBytes*0.95 {
		newState = MemoryStateEmergency
	} else if float64(memStats.HeapAlloc) >= softLimitBytes {
		newState = MemoryStateCritical
	} else if float64(memStats.HeapAlloc) >= softLimitBytes*0.8 {
		newState = MemoryStateWarning
	} else {
		newState = MemoryStateNormal
	}

	// Update stats
	stats := MemoryStats{
		AllocMB:    float64(memStats.Alloc) / 1024 / 1024,
		HeapMB:     heapMB,
		SysMB:      float64(memStats.Sys) / 1024 / 1024,
		NumGC:      memStats.NumGC,
		State:      newState,
		UsageRatio: usageRatio,
	}

	m.mu.Lock()
	oldState := m.currentState
	m.currentState = newState
	m.stats = stats
	listeners := make([]MemoryListener, len(m.listeners))
	copy(listeners, m.listeners)
	m.mu.Unlock()

	// Update atomic flags
	m.isWarning.Store(newState >= MemoryStateWarning)
	m.isCritical.Store(newState >= MemoryStateCritical)

	// Notify listeners on state change
	if oldState != newState {
		log.Printf("Memory state changed: %s -> %s (heap: %.1fMB, ratio: %.2f)",
			oldState, newState, heapMB, usageRatio)

		for _, l := range listeners {
			l(oldState, newState, stats)
		}

		// Take automatic action in emergency
		if newState == MemoryStateEmergency {
			log.Println("Emergency: triggering GC")
			runtime.GC()
		}
	}
}

// ---------------------------------------------------------------------------
// Memory Pressure Handler
// ---------------------------------------------------------------------------

// PressureHandler handles memory pressure situations.
type PressureHandler struct {
	monitor  *MemoryMonitor
	config   Config
	onEvict  func(count int) // Called when data needs to be evicted
}

// NewPressureHandler creates a pressure handler.
func NewPressureHandler(monitor *MemoryMonitor, cfg Config) *PressureHandler {
	return &PressureHandler{
		monitor: monitor,
		config:  cfg,
	}
}

// SetEvictCallback sets the function to call when data needs eviction.
func (p *PressureHandler) SetEvictCallback(fn func(count int)) {
	p.onEvict = fn
}

// HandlePressure handles memory pressure based on configuration.
func (p *PressureHandler) HandlePressure() {
	if !p.config.EnableDegradation {
		return
	}

	state := p.monitor.State()
	if state < MemoryStateCritical {
		return
	}

	switch p.config.DegradationAction {
	case DegradationDropOldest:
		// Evict 10% of max nodes
		evictCount := p.config.MaxNodes / 10
		if evictCount < 100 {
			evictCount = 100
		}
		if p.onEvict != nil {
			p.onEvict(evictCount)
		}

	case DegradationCompact:
		// Trigger GC
		runtime.GC()

	case DegradationRejectNew:
		// Handled by ShouldRejectWrites()
	}
}
