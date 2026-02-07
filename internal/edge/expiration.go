package edge

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// Data Expiration Manager
// ---------------------------------------------------------------------------

// ExpirableNode represents a node that can expire.
type ExpirableNode struct {
	ID        string
	Timestamp time.Time
}

// ExpirationCallback is called when nodes should be expired.
type ExpirationCallback func(nodeIDs []string) int

// ExpirationManager handles automatic data expiration.
type ExpirationManager struct {
	config Config

	mu            sync.RWMutex
	nodeTimestamps map[string]time.Time // Node ID -> creation/update time
	callback      ExpirationCallback

	// Stats
	totalExpired atomic.Int64
	lastCleanup  atomic.Int64 // Unix timestamp

	running atomic.Bool
	cancel  context.CancelFunc
}

// NewExpirationManager creates an expiration manager.
func NewExpirationManager(cfg Config) *ExpirationManager {
	return &ExpirationManager{
		config:         cfg,
		nodeTimestamps: make(map[string]time.Time, cfg.BufferSizes().IndexCapacity),
	}
}

// SetCallback sets the function to call when expiring nodes.
func (e *ExpirationManager) SetCallback(cb ExpirationCallback) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.callback = cb
}

// RecordNode records a node's timestamp for expiration tracking.
func (e *ExpirationManager) RecordNode(nodeID string, timestamp time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.nodeTimestamps[nodeID] = timestamp
}

// RecordNodeNow records a node with the current timestamp.
func (e *ExpirationManager) RecordNodeNow(nodeID string) {
	e.RecordNode(nodeID, time.Now())
}

// RemoveNode removes a node from expiration tracking.
func (e *ExpirationManager) RemoveNode(nodeID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.nodeTimestamps, nodeID)
}

// Start begins the expiration background process.
func (e *ExpirationManager) Start(ctx context.Context) {
	if e.config.DataRetentionHours <= 0 {
		log.Println("Data expiration disabled (retention = 0)")
		return
	}

	if e.running.Swap(true) {
		return
	}

	ctx, e.cancel = context.WithCancel(ctx)
	go e.expirationLoop(ctx)

	log.Printf("Data expiration started (retention: %d hours)", e.config.DataRetentionHours)
}

// Stop halts the expiration process.
func (e *ExpirationManager) Stop() {
	if e.cancel != nil {
		e.cancel()
	}
	e.running.Store(false)
}

// RunCleanupNow forces an immediate cleanup cycle.
func (e *ExpirationManager) RunCleanupNow() int {
	return e.cleanup()
}

// Stats returns expiration statistics.
func (e *ExpirationManager) Stats() ExpirationStats {
	e.mu.RLock()
	trackedNodes := len(e.nodeTimestamps)
	e.mu.RUnlock()

	return ExpirationStats{
		TrackedNodes:  trackedNodes,
		TotalExpired:  e.totalExpired.Load(),
		LastCleanup:   time.Unix(e.lastCleanup.Load(), 0),
		RetentionHours: e.config.DataRetentionHours,
	}
}

// ExpirationStats holds expiration statistics.
type ExpirationStats struct {
	TrackedNodes   int
	TotalExpired   int64
	LastCleanup    time.Time
	RetentionHours int
}

func (e *ExpirationManager) expirationLoop(ctx context.Context) {
	// Run cleanup every 1/12 of retention period (e.g., every 30 min for 6 hour retention)
	interval := time.Duration(e.config.DataRetentionHours) * time.Hour / 12
	if interval < time.Minute {
		interval = time.Minute
	}
	if interval > 30*time.Minute {
		interval = 30 * time.Minute
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Initial cleanup after short delay
	time.Sleep(10 * time.Second)
	e.cleanup()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.cleanup()
		}
	}
}

func (e *ExpirationManager) cleanup() int {
	retention := e.config.RetentionDuration()
	if retention <= 0 {
		return 0
	}

	cutoff := time.Now().Add(-retention)

	e.mu.Lock()
	var expired []string
	for id, ts := range e.nodeTimestamps {
		if ts.Before(cutoff) {
			expired = append(expired, id)
		}
	}

	// Remove from tracking
	for _, id := range expired {
		delete(e.nodeTimestamps, id)
	}

	callback := e.callback
	e.mu.Unlock()

	if len(expired) == 0 {
		e.lastCleanup.Store(time.Now().Unix())
		return 0
	}

	// Call the expiration callback
	actualExpired := 0
	if callback != nil {
		actualExpired = callback(expired)
	}

	e.totalExpired.Add(int64(actualExpired))
	e.lastCleanup.Store(time.Now().Unix())

	log.Printf("Expiration cleanup: identified=%d, expired=%d", len(expired), actualExpired)

	return actualExpired
}

// ---------------------------------------------------------------------------
// Node Limit Enforcer
// ---------------------------------------------------------------------------

// NodeLimitEnforcer enforces maximum node limits.
type NodeLimitEnforcer struct {
	config Config

	mu          sync.RWMutex
	nodesByTime []ExpirableNode // Ordered by timestamp (oldest first)
	nodeIndex   map[string]int  // Node ID -> index in slice

	onEvict func([]string) int
}

// NewNodeLimitEnforcer creates a node limit enforcer.
func NewNodeLimitEnforcer(cfg Config) *NodeLimitEnforcer {
	capacity := cfg.MaxNodes
	if capacity <= 0 {
		capacity = 10000
	}

	return &NodeLimitEnforcer{
		config:      cfg,
		nodesByTime: make([]ExpirableNode, 0, capacity),
		nodeIndex:   make(map[string]int, capacity),
	}
}

// SetEvictCallback sets the eviction callback.
func (n *NodeLimitEnforcer) SetEvictCallback(fn func([]string) int) {
	n.onEvict = fn
}

// RecordNode records a node for limit enforcement.
func (n *NodeLimitEnforcer) RecordNode(nodeID string, timestamp time.Time) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Check if already tracked
	if _, exists := n.nodeIndex[nodeID]; exists {
		// Update timestamp - for simplicity, we don't reorder
		return
	}

	// Add new node
	n.nodesByTime = append(n.nodesByTime, ExpirableNode{ID: nodeID, Timestamp: timestamp})
	n.nodeIndex[nodeID] = len(n.nodesByTime) - 1
}

// RemoveNode removes a node from tracking.
func (n *NodeLimitEnforcer) RemoveNode(nodeID string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if idx, exists := n.nodeIndex[nodeID]; exists {
		// Mark as removed (lazy cleanup)
		n.nodesByTime[idx].ID = ""
		delete(n.nodeIndex, nodeID)
	}
}

// ShouldEvict returns true if we need to evict nodes.
func (n *NodeLimitEnforcer) ShouldEvict() bool {
	if n.config.MaxNodes <= 0 {
		return false
	}

	n.mu.RLock()
	defer n.mu.RUnlock()

	return len(n.nodeIndex) >= n.config.MaxNodes
}

// EvictOldest evicts the oldest N nodes.
func (n *NodeLimitEnforcer) EvictOldest(count int) int {
	n.mu.Lock()

	var toEvict []string
	evicted := 0

	for i := 0; i < len(n.nodesByTime) && evicted < count; i++ {
		node := n.nodesByTime[i]
		if node.ID == "" {
			continue // Already removed
		}

		toEvict = append(toEvict, node.ID)
		n.nodesByTime[i].ID = "" // Mark as removed
		delete(n.nodeIndex, node.ID)
		evicted++
	}

	callback := n.onEvict
	n.mu.Unlock()

	if len(toEvict) > 0 && callback != nil {
		return callback(toEvict)
	}

	return evicted
}

// Count returns the number of tracked nodes.
func (n *NodeLimitEnforcer) Count() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return len(n.nodeIndex)
}

// Compact removes holes in the slice (call periodically).
func (n *NodeLimitEnforcer) Compact() {
	n.mu.Lock()
	defer n.mu.Unlock()

	newSlice := make([]ExpirableNode, 0, len(n.nodeIndex))
	newIndex := make(map[string]int, len(n.nodeIndex))

	for _, node := range n.nodesByTime {
		if node.ID != "" {
			newIndex[node.ID] = len(newSlice)
			newSlice = append(newSlice, node)
		}
	}

	n.nodesByTime = newSlice
	n.nodeIndex = newIndex
}
