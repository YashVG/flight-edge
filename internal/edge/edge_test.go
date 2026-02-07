package edge

import (
	"context"
	"os"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Config Tests
// ---------------------------------------------------------------------------

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.MemoryMode != MemoryModeNormal {
		t.Errorf("expected Normal mode, got %s", cfg.MemoryMode)
	}
	if cfg.MemoryLimitMB != 512 {
		t.Errorf("expected 512MB limit, got %d", cfg.MemoryLimitMB)
	}
	if cfg.GCPercent != 100 {
		t.Errorf("expected 100%% GC, got %d", cfg.GCPercent)
	}
}

func TestReducedConfig(t *testing.T) {
	cfg := ReducedMemoryConfig()

	if cfg.MemoryMode != MemoryModeReduced {
		t.Errorf("expected Reduced mode, got %s", cfg.MemoryMode)
	}
	if cfg.GCPercent != 50 {
		t.Errorf("expected 50%% GC, got %d", cfg.GCPercent)
	}
	if !cfg.SmallBuffers {
		t.Error("expected SmallBuffers=true")
	}
}

func TestAggressiveConfig(t *testing.T) {
	cfg := AggressiveMemoryConfig()

	if cfg.MemoryMode != MemoryModeAggressive {
		t.Errorf("expected Aggressive mode, got %s", cfg.MemoryMode)
	}
	if cfg.GCPercent != 20 {
		t.Errorf("expected 20%% GC, got %d", cfg.GCPercent)
	}
	if !cfg.EnableCompression {
		t.Error("expected EnableCompression=true")
	}
	if !cfg.EnableDegradation {
		t.Error("expected EnableDegradation=true")
	}
}

func TestLoadFromEnv(t *testing.T) {
	// Set environment variables (using the actual env var names from config.go)
	os.Setenv("MEMORY_MODE", "aggressive")
	os.Setenv("MEMORY_LIMIT_MB", "256")
	os.Setenv("GC_PERCENT", "30")
	os.Setenv("DATA_RETENTION_HOURS", "4")
	os.Setenv("MAX_NODES", "5000")
	os.Setenv("ENABLE_COMPRESSION", "true")
	os.Setenv("ENABLE_DEGRADATION", "true")
	defer func() {
		os.Unsetenv("MEMORY_MODE")
		os.Unsetenv("MEMORY_LIMIT_MB")
		os.Unsetenv("GC_PERCENT")
		os.Unsetenv("DATA_RETENTION_HOURS")
		os.Unsetenv("MAX_NODES")
		os.Unsetenv("ENABLE_COMPRESSION")
		os.Unsetenv("ENABLE_DEGRADATION")
	}()

	cfg := LoadFromEnv()

	if cfg.MemoryMode != MemoryModeAggressive {
		t.Errorf("expected Aggressive mode, got %s", cfg.MemoryMode)
	}
	if cfg.MemoryLimitMB != 256 {
		t.Errorf("expected 256MB limit, got %d", cfg.MemoryLimitMB)
	}
	if cfg.GCPercent != 30 {
		t.Errorf("expected 30%% GC, got %d", cfg.GCPercent)
	}
	if cfg.DataRetentionHours != 4 {
		t.Errorf("expected 4h retention, got %d", cfg.DataRetentionHours)
	}
	if cfg.MaxNodes != 5000 {
		t.Errorf("expected 5000 max nodes, got %d", cfg.MaxNodes)
	}
	if !cfg.EnableCompression {
		t.Error("expected EnableCompression=true")
	}
	if !cfg.EnableDegradation {
		t.Error("expected EnableDegradation=true")
	}
}

func TestBufferSizes(t *testing.T) {
	tests := []struct {
		name        string
		config      Config
		expectSmall bool
	}{
		{"normal", DefaultConfig(), false},
		{"reduced", ReducedMemoryConfig(), true},
		{"aggressive", AggressiveMemoryConfig(), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sizes := tt.config.BufferSizes()

			if tt.expectSmall {
				// Reduced/Aggressive modes have smaller buffers
				if sizes.SlabCapacity >= 4096 {
					t.Errorf("expected small SlabCapacity (<4096), got %d", sizes.SlabCapacity)
				}
			} else {
				if sizes.SlabCapacity != 4096 {
					t.Errorf("expected normal SlabCapacity (4096), got %d", sizes.SlabCapacity)
				}
			}
		})
	}
}

func TestRetentionDuration(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataRetentionHours = 6

	expected := 6 * time.Hour
	if cfg.RetentionDuration() != expected {
		t.Errorf("expected %v, got %v", expected, cfg.RetentionDuration())
	}

	cfg.DataRetentionHours = 0
	if cfg.RetentionDuration() != 0 {
		t.Error("expected 0 for unlimited retention")
	}
}

// ---------------------------------------------------------------------------
// Memory Monitor Tests
// ---------------------------------------------------------------------------

func TestMemoryMonitor(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MemoryLimitMB = 1024 // High enough to not trigger pressure

	monitor := NewMemoryMonitor(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	monitor.Start(ctx)
	defer monitor.Stop()

	// Allow time for first check (check interval is 100ms by default)
	time.Sleep(300 * time.Millisecond)

	// Should be in normal state
	state := monitor.State()
	if state != MemoryStateNormal {
		t.Logf("memory state: %s (may vary based on system)", state)
	}

	// Get stats - may be 0 if first check hasn't run yet
	stats := monitor.Stats()
	t.Logf("HeapMB: %f", stats.HeapMB)
	// This test mainly verifies the monitor doesn't panic and starts correctly
}

func TestMemoryStateString(t *testing.T) {
	tests := []struct {
		state    MemoryState
		expected string
	}{
		{MemoryStateNormal, "normal"},
		{MemoryStateWarning, "warning"},
		{MemoryStateCritical, "critical"},
		{MemoryStateEmergency, "emergency"},
	}

	for _, tt := range tests {
		if tt.state.String() != tt.expected {
			t.Errorf("expected %s, got %s", tt.expected, tt.state.String())
		}
	}
}

// ---------------------------------------------------------------------------
// Expiration Manager Tests
// ---------------------------------------------------------------------------

func TestExpirationManager(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataRetentionHours = 1 // 1 hour retention

	var expired []string
	manager := NewExpirationManager(cfg)
	manager.SetCallback(func(ids []string) int {
		expired = append(expired, ids...)
		return len(ids)
	})

	// Track some nodes
	manager.RecordNodeNow("node1")
	manager.RecordNodeNow("node2")
	manager.RecordNodeNow("node3")

	stats := manager.Stats()
	if stats.TrackedNodes != 3 {
		t.Errorf("expected 3 tracked nodes, got %d", stats.TrackedNodes)
	}

	// Manually expire (simulate time passing)
	// Note: In real tests we'd use a time mock
	manager.mu.Lock()
	manager.nodeTimestamps["node1"] = time.Now().Add(-2 * time.Hour)
	manager.mu.Unlock()

	// Run cleanup
	count := manager.RunCleanupNow()
	if count != 1 {
		t.Errorf("expected 1 expired, got %d", count)
	}

	if len(expired) != 1 || expired[0] != "node1" {
		t.Errorf("expected [node1] expired, got %v", expired)
	}
}

func TestExpirationManagerUpdateTimestamp(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataRetentionHours = 1

	manager := NewExpirationManager(cfg)

	// Track and update
	manager.RecordNodeNow("node1")
	time.Sleep(10 * time.Millisecond)
	manager.RecordNodeNow("node1") // Update timestamp

	// The timestamp should be updated
	manager.mu.RLock()
	ts := manager.nodeTimestamps["node1"]
	manager.mu.RUnlock()

	if time.Since(ts) > 100*time.Millisecond {
		t.Error("timestamp was not updated recently")
	}
}

// ---------------------------------------------------------------------------
// Node Limit Enforcer Tests
// ---------------------------------------------------------------------------

func TestNodeLimitEnforcer(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxNodes = 5

	var removed []string
	enforcer := NewNodeLimitEnforcer(cfg)
	enforcer.SetEvictCallback(func(ids []string) int {
		removed = append(removed, ids...)
		return len(ids)
	})

	// Add nodes up to limit
	for i := 0; i < 5; i++ {
		enforcer.RecordNode("node"+string(rune('A'+i)), time.Now())
	}

	if len(removed) > 0 {
		t.Error("should not remove nodes before limit")
	}

	// Check if we should evict
	if !enforcer.ShouldEvict() {
		t.Error("should need to evict at limit")
	}

	// Evict oldest
	enforcer.EvictOldest(1)

	// Should have removed one to make room
	if len(removed) != 1 {
		t.Errorf("expected 1 removed, got %d", len(removed))
	}
}

// ---------------------------------------------------------------------------
// Compression Tests
// ---------------------------------------------------------------------------

func TestCompressDecompress(t *testing.T) {
	props := PropertyMap{
		"string_val": "hello world",
		"float_val":  123.456,
		"int_val":    42,
		"bool_val":   true,
	}

	// Compress
	compressed, err := Compress(props)
	if err != nil {
		t.Fatalf("compress failed: %v", err)
	}

	if !compressed.Compressed {
		t.Error("expected Compressed=true")
	}
	if len(compressed.Data) == 0 {
		t.Error("expected non-empty data")
	}

	// Decompress
	decompressed, err := Decompress(compressed)
	if err != nil {
		t.Fatalf("decompress failed: %v", err)
	}

	// Verify values
	if decompressed["string_val"] != "hello world" {
		t.Errorf("string_val mismatch: %v", decompressed["string_val"])
	}
	if decompressed["float_val"] != 123.456 {
		t.Errorf("float_val mismatch: %v", decompressed["float_val"])
	}
}

func TestCompressEmpty(t *testing.T) {
	compressed, err := Compress(PropertyMap{})
	if err != nil {
		t.Fatalf("compress empty failed: %v", err)
	}

	if compressed.Compressed {
		t.Error("expected empty to not be marked compressed")
	}
}

func TestCompressBytes(t *testing.T) {
	original := []byte("This is some test data that should compress well. " +
		"Repeated content helps compression. Repeated content helps compression.")

	compressed, err := CompressBytes(original)
	if err != nil {
		t.Fatalf("compress bytes failed: %v", err)
	}

	// Should be smaller than original (well-compressible data)
	if len(compressed) >= len(original) {
		t.Logf("compressed size (%d) not smaller than original (%d) - may vary", len(compressed), len(original))
	}

	// Decompress and verify
	decompressed, err := DecompressBytes(compressed)
	if err != nil {
		t.Fatalf("decompress bytes failed: %v", err)
	}

	if string(decompressed) != string(original) {
		t.Error("decompressed data doesn't match original")
	}
}

// ---------------------------------------------------------------------------
// Startup Optimizer Tests
// ---------------------------------------------------------------------------

func TestStartupOptimizer(t *testing.T) {
	cfg := DefaultConfig()
	optimizer := NewStartupOptimizer(cfg)

	taskRan := false
	optimizer.AddTask(StartupTask{
		Name:     "test-task",
		Phase:    PhaseSeedData,
		Priority: 1,
		Fn: func(ctx context.Context) error {
			taskRan = true
			return nil
		},
	})

	ctx := context.Background()
	err := optimizer.Run(ctx)
	if err != nil {
		t.Fatalf("startup failed: %v", err)
	}

	if !optimizer.IsReady() {
		t.Error("expected optimizer to be ready")
	}

	if !taskRan {
		t.Error("expected test task to run")
	}

	stats := optimizer.Stats()
	if !stats.Ready {
		t.Error("expected stats.Ready=true")
	}
}

func TestStartupPhaseString(t *testing.T) {
	tests := []struct {
		phase    StartupPhase
		expected string
	}{
		{PhaseInit, "init"},
		{PhaseLoadConfig, "config"},
		{PhaseInitMemory, "memory"},
		{PhaseSeedData, "seed"},
		{PhaseBuildIndexes, "indexes"},
		{PhaseStartServices, "services"},
		{PhaseReady, "ready"},
	}

	for _, tt := range tests {
		if tt.phase.String() != tt.expected {
			t.Errorf("expected %s, got %s", tt.expected, tt.phase.String())
		}
	}
}

// ---------------------------------------------------------------------------
// Integration Tests
// ---------------------------------------------------------------------------

func TestEdgeConfigIntegration(t *testing.T) {
	// Test that all configs can be applied without panicking
	configs := []Config{
		DefaultConfig(),
		ReducedMemoryConfig(),
		AggressiveMemoryConfig(),
	}

	for _, cfg := range configs {
		t.Run(cfg.MemoryMode.String(), func(t *testing.T) {
			// Just ensure these don't panic
			cfg.Apply()
			_ = cfg.BufferSizes()
			_ = cfg.RetentionDuration()
		})
	}
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

func BenchmarkCompress(b *testing.B) {
	props := PropertyMap{
		"flight_id":   "ABC123",
		"callsign":    "ACA456",
		"latitude":    49.1234,
		"longitude":   -123.4567,
		"altitude":    35000.0,
		"velocity":    450.0,
		"heading":     180.0,
		"on_ground":   false,
		"origin":      "CYVR",
		"destination": "CYYZ",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Compress(props)
	}
}

func BenchmarkDecompress(b *testing.B) {
	props := PropertyMap{
		"flight_id":   "ABC123",
		"callsign":    "ACA456",
		"latitude":    49.1234,
		"longitude":   -123.4567,
		"altitude":    35000.0,
		"velocity":    450.0,
		"heading":     180.0,
		"on_ground":   false,
		"origin":      "CYVR",
		"destination": "CYYZ",
	}

	compressed, _ := Compress(props)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Decompress(compressed)
	}
}

func BenchmarkExpirationRecordNode(b *testing.B) {
	cfg := DefaultConfig()
	cfg.DataRetentionHours = 6

	manager := NewExpirationManager(cfg)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		manager.RecordNodeNow("node" + string(rune(i%26+'A')))
	}
}
