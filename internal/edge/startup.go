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
// Startup Optimizer
// ---------------------------------------------------------------------------

// StartupPhase represents a phase of startup.
type StartupPhase int

const (
	PhaseInit StartupPhase = iota
	PhaseLoadConfig
	PhaseInitMemory
	PhaseSeedData
	PhaseBuildIndexes
	PhaseStartServices
	PhaseReady
)

func (p StartupPhase) String() string {
	switch p {
	case PhaseInit:
		return "init"
	case PhaseLoadConfig:
		return "config"
	case PhaseInitMemory:
		return "memory"
	case PhaseSeedData:
		return "seed"
	case PhaseBuildIndexes:
		return "indexes"
	case PhaseStartServices:
		return "services"
	case PhaseReady:
		return "ready"
	default:
		return "unknown"
	}
}

// StartupOptimizer optimizes application startup time.
type StartupOptimizer struct {
	config    Config
	startTime time.Time

	mu           sync.RWMutex
	phase        StartupPhase
	phaseTimes   map[StartupPhase]time.Duration
	errors       []error
	
	tasks        []StartupTask
	taskResults  chan taskResult

	ready        atomic.Bool
}

// StartupTask represents a startup task.
type StartupTask struct {
	Name     string
	Phase    StartupPhase
	Priority int // Lower = earlier
	Fn       func(context.Context) error
	Optional bool // Don't fail startup if this fails
	Async    bool // Can run in background after startup
}

type taskResult struct {
	Task  StartupTask
	Error error
	Time  time.Duration
}

// NewStartupOptimizer creates a startup optimizer.
func NewStartupOptimizer(cfg Config) *StartupOptimizer {
	return &StartupOptimizer{
		config:      cfg,
		startTime:   time.Now(),
		phaseTimes:  make(map[StartupPhase]time.Duration),
		taskResults: make(chan taskResult, 100),
	}
}

// AddTask adds a startup task.
func (s *StartupOptimizer) AddTask(task StartupTask) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks = append(s.tasks, task)
}

// SetPhase updates the current startup phase.
func (s *StartupOptimizer) SetPhase(phase StartupPhase) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	now := time.Now()
	if s.phase > PhaseInit {
		s.phaseTimes[s.phase] = now.Sub(s.startTime) - s.totalPreviousTime()
	}
	
	s.phase = phase
	log.Printf("Startup phase: %s (elapsed: %v)", phase, now.Sub(s.startTime))
	
	if phase == PhaseReady {
		s.ready.Store(true)
	}
}

func (s *StartupOptimizer) totalPreviousTime() time.Duration {
	var total time.Duration
	for _, d := range s.phaseTimes {
		total += d
	}
	return total
}

// Run executes the startup sequence.
func (s *StartupOptimizer) Run(ctx context.Context) error {
	deadline := 5 * time.Second
	if s.config.MemoryMode == MemoryModeAggressive {
		deadline = 3 * time.Second
	}
	
	ctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()

	s.SetPhase(PhaseLoadConfig)
	
	// Apply runtime configuration immediately
	s.config.Apply()
	
	s.SetPhase(PhaseInitMemory)
	
	// Pre-allocate memory in background if not in aggressive mode
	if s.config.MemoryMode != MemoryModeAggressive {
		runtime.GC() // Clean slate
	}
	
	// Sort tasks by priority and phase
	s.sortTasks()
	
	// Execute sync tasks by phase
	for phase := PhaseSeedData; phase <= PhaseStartServices; phase++ {
		s.SetPhase(phase)
		
		if err := s.runPhaseTasks(ctx, phase, false); err != nil {
			return err
		}
	}
	
	// Start async tasks in background
	go s.runAsyncTasks(context.Background())
	
	s.SetPhase(PhaseReady)
	
	totalTime := time.Since(s.startTime)
	log.Printf("Startup complete in %v", totalTime)
	
	if totalTime > deadline {
		log.Printf("WARNING: Startup exceeded target of %v", deadline)
	}
	
	return nil
}

func (s *StartupOptimizer) sortTasks() {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// Simple bubble sort (small list)
	for i := 0; i < len(s.tasks)-1; i++ {
		for j := 0; j < len(s.tasks)-i-1; j++ {
			if s.tasks[j].Phase > s.tasks[j+1].Phase ||
				(s.tasks[j].Phase == s.tasks[j+1].Phase && s.tasks[j].Priority > s.tasks[j+1].Priority) {
				s.tasks[j], s.tasks[j+1] = s.tasks[j+1], s.tasks[j]
			}
		}
	}
}

func (s *StartupOptimizer) runPhaseTasks(ctx context.Context, phase StartupPhase, async bool) error {
	s.mu.RLock()
	var phaseTasks []StartupTask
	for _, t := range s.tasks {
		if t.Phase == phase && t.Async == async {
			phaseTasks = append(phaseTasks, t)
		}
	}
	s.mu.RUnlock()
	
	if len(phaseTasks) == 0 {
		return nil
	}
	
	// Run tasks concurrently within phase (if multiple same-priority tasks)
	var wg sync.WaitGroup
	errChan := make(chan error, len(phaseTasks))
	
	for _, task := range phaseTasks {
		wg.Add(1)
		go func(t StartupTask) {
			defer wg.Done()
			
			start := time.Now()
			err := t.Fn(ctx)
			elapsed := time.Since(start)
			
			s.taskResults <- taskResult{Task: t, Error: err, Time: elapsed}
			
			if err != nil {
				if !t.Optional {
					errChan <- err
				} else {
					log.Printf("Optional task %s failed: %v", t.Name, err)
				}
			}
		}(task)
	}
	
	wg.Wait()
	close(errChan)
	
	for err := range errChan {
		return err
	}
	
	return nil
}

func (s *StartupOptimizer) runAsyncTasks(ctx context.Context) {
	s.mu.RLock()
	var asyncTasks []StartupTask
	for _, t := range s.tasks {
		if t.Async {
			asyncTasks = append(asyncTasks, t)
		}
	}
	s.mu.RUnlock()
	
	for _, task := range asyncTasks {
		start := time.Now()
		err := task.Fn(ctx)
		elapsed := time.Since(start)
		
		if err != nil {
			log.Printf("Async task %s failed: %v", task.Name, err)
		} else {
			log.Printf("Async task %s completed in %v", task.Name, elapsed)
		}
	}
}

// IsReady returns true when startup is complete.
func (s *StartupOptimizer) IsReady() bool {
	return s.ready.Load()
}

// ElapsedTime returns time since startup began.
func (s *StartupOptimizer) ElapsedTime() time.Duration {
	return time.Since(s.startTime)
}

// Phase returns the current startup phase.
func (s *StartupOptimizer) Phase() StartupPhase {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.phase
}

// Stats returns startup timing statistics.
func (s *StartupOptimizer) Stats() StartupStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	phaseTimes := make(map[StartupPhase]time.Duration)
	for k, v := range s.phaseTimes {
		phaseTimes[k] = v
	}
	
	return StartupStats{
		TotalTime:   time.Since(s.startTime),
		PhaseTimes:  phaseTimes,
		Ready:       s.ready.Load(),
		TaskCount:   len(s.tasks),
	}
}

// StartupStats holds startup timing information.
type StartupStats struct {
	TotalTime  time.Duration
	PhaseTimes map[StartupPhase]time.Duration
	Ready      bool
	TaskCount  int
}

// ---------------------------------------------------------------------------
// Fast Startup Helpers
// ---------------------------------------------------------------------------

// LazyIndex is an index that builds in the background.
type LazyIndex struct {
	mu       sync.RWMutex
	built    atomic.Bool
	building atomic.Bool
	data     map[string][]uint32
}

// NewLazyIndex creates a lazy index.
func NewLazyIndex() *LazyIndex {
	return &LazyIndex{
		data: make(map[string][]uint32),
	}
}

// IsBuilt returns true if the index is ready.
func (l *LazyIndex) IsBuilt() bool {
	return l.built.Load()
}

// BuildAsync builds the index in the background.
func (l *LazyIndex) BuildAsync(buildFn func() map[string][]uint32) {
	if l.building.Swap(true) {
		return // Already building
	}
	
	go func() {
		data := buildFn()
		
		l.mu.Lock()
		l.data = data
		l.mu.Unlock()
		
		l.built.Store(true)
		l.building.Store(false)
	}()
}

// Get retrieves from the index, blocking if not built.
func (l *LazyIndex) Get(key string) []uint32 {
	// Spin wait if building (should be fast)
	for l.building.Load() && !l.built.Load() {
		runtime.Gosched()
	}
	
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.data[key]
}
