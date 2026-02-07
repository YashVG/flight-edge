package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/yash/flightedge/internal/edge"
	"github.com/yash/flightedge/internal/ingestion"
	"github.com/yash/flightedge/internal/metrics"
	"github.com/yash/flightedge/internal/ontology"
	"github.com/yash/flightedge/internal/query"
	"github.com/yash/flightedge/pkg/models"
)

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

// Config holds application configuration.
type Config struct {
	// Server
	HTTPAddr string
	HTTPPort int

	// OpenSky API - OAuth2 (preferred)
	OpenSkyClientID     string
	OpenSkyClientSecret string

	// OpenSky API - Basic Auth (legacy, deprecated)
	OpenSkyUsername string
	OpenSkyPassword string

	// Path to credentials.json (optional, overrides env vars)
	CredentialsFile string

	// Ingestion
	PollInterval    time.Duration
	EnableIngestion bool

	// Edge deployment configuration
	Edge edge.Config
}

func loadConfig() Config {
	// Load edge configuration from environment
	edgeCfg := edge.LoadFromEnv()

	cfg := Config{
		HTTPAddr:            getEnv("HTTP_ADDR", "0.0.0.0"),
		HTTPPort:            getEnvInt("HTTP_PORT", 8080),
		OpenSkyClientID:     getEnv("OPENSKY_CLIENT_ID", ""),
		OpenSkyClientSecret: getEnv("OPENSKY_CLIENT_SECRET", ""),
		OpenSkyUsername:      getEnv("OPENSKY_USERNAME", ""),
		OpenSkyPassword:     getEnv("OPENSKY_PASSWORD", ""),
		CredentialsFile:     getEnv("CREDENTIALS_FILE", "credentials.json"),
		PollInterval:        getEnvDuration("POLL_INTERVAL", 10*time.Second),
		EnableIngestion:     getEnvBool("ENABLE_INGESTION", true),
		Edge:                edgeCfg,
	}

	// Try loading credentials.json if OAuth2 env vars are not set
	if cfg.OpenSkyClientID == "" || cfg.OpenSkyClientSecret == "" {
		if creds, err := ingestion.LoadCredentials(cfg.CredentialsFile); err == nil {
			cfg.OpenSkyClientID = creds.ClientID
			cfg.OpenSkyClientSecret = creds.ClientSecret
			log.Printf("Loaded OAuth2 credentials from %s (client_id=%s)", cfg.CredentialsFile, creds.ClientID)
		}
	}

	// Apply edge runtime settings (GOMAXPROCS, GC, memory limit)
	cfg.Edge.Apply()

	// Log auth method
	switch {
	case cfg.OpenSkyClientID != "":
		log.Printf("OpenSky auth: OAuth2 client credentials (client_id=%s)", cfg.OpenSkyClientID)
	case cfg.OpenSkyUsername != "":
		log.Printf("OpenSky auth: Basic Auth (legacy, username=%s)", cfg.OpenSkyUsername)
	default:
		log.Println("OpenSky auth: anonymous (rate limited to 400 credits/day)")
	}

	log.Printf("Edge configuration: mode=%s memory_limit=%dMB gc=%d%% retention=%dh compression=%v",
		cfg.Edge.MemoryMode, cfg.Edge.MemoryLimitMB, cfg.Edge.GCPercent,
		cfg.Edge.DataRetentionHours, cfg.Edge.EnableCompression)

	return cfg
}

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return defaultVal
}

func getEnvBool(key string, defaultVal bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return defaultVal
}

func getEnvDuration(key string, defaultVal time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return defaultVal
}

// ---------------------------------------------------------------------------
// Application
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Airport Position Cache & Geo Helpers
// ---------------------------------------------------------------------------

// airportPosition caches airport locations for fast proximity lookups.
type airportPosition struct {
	code string
	lat  float64
	lon  float64
}

// haversineKm returns the distance in kilometers between two lat/lon points.
func haversineKm(lat1, lon1, lat2, lon2 float64) float64 {
	const earthRadiusKm = 6371.0
	dLat := (lat2 - lat1) * math.Pi / 180.0
	dLon := (lon2 - lon1) * math.Pi / 180.0
	lat1r := lat1 * math.Pi / 180.0
	lat2r := lat2 * math.Pi / 180.0

	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1r)*math.Cos(lat2r)*math.Sin(dLon/2)*math.Sin(dLon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return earthRadiusKm * c
}

// App holds all application components.
type App struct {
	config    Config
	ontology  *ontology.Engine
	query     *query.Engine
	ingestion *ingestion.Processor
	client    *ingestion.Client
	server    *http.Server

	// Cached airport positions for proximity matching
	airports []airportPosition

	// Edge components
	memoryMonitor     *edge.MemoryMonitor
	expirationManager *edge.ExpirationManager
	nodeLimitEnforcer *edge.NodeLimitEnforcer

	startTime time.Time
	ready     bool
}

// NewApp creates a new application instance.
func NewApp(cfg Config) *App {
	// Get buffer sizes from edge config
	buffers := cfg.Edge.BufferSizes()

	// Create ontology engine with edge-optimized capacities
	ont := ontology.New(
		ontology.WithCapacity(buffers.NodeSlab, buffers.FlightIndex, buffers.AirportIndex),
	)

	// Seed airports (YVR and major hubs)
	seedAirports(ont)

	// Create query engine
	qe := query.New(ont)

	// Create ingestion client - prefer OAuth2, fall back to Basic Auth
	clientOpts := []ingestion.ClientOption{}
	if cfg.OpenSkyClientID != "" && cfg.OpenSkyClientSecret != "" {
		clientOpts = append(clientOpts, ingestion.WithClientCredentials(cfg.OpenSkyClientID, cfg.OpenSkyClientSecret))
	} else if cfg.OpenSkyUsername != "" && cfg.OpenSkyPassword != "" {
		clientOpts = append(clientOpts, ingestion.WithCredentials(cfg.OpenSkyUsername, cfg.OpenSkyPassword))
	}
	client := ingestion.NewClient(clientOpts...)

	app := &App{
		config:    cfg,
		ontology:  ont,
		query:     qe,
		client:    client,
		startTime: time.Now(),
	}

	// Cache airport positions for proximity matching during ingestion
	ont.ForEachNodeOfType(ontology.TypeAirport, func(n *ontology.Node) bool {
		if code, ok := n.GetString("code"); ok {
			if lat, lon, ok := n.GetLocation("position"); ok {
				app.airports = append(app.airports, airportPosition{code: code, lat: lat, lon: lon})
			}
		}
		return true
	})
	log.Printf("Cached %d airport positions for proximity matching", len(app.airports))

	// Set up edge components
	app.setupEdgeComponents()

	// Create ingestion processor
	procConfig := ingestion.ProcessorConfig{
		PollInterval: cfg.PollInterval,
		Filter:       ingestion.YVRFilter(),
		BatchSize:    100,
		Workers:      4,
	}
	app.ingestion = ingestion.NewProcessor(client, procConfig, app.handleFlightBatch)

	return app
}

// setupEdgeComponents initializes memory monitoring, expiration, and degradation.
func (a *App) setupEdgeComponents() {
	// Set up memory monitoring
	a.memoryMonitor = edge.NewMemoryMonitor(a.config.Edge)

	// Set up memory pressure listener for graceful degradation
	if a.config.Edge.EnableDegradation {
		a.memoryMonitor.AddListener(func(oldState, newState edge.MemoryState, stats edge.MemoryStats) {
			if newState >= edge.MemoryStateCritical && oldState < edge.MemoryStateCritical {
				// Entering critical state - evict oldest nodes
				log.Printf("Memory pressure: %s -> %s, evicting nodes", oldState, newState)
				a.evictOldestNodes(100)
			}
			if newState >= edge.MemoryStateEmergency {
				// Emergency - force GC
				runtime.GC()
			}
		})
	}

	// Set up data expiration if configured
	if a.config.Edge.DataRetentionHours > 0 {
		a.expirationManager = edge.NewExpirationManager(a.config.Edge)
		a.expirationManager.SetCallback(func(ids []string) int {
			for _, id := range ids {
				a.ontology.RemoveNode(id)
			}
			log.Printf("Expired %d nodes due to retention policy", len(ids))
			return len(ids)
		})
	}

	// Set up node limit enforcer if configured
	if a.config.Edge.MaxNodes > 0 {
		a.nodeLimitEnforcer = edge.NewNodeLimitEnforcer(a.config.Edge)
		a.nodeLimitEnforcer.SetEvictCallback(func(ids []string) int {
			for _, id := range ids {
				a.ontology.RemoveNode(id)
			}
			log.Printf("Removed %d nodes due to node limit", len(ids))
			return len(ids)
		})
	}
}

// evictOldestNodes removes the oldest N nodes (flight nodes only).
func (a *App) evictOldestNodes(count int) {
	// Get all flight node IDs
	var flightIDs []string
	a.ontology.ForEachNodeOfType(ontology.TypeFlight, func(n *ontology.Node) bool {
		flightIDs = append(flightIDs, n.ID)
		return len(flightIDs) < count
	})

	// Remove them
	for _, id := range flightIDs {
		a.ontology.RemoveNode(id)
	}

	if len(flightIDs) > 0 {
		log.Printf("Evicted %d flight nodes due to memory pressure", len(flightIDs))
	}
}

// findNearestAirport returns the nearest cached airport and its distance in km.
func (a *App) findNearestAirport(lat, lon float64) (string, float64) {
	var bestCode string
	bestDist := math.MaxFloat64

	for _, apt := range a.airports {
		d := haversineKm(lat, lon, apt.lat, apt.lon)
		if d < bestDist {
			bestDist = d
			bestCode = apt.code
		}
	}
	return bestCode, bestDist
}

// handleFlightBatch processes a batch of flights from ingestion.
func (a *App) handleFlightBatch(ctx context.Context, flights []models.Flight) error {
	// Check if we're under memory pressure - reject if degradation says so
	if a.memoryMonitor != nil {
		state := a.memoryMonitor.State()
		if state >= edge.MemoryStateCritical && a.config.Edge.DegradationAction == edge.DegradationRejectNew {
			log.Printf("Rejecting batch of %d flights due to memory pressure", len(flights))
			return nil // Silent rejection
		}
	}

	for _, f := range flights {
		n := ontology.Node{ID: f.ICAO24, Type: ontology.TypeFlight}
		n.SetString("flight_id", f.ICAO24)
		n.SetString("callsign", f.Callsign)
		n.SetString("origin_country", f.Origin)
		n.SetLocation("position", f.Latitude, f.Longitude)
		n.SetFloat("altitude", f.Altitude)
		n.SetFloat("velocity", f.Velocity)
		n.SetFloat("heading", f.Heading)
		n.SetBool("on_ground", f.OnGround)
		n.SetTimestamp("last_contact", f.LastContact)

		// Match flight to nearest airport by proximity
		var nearestCode string
		if f.Latitude != 0 || f.Longitude != 0 {
			code, dist := a.findNearestAirport(f.Latitude, f.Longitude)
			// On-ground flights within 50km are likely at the airport
			// Airborne flights within 100km are associated with the airport
			threshold := 100.0
			if f.OnGround {
				threshold = 50.0
			}
			if dist <= threshold {
				nearestCode = code
				n.SetString("nearest_airport", code)
				n.SetString("departure_code", code)

				// Resolve airport name from callsign direction hint
				for _, apt := range a.airports {
					if apt.code == code {
						n.SetString("departure_name", apt.code+" Airport")
						break
					}
				}
			}
		}

		// Extract airline code from callsign (first 3 chars are ICAO airline code)
		callsign := strings.TrimSpace(f.Callsign)
		if len(callsign) >= 3 {
			n.SetString("airline", callsign[:3])
		}

		a.ontology.AddNode(n)

		// Create edge from flight to nearest airport
		if nearestCode != "" {
			a.ontology.AddEdge(f.ICAO24, "apt-"+nearestCode, ontology.RelDepartsFrom)
		}

		// Track node for expiration
		if a.expirationManager != nil {
			a.expirationManager.RecordNodeNow(f.ICAO24)
		}
		if a.nodeLimitEnforcer != nil {
			a.nodeLimitEnforcer.RecordNode(f.ICAO24, time.Now())
		}

		// Index for queries
		if len(callsign) >= 3 {
			a.query.IndexFlight(f.ICAO24, callsign, nearestCode, "")
		}
	}

	// Update metrics
	metrics.IngestionFlights.Add(int64(len(flights)))
	metrics.OntologyNodes.Set(float64(a.ontology.Size()))
	metrics.OntologyFlights.Set(float64(a.ontology.TypeCount(ontology.TypeFlight)))

	return nil
}

// Run starts the application.
func (a *App) Run(ctx context.Context) error {
	log.Println("FlightEdge starting...")
	log.Printf("Configuration: addr=%s:%d poll=%s", a.config.HTTPAddr, a.config.HTTPPort, a.config.PollInterval)

	// Start edge components (memory monitor, expiration)
	if a.memoryMonitor != nil {
		a.memoryMonitor.Start(ctx)
	}
	if a.expirationManager != nil {
		a.expirationManager.Start(ctx)
	}

	// Start HTTP server
	a.startHTTPServer()

	// Initial data fetch
	log.Println("Fetching initial flight data from OpenSky...")
	if count, err := a.ingestion.ProcessOnce(ctx); err != nil {
		log.Printf("Initial fetch failed: %v", err)
	} else {
		log.Printf("Ingested %d flights", count)
	}

	// Mark as ready
	a.ready = true
	log.Printf("FlightEdge ready. Ontology has %d nodes, %d edges",
		a.ontology.Size(), a.ontology.EdgeCount())

	// Start continuous ingestion if enabled
	if a.config.EnableIngestion {
		log.Println("Starting continuous ingestion...")
		if err := a.ingestion.Start(ctx); err != nil {
			log.Printf("Failed to start ingestion: %v", err)
		}
	}

	// Wait for shutdown signal
	<-ctx.Done()
	log.Println("Shutting down...")

	return a.Shutdown()
}

// Shutdown gracefully stops the application.
func (a *App) Shutdown() error {
	// Stop ingestion
	if a.ingestion != nil {
		a.ingestion.Stop()
	}

	// Stop edge components
	if a.memoryMonitor != nil {
		a.memoryMonitor.Stop()
	}
	if a.expirationManager != nil {
		a.expirationManager.Stop()
	}

	// Shutdown HTTP server
	if a.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := a.server.Shutdown(ctx); err != nil {
			log.Printf("HTTP server shutdown error: %v", err)
		}
	}

	log.Println("FlightEdge stopped")
	return nil
}

// ---------------------------------------------------------------------------
// HTTP Server
// ---------------------------------------------------------------------------

func (a *App) startHTTPServer() {
	mux := http.NewServeMux()

	// Health endpoints
	mux.HandleFunc("/health", a.handleHealth)
	mux.HandleFunc("/ready", a.handleReady)
	mux.HandleFunc("/live", a.handleLive)

	// Metrics endpoint
	mux.HandleFunc("/metrics", a.handleMetrics)

	// API endpoints
	mux.HandleFunc("/api/v1/flights", a.handleFlights)
	mux.HandleFunc("/api/v1/flights/", a.handleFlightByID)
	mux.HandleFunc("/api/v1/airports/", a.handleAirportFlights)
	mux.HandleFunc("/api/v1/delayed", a.handleDelayedFlights)
	mux.HandleFunc("/api/v1/predict/", a.handlePredictDelay)
	mux.HandleFunc("/api/v1/stats", a.handleStats)

	addr := fmt.Sprintf("%s:%d", a.config.HTTPAddr, a.config.HTTPPort)
	a.server = &http.Server{
		Addr:         addr,
		Handler:      a.metricsMiddleware(mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("HTTP server listening on %s", addr)
		if err := a.server.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()
}

func (a *App) metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		metrics.HTTPRequests.Inc()
		metrics.ActiveConnections.Inc()
		defer metrics.ActiveConnections.Dec()

		next.ServeHTTP(w, r)

		metrics.HTTPLatency.Observe(time.Since(start).Seconds())
	})
}

// ---------------------------------------------------------------------------
// Health Handlers
// ---------------------------------------------------------------------------

func (a *App) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	health := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"uptime":    time.Since(a.startTime).String(),
		"version":   "1.0.0",
	}

	if !a.ready {
		health["status"] = "starting"
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	json.NewEncoder(w).Encode(health)
}

func (a *App) handleReady(w http.ResponseWriter, r *http.Request) {
	if a.ready {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ready"))
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("not ready"))
	}
}

func (a *App) handleLive(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("alive"))
}

// ---------------------------------------------------------------------------
// Metrics Handler
// ---------------------------------------------------------------------------

func (a *App) handleMetrics(w http.ResponseWriter, r *http.Request) {
	// Update ontology metrics
	metrics.OntologyNodes.Set(float64(a.ontology.Size()))
	metrics.OntologyEdges.Set(float64(a.ontology.EdgeCount()))
	metrics.OntologyFlights.Set(float64(a.ontology.TypeCount(ontology.TypeFlight)))
	metrics.OntologyAirports.Set(float64(a.ontology.TypeCount(ontology.TypeAirport)))

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Write([]byte(metrics.Default().Export()))
}

// ---------------------------------------------------------------------------
// API Handlers
// ---------------------------------------------------------------------------

func (a *App) handleFlights(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	metrics.QueryRequests.Inc()

	maxResults := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxResults = n
		}
	}

	result := a.query.Execute(query.Request{
		NodeType:   ontology.TypeFlight,
		MaxResults: maxResults,
	})

	metrics.QueryLatency.Observe(time.Since(start).Seconds())

	respondJSON(w, map[string]interface{}{
		"flights": result.Nodes,
		"count":   len(result.Nodes),
		"elapsed": result.Elapsed.String(),
	})
}

func (a *App) handleFlightByID(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	metrics.QueryRequests.Inc()

	// Extract flight ID from path
	flightID := r.URL.Path[len("/api/v1/flights/"):]
	if flightID == "" {
		http.Error(w, "flight ID required", http.StatusBadRequest)
		return
	}

	path, ok := a.query.GetFlightPath(flightID)
	metrics.QueryLatency.Observe(time.Since(start).Seconds())

	if !ok {
		http.Error(w, "flight not found", http.StatusNotFound)
		return
	}

	respondJSON(w, path)
}

func (a *App) handleAirportFlights(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	metrics.QueryRequests.Inc()

	// Extract airport code from path
	code := r.URL.Path[len("/api/v1/airports/"):]
	if code == "" {
		http.Error(w, "airport code required", http.StatusBadRequest)
		return
	}

	// Parse time range
	tr := query.TimeRange{}
	if v := r.URL.Query().Get("from"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			tr.Start = t
		}
	}
	if v := r.URL.Query().Get("to"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			tr.End = t
		}
	}

	maxResults := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxResults = n
		}
	}

	result := a.query.GetFlightsByAirport(code, tr, maxResults)
	metrics.QueryLatency.Observe(time.Since(start).Seconds())

	respondJSON(w, map[string]interface{}{
		"airport": code,
		"flights": result.Flights,
		"count":   len(result.Flights),
		"total":   result.Total,
		"elapsed": result.Elapsed.String(),
	})
}

func (a *App) handleDelayedFlights(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	metrics.QueryRequests.Inc()

	threshold := 15 * time.Minute
	if v := r.URL.Query().Get("threshold"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			threshold = d
		}
	}

	airline := r.URL.Query().Get("airline")

	maxResults := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxResults = n
		}
	}

	result := a.query.GetDelayedFlights(threshold, airline, maxResults)
	metrics.QueryLatency.Observe(time.Since(start).Seconds())

	respondJSON(w, map[string]interface{}{
		"threshold": threshold.String(),
		"airline":   airline,
		"flights":   result.Flights,
		"count":     len(result.Flights),
		"total":     result.Total,
		"elapsed":   result.Elapsed.String(),
	})
}

func (a *App) handlePredictDelay(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	metrics.QueryRequests.Inc()

	flightID := r.URL.Path[len("/api/v1/predict/"):]
	if flightID == "" {
		http.Error(w, "flight ID required", http.StatusBadRequest)
		return
	}

	pred, ok := a.query.PredictDelay(flightID)
	metrics.QueryLatency.Observe(time.Since(start).Seconds())

	if !ok {
		http.Error(w, "flight not found", http.StatusNotFound)
		return
	}

	respondJSON(w, pred)
}

func (a *App) handleStats(w http.ResponseWriter, r *http.Request) {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	ingestionMetrics := a.ingestion.Metrics().Snapshot()

	stats := map[string]interface{}{
		"ontology": map[string]interface{}{
			"total_nodes":    a.ontology.Size(),
			"total_edges":    a.ontology.EdgeCount(),
			"flights":        a.ontology.TypeCount(ontology.TypeFlight),
			"airports":       a.ontology.TypeCount(ontology.TypeAirport),
			"aircraft":       a.ontology.TypeCount(ontology.TypeAircraft),
			"airlines":       a.ontology.TypeCount(ontology.TypeAirline),
			"weather":        a.ontology.TypeCount(ontology.TypeWeather),
		},
		"ingestion": map[string]interface{}{
			"total_requests":   ingestionMetrics.TotalRequests,
			"success_requests": ingestionMetrics.SuccessRequests,
			"failed_requests":  ingestionMetrics.FailedRequests,
			"total_flights":    ingestionMetrics.TotalFlights,
			"filtered_flights": ingestionMetrics.FilteredFlights,
			"last_latency_ms":  ingestionMetrics.LastLatencyMs,
			"avg_latency_ms":   ingestionMetrics.AvgLatencyMs,
			"events_per_sec":   ingestionMetrics.EventsPerSecond,
		},
		"memory": map[string]interface{}{
			"alloc_mb":      float64(memStats.Alloc) / 1024 / 1024,
			"heap_alloc_mb": float64(memStats.HeapAlloc) / 1024 / 1024,
			"heap_sys_mb":   float64(memStats.HeapSys) / 1024 / 1024,
			"sys_mb":        float64(memStats.Sys) / 1024 / 1024,
			"gc_runs":       memStats.NumGC,
		},
		"runtime": map[string]interface{}{
			"goroutines": runtime.NumGoroutine(),
			"gomaxprocs": runtime.GOMAXPROCS(0),
			"uptime":     time.Since(a.startTime).String(),
		},
		"edge": map[string]interface{}{
			"memory_mode":       a.config.Edge.MemoryMode.String(),
			"memory_limit_mb":   a.config.Edge.MemoryLimitMB,
			"gc_percent":        a.config.Edge.GCPercent,
			"retention_hours":   a.config.Edge.DataRetentionHours,
			"max_nodes":         a.config.Edge.MaxNodes,
			"compression":       a.config.Edge.EnableCompression,
			"degradation":       a.config.Edge.EnableDegradation,
		},
	}

	// Add memory monitor stats if available
	if a.memoryMonitor != nil {
		monitorStats := a.memoryMonitor.Stats()
		stats["edge"].(map[string]interface{})["memory_state"] = a.memoryMonitor.State().String()
		stats["edge"].(map[string]interface{})["heap_mb"] = monitorStats.HeapMB
		stats["edge"].(map[string]interface{})["usage_ratio"] = monitorStats.UsageRatio
		stats["edge"].(map[string]interface{})["is_warning"] = a.memoryMonitor.IsWarning()
	}

	// Add expiration stats if available
	if a.expirationManager != nil {
		expStats := a.expirationManager.Stats()
		stats["edge"].(map[string]interface{})["tracked_nodes"] = expStats.TrackedNodes
		stats["edge"].(map[string]interface{})["expired_total"] = expStats.TotalExpired
	}

	respondJSON(w, stats)
}

func respondJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

// ---------------------------------------------------------------------------
// Seed Data
// ---------------------------------------------------------------------------

func seedAirports(ont *ontology.Engine) {
	airports := []struct {
		code    string
		name    string
		city    string
		country string
		lat     float64
		lon     float64
	}{
		{"YVR", "Vancouver International Airport", "Vancouver", "CA", 49.1967, -123.1815},
		{"YYZ", "Toronto Pearson International Airport", "Toronto", "CA", 43.6777, -79.6248},
		{"YUL", "Montréal-Pierre Elliott Trudeau International Airport", "Montreal", "CA", 45.4706, -73.7408},
		{"YYC", "Calgary International Airport", "Calgary", "CA", 51.1215, -114.0076},
		{"JFK", "John F. Kennedy International Airport", "New York", "US", 40.6413, -73.7781},
		{"LAX", "Los Angeles International Airport", "Los Angeles", "US", 33.9416, -118.4085},
		{"SFO", "San Francisco International Airport", "San Francisco", "US", 37.6213, -122.3790},
		{"SEA", "Seattle-Tacoma International Airport", "Seattle", "US", 47.4502, -122.3088},
		{"ORD", "O'Hare International Airport", "Chicago", "US", 41.9742, -87.9073},
		{"DFW", "Dallas/Fort Worth International Airport", "Dallas", "US", 32.8998, -97.0403},
		{"LHR", "Heathrow Airport", "London", "GB", 51.4700, -0.4543},
		{"CDG", "Charles de Gaulle Airport", "Paris", "FR", 49.0097, 2.5479},
		{"FRA", "Frankfurt Airport", "Frankfurt", "DE", 50.0379, 8.5622},
		{"NRT", "Narita International Airport", "Tokyo", "JP", 35.7720, 140.3929},
		{"HND", "Tokyo Haneda Airport", "Tokyo", "JP", 35.5494, 139.7798},
		{"PEK", "Beijing Capital International Airport", "Beijing", "CN", 40.0799, 116.6031},
		{"HKG", "Hong Kong International Airport", "Hong Kong", "HK", 22.3080, 113.9185},
		{"SIN", "Singapore Changi Airport", "Singapore", "SG", 1.3644, 103.9915},
		{"SYD", "Sydney Kingsford Smith Airport", "Sydney", "AU", -33.9399, 151.1753},
		{"DXB", "Dubai International Airport", "Dubai", "AE", 25.2532, 55.3657},
	}

	for _, a := range airports {
		n := ontology.Node{ID: "apt-" + a.code, Type: ontology.TypeAirport}
		n.SetString("code", a.code)
		n.SetString("name", a.name)
		n.SetString("city", a.city)
		n.SetString("country", a.country)
		n.SetLocation("position", a.lat, a.lon)
		ont.AddNode(n)
	}

	log.Printf("Seeded %d airports", len(airports))
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	// Handle shutdown signals
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Load configuration
	cfg := loadConfig()

	// Create and run application
	app := NewApp(cfg)
	if err := app.Run(ctx); err != nil {
		log.Fatalf("Application error: %v", err)
	}
}
