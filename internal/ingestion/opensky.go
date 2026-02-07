package ingestion

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yash/flightedge/pkg/models"
)

const (
	defaultBaseURL = "https://opensky-network.org/api"

	// OpenSky OAuth2 token endpoint
	defaultTokenURL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

	// OpenSky rate limits:
	// - Anonymous: 10 seconds between /states/all calls
	// - Authenticated: 5 seconds between /states/all calls
	defaultPollInterval = 10 * time.Second

	// Token refresh buffer - refresh before actual expiry
	tokenRefreshBuffer = 2 * time.Minute

	// Connection pool settings
	maxIdleConns        = 10
	maxConnsPerHost     = 5
	idleConnTimeout     = 90 * time.Second
	tlsHandshakeTimeout = 10 * time.Second

	// Retry settings
	maxRetries     = 5
	baseBackoff    = 1 * time.Second
	maxBackoff     = 60 * time.Second
	backoffFactor  = 2.0
)

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

// Metrics collects ingestion performance data.
type Metrics struct {
	TotalRequests    atomic.Int64
	SuccessRequests  atomic.Int64
	FailedRequests   atomic.Int64
	TotalFlights     atomic.Int64
	FilteredFlights  atomic.Int64
	LastLatencyNs    atomic.Int64
	AvgLatencyNs     atomic.Int64
	EventsPerSecond  atomic.Int64

	mu              sync.Mutex
	latencySum      int64
	latencyCount    int64
	lastEventCount  int64
	lastEventTime   time.Time
}

// RecordLatency updates latency metrics.
func (m *Metrics) RecordLatency(d time.Duration) {
	ns := d.Nanoseconds()
	m.LastLatencyNs.Store(ns)

	m.mu.Lock()
	m.latencySum += ns
	m.latencyCount++
	if m.latencyCount > 0 {
		m.AvgLatencyNs.Store(m.latencySum / m.latencyCount)
	}
	m.mu.Unlock()
}

// RecordEvents updates throughput metrics.
func (m *Metrics) RecordEvents(count int64) {
	m.FilteredFlights.Add(count)

	m.mu.Lock()
	now := time.Now()
	if !m.lastEventTime.IsZero() {
		elapsed := now.Sub(m.lastEventTime).Seconds()
		if elapsed > 0 {
			eps := int64(float64(count) / elapsed)
			m.EventsPerSecond.Store(eps)
		}
	}
	m.lastEventCount = count
	m.lastEventTime = now
	m.mu.Unlock()
}

// Snapshot returns a copy of current metrics.
func (m *Metrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		TotalRequests:   m.TotalRequests.Load(),
		SuccessRequests: m.SuccessRequests.Load(),
		FailedRequests:  m.FailedRequests.Load(),
		TotalFlights:    m.TotalFlights.Load(),
		FilteredFlights: m.FilteredFlights.Load(),
		LastLatencyMs:   float64(m.LastLatencyNs.Load()) / 1e6,
		AvgLatencyMs:    float64(m.AvgLatencyNs.Load()) / 1e6,
		EventsPerSecond: m.EventsPerSecond.Load(),
	}
}

// MetricsSnapshot is a point-in-time copy of metrics.
type MetricsSnapshot struct {
	TotalRequests   int64
	SuccessRequests int64
	FailedRequests  int64
	TotalFlights    int64
	FilteredFlights int64
	LastLatencyMs   float64
	AvgLatencyMs    float64
	EventsPerSecond int64
}

// ---------------------------------------------------------------------------
// Rate Limiter
// ---------------------------------------------------------------------------

// RateLimiter controls request frequency to respect OpenSky limits.
type RateLimiter struct {
	interval time.Duration
	mu       sync.Mutex
	lastCall time.Time
}

// NewRateLimiter creates a rate limiter with the given interval.
func NewRateLimiter(interval time.Duration) *RateLimiter {
	return &RateLimiter{interval: interval}
}

// Wait blocks until the next request is allowed.
func (r *RateLimiter) Wait(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.lastCall.IsZero() {
		r.lastCall = time.Now()
		return nil
	}

	elapsed := time.Since(r.lastCall)
	if elapsed < r.interval {
		wait := r.interval - elapsed
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	r.lastCall = time.Now()
	return nil
}

// ---------------------------------------------------------------------------
// Filter Configuration
// ---------------------------------------------------------------------------

// Filter defines criteria for selecting flights.
type Filter struct {
	// AirportICAO filters flights near specific airports (e.g., "CYVR" for Vancouver)
	AirportICAO []string

	// CallsignPrefixes filters by airline callsign prefix (e.g., "ACA" for Air Canada)
	CallsignPrefixes []string

	// BoundingBox filters by geographic area [minLat, maxLat, minLon, maxLon]
	BoundingBox *[4]float64
}

// YVRFilter returns a filter for YVR-relevant flights and Air Canada.
func YVRFilter() Filter {
	return Filter{
		// Vancouver International Airport area (roughly 50nm radius)
		BoundingBox: &[4]float64{48.5, 50.0, -124.5, -122.0},
		// Air Canada callsigns start with "ACA"
		CallsignPrefixes: []string{"ACA"},
	}
}

// Matches checks if a flight passes the filter criteria.
// When both criteria are set, uses OR logic (matches if ANY criterion is satisfied).
// When only one criterion is set, that criterion must match.
func (f *Filter) Matches(flight *models.Flight) bool {
	// If no filters set, match all
	if f.BoundingBox == nil && len(f.CallsignPrefixes) == 0 {
		return true
	}

	// Check callsign prefix
	callsignMatch := false
	if len(f.CallsignPrefixes) > 0 {
		trimmedCallsign := strings.TrimSpace(flight.Callsign)
		for _, prefix := range f.CallsignPrefixes {
			if strings.HasPrefix(trimmedCallsign, prefix) {
				callsignMatch = true
				break
			}
		}
	}

	// Check bounding box
	bboxMatch := false
	if f.BoundingBox != nil {
		lat, lon := flight.Latitude, flight.Longitude
		bb := f.BoundingBox
		if lat >= bb[0] && lat <= bb[1] && lon >= bb[2] && lon <= bb[3] {
			bboxMatch = true
		}
	}

	// If both filters set: OR logic (match either)
	// If only one filter set: that filter must match
	if len(f.CallsignPrefixes) > 0 && f.BoundingBox != nil {
		return callsignMatch || bboxMatch
	}
	if len(f.CallsignPrefixes) > 0 {
		return callsignMatch
	}
	return bboxMatch
}

// ---------------------------------------------------------------------------
// OAuth2 Token Management
// ---------------------------------------------------------------------------

// tokenResponse mirrors the JSON from the OpenSky token endpoint.
type tokenResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"` // seconds
	TokenType   string `json:"token_type"`
}

// TokenManager handles OAuth2 client-credentials token lifecycle.
type TokenManager struct {
	clientID     string
	clientSecret string
	tokenURL     string
	httpClient   *http.Client

	mu        sync.RWMutex
	token     string
	expiresAt time.Time
}

// NewTokenManager creates a token manager for OAuth2 client credentials flow.
func NewTokenManager(clientID, clientSecret string) *TokenManager {
	return &TokenManager{
		clientID:     clientID,
		clientSecret: clientSecret,
		tokenURL:     defaultTokenURL,
		httpClient:   &http.Client{Timeout: 15 * time.Second},
	}
}

// Token returns a valid access token, refreshing if needed.
func (tm *TokenManager) Token(ctx context.Context) (string, error) {
	tm.mu.RLock()
	if tm.token != "" && time.Now().Before(tm.expiresAt) {
		tok := tm.token
		tm.mu.RUnlock()
		return tok, nil
	}
	tm.mu.RUnlock()

	return tm.refresh(ctx)
}

// refresh fetches a new token from the OAuth2 endpoint.
func (tm *TokenManager) refresh(ctx context.Context) (string, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Double-check after acquiring write lock
	if tm.token != "" && time.Now().Before(tm.expiresAt) {
		return tm.token, nil
	}

	data := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {tm.clientID},
		"client_secret": {tm.clientSecret},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tm.tokenURL,
		strings.NewReader(data.Encode()))
	if err != nil {
		return "", fmt.Errorf("creating token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := tm.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("requesting token: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("token request failed (status %d): %s", resp.StatusCode, string(body))
	}

	var tokResp tokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tokResp); err != nil {
		return "", fmt.Errorf("decoding token response: %w", err)
	}

	tm.token = tokResp.AccessToken
	// Refresh before actual expiry to avoid edge-case failures
	tm.expiresAt = time.Now().Add(time.Duration(tokResp.ExpiresIn)*time.Second - tokenRefreshBuffer)

	return tm.token, nil
}

// Credentials holds OAuth2 client credentials loaded from credentials.json.
type Credentials struct {
	ClientID     string `json:"clientId"`
	ClientSecret string `json:"clientSecret"`
}

// LoadCredentials reads OAuth2 credentials from a JSON file.
func LoadCredentials(path string) (*Credentials, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading credentials file: %w", err)
	}

	var creds Credentials
	if err := json.Unmarshal(data, &creds); err != nil {
		return nil, fmt.Errorf("parsing credentials file: %w", err)
	}

	if creds.ClientID == "" || creds.ClientSecret == "" {
		return nil, fmt.Errorf("credentials file missing clientId or clientSecret")
	}

	return &creds, nil
}

// ---------------------------------------------------------------------------
// Client with Connection Pooling
// ---------------------------------------------------------------------------

// ClientOption configures the Client.
type ClientOption func(*Client)

// WithHTTPClient sets a custom HTTP client.
func WithHTTPClient(hc *http.Client) ClientOption {
	return func(c *Client) { c.httpClient = hc }
}

// WithBaseURLOption sets the base URL.
func WithBaseURLOption(url string) ClientOption {
	return func(c *Client) { c.baseURL = url }
}

// WithCredentials sets Basic Auth credentials (legacy, deprecated by OpenSky).
func WithCredentials(username, password string) ClientOption {
	return func(c *Client) {
		c.username = username
		c.password = password
	}
}

// WithClientCredentials sets OAuth2 client credentials for token-based auth.
func WithClientCredentials(clientID, clientSecret string) ClientOption {
	return func(c *Client) {
		c.tokenManager = NewTokenManager(clientID, clientSecret)
	}
}

// WithTokenManager sets a custom token manager (useful for testing).
func WithTokenManager(tm *TokenManager) ClientOption {
	return func(c *Client) {
		c.tokenManager = tm
	}
}

// Client fetches live flight data from the OpenSky Network API.
type Client struct {
	baseURL      string
	httpClient   *http.Client
	username     string
	password     string
	tokenManager *TokenManager
}

// NewClient creates an OpenSky API client with connection pooling.
func NewClient(opts ...ClientOption) *Client {
	transport := &http.Transport{
		MaxIdleConns:        maxIdleConns,
		MaxConnsPerHost:     maxConnsPerHost,
		IdleConnTimeout:     idleConnTimeout,
		TLSHandshakeTimeout: tlsHandshakeTimeout,
		DisableCompression:  false,
	}

	c := &Client{
		baseURL: defaultBaseURL,
		httpClient: &http.Client{
			Timeout:   30 * time.Second,
			Transport: transport,
		},
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// WithBaseURL overrides the API endpoint (useful for testing).
func (c *Client) WithBaseURL(url string) *Client {
	c.baseURL = url
	return c
}

// openSkyResponse mirrors the JSON shape returned by /states/all.
type openSkyResponse struct {
	Time   int64           `json:"time"`
	States [][]interface{} `json:"states"`
}

// FetchAllStates retrieves the current state vectors for all flights.
func (c *Client) FetchAllStates(ctx context.Context) ([]models.Flight, error) {
	url := fmt.Sprintf("%s/states/all", c.baseURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	// Add auth: prefer OAuth2 Bearer token, fall back to Basic Auth (legacy)
	if c.tokenManager != nil {
		token, err := c.tokenManager.Token(ctx)
		if err != nil {
			return nil, fmt.Errorf("obtaining access token: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
	} else if c.username != "" && c.password != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "gzip")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading body: %w", err)
	}

	var raw openSkyResponse
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("parsing response: %w", err)
	}

	return parseStates(raw), nil
}

// FetchStatesWithRetry fetches states with exponential backoff on failure.
func (c *Client) FetchStatesWithRetry(ctx context.Context) ([]models.Flight, error) {
	var lastErr error
	backoff := baseBackoff

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			// Exponential backoff with cap
			backoff = time.Duration(float64(backoff) * backoffFactor)
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}

		flights, err := c.FetchAllStates(ctx)
		if err == nil {
			return flights, nil
		}
		lastErr = err

		// Don't retry on context cancellation
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}

	return nil, fmt.Errorf("after %d retries: %w", maxRetries, lastErr)
}

func parseStates(raw openSkyResponse) []models.Flight {
	flights := make([]models.Flight, 0, len(raw.States))
	for _, s := range raw.States {
		if len(s) < 17 {
			continue
		}
		f := models.Flight{
			ICAO24:   stringVal(s[0]),
			Callsign: stringVal(s[1]),
			Origin:   stringVal(s[2]),
			OnGround: boolVal(s[8]),
		}
		if v, ok := s[5].(float64); ok {
			f.Longitude = v
		}
		if v, ok := s[6].(float64); ok {
			f.Latitude = v
		}
		if v, ok := s[7].(float64); ok {
			f.Altitude = v
		}
		if v, ok := s[9].(float64); ok {
			f.Velocity = v
		}
		if v, ok := s[10].(float64); ok {
			f.Heading = v
		}
		if v, ok := s[4].(float64); ok {
			f.LastContact = time.Unix(int64(v), 0)
		}
		flights = append(flights, f)
	}
	return flights
}

func stringVal(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func boolVal(v interface{}) bool {
	if b, ok := v.(bool); ok {
		return b
	}
	return false
}

// ---------------------------------------------------------------------------
// Batch Processor
// ---------------------------------------------------------------------------

// BatchHandler processes a batch of filtered flights.
type BatchHandler func(ctx context.Context, flights []models.Flight) error

// ProcessorConfig configures the batch processor.
type ProcessorConfig struct {
	PollInterval time.Duration
	Filter       Filter
	BatchSize    int
	Workers      int
}

// DefaultProcessorConfig returns sensible defaults.
func DefaultProcessorConfig() ProcessorConfig {
	return ProcessorConfig{
		PollInterval: defaultPollInterval,
		Filter:       YVRFilter(),
		BatchSize:    100, // Process in batches of 100
		Workers:      4,   // 4 parallel workers for batch processing
	}
}

// Processor continuously ingests and processes flight data.
type Processor struct {
	client  *Client
	config  ProcessorConfig
	limiter *RateLimiter
	metrics *Metrics
	handler BatchHandler

	mu      sync.Mutex
	running bool
	cancel  context.CancelFunc
}

// NewProcessor creates a batch processor.
func NewProcessor(client *Client, config ProcessorConfig, handler BatchHandler) *Processor {
	return &Processor{
		client:  client,
		config:  config,
		limiter: NewRateLimiter(config.PollInterval),
		metrics: &Metrics{},
		handler: handler,
	}
}

// Metrics returns the processor's metrics collector.
func (p *Processor) Metrics() *Metrics {
	return p.metrics
}

// Start begins continuous ingestion. Non-blocking.
func (p *Processor) Start(ctx context.Context) error {
	p.mu.Lock()
	if p.running {
		p.mu.Unlock()
		return fmt.Errorf("processor already running")
	}
	p.running = true

	ctx, p.cancel = context.WithCancel(ctx)
	p.mu.Unlock()

	go p.run(ctx)
	return nil
}

// Stop halts the processor.
func (p *Processor) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.cancel != nil {
		p.cancel()
	}
	p.running = false
}

// IsRunning returns whether the processor is active.
func (p *Processor) IsRunning() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.running
}

// run is the main processing loop.
func (p *Processor) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			p.mu.Lock()
			p.running = false
			p.mu.Unlock()
			return
		default:
		}

		// Wait for rate limiter
		if err := p.limiter.Wait(ctx); err != nil {
			continue
		}

		// Fetch with timing
		start := time.Now()
		p.metrics.TotalRequests.Add(1)

		flights, err := p.client.FetchStatesWithRetry(ctx)
		latency := time.Since(start)
		p.metrics.RecordLatency(latency)

		if err != nil {
			p.metrics.FailedRequests.Add(1)
			continue
		}
		p.metrics.SuccessRequests.Add(1)
		p.metrics.TotalFlights.Add(int64(len(flights)))

		// Filter flights
		filtered := p.filterFlights(flights)
		if len(filtered) == 0 {
			continue
		}

		// Process in batches for throughput
		p.processBatches(ctx, filtered)
	}
}

// filterFlights applies the configured filter.
func (p *Processor) filterFlights(flights []models.Flight) []models.Flight {
	filtered := make([]models.Flight, 0, len(flights)/10) // Estimate 10% match
	for i := range flights {
		if p.config.Filter.Matches(&flights[i]) {
			filtered = append(filtered, flights[i])
		}
	}
	return filtered
}

// processBatches splits filtered flights into batches and processes in parallel.
func (p *Processor) processBatches(ctx context.Context, flights []models.Flight) {
	p.metrics.RecordEvents(int64(len(flights)))

	// Split into batches
	batchSize := p.config.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}

	var batches [][]models.Flight
	for i := 0; i < len(flights); i += batchSize {
		end := i + batchSize
		if end > len(flights) {
			end = len(flights)
		}
		batches = append(batches, flights[i:end])
	}

	// Process batches in parallel with worker pool
	workers := p.config.Workers
	if workers <= 0 {
		workers = 4
	}

	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup

	for _, batch := range batches {
		select {
		case <-ctx.Done():
			return
		case sem <- struct{}{}:
		}

		wg.Add(1)
		go func(b []models.Flight) {
			defer wg.Done()
			defer func() { <-sem }()

			if p.handler != nil {
				_ = p.handler(ctx, b)
			}
		}(batch)
	}

	wg.Wait()
}

// ProcessOnce fetches and processes a single batch (useful for testing).
func (p *Processor) ProcessOnce(ctx context.Context) (int, error) {
	start := time.Now()
	p.metrics.TotalRequests.Add(1)

	flights, err := p.client.FetchStatesWithRetry(ctx)
	p.metrics.RecordLatency(time.Since(start))

	if err != nil {
		p.metrics.FailedRequests.Add(1)
		return 0, err
	}
	p.metrics.SuccessRequests.Add(1)
	p.metrics.TotalFlights.Add(int64(len(flights)))

	filtered := p.filterFlights(flights)
	if len(filtered) == 0 {
		return 0, nil
	}

	p.processBatches(ctx, filtered)
	return len(filtered), nil
}

// ---------------------------------------------------------------------------
// Convenience: Ingester with Ontology Integration
// ---------------------------------------------------------------------------

// FlightEvent represents a processed flight event.
type FlightEvent struct {
	Flight    models.Flight
	Timestamp time.Time
}

// EventChannel returns a channel-based handler for streaming events.
func EventChannel(ch chan<- FlightEvent) BatchHandler {
	return func(ctx context.Context, flights []models.Flight) error {
		now := time.Now()
		for _, f := range flights {
			select {
			case ch <- FlightEvent{Flight: f, Timestamp: now}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
}
