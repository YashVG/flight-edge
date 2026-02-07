package ingestion

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yash/flightedge/pkg/models"
)

// ---------------------------------------------------------------------------
// Client Tests
// ---------------------------------------------------------------------------

func TestFetchAllStates(t *testing.T) {
	payload := map[string]interface{}{
		"time": 1700000000,
		"states": [][]interface{}{
			{
				"abc123",   // 0  icao24
				"UAL456 ",  // 1  callsign
				"US",       // 2  origin
				1700000000, // 3  time_position
				1700000000, // 4  last_contact
				-73.9,      // 5  longitude
				40.7,       // 6  latitude
				10000.0,    // 7  baro_altitude
				false,      // 8  on_ground
				250.0,      // 9  velocity
				180.0,      // 10 true_track
				0.0,        // 11 vertical_rate
				nil,        // 12 sensors
				10500.0,    // 13 geo_altitude
				"1234",     // 14 squawk
				false,      // 15 spi
				0,          // 16 position_source
			},
		},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/states/all", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(payload)
	}))
	defer srv.Close()

	client := NewClient().WithBaseURL(srv.URL)
	flights, err := client.FetchAllStates(context.Background())
	require.NoError(t, err)
	require.Len(t, flights, 1)

	f := flights[0]
	assert.Equal(t, "abc123", f.ICAO24)
	assert.Equal(t, "UAL456 ", f.Callsign)
	assert.Equal(t, "US", f.Origin)
	assert.InDelta(t, 40.7, f.Latitude, 0.01)
	assert.InDelta(t, -73.9, f.Longitude, 0.01)
	assert.InDelta(t, 250.0, f.Velocity, 0.01)
	assert.False(t, f.OnGround)
}

func TestFetchAllStatesServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	client := NewClient().WithBaseURL(srv.URL)
	_, err := client.FetchAllStates(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected status: 500")
}

func TestFetchWithRetrySuccess(t *testing.T) {
	attempts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		payload := map[string]interface{}{"time": 1700000000, "states": [][]interface{}{}}
		json.NewEncoder(w).Encode(payload)
	}))
	defer srv.Close()

	client := NewClient().WithBaseURL(srv.URL)
	_, err := client.FetchStatesWithRetry(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 3, attempts)
}

func TestFetchWithRetryExhausted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	client := NewClient().WithBaseURL(srv.URL)
	// Long enough timeout for all retries to complete
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	_, err := client.FetchStatesWithRetry(ctx)
	assert.Error(t, err)
	// Either exhausted retries or hit context deadline
	assert.True(t, err != nil)
}

func TestClientWithCredentials(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		json.NewEncoder(w).Encode(map[string]interface{}{"time": 0, "states": [][]interface{}{}})
	}))
	defer srv.Close()

	client := NewClient(
		WithBaseURLOption(srv.URL),
		WithCredentials("user", "pass"),
	)
	_, err := client.FetchAllStates(context.Background())
	require.NoError(t, err)
	assert.Contains(t, gotAuth, "Basic")
}

// ---------------------------------------------------------------------------
// Rate Limiter Tests
// ---------------------------------------------------------------------------

func TestRateLimiterFirstCallImmediate(t *testing.T) {
	rl := NewRateLimiter(100 * time.Millisecond)
	start := time.Now()
	err := rl.Wait(context.Background())
	require.NoError(t, err)
	assert.Less(t, time.Since(start), 50*time.Millisecond)
}

func TestRateLimiterEnforcesInterval(t *testing.T) {
	rl := NewRateLimiter(100 * time.Millisecond)

	// First call
	err := rl.Wait(context.Background())
	require.NoError(t, err)

	// Second call should wait
	start := time.Now()
	err = rl.Wait(context.Background())
	require.NoError(t, err)
	assert.GreaterOrEqual(t, time.Since(start), 90*time.Millisecond)
}

func TestRateLimiterContextCancel(t *testing.T) {
	rl := NewRateLimiter(1 * time.Second)
	rl.Wait(context.Background()) // First call

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Already cancelled

	err := rl.Wait(ctx)
	assert.ErrorIs(t, err, context.Canceled)
}

// ---------------------------------------------------------------------------
// Filter Tests
// ---------------------------------------------------------------------------

func TestYVRFilter(t *testing.T) {
	f := YVRFilter()

	tests := []struct {
		name    string
		flight  models.Flight
		matches bool
	}{
		{
			name:    "Air Canada flight anywhere",
			flight:  models.Flight{Callsign: "ACA123", Latitude: 0, Longitude: 0},
			matches: true,
		},
		{
			name:    "Air Canada with whitespace",
			flight:  models.Flight{Callsign: "ACA456  ", Latitude: 0, Longitude: 0},
			matches: true,
		},
		{
			name:    "Flight in YVR bounding box",
			flight:  models.Flight{Callsign: "UAL999", Latitude: 49.2, Longitude: -123.2},
			matches: true,
		},
		{
			name:    "Non-ACA flight outside YVR",
			flight:  models.Flight{Callsign: "UAL123", Latitude: 40.7, Longitude: -73.9},
			matches: false,
		},
		{
			name:    "WestJet in YVR area",
			flight:  models.Flight{Callsign: "WJA555", Latitude: 49.0, Longitude: -123.0},
			matches: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.matches, f.Matches(&tc.flight))
		})
	}
}

func TestEmptyFilterMatchesAll(t *testing.T) {
	f := Filter{}
	flight := models.Flight{Callsign: "ANY123", Latitude: 0, Longitude: 0}
	assert.True(t, f.Matches(&flight))
}

func TestFilterCallsignOnly(t *testing.T) {
	f := Filter{CallsignPrefixes: []string{"DAL", "UAL"}}

	assert.True(t, f.Matches(&models.Flight{Callsign: "DAL100"}))
	assert.True(t, f.Matches(&models.Flight{Callsign: "UAL200"}))
	assert.False(t, f.Matches(&models.Flight{Callsign: "ACA300"}))
}

func TestFilterBoundingBoxOnly(t *testing.T) {
	f := Filter{BoundingBox: &[4]float64{40.0, 42.0, -75.0, -73.0}} // NYC area

	assert.True(t, f.Matches(&models.Flight{Latitude: 41.0, Longitude: -74.0}))
	assert.False(t, f.Matches(&models.Flight{Latitude: 49.0, Longitude: -123.0}))
}

// ---------------------------------------------------------------------------
// Metrics Tests
// ---------------------------------------------------------------------------

func TestMetricsRecordLatency(t *testing.T) {
	m := &Metrics{}

	m.RecordLatency(100 * time.Millisecond)
	assert.Equal(t, int64(100_000_000), m.LastLatencyNs.Load())
	assert.Equal(t, int64(100_000_000), m.AvgLatencyNs.Load())

	m.RecordLatency(200 * time.Millisecond)
	assert.Equal(t, int64(200_000_000), m.LastLatencyNs.Load())
	assert.Equal(t, int64(150_000_000), m.AvgLatencyNs.Load()) // Average of 100 and 200
}

func TestMetricsSnapshot(t *testing.T) {
	m := &Metrics{}
	m.TotalRequests.Store(10)
	m.SuccessRequests.Store(8)
	m.FailedRequests.Store(2)
	m.TotalFlights.Store(1000)
	m.FilteredFlights.Store(50)
	m.LastLatencyNs.Store(50_000_000)
	m.AvgLatencyNs.Store(45_000_000)

	snap := m.Snapshot()
	assert.Equal(t, int64(10), snap.TotalRequests)
	assert.Equal(t, int64(8), snap.SuccessRequests)
	assert.Equal(t, int64(2), snap.FailedRequests)
	assert.Equal(t, int64(1000), snap.TotalFlights)
	assert.Equal(t, int64(50), snap.FilteredFlights)
	assert.InDelta(t, 50.0, snap.LastLatencyMs, 0.1)
	assert.InDelta(t, 45.0, snap.AvgLatencyMs, 0.1)
}

// ---------------------------------------------------------------------------
// Processor Tests
// ---------------------------------------------------------------------------

func TestProcessorProcessOnce(t *testing.T) {
	payload := map[string]interface{}{
		"time": 1700000000,
		"states": [][]interface{}{
			{"abc123", "ACA100 ", "CA", 0, 1700000000, -123.2, 49.2, 10000.0, false, 250.0, 180.0, 0.0, nil, 10500.0, "1234", false, 0},
			{"def456", "UAL200 ", "US", 0, 1700000000, -73.9, 40.7, 10000.0, false, 250.0, 180.0, 0.0, nil, 10500.0, "1234", false, 0},
			{"ghi789", "WJA300 ", "CA", 0, 1700000000, -123.0, 49.0, 10000.0, false, 250.0, 180.0, 0.0, nil, 10500.0, "1234", false, 0},
		},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(payload)
	}))
	defer srv.Close()

	var processed int32
	handler := func(ctx context.Context, flights []models.Flight) error {
		atomic.AddInt32(&processed, int32(len(flights)))
		return nil
	}

	client := NewClient().WithBaseURL(srv.URL)
	proc := NewProcessor(client, DefaultProcessorConfig(), handler)

	count, err := proc.ProcessOnce(context.Background())
	require.NoError(t, err)

	// ACA100 matches (Air Canada), WJA300 matches (in YVR bbox), UAL200 doesn't match
	assert.Equal(t, 2, count)
	assert.Equal(t, int32(2), atomic.LoadInt32(&processed))

	// Check metrics
	m := proc.Metrics().Snapshot()
	assert.Equal(t, int64(1), m.TotalRequests)
	assert.Equal(t, int64(1), m.SuccessRequests)
	assert.Equal(t, int64(3), m.TotalFlights)
	assert.Equal(t, int64(2), m.FilteredFlights)
}

func TestProcessorStartStop(t *testing.T) {
	payload := map[string]interface{}{"time": 0, "states": [][]interface{}{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(payload)
	}))
	defer srv.Close()

	client := NewClient().WithBaseURL(srv.URL)
	config := ProcessorConfig{
		PollInterval: 50 * time.Millisecond,
		Filter:       Filter{},
	}
	proc := NewProcessor(client, config, nil)

	assert.False(t, proc.IsRunning())

	err := proc.Start(context.Background())
	require.NoError(t, err)
	assert.True(t, proc.IsRunning())

	// Can't start twice
	err = proc.Start(context.Background())
	assert.Error(t, err)

	time.Sleep(100 * time.Millisecond)
	proc.Stop()

	// Give it time to stop
	time.Sleep(50 * time.Millisecond)
	assert.False(t, proc.IsRunning())
}

func TestEventChannel(t *testing.T) {
	ch := make(chan FlightEvent, 10)
	handler := EventChannel(ch)

	flights := []models.Flight{
		{ICAO24: "abc123", Callsign: "ACA100"},
		{ICAO24: "def456", Callsign: "ACA200"},
	}

	err := handler(context.Background(), flights)
	require.NoError(t, err)

	assert.Len(t, ch, 2)

	event1 := <-ch
	assert.Equal(t, "abc123", event1.Flight.ICAO24)

	event2 := <-ch
	assert.Equal(t, "def456", event2.Flight.ICAO24)
}

func TestEventChannelContextCancel(t *testing.T) {
	ch := make(chan FlightEvent) // Unbuffered, will block
	handler := EventChannel(ch)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	flights := []models.Flight{{ICAO24: "abc123"}}
	err := handler(ctx, flights)
	assert.ErrorIs(t, err, context.Canceled)
}

// ---------------------------------------------------------------------------
// Batch Processing Tests
// ---------------------------------------------------------------------------

func TestProcessorBatchSplitting(t *testing.T) {
	// Create 250 flights
	states := make([][]interface{}, 250)
	for i := 0; i < 250; i++ {
		states[i] = []interface{}{
			"abc" + string(rune(i)),
			"ACA100 ", // All Air Canada to pass filter
			"CA", 0, 1700000000, -123.2, 49.2, 10000.0, false, 250.0, 180.0, 0.0, nil, 10500.0, "1234", false, 0,
		}
	}

	payload := map[string]interface{}{"time": 1700000000, "states": states}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(payload)
	}))
	defer srv.Close()

	var batchCount int32
	var totalProcessed int32
	handler := func(ctx context.Context, flights []models.Flight) error {
		atomic.AddInt32(&batchCount, 1)
		atomic.AddInt32(&totalProcessed, int32(len(flights)))
		return nil
	}

	client := NewClient().WithBaseURL(srv.URL)
	config := ProcessorConfig{
		PollInterval: 10 * time.Second,
		Filter:       YVRFilter(),
		BatchSize:    100,
		Workers:      2,
	}
	proc := NewProcessor(client, config, handler)

	count, err := proc.ProcessOnce(context.Background())
	require.NoError(t, err)

	assert.Equal(t, 250, count)
	assert.Equal(t, int32(250), atomic.LoadInt32(&totalProcessed))
	assert.Equal(t, int32(3), atomic.LoadInt32(&batchCount)) // 100 + 100 + 50
}
