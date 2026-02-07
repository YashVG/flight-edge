package query

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yash/flightedge/internal/ontology"
)

// ---------------------------------------------------------------------------
// Test Helpers
// ---------------------------------------------------------------------------

func setupTestOntology() *ontology.Engine {
	ont := ontology.New()

	f1 := ontology.Node{ID: "f1", Type: ontology.TypeFlight}
	f1.SetString("callsign", "UAL123")
	f1.SetString("origin", "US")
	ont.AddNode(f1)

	f2 := ontology.Node{ID: "f2", Type: ontology.TypeFlight}
	f2.SetString("callsign", "BAW456")
	f2.SetString("origin", "UK")
	ont.AddNode(f2)

	d1 := ontology.Node{ID: "d1", Type: ontology.TypeWeather}
	d1.SetString("condition", "IFR")
	ont.AddNode(d1)

	return ont
}

func setupFullOntology() (*ontology.Engine, *Engine) {
	ont := ontology.New()
	
	// Airports (using uppercase codes in IDs for consistency with flight edges)
	jfk := ontology.Node{ID: "apt-JFK", Type: ontology.TypeAirport}
	jfk.SetString("code", "JFK")
	jfk.SetString("name", "John F Kennedy International")
	jfk.SetString("city", "New York")
	jfk.SetString("country", "US")
	jfk.SetLocation("position", 40.6413, -73.7781)
	ont.AddNode(jfk)
	
	lax := ontology.Node{ID: "apt-LAX", Type: ontology.TypeAirport}
	lax.SetString("code", "LAX")
	lax.SetString("name", "Los Angeles International")
	lax.SetString("city", "Los Angeles")
	lax.SetString("country", "US")
	lax.SetLocation("position", 33.9416, -118.4085)
	ont.AddNode(lax)
	
	yvr := ontology.Node{ID: "apt-YVR", Type: ontology.TypeAirport}
	yvr.SetString("code", "YVR")
	yvr.SetString("name", "Vancouver International")
	yvr.SetString("city", "Vancouver")
	yvr.SetString("country", "CA")
	yvr.SetLocation("position", 49.1967, -123.1815)
	ont.AddNode(yvr)
	
	// Weather
	wxJfk := ontology.Node{ID: "wx-JFK", Type: ontology.TypeWeather}
	wxJfk.SetString("station", "KJFK")
	wxJfk.SetString("condition", "IFR")
	wxJfk.SetFloat("visibility", 2.0)
	wxJfk.SetFloat("wind_speed", 30.0)
	wxJfk.SetTimestamp("observed_at", time.Now())
	ont.AddNode(wxJfk)
	
	// Base time for flights
	baseTime := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	
	// Flights
	flights := []struct {
		id         string
		flightID   string
		callsign   string
		depCode    string
		arrCode    string
		schedDep   time.Time
		actualDep  time.Time
		status     string
		lat, lon   float64
		altitude   float64
		affectedBy string
	}{
		{"fl-1", "UA100", "UAL100", "JFK", "LAX", baseTime, baseTime.Add(15 * time.Minute), "delayed", 40.0, -100.0, 35000, ""},
		{"fl-2", "UA200", "UAL200", "JFK", "YVR", baseTime.Add(1 * time.Hour), baseTime.Add(1 * time.Hour), "on_time", 42.0, -90.0, 38000, "wx-JFK"},
		{"fl-3", "AC300", "ACA300", "YVR", "JFK", baseTime.Add(2 * time.Hour), baseTime.Add(2*time.Hour + 45*time.Minute), "delayed", 45.0, -80.0, 32000, ""},
		{"fl-4", "AC400", "ACA400", "YVR", "LAX", baseTime.Add(3 * time.Hour), baseTime.Add(3 * time.Hour), "on_time", 40.0, -115.0, 30000, ""},
		{"fl-5", "BA500", "BAW500", "JFK", "LAX", baseTime.Add(4 * time.Hour), baseTime.Add(4 * time.Hour), "scheduled", 0, 0, 0, ""},
	}
	
	for _, f := range flights {
		n := ontology.Node{ID: f.id, Type: ontology.TypeFlight}
		n.SetString("flight_id", f.flightID)
		n.SetString("callsign", f.callsign)
		n.SetString("departure_code", f.depCode)
		n.SetString("arrival_code", f.arrCode)
		n.SetString("status", f.status)
		n.SetTimestamp("scheduled_departure", f.schedDep)
		n.SetTimestamp("actual_departure", f.actualDep)
		if f.lat != 0 && f.lon != 0 {
			n.SetLocation("position", f.lat, f.lon)
			n.SetFloat("altitude", f.altitude)
		}
		ont.AddNode(n)
		
		// Add edges
		ont.AddEdge(f.id, "apt-"+f.depCode, ontology.RelDepartsFrom)
		ont.AddEdge(f.id, "apt-"+f.arrCode, ontology.RelArrivesAt)
		if f.affectedBy != "" {
			ont.AddEdge(f.id, f.affectedBy, ontology.RelAffectedBy)
		}
	}
	
	// Create query engine and build indexes
	qe := New(ont)
	
	// Manually index flights (normally done during ingestion)
	for _, f := range flights {
		qe.IndexFlight(f.id, f.callsign, f.depCode, f.arrCode)
	}
	
	return ont, qe
}

// ---------------------------------------------------------------------------
// Basic Execute Tests (existing)
// ---------------------------------------------------------------------------

func TestExecuteByType(t *testing.T) {
	eng := New(setupTestOntology())
	result := eng.Execute(Request{NodeType: ontology.TypeFlight})
	assert.Len(t, result.Nodes, 2)
}

func TestExecuteWithFilter(t *testing.T) {
	eng := New(setupTestOntology())
	result := eng.Execute(Request{
		NodeType: ontology.TypeFlight,
		Filters:  map[string]string{"origin": "UK"},
	})
	assert.Len(t, result.Nodes, 1)
	v, _ := result.Nodes[0].GetString("callsign")
	assert.Equal(t, "BAW456", v)
}

func TestExecuteMaxResults(t *testing.T) {
	eng := New(setupTestOntology())
	result := eng.Execute(Request{
		NodeType:   ontology.TypeFlight,
		MaxResults: 1,
	})
	assert.Len(t, result.Nodes, 1)
}

func TestExecuteNoMatches(t *testing.T) {
	eng := New(setupTestOntology())
	result := eng.Execute(Request{
		NodeType: ontology.TypeFlight,
		Filters:  map[string]string{"origin": "JP"},
	})
	assert.Empty(t, result.Nodes)
}

func TestExecuteElapsed(t *testing.T) {
	eng := New(setupTestOntology())
	result := eng.Execute(Request{NodeType: ontology.TypeFlight})
	assert.True(t, result.Elapsed >= 0)
}

// ---------------------------------------------------------------------------
// TimeRange Tests
// ---------------------------------------------------------------------------

func TestTimeRangeContains(t *testing.T) {
	now := time.Now()
	
	tests := []struct {
		name     string
		tr       TimeRange
		t        time.Time
		expected bool
	}{
		{"empty range matches all", TimeRange{}, now, true},
		{"within range", TimeRange{now.Add(-1 * time.Hour), now.Add(1 * time.Hour)}, now, true},
		{"before range", TimeRange{now.Add(1 * time.Hour), now.Add(2 * time.Hour)}, now, false},
		{"after range", TimeRange{now.Add(-2 * time.Hour), now.Add(-1 * time.Hour)}, now, false},
		{"start only", TimeRange{Start: now.Add(-1 * time.Hour)}, now, true},
		{"end only", TimeRange{End: now.Add(1 * time.Hour)}, now, true},
	}
	
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.tr.Contains(tc.t))
		})
	}
}

// ---------------------------------------------------------------------------
// Index Tests
// ---------------------------------------------------------------------------

func TestAirlineIndex(t *testing.T) {
	idx := NewAirlineIndex()
	
	idx.Add("UAL", "flight-1")
	idx.Add("UAL", "flight-2")
	idx.Add("ACA", "flight-3")
	
	ual := idx.Get("UAL")
	assert.Len(t, ual, 2)
	assert.Contains(t, ual, "flight-1")
	assert.Contains(t, ual, "flight-2")
	
	aca := idx.Get("ACA")
	assert.Len(t, aca, 1)
	assert.Contains(t, aca, "flight-3")
	
	// Remove
	idx.Remove("UAL", "flight-1")
	ual = idx.Get("UAL")
	assert.Len(t, ual, 1)
	assert.Contains(t, ual, "flight-2")
}

func TestAirportFlightIndex(t *testing.T) {
	idx := NewAirportFlightIndex()
	
	idx.AddDeparture("JFK", "fl-1")
	idx.AddDeparture("JFK", "fl-2")
	idx.AddArrival("JFK", "fl-3")
	idx.AddArrival("LAX", "fl-1")
	
	deps := idx.GetDepartures("JFK")
	assert.Len(t, deps, 2)
	
	arrs := idx.GetArrivals("JFK")
	assert.Len(t, arrs, 1)
	
	all := idx.GetAll("JFK")
	assert.Len(t, all, 3)
}

// ---------------------------------------------------------------------------
// GetFlightsByAirport Tests
// ---------------------------------------------------------------------------

func TestGetFlightsByAirport(t *testing.T) {
	_, qe := setupFullOntology()
	
	// Get all JFK flights
	result := qe.GetFlightsByAirport("JFK", TimeRange{}, 0)
	assert.Equal(t, 4, result.Total) // 3 departures + 1 arrival (fl-2 JFK->YVR counts as departure)
	assert.Len(t, result.Flights, 4)
	
	// With max results
	result = qe.GetFlightsByAirport("JFK", TimeRange{}, 2)
	assert.Equal(t, 4, result.Total)
	assert.Len(t, result.Flights, 2)
}

func TestGetFlightsByAirportWithTimeRange(t *testing.T) {
	_, qe := setupFullOntology()
	
	baseTime := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	
	// First 2 hours only
	tr := TimeRange{
		Start: baseTime,
		End:   baseTime.Add(2 * time.Hour),
	}
	
	result := qe.GetFlightsByAirport("JFK", tr, 0)
	// fl-1 (10:00), fl-2 (11:00) match
	assert.GreaterOrEqual(t, result.Total, 2)
}

func TestGetFlightsByAirportDepartures(t *testing.T) {
	_, qe := setupFullOntology()
	
	result := qe.GetFlightsByAirportDepartures("JFK", TimeRange{}, 0)
	assert.Equal(t, 3, result.Total) // UA100, UA200, BA500
}

func TestGetFlightsByAirportArrivals(t *testing.T) {
	_, qe := setupFullOntology()
	
	result := qe.GetFlightsByAirportArrivals("JFK", TimeRange{}, 0)
	assert.Equal(t, 1, result.Total) // AC300 YVR->JFK
}

// ---------------------------------------------------------------------------
// GetDelayedFlights Tests
// ---------------------------------------------------------------------------

func TestGetDelayedFlights(t *testing.T) {
	_, qe := setupFullOntology()
	
	// Any delay
	result := qe.GetDelayedFlights(1*time.Minute, "", 0)
	// fl-1: 15 min delay, fl-3: 45 min delay
	assert.Equal(t, 2, result.Total)
	
	// Sorted by delay descending
	if len(result.Flights) >= 2 {
		assert.Greater(t, result.Flights[0].DelayMinutes, result.Flights[1].DelayMinutes)
	}
}

func TestGetDelayedFlightsByAirline(t *testing.T) {
	_, qe := setupFullOntology()
	
	// Air Canada delays
	result := qe.GetDelayedFlights(1*time.Minute, "ACA", 0)
	assert.Equal(t, 1, result.Total) // Only AC300 is delayed
	if len(result.Flights) > 0 {
		assert.Equal(t, "ACA300", result.Flights[0].Callsign)
	}
}

func TestGetDelayedFlightsWithThreshold(t *testing.T) {
	_, qe := setupFullOntology()
	
	// Only delays >= 30 minutes
	result := qe.GetDelayedFlights(30*time.Minute, "", 0)
	assert.Equal(t, 1, result.Total) // Only fl-3 with 45 min delay
}

// ---------------------------------------------------------------------------
// GetFlightPath Tests
// ---------------------------------------------------------------------------

func TestGetFlightPath(t *testing.T) {
	_, qe := setupFullOntology()
	
	// By flight_id
	path, ok := qe.GetFlightPath("UA100")
	require.True(t, ok)
	
	assert.Equal(t, "UA100", path.FlightID)
	assert.Equal(t, "UAL100", path.Callsign)
	assert.Equal(t, "JFK", path.Departure.Code)
	assert.Equal(t, "LAX", path.Arrival.Code)
}

func TestGetFlightPathWithWeather(t *testing.T) {
	_, qe := setupFullOntology()
	
	// fl-2 is affected by weather
	path, ok := qe.GetFlightPath("UA200")
	require.True(t, ok)
	
	assert.Len(t, path.WeatherAlerts, 1)
	assert.Equal(t, "KJFK", path.WeatherAlerts[0].Station)
	assert.Equal(t, "IFR", path.WeatherAlerts[0].Condition)
}

func TestGetFlightPathNotFound(t *testing.T) {
	_, qe := setupFullOntology()
	
	_, ok := qe.GetFlightPath("NONEXISTENT")
	assert.False(t, ok)
}

func TestGetFlightPathByNodeID(t *testing.T) {
	_, qe := setupFullOntology()
	
	// By node ID directly
	path, ok := qe.GetFlightPath("fl-1")
	require.True(t, ok)
	assert.Equal(t, "UAL100", path.Callsign)
}

// ---------------------------------------------------------------------------
// PredictDelay Tests
// ---------------------------------------------------------------------------

func TestPredictDelay(t *testing.T) {
	_, qe := setupFullOntology()
	
	// Add some historical data
	qe.RecordDelay("JFK", "LAX", "UAL", time.Now(), 20*time.Minute)
	qe.RecordDelay("JFK", "LAX", "UAL", time.Now(), 10*time.Minute)
	qe.RecordDelay("JFK", "LAX", "UAL", time.Now(), 30*time.Minute)
	
	pred, ok := qe.PredictDelay("UA100")
	require.True(t, ok)
	
	assert.Equal(t, "UA100", pred.FlightID)
	assert.True(t, pred.PredictedDelay > 0)
	assert.True(t, pred.Confidence > 0)
	assert.NotEmpty(t, pred.Factors)
}

func TestPredictDelayWithWeatherImpact(t *testing.T) {
	_, qe := setupFullOntology()
	
	// UA200 is affected by IFR weather
	pred, ok := qe.PredictDelay("UA200")
	require.True(t, ok)
	
	// Should have weather impact factor
	hasWeatherFactor := false
	for _, f := range pred.Factors {
		if f.Name == "weather" {
			hasWeatherFactor = true
			assert.True(t, f.Impact > 0)
		}
	}
	assert.True(t, hasWeatherFactor || pred.WeatherImpact > 0)
}

func TestPredictDelayNotFound(t *testing.T) {
	_, qe := setupFullOntology()
	
	_, ok := qe.PredictDelay("NONEXISTENT")
	assert.False(t, ok)
}

// ---------------------------------------------------------------------------
// DelayHistory Tests
// ---------------------------------------------------------------------------

func TestDelayHistoryRouteStats(t *testing.T) {
	h := NewDelayHistory()
	
	h.Record("JFK", "LAX", "UAL", time.Now(), 10*time.Minute)
	h.Record("JFK", "LAX", "UAL", time.Now(), 20*time.Minute)
	h.Record("JFK", "LAX", "UAL", time.Now(), 30*time.Minute)
	
	avg := h.GetRouteAvgDelay("JFK", "LAX")
	assert.Equal(t, 20*time.Minute, avg)
}

func TestDelayHistoryAirlineStats(t *testing.T) {
	h := NewDelayHistory()
	
	h.Record("JFK", "LAX", "UAL", time.Now(), 15*time.Minute)
	h.Record("JFK", "SFO", "UAL", time.Now(), 25*time.Minute)
	
	avg := h.GetAirlineAvgDelay("UAL")
	assert.Equal(t, 20*time.Minute, avg)
}

func TestDelayHistoryHourStats(t *testing.T) {
	h := NewDelayHistory()
	
	morning := time.Date(2024, 1, 1, 8, 0, 0, 0, time.UTC)
	h.Record("JFK", "LAX", "UAL", morning, 10*time.Minute)
	h.Record("JFK", "SFO", "UAL", morning, 20*time.Minute)
	
	avg := h.GetHourAvgDelay(8)
	assert.Equal(t, 15*time.Minute, avg)
}

func TestDelayHistoryAirportCongestion(t *testing.T) {
	h := NewDelayHistory()
	
	h.SetAirportCongestion("JFK", 0.75)
	
	congestion := h.GetAirportCongestion("JFK")
	assert.Equal(t, 0.75, congestion)
}

// ---------------------------------------------------------------------------
// Memory Pool Tests
// ---------------------------------------------------------------------------

func TestNodeSlicePool(t *testing.T) {
	// Acquire and release to test pool functionality
	slice := acquireNodeSlice()
	assert.NotNil(t, slice)
	assert.Equal(t, 0, len(*slice))
	
	*slice = append(*slice, ontology.Node{ID: "test"})
	assert.Equal(t, 1, len(*slice))
	
	releaseNodeSlice(slice)
	
	// Get from pool again - should be reset
	slice2 := acquireNodeSlice()
	assert.Equal(t, 0, len(*slice2))
	releaseNodeSlice(slice2)
}

func TestEdgeSlicePool(t *testing.T) {
	slice := acquireEdgeSlice()
	assert.NotNil(t, slice)
	assert.Equal(t, 0, len(*slice))
	
	*slice = append(*slice, ontology.Edge{FromID: "a", ToID: "b"})
	releaseEdgeSlice(slice)
	
	slice2 := acquireEdgeSlice()
	assert.Equal(t, 0, len(*slice2))
	releaseEdgeSlice(slice2)
}

// ---------------------------------------------------------------------------
// Rebuild Index Tests
// ---------------------------------------------------------------------------

func TestRebuildIndexes(t *testing.T) {
	ont := ontology.New()
	
	// Add flights with departure/arrival codes
	f1 := ontology.Node{ID: "f1", Type: ontology.TypeFlight}
	f1.SetString("callsign", "UAL100")
	f1.SetString("departure_code", "JFK")
	f1.SetString("arrival_code", "LAX")
	ont.AddNode(f1)
	
	f2 := ontology.Node{ID: "f2", Type: ontology.TypeFlight}
	f2.SetString("callsign", "UAL200")
	f2.SetString("departure_code", "JFK")
	f2.SetString("arrival_code", "SFO")
	ont.AddNode(f2)
	
	qe := New(ont)
	qe.RebuildIndexes()
	
	// Check airline index
	ual := qe.airlineIdx.Get("UAL")
	assert.Len(t, ual, 2)
	
	// Check airport index
	jfkDeps := qe.airportIdx.GetDepartures("JFK")
	assert.Len(t, jfkDeps, 2)
}

// ---------------------------------------------------------------------------
// Performance Tests
// ---------------------------------------------------------------------------

func TestQueryLatencyUnder50ms(t *testing.T) {
	_, qe := setupFullOntology()
	
	// GetFlightsByAirport
	result := qe.GetFlightsByAirport("JFK", TimeRange{}, 10)
	assert.Less(t, result.Elapsed, 50*time.Millisecond)
	
	// GetDelayedFlights
	delayResult := qe.GetDelayedFlights(1*time.Minute, "", 10)
	assert.Less(t, delayResult.Elapsed, 50*time.Millisecond)
	
	// GetFlightPath
	path, ok := qe.GetFlightPath("UA100")
	require.True(t, ok)
	assert.Less(t, path.Elapsed, 50*time.Millisecond)
	
	// PredictDelay
	pred, ok := qe.PredictDelay("UA100")
	require.True(t, ok)
	assert.Less(t, pred.Elapsed, 50*time.Millisecond)
}
