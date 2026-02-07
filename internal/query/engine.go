package query

import (
	"sort"
	"sync"
	"time"

	"github.com/yash/flightedge/internal/ontology"
)

// ---------------------------------------------------------------------------
// Result Pool - Memory pooling for zero-allocation queries
// ---------------------------------------------------------------------------

// NodeSlice is a poolable slice of nodes.
type NodeSlice []ontology.Node

var nodeSlicePool = sync.Pool{
	New: func() interface{} {
		s := make(NodeSlice, 0, 64)
		return &s
	},
}

// acquireNodeSlice gets a slice from the pool.
func acquireNodeSlice() *NodeSlice {
	return nodeSlicePool.Get().(*NodeSlice)
}

// releaseNodeSlice returns a slice to the pool.
func releaseNodeSlice(s *NodeSlice) {
	*s = (*s)[:0]
	nodeSlicePool.Put(s)
}

// EdgeSlice is a poolable slice of edges.
type EdgeSlice []ontology.Edge

var edgeSlicePool = sync.Pool{
	New: func() interface{} {
		s := make(EdgeSlice, 0, 32)
		return &s
	},
}

func acquireEdgeSlice() *EdgeSlice {
	return edgeSlicePool.Get().(*EdgeSlice)
}

func releaseEdgeSlice(s *EdgeSlice) {
	*s = (*s)[:0]
	edgeSlicePool.Put(s)
}

// ---------------------------------------------------------------------------
// Time Range
// ---------------------------------------------------------------------------

// TimeRange specifies a time window for queries.
type TimeRange struct {
	Start time.Time
	End   time.Time
}

// Contains checks if a timestamp falls within the range.
func (tr TimeRange) Contains(t time.Time) bool {
	if tr.Start.IsZero() && tr.End.IsZero() {
		return true
	}
	if tr.Start.IsZero() {
		return !t.After(tr.End)
	}
	if tr.End.IsZero() {
		return !t.Before(tr.Start)
	}
	return !t.Before(tr.Start) && !t.After(tr.End)
}

// ---------------------------------------------------------------------------
// Query Results
// ---------------------------------------------------------------------------

// Request describes a structured query against the ontology.
type Request struct {
	NodeType   ontology.NodeType
	Filters    map[string]string // property key → expected string value
	MaxResults int
}

// Result holds the response from an ontology query.
type Result struct {
	Nodes   []ontology.Node
	Elapsed time.Duration
}

// FlightResult holds flight query results with metadata.
type FlightResult struct {
	Flights []FlightInfo
	Elapsed time.Duration
	Total   int // Total matches before limit applied
}

// FlightInfo contains flight details with resolved relationships.
type FlightInfo struct {
	ID              string
	FlightID        string
	Callsign        string
	Airline         string
	DepartureCode   string
	DepartureName   string
	ArrivalCode     string
	ArrivalName     string
	ScheduledDep    time.Time
	ActualDep       time.Time
	ScheduledArr    time.Time
	ActualArr       time.Time
	Status          string
	DelayMinutes    int
	Altitude        float64
	Velocity        float64
	Latitude        float64
	Longitude       float64
	OnGround        bool
}

// FlightPath represents the route of a flight.
type FlightPath struct {
	FlightID      string
	Callsign      string
	Departure     AirportInfo
	Arrival       AirportInfo
	Waypoints     []Waypoint
	WeatherAlerts []WeatherInfo
	Elapsed       time.Duration
}

// AirportInfo contains airport details.
type AirportInfo struct {
	Code      string
	Name      string
	City      string
	Country   string
	Latitude  float64
	Longitude float64
}

// Waypoint is a position along the flight path.
type Waypoint struct {
	Timestamp time.Time
	Latitude  float64
	Longitude float64
	Altitude  float64
	Velocity  float64
}

// WeatherInfo contains weather conditions.
type WeatherInfo struct {
	Station     string
	Condition   string
	WindSpeed   float64
	Visibility  float64
	Temperature float64
	ObservedAt  time.Time
}

// DelayPrediction holds prediction results.
type DelayPrediction struct {
	FlightID         string
	PredictedDelay   time.Duration
	Confidence       float64 // 0.0 to 1.0
	Factors          []DelayFactor
	HistoricalAvg    time.Duration
	WeatherImpact    time.Duration
	AirportCongestion time.Duration
	Elapsed          time.Duration
}

// DelayFactor describes a contributing factor to predicted delay.
type DelayFactor struct {
	Name        string
	Impact      time.Duration
	Description string
}

// ---------------------------------------------------------------------------
// Secondary Indexes for Query Optimization
// ---------------------------------------------------------------------------

// AirlineIndex provides O(1) lookup of flights by airline.
type AirlineIndex struct {
	mu      sync.RWMutex
	byCode  map[string][]string // airline code → flight node IDs
	dirty   bool
}

// NewAirlineIndex creates an airline index.
func NewAirlineIndex() *AirlineIndex {
	return &AirlineIndex{
		byCode: make(map[string][]string, 64),
	}
}

// Add indexes a flight under its airline.
func (idx *AirlineIndex) Add(airlineCode, flightNodeID string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.byCode[airlineCode] = append(idx.byCode[airlineCode], flightNodeID)
}

// Remove removes a flight from its airline's list.
func (idx *AirlineIndex) Remove(airlineCode, flightNodeID string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	flights := idx.byCode[airlineCode]
	for i, id := range flights {
		if id == flightNodeID {
			idx.byCode[airlineCode] = append(flights[:i], flights[i+1:]...)
			return
		}
	}
}

// Get returns all flight node IDs for an airline.
func (idx *AirlineIndex) Get(airlineCode string) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.byCode[airlineCode]
}

// AirportFlightIndex maps airports to their flights.
type AirportFlightIndex struct {
	mu         sync.RWMutex
	departures map[string][]string // airport code → departing flight IDs
	arrivals   map[string][]string // airport code → arriving flight IDs
}

// NewAirportFlightIndex creates an airport-flight index.
func NewAirportFlightIndex() *AirportFlightIndex {
	return &AirportFlightIndex{
		departures: make(map[string][]string, 256),
		arrivals:   make(map[string][]string, 256),
	}
}

// AddDeparture indexes a flight departing from an airport.
func (idx *AirportFlightIndex) AddDeparture(airportCode, flightNodeID string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.departures[airportCode] = append(idx.departures[airportCode], flightNodeID)
}

// AddArrival indexes a flight arriving at an airport.
func (idx *AirportFlightIndex) AddArrival(airportCode, flightNodeID string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.arrivals[airportCode] = append(idx.arrivals[airportCode], flightNodeID)
}

// GetDepartures returns flight IDs departing from an airport.
func (idx *AirportFlightIndex) GetDepartures(airportCode string) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.departures[airportCode]
}

// GetArrivals returns flight IDs arriving at an airport.
func (idx *AirportFlightIndex) GetArrivals(airportCode string) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.arrivals[airportCode]
}

// GetAll returns all flights (departures + arrivals) for an airport.
func (idx *AirportFlightIndex) GetAll(airportCode string) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	
	deps := idx.departures[airportCode]
	arrs := idx.arrivals[airportCode]
	
	// Deduplicate
	seen := make(map[string]struct{}, len(deps)+len(arrs))
	result := make([]string, 0, len(deps)+len(arrs))
	
	for _, id := range deps {
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			result = append(result, id)
		}
	}
	for _, id := range arrs {
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			result = append(result, id)
		}
	}
	return result
}

// ---------------------------------------------------------------------------
// Historical Data for Delay Prediction
// ---------------------------------------------------------------------------

// DelayHistory tracks historical delay patterns.
type DelayHistory struct {
	mu sync.RWMutex
	
	// By route (departure-arrival pair)
	byRoute map[string]*RouteStats
	
	// By airline
	byAirline map[string]*AirlineStats
	
	// By airport
	byAirport map[string]*AirportStats
	
	// By hour of day (0-23)
	byHour [24]*HourStats
}

// RouteStats holds delay statistics for a route.
type RouteStats struct {
	TotalFlights  int
	TotalDelay    time.Duration
	DelayedCount  int
	MaxDelay      time.Duration
}

// AirlineStats holds delay statistics for an airline.
type AirlineStats struct {
	TotalFlights int
	TotalDelay   time.Duration
	DelayedCount int
}

// AirportStats holds delay statistics for an airport.
type AirportStats struct {
	TotalFlights int
	TotalDelay   time.Duration
	DelayedCount int
	Congestion   float64 // Current congestion factor (0-1)
}

// HourStats holds delay statistics for an hour of day.
type HourStats struct {
	TotalFlights int
	TotalDelay   time.Duration
	DelayedCount int
}

// NewDelayHistory creates a delay history tracker.
func NewDelayHistory() *DelayHistory {
	h := &DelayHistory{
		byRoute:   make(map[string]*RouteStats),
		byAirline: make(map[string]*AirlineStats),
		byAirport: make(map[string]*AirportStats),
	}
	for i := range h.byHour {
		h.byHour[i] = &HourStats{}
	}
	return h
}

// Record adds a flight's delay to historical data.
func (h *DelayHistory) Record(depCode, arrCode, airline string, scheduledDep time.Time, delay time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	routeKey := depCode + "-" + arrCode
	
	// Route stats
	rs, ok := h.byRoute[routeKey]
	if !ok {
		rs = &RouteStats{}
		h.byRoute[routeKey] = rs
	}
	rs.TotalFlights++
	rs.TotalDelay += delay
	if delay > 0 {
		rs.DelayedCount++
	}
	if delay > rs.MaxDelay {
		rs.MaxDelay = delay
	}
	
	// Airline stats
	as, ok := h.byAirline[airline]
	if !ok {
		as = &AirlineStats{}
		h.byAirline[airline] = as
	}
	as.TotalFlights++
	as.TotalDelay += delay
	if delay > 0 {
		as.DelayedCount++
	}
	
	// Airport stats (departure)
	aps, ok := h.byAirport[depCode]
	if !ok {
		aps = &AirportStats{}
		h.byAirport[depCode] = aps
	}
	aps.TotalFlights++
	aps.TotalDelay += delay
	if delay > 0 {
		aps.DelayedCount++
	}
	
	// Hour stats
	hour := scheduledDep.Hour()
	h.byHour[hour].TotalFlights++
	h.byHour[hour].TotalDelay += delay
	if delay > 0 {
		h.byHour[hour].DelayedCount++
	}
}

// GetRouteAvgDelay returns average delay for a route.
func (h *DelayHistory) GetRouteAvgDelay(depCode, arrCode string) time.Duration {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	rs, ok := h.byRoute[depCode+"-"+arrCode]
	if !ok || rs.TotalFlights == 0 {
		return 0
	}
	return rs.TotalDelay / time.Duration(rs.TotalFlights)
}

// GetAirlineAvgDelay returns average delay for an airline.
func (h *DelayHistory) GetAirlineAvgDelay(airline string) time.Duration {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	as, ok := h.byAirline[airline]
	if !ok || as.TotalFlights == 0 {
		return 0
	}
	return as.TotalDelay / time.Duration(as.TotalFlights)
}

// GetHourAvgDelay returns average delay for an hour of day.
func (h *DelayHistory) GetHourAvgDelay(hour int) time.Duration {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	if hour < 0 || hour > 23 || h.byHour[hour].TotalFlights == 0 {
		return 0
	}
	return h.byHour[hour].TotalDelay / time.Duration(h.byHour[hour].TotalFlights)
}

// SetAirportCongestion updates congestion factor for an airport.
func (h *DelayHistory) SetAirportCongestion(code string, congestion float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	aps, ok := h.byAirport[code]
	if !ok {
		aps = &AirportStats{}
		h.byAirport[code] = aps
	}
	aps.Congestion = congestion
}

// GetAirportCongestion returns congestion factor for an airport.
func (h *DelayHistory) GetAirportCongestion(code string) float64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	aps, ok := h.byAirport[code]
	if !ok {
		return 0
	}
	return aps.Congestion
}

// ---------------------------------------------------------------------------
// Query Engine
// ---------------------------------------------------------------------------

// Engine executes structured queries against an ontology graph.
type Engine struct {
	ont           *ontology.Engine
	airlineIdx    *AirlineIndex
	airportIdx    *AirportFlightIndex
	history       *DelayHistory
	
	// Result pools
	flightPool    sync.Pool
}

// New creates a query engine backed by the given ontology.
func New(ont *ontology.Engine) *Engine {
	return &Engine{
		ont:        ont,
		airlineIdx: NewAirlineIndex(),
		airportIdx: NewAirportFlightIndex(),
		history:    NewDelayHistory(),
		flightPool: sync.Pool{
			New: func() interface{} {
				return make([]FlightInfo, 0, 64)
			},
		},
	}
}

// RebuildIndexes scans the ontology and rebuilds secondary indexes.
// Should be called after bulk loading data.
func (e *Engine) RebuildIndexes() {
	// Clear existing indexes
	e.airlineIdx = NewAirlineIndex()
	e.airportIdx = NewAirportFlightIndex()
	
	// Scan all flights
	e.ont.ForEachNodeOfType(ontology.TypeFlight, func(n *ontology.Node) bool {
		// Index by airline (extract from callsign, first 3 chars typically ICAO code)
		if callsign, ok := n.GetString("callsign"); ok && len(callsign) >= 3 {
			airline := callsign[:3]
			e.airlineIdx.Add(airline, n.ID)
		}
		
		// Index by departure airport
		if depCode, ok := n.GetString("departure_code"); ok {
			e.airportIdx.AddDeparture(depCode, n.ID)
		}
		
		// Index by arrival airport
		if arrCode, ok := n.GetString("arrival_code"); ok {
			e.airportIdx.AddArrival(arrCode, n.ID)
		}
		
		return true
	})
}

// IndexFlight adds a single flight to the indexes.
func (e *Engine) IndexFlight(nodeID, callsign, depCode, arrCode string) {
	if len(callsign) >= 3 {
		e.airlineIdx.Add(callsign[:3], nodeID)
	}
	if depCode != "" {
		e.airportIdx.AddDeparture(depCode, nodeID)
	}
	if arrCode != "" {
		e.airportIdx.AddArrival(arrCode, nodeID)
	}
}

// RecordDelay adds delay data to historical tracking.
func (e *Engine) RecordDelay(depCode, arrCode, airline string, scheduledDep time.Time, delay time.Duration) {
	e.history.Record(depCode, arrCode, airline, scheduledDep, delay)
}

// Execute runs a query using the type index and visitor pattern.
func (e *Engine) Execute(req Request) Result {
	start := time.Now()

	var matched []ontology.Node
	e.ont.ForEachNodeOfType(req.NodeType, func(n *ontology.Node) bool {
		if matchesFilters(n, req.Filters) {
			matched = append(matched, cloneNode(n))
			if req.MaxResults > 0 && len(matched) >= req.MaxResults {
				return false
			}
		}
		return true
	})

	return Result{
		Nodes:   matched,
		Elapsed: time.Since(start),
	}
}

// ---------------------------------------------------------------------------
// GetFlightsByAirport - O(k) where k is flights at airport
// ---------------------------------------------------------------------------

// GetFlightsByAirport returns flights associated with an airport within a time range.
func (e *Engine) GetFlightsByAirport(airportCode string, tr TimeRange, maxResults int) FlightResult {
	start := time.Now()
	
	// Use pre-built index for O(1) airport lookup
	flightIDs := e.airportIdx.GetAll(airportCode)
	
	results := e.flightPool.Get().([]FlightInfo)
	results = results[:0]
	total := 0
	
	for _, fid := range flightIDs {
		node, ok := e.ont.GetNode(fid)
		if !ok {
			continue
		}
		
		// Check time range
		if !tr.Start.IsZero() || !tr.End.IsZero() {
			if depTime, ok := node.GetTimestamp("scheduled_departure"); ok {
				if !tr.Contains(depTime) {
					continue
				}
			}
		}
		
		total++
		if maxResults > 0 && len(results) >= maxResults {
			continue // Still count total
		}
		
		results = append(results, e.nodeToFlightInfo(&node))
	}
	
	// Sort by scheduled departure
	sort.Slice(results, func(i, j int) bool {
		return results[i].ScheduledDep.Before(results[j].ScheduledDep)
	})
	
	// Copy out of pool
	final := make([]FlightInfo, len(results))
	copy(final, results)
	e.flightPool.Put(results)
	
	return FlightResult{
		Flights: final,
		Elapsed: time.Since(start),
		Total:   total,
	}
}

// GetFlightsByAirportDepartures returns only departing flights.
func (e *Engine) GetFlightsByAirportDepartures(airportCode string, tr TimeRange, maxResults int) FlightResult {
	start := time.Now()
	
	flightIDs := e.airportIdx.GetDepartures(airportCode)
	results := make([]FlightInfo, 0, len(flightIDs))
	total := 0
	
	for _, fid := range flightIDs {
		node, ok := e.ont.GetNode(fid)
		if !ok {
			continue
		}
		
		if !tr.Start.IsZero() || !tr.End.IsZero() {
			if depTime, ok := node.GetTimestamp("scheduled_departure"); ok {
				if !tr.Contains(depTime) {
					continue
				}
			}
		}
		
		total++
		if maxResults > 0 && len(results) >= maxResults {
			continue
		}
		
		results = append(results, e.nodeToFlightInfo(&node))
	}
	
	return FlightResult{
		Flights: results,
		Elapsed: time.Since(start),
		Total:   total,
	}
}

// GetFlightsByAirportArrivals returns only arriving flights.
func (e *Engine) GetFlightsByAirportArrivals(airportCode string, tr TimeRange, maxResults int) FlightResult {
	start := time.Now()
	
	flightIDs := e.airportIdx.GetArrivals(airportCode)
	results := make([]FlightInfo, 0, len(flightIDs))
	total := 0
	
	for _, fid := range flightIDs {
		node, ok := e.ont.GetNode(fid)
		if !ok {
			continue
		}
		
		if !tr.Start.IsZero() || !tr.End.IsZero() {
			if arrTime, ok := node.GetTimestamp("scheduled_arrival"); ok {
				if !tr.Contains(arrTime) {
					continue
				}
			}
		}
		
		total++
		if maxResults > 0 && len(results) >= maxResults {
			continue
		}
		
		results = append(results, e.nodeToFlightInfo(&node))
	}
	
	return FlightResult{
		Flights: results,
		Elapsed: time.Since(start),
		Total:   total,
	}
}

// ---------------------------------------------------------------------------
// GetDelayedFlights - Scans flights with delay threshold
// ---------------------------------------------------------------------------

// GetDelayedFlights returns flights with delays exceeding the threshold.
// If airline is non-empty, filters by airline code (first 3 chars of callsign).
func (e *Engine) GetDelayedFlights(threshold time.Duration, airline string, maxResults int) FlightResult {
	start := time.Now()
	
	var flightIDs []string
	
	// Use airline index if filtering by airline
	if airline != "" {
		flightIDs = e.airlineIdx.Get(airline)
	}
	
	results := make([]FlightInfo, 0, 64)
	total := 0
	thresholdMinutes := int(threshold.Minutes())
	
	if airline != "" {
		// Scan only flights from this airline
		for _, fid := range flightIDs {
			node, ok := e.ont.GetNode(fid)
			if !ok {
				continue
			}
			
			delay := e.getDelayMinutes(&node)
			if delay >= thresholdMinutes {
				total++
				if maxResults <= 0 || len(results) < maxResults {
					info := e.nodeToFlightInfo(&node)
					info.DelayMinutes = delay
					results = append(results, info)
				}
			}
		}
	} else {
		// Scan all flights
		e.ont.ForEachNodeOfType(ontology.TypeFlight, func(n *ontology.Node) bool {
			delay := e.getDelayMinutes(n)
			if delay >= thresholdMinutes {
				total++
				if maxResults <= 0 || len(results) < maxResults {
					info := e.nodeToFlightInfo(n)
					info.DelayMinutes = delay
					results = append(results, info)
				}
			}
			return true
		})
	}
	
	// Sort by delay descending
	sort.Slice(results, func(i, j int) bool {
		return results[i].DelayMinutes > results[j].DelayMinutes
	})
	
	return FlightResult{
		Flights: results,
		Elapsed: time.Since(start),
		Total:   total,
	}
}

// ---------------------------------------------------------------------------
// GetFlightPath - Returns full flight route with waypoints
// ---------------------------------------------------------------------------

// GetFlightPath retrieves the complete path for a flight including airports and weather.
func (e *Engine) GetFlightPath(flightID string) (FlightPath, bool) {
	start := time.Now()
	
	// Try flight_id index first
	node, ok := e.ont.GetFlightByID(flightID)
	if !ok {
		// Try direct node ID
		node, ok = e.ont.GetNode(flightID)
		if !ok {
			return FlightPath{}, false
		}
	}
	
	path := FlightPath{
		FlightID: flightID,
	}
	
	// Get callsign
	if cs, ok := node.GetString("callsign"); ok {
		path.Callsign = cs
	}
	
	// Traverse relationships to get airports
	e.ont.ForEachEdgeFrom(node.ID, func(rel ontology.RelationType, neighbor *ontology.Node) bool {
		switch rel {
		case ontology.RelDepartsFrom:
			path.Departure = e.nodeToAirportInfo(neighbor)
		case ontology.RelArrivesAt:
			path.Arrival = e.nodeToAirportInfo(neighbor)
		case ontology.RelAffectedBy:
			if neighbor.Type == ontology.TypeWeather {
				path.WeatherAlerts = append(path.WeatherAlerts, e.nodeToWeatherInfo(neighbor))
			}
		}
		return true
	})
	
	// Add current position as waypoint if available
	if lat, lon, ok := node.GetLocation("position"); ok {
		wp := Waypoint{
			Latitude:  lat,
			Longitude: lon,
		}
		if alt, ok := node.GetFloat("altitude"); ok {
			wp.Altitude = alt
		}
		if vel, ok := node.GetFloat("velocity"); ok {
			wp.Velocity = vel
		}
		if ts, ok := node.GetTimestamp("last_contact"); ok {
			wp.Timestamp = ts
		}
		path.Waypoints = append(path.Waypoints, wp)
	}
	
	path.Elapsed = time.Since(start)
	return path, true
}

// ---------------------------------------------------------------------------
// PredictDelay - Heuristic-based delay prediction
// ---------------------------------------------------------------------------

// PredictDelay estimates the expected delay for a flight based on historical patterns.
func (e *Engine) PredictDelay(flightID string) (DelayPrediction, bool) {
	start := time.Now()
	
	// Get flight node
	node, ok := e.ont.GetFlightByID(flightID)
	if !ok {
		node, ok = e.ont.GetNode(flightID)
		if !ok {
			return DelayPrediction{}, false
		}
	}
	
	pred := DelayPrediction{
		FlightID: flightID,
		Factors:  make([]DelayFactor, 0, 4),
	}
	
	// Extract flight details
	var depCode, arrCode, airline string
	var scheduledDep time.Time
	
	if cs, ok := node.GetString("callsign"); ok && len(cs) >= 3 {
		airline = cs[:3]
	}
	if dc, ok := node.GetString("departure_code"); ok {
		depCode = dc
	}
	if ac, ok := node.GetString("arrival_code"); ok {
		arrCode = ac
	}
	if sd, ok := node.GetTimestamp("scheduled_departure"); ok {
		scheduledDep = sd
	}
	
	var totalDelay time.Duration
	var factors int
	
	// Factor 1: Historical route delay
	routeDelay := e.history.GetRouteAvgDelay(depCode, arrCode)
	if routeDelay > 0 {
		pred.Factors = append(pred.Factors, DelayFactor{
			Name:        "route_history",
			Impact:      routeDelay,
			Description: "Historical average delay on this route",
		})
		totalDelay += routeDelay
		factors++
	}
	pred.HistoricalAvg = routeDelay
	
	// Factor 2: Airline performance
	airlineDelay := e.history.GetAirlineAvgDelay(airline)
	if airlineDelay > 0 {
		pred.Factors = append(pred.Factors, DelayFactor{
			Name:        "airline_history",
			Impact:      airlineDelay,
			Description: "Airline's historical delay performance",
		})
		totalDelay += airlineDelay
		factors++
	}
	
	// Factor 3: Time of day
	if !scheduledDep.IsZero() {
		hourDelay := e.history.GetHourAvgDelay(scheduledDep.Hour())
		if hourDelay > 0 {
			pred.Factors = append(pred.Factors, DelayFactor{
				Name:        "time_of_day",
				Impact:      hourDelay,
				Description: "Typical delays at this departure time",
			})
			totalDelay += hourDelay
			factors++
		}
	}
	
	// Factor 4: Airport congestion
	congestion := e.history.GetAirportCongestion(depCode)
	if congestion > 0 {
		// Convert congestion factor to delay (0.5 congestion = 15 min avg delay)
		congestionDelay := time.Duration(congestion * 30 * float64(time.Minute))
		pred.Factors = append(pred.Factors, DelayFactor{
			Name:        "airport_congestion",
			Impact:      congestionDelay,
			Description: "Current departure airport congestion",
		})
		pred.AirportCongestion = congestionDelay
		totalDelay += congestionDelay
		factors++
	}
	
	// Factor 5: Weather impact (check affected_by relationships)
	var weatherImpact time.Duration
	e.ont.ForEachEdgeFrom(node.ID, func(rel ontology.RelationType, neighbor *ontology.Node) bool {
		if rel == ontology.RelAffectedBy && neighbor.Type == ontology.TypeWeather {
			if cond, ok := neighbor.GetString("condition"); ok {
				switch cond {
				case "IFR":
					weatherImpact += 20 * time.Minute
				case "MVFR":
					weatherImpact += 10 * time.Minute
				case "LIFR":
					weatherImpact += 45 * time.Minute
				}
			}
			if vis, ok := neighbor.GetFloat("visibility"); ok && vis < 3 {
				weatherImpact += time.Duration((3-vis)*10) * time.Minute
			}
			if wind, ok := neighbor.GetFloat("wind_speed"); ok && wind > 25 {
				weatherImpact += time.Duration((wind-25)*2) * time.Minute
			}
		}
		return true
	})
	
	if weatherImpact > 0 {
		pred.Factors = append(pred.Factors, DelayFactor{
			Name:        "weather",
			Impact:      weatherImpact,
			Description: "Weather conditions affecting flight",
		})
		pred.WeatherImpact = weatherImpact
		totalDelay += weatherImpact
		factors++
	}
	
	// Calculate predicted delay as weighted average
	if factors > 0 {
		pred.PredictedDelay = totalDelay / time.Duration(factors)
	}
	
	// Calculate confidence based on data availability
	pred.Confidence = float64(factors) / 5.0
	if pred.Confidence > 1.0 {
		pred.Confidence = 1.0
	}
	
	pred.Elapsed = time.Since(start)
	return pred, true
}

// ---------------------------------------------------------------------------
// Helper Functions
// ---------------------------------------------------------------------------

func (e *Engine) getDelayMinutes(n *ontology.Node) int {
	// Check explicit delay property
	if delay, ok := n.GetFloat("delay_minutes"); ok {
		return int(delay)
	}
	
	// Calculate from scheduled vs actual
	scheduled, hasScheduled := n.GetTimestamp("scheduled_departure")
	actual, hasActual := n.GetTimestamp("actual_departure")
	
	if hasScheduled && hasActual && actual.After(scheduled) {
		return int(actual.Sub(scheduled).Minutes())
	}
	
	return 0
}

func (e *Engine) nodeToFlightInfo(n *ontology.Node) FlightInfo {
	info := FlightInfo{ID: n.ID}
	
	if v, ok := n.GetString("flight_id"); ok {
		info.FlightID = v
	}
	if v, ok := n.GetString("callsign"); ok {
		info.Callsign = v
		if len(v) >= 3 {
			info.Airline = v[:3]
		}
	}
	if v, ok := n.GetString("departure_code"); ok {
		info.DepartureCode = v
	}
	if v, ok := n.GetString("departure_name"); ok {
		info.DepartureName = v
	}
	if v, ok := n.GetString("arrival_code"); ok {
		info.ArrivalCode = v
	}
	if v, ok := n.GetString("arrival_name"); ok {
		info.ArrivalName = v
	}
	if v, ok := n.GetString("status"); ok {
		info.Status = v
	}
	if v, ok := n.GetTimestamp("scheduled_departure"); ok {
		info.ScheduledDep = v
	}
	if v, ok := n.GetTimestamp("actual_departure"); ok {
		info.ActualDep = v
	}
	if v, ok := n.GetTimestamp("scheduled_arrival"); ok {
		info.ScheduledArr = v
	}
	if v, ok := n.GetTimestamp("actual_arrival"); ok {
		info.ActualArr = v
	}
	if v, ok := n.GetFloat("altitude"); ok {
		info.Altitude = v
	}
	if v, ok := n.GetFloat("velocity"); ok {
		info.Velocity = v
	}
	if lat, lon, ok := n.GetLocation("position"); ok {
		info.Latitude = lat
		info.Longitude = lon
	}
	if v, ok := n.GetBool("on_ground"); ok {
		info.OnGround = v
	}
	
	info.DelayMinutes = e.getDelayMinutes(n)
	
	return info
}

func (e *Engine) nodeToAirportInfo(n *ontology.Node) AirportInfo {
	info := AirportInfo{}
	
	if v, ok := n.GetString("code"); ok {
		info.Code = v
	}
	if v, ok := n.GetString("name"); ok {
		info.Name = v
	}
	if v, ok := n.GetString("city"); ok {
		info.City = v
	}
	if v, ok := n.GetString("country"); ok {
		info.Country = v
	}
	if lat, lon, ok := n.GetLocation("position"); ok {
		info.Latitude = lat
		info.Longitude = lon
	}
	
	return info
}

func (e *Engine) nodeToWeatherInfo(n *ontology.Node) WeatherInfo {
	info := WeatherInfo{}
	
	if v, ok := n.GetString("station"); ok {
		info.Station = v
	}
	if v, ok := n.GetString("condition"); ok {
		info.Condition = v
	}
	if v, ok := n.GetFloat("wind_speed"); ok {
		info.WindSpeed = v
	}
	if v, ok := n.GetFloat("visibility"); ok {
		info.Visibility = v
	}
	if v, ok := n.GetFloat("temperature"); ok {
		info.Temperature = v
	}
	if v, ok := n.GetTimestamp("observed_at"); ok {
		info.ObservedAt = v
	}
	
	return info
}

func matchesFilters(n *ontology.Node, filters map[string]string) bool {
	for k, v := range filters {
		got, ok := n.GetString(k)
		if !ok || got != v {
			return false
		}
	}
	return true
}

func cloneNode(n *ontology.Node) ontology.Node {
	cp := *n
	if len(n.Props) > 0 {
		cp.Props = make([]ontology.Property, len(n.Props))
		copy(cp.Props, n.Props)
	}
	return cp
}
