package ontology

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Type enums
// ---------------------------------------------------------------------------

func TestNodeTypeString(t *testing.T) {
	assert.Equal(t, "flight", TypeFlight.String())
	assert.Equal(t, "airport", TypeAirport.String())
	assert.Equal(t, "aircraft", TypeAircraft.String())
	assert.Equal(t, "airline", TypeAirline.String())
	assert.Equal(t, "weather", TypeWeather.String())
	assert.Equal(t, "unknown", NodeType(255).String())
}

func TestParseNodeType(t *testing.T) {
	for _, tc := range []struct {
		in  string
		out NodeType
		ok  bool
	}{
		{"flight", TypeFlight, true},
		{"airport", TypeAirport, true},
		{"aircraft", TypeAircraft, true},
		{"airline", TypeAirline, true},
		{"weather", TypeWeather, true},
		{"invalid", 0, false},
	} {
		got, ok := ParseNodeType(tc.in)
		assert.Equal(t, tc.ok, ok, tc.in)
		if ok {
			assert.Equal(t, tc.out, got, tc.in)
		}
	}
}

func TestRelationTypeString(t *testing.T) {
	assert.Equal(t, "operates", RelOperates.String())
	assert.Equal(t, "departs_from", RelDepartsFrom.String())
	assert.Equal(t, "arrives_at", RelArrivesAt.String())
	assert.Equal(t, "affected_by", RelAffectedBy.String())
	assert.Equal(t, "unknown", RelationType(255).String())
}

func TestParseRelationType(t *testing.T) {
	rt, ok := ParseRelationType("operates")
	assert.True(t, ok)
	assert.Equal(t, RelOperates, rt)

	_, ok = ParseRelationType("invalid")
	assert.False(t, ok)
}

// ---------------------------------------------------------------------------
// Property system
// ---------------------------------------------------------------------------

func TestPropertyTypedValues(t *testing.T) {
	n := Node{ID: "test", Type: TypeFlight}

	// String
	n.SetString("callsign", "UAL123")
	v, ok := n.GetString("callsign")
	assert.True(t, ok)
	assert.Equal(t, "UAL123", v)

	// Float
	n.SetFloat("altitude", 35000)
	f, ok := n.GetFloat("altitude")
	assert.True(t, ok)
	assert.Equal(t, 35000.0, f)

	// Bool
	n.SetBool("on_ground", false)
	b, ok := n.GetBool("on_ground")
	assert.True(t, ok)
	assert.False(t, b)

	// Timestamp
	ts := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	n.SetTimestamp("departure", ts)
	got, ok := n.GetTimestamp("departure")
	assert.True(t, ok)
	assert.Equal(t, ts.Unix(), got.Unix())

	// Location
	n.SetLocation("position", 40.7128, -74.0060)
	lat, lon, ok := n.GetLocation("position")
	assert.True(t, ok)
	assert.InDelta(t, 40.7128, lat, 0.0001)
	assert.InDelta(t, -74.0060, lon, 0.0001)

	// Not found
	_, ok = n.GetString("missing")
	assert.False(t, ok)
}

func TestPropertySortOrder(t *testing.T) {
	n := Node{ID: "test", Type: TypeFlight}
	n.SetString("zebra", "z")
	n.SetString("alpha", "a")
	n.SetString("middle", "m")

	require.Len(t, n.Props, 3)
	assert.Equal(t, "alpha", n.Props[0].Key)
	assert.Equal(t, "middle", n.Props[1].Key)
	assert.Equal(t, "zebra", n.Props[2].Key)
}

func TestPropertyUpdate(t *testing.T) {
	n := Node{ID: "test", Type: TypeFlight}
	n.SetString("status", "scheduled")
	n.SetString("status", "airborne")

	v, ok := n.GetString("status")
	assert.True(t, ok)
	assert.Equal(t, "airborne", v)
	assert.Len(t, n.Props, 1)
}

func TestPropValConstructorsAndAccessors(t *testing.T) {
	sv := StringVal("hello")
	assert.Equal(t, PropString, sv.Type)
	assert.Equal(t, "hello", sv.AsString())

	fv := FloatVal(3.14)
	assert.Equal(t, PropFloat, fv.Type)
	assert.InDelta(t, 3.14, fv.AsFloat(), 0.001)

	bv := BoolVal(true)
	assert.Equal(t, PropBool, bv.Type)
	assert.True(t, bv.AsBool())

	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	tv := TimestampVal(ts)
	assert.Equal(t, PropTimestamp, tv.Type)
	assert.Equal(t, ts.Unix(), tv.AsTimestamp().Unix())

	lv := LocationVal(51.47, -0.4543)
	assert.Equal(t, PropLocation, lv.Type)
	lat, lon := lv.AsLocation()
	assert.InDelta(t, 51.47, lat, 0.001)
	assert.InDelta(t, -0.4543, lon, 0.001)
}

// ---------------------------------------------------------------------------
// Engine — node CRUD
// ---------------------------------------------------------------------------

func TestAddAndGetNode(t *testing.T) {
	eng := New()

	n := Node{ID: "flight-001", Type: TypeFlight}
	n.SetString("callsign", "UAL123")
	eng.AddNode(n)

	got, ok := eng.GetNode("flight-001")
	require.True(t, ok)
	assert.Equal(t, TypeFlight, got.Type)
	v, _ := got.GetString("callsign")
	assert.Equal(t, "UAL123", v)
}

func TestGetNodeNotFound(t *testing.T) {
	eng := New()
	_, ok := eng.GetNode("missing")
	assert.False(t, ok)
}

func TestAddNodeReplace(t *testing.T) {
	eng := New()

	n1 := Node{ID: "f1", Type: TypeFlight}
	n1.SetString("callsign", "OLD")
	n1.SetString("flight_id", "FL-OLD")
	eng.AddNode(n1)

	n2 := Node{ID: "f1", Type: TypeFlight}
	n2.SetString("callsign", "NEW")
	n2.SetString("flight_id", "FL-NEW")
	eng.AddNode(n2)

	got, ok := eng.GetNode("f1")
	require.True(t, ok)
	v, _ := got.GetString("callsign")
	assert.Equal(t, "NEW", v)
	assert.Equal(t, 1, eng.Size())

	// Old flight_id index entry must be gone.
	_, ok = eng.GetFlightByID("FL-OLD")
	assert.False(t, ok)
	// New entry must work.
	got, ok = eng.GetFlightByID("FL-NEW")
	require.True(t, ok)
	assert.Equal(t, "f1", got.ID)
}

func TestRemoveNode(t *testing.T) {
	eng := New()

	n := Node{ID: "f1", Type: TypeFlight}
	n.SetString("flight_id", "FL100")
	eng.AddNode(n)

	ok := eng.RemoveNode("f1")
	assert.True(t, ok)
	assert.Equal(t, 0, eng.Size())
	assert.Equal(t, 0, eng.TypeCount(TypeFlight))

	_, found := eng.GetNode("f1")
	assert.False(t, found)

	_, found = eng.GetFlightByID("FL100")
	assert.False(t, found)
}

func TestRemoveNodeNotFound(t *testing.T) {
	eng := New()
	assert.False(t, eng.RemoveNode("nonexistent"))
}

func TestRemoveNodeWithEdges(t *testing.T) {
	eng := New()

	eng.AddNode(Node{ID: "f1", Type: TypeFlight})
	eng.AddNode(Node{ID: "a1", Type: TypeAirport})
	eng.AddEdge("f1", "a1", RelDepartsFrom)

	eng.RemoveNode("f1")

	edges := eng.EdgesTo("a1")
	assert.Empty(t, edges)
	assert.Equal(t, 0, eng.EdgeCount())
}

func TestSlotReuse(t *testing.T) {
	eng := New()
	eng.AddNode(Node{ID: "f1", Type: TypeFlight})
	eng.AddNode(Node{ID: "f2", Type: TypeFlight})
	eng.RemoveNode("f1")

	eng.AddNode(Node{ID: "f3", Type: TypeFlight})
	assert.Equal(t, 2, eng.Size())

	_, ok := eng.GetNode("f3")
	assert.True(t, ok)
}

// ---------------------------------------------------------------------------
// Engine — edges
// ---------------------------------------------------------------------------

func TestAddEdge(t *testing.T) {
	eng := New()
	eng.AddNode(Node{ID: "f1", Type: TypeFlight})

	a := Node{ID: "jfk", Type: TypeAirport}
	a.SetString("code", "JFK")
	eng.AddNode(a)

	ok := eng.AddEdge("f1", "jfk", RelDepartsFrom)
	assert.True(t, ok)

	out := eng.EdgesFrom("f1")
	require.Len(t, out, 1)
	assert.Equal(t, "jfk", out[0].ToID)
	assert.Equal(t, RelDepartsFrom, out[0].Relation)

	in := eng.EdgesTo("jfk")
	require.Len(t, in, 1)
	assert.Equal(t, "f1", in[0].FromID)
}

func TestAddEdgeMissingNode(t *testing.T) {
	eng := New()
	eng.AddNode(Node{ID: "f1", Type: TypeFlight})

	assert.False(t, eng.AddEdge("f1", "missing", RelDepartsFrom))
	assert.False(t, eng.AddEdge("missing", "f1", RelDepartsFrom))
}

func TestEdgesFromNotFound(t *testing.T) {
	eng := New()
	assert.Nil(t, eng.EdgesFrom("nonexistent"))
}

func TestEdgesToNotFound(t *testing.T) {
	eng := New()
	assert.Nil(t, eng.EdgesTo("nonexistent"))
}

func TestEdgeCount(t *testing.T) {
	eng := New()
	eng.AddNode(Node{ID: "f1", Type: TypeFlight})
	eng.AddNode(Node{ID: "jfk", Type: TypeAirport})
	eng.AddEdge("f1", "jfk", RelDepartsFrom)
	assert.Equal(t, 1, eng.EdgeCount())
}

// ---------------------------------------------------------------------------
// Engine — secondary indexes
// ---------------------------------------------------------------------------

func TestFlightIDIndex(t *testing.T) {
	eng := New()

	n := Node{ID: "abc123", Type: TypeFlight}
	n.SetString("flight_id", "UA100")
	eng.AddNode(n)

	got, ok := eng.GetFlightByID("UA100")
	require.True(t, ok)
	assert.Equal(t, "abc123", got.ID)

	_, ok = eng.GetFlightByID("NOTEXIST")
	assert.False(t, ok)
}

func TestAirportCodeIndex(t *testing.T) {
	eng := New()

	n := Node{ID: "airport-jfk", Type: TypeAirport}
	n.SetString("code", "JFK")
	n.SetString("name", "John F Kennedy")
	eng.AddNode(n)

	got, ok := eng.GetAirportByCode("JFK")
	require.True(t, ok)
	assert.Equal(t, "airport-jfk", got.ID)
	name, _ := got.GetString("name")
	assert.Equal(t, "John F Kennedy", name)
}

func TestSetPropUpdatesIndex(t *testing.T) {
	eng := New()

	n := Node{ID: "f1", Type: TypeFlight}
	n.SetString("flight_id", "OLD-ID")
	eng.AddNode(n)

	eng.SetProp("f1", "flight_id", StringVal("NEW-ID"))

	_, ok := eng.GetFlightByID("OLD-ID")
	assert.False(t, ok)

	got, ok := eng.GetFlightByID("NEW-ID")
	require.True(t, ok)
	assert.Equal(t, "f1", got.ID)
}

func TestSetPropNotFound(t *testing.T) {
	eng := New()
	assert.False(t, eng.SetProp("nonexistent", "key", StringVal("val")))
}

// ---------------------------------------------------------------------------
// Engine — zero-allocation queries
// ---------------------------------------------------------------------------

func TestPropZeroAlloc(t *testing.T) {
	eng := New()

	n := Node{ID: "f1", Type: TypeFlight}
	n.SetString("callsign", "UAL123")
	eng.AddNode(n)

	val, ok := eng.Prop("f1", "callsign")
	require.True(t, ok)
	assert.Equal(t, "UAL123", val.Str)

	_, ok = eng.Prop("missing", "callsign")
	assert.False(t, ok)
}

func TestFindByType(t *testing.T) {
	eng := New()
	eng.AddNode(Node{ID: "f1", Type: TypeFlight})
	eng.AddNode(Node{ID: "f2", Type: TypeFlight})
	eng.AddNode(Node{ID: "a1", Type: TypeAirport})

	assert.Len(t, eng.FindByType(TypeFlight), 2)
	assert.Len(t, eng.FindByType(TypeAirport), 1)
	assert.Empty(t, eng.FindByType(TypeWeather))
}

func TestForEachNodeOfType(t *testing.T) {
	eng := New()
	eng.AddNode(Node{ID: "f1", Type: TypeFlight})
	eng.AddNode(Node{ID: "f2", Type: TypeFlight})
	eng.AddNode(Node{ID: "a1", Type: TypeAirport})

	var ids []string
	eng.ForEachNodeOfType(TypeFlight, func(n *Node) bool {
		ids = append(ids, n.ID)
		return true
	})
	assert.Len(t, ids, 2)
}

func TestForEachNodeOfTypeEarlyStop(t *testing.T) {
	eng := New()
	for i := 0; i < 100; i++ {
		eng.AddNode(Node{ID: fmt.Sprintf("f%d", i), Type: TypeFlight})
	}

	count := 0
	eng.ForEachNodeOfType(TypeFlight, func(n *Node) bool {
		count++
		return count < 5
	})
	assert.Equal(t, 5, count)
}

func TestForEachNode(t *testing.T) {
	eng := New()
	eng.AddNode(Node{ID: "f1", Type: TypeFlight})
	eng.AddNode(Node{ID: "a1", Type: TypeAirport})

	count := 0
	eng.ForEachNode(func(n *Node) bool {
		count++
		return true
	})
	assert.Equal(t, 2, count)
}

func TestForEachEdgeFrom(t *testing.T) {
	eng := New()
	eng.AddNode(Node{ID: "f1", Type: TypeFlight})
	eng.AddNode(Node{ID: "jfk", Type: TypeAirport})
	eng.AddNode(Node{ID: "lax", Type: TypeAirport})
	eng.AddEdge("f1", "jfk", RelDepartsFrom)
	eng.AddEdge("f1", "lax", RelArrivesAt)

	var rels []RelationType
	eng.ForEachEdgeFrom("f1", func(rel RelationType, neighbor *Node) bool {
		rels = append(rels, rel)
		return true
	})
	assert.Len(t, rels, 2)
}

func TestForEachEdgeTo(t *testing.T) {
	eng := New()
	eng.AddNode(Node{ID: "f1", Type: TypeFlight})
	eng.AddNode(Node{ID: "f2", Type: TypeFlight})
	eng.AddNode(Node{ID: "jfk", Type: TypeAirport})
	eng.AddEdge("f1", "jfk", RelDepartsFrom)
	eng.AddEdge("f2", "jfk", RelArrivesAt)

	var fromIDs []string
	eng.ForEachEdgeTo("jfk", func(rel RelationType, neighbor *Node) bool {
		fromIDs = append(fromIDs, neighbor.ID)
		return true
	})
	assert.Len(t, fromIDs, 2)
}

func TestForEachEdgeFromNotFound(t *testing.T) {
	eng := New()
	called := false
	eng.ForEachEdgeFrom("nope", func(rel RelationType, n *Node) bool {
		called = true
		return true
	})
	assert.False(t, called)
}

// ---------------------------------------------------------------------------
// Engine — stats
// ---------------------------------------------------------------------------

func TestTypeCount(t *testing.T) {
	eng := New()
	eng.AddNode(Node{ID: "f1", Type: TypeFlight})
	eng.AddNode(Node{ID: "f2", Type: TypeFlight})
	eng.AddNode(Node{ID: "a1", Type: TypeAirport})

	assert.Equal(t, 2, eng.TypeCount(TypeFlight))
	assert.Equal(t, 1, eng.TypeCount(TypeAirport))
	assert.Equal(t, 0, eng.TypeCount(TypeWeather))
}

// ---------------------------------------------------------------------------
// Engine — full domain scenario
// ---------------------------------------------------------------------------

func TestAllNodeTypesAndRelationships(t *testing.T) {
	eng := New()

	// Flight
	f := Node{ID: "flight-1", Type: TypeFlight}
	f.SetString("flight_id", "UA100")
	f.SetString("callsign", "UAL100")
	f.SetString("status", "airborne")
	f.SetFloat("altitude", 35000)
	f.SetBool("on_ground", false)
	f.SetTimestamp("scheduled_dep", time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC))
	f.SetLocation("position", 40.7128, -74.0060)
	eng.AddNode(f)

	// Airport
	a := Node{ID: "apt-jfk", Type: TypeAirport}
	a.SetString("code", "JFK")
	a.SetString("name", "John F Kennedy International")
	a.SetString("city", "New York")
	a.SetString("country", "US")
	a.SetLocation("position", 40.6413, -73.7781)
	a.SetFloat("elevation", 13)
	eng.AddNode(a)

	a2 := Node{ID: "apt-lax", Type: TypeAirport}
	a2.SetString("code", "LAX")
	a2.SetString("name", "Los Angeles International")
	eng.AddNode(a2)

	// Aircraft
	ac := Node{ID: "ac-N12345", Type: TypeAircraft}
	ac.SetString("registration", "N12345")
	ac.SetString("type_code", "B738")
	ac.SetString("model", "Boeing 737-800")
	eng.AddNode(ac)

	// Airline
	al := Node{ID: "al-UA", Type: TypeAirline}
	al.SetString("iata_code", "UA")
	al.SetString("icao_code", "UAL")
	al.SetString("name", "United Airlines")
	al.SetString("country", "US")
	eng.AddNode(al)

	// Weather
	w := Node{ID: "wx-jfk-1", Type: TypeWeather}
	w.SetString("station", "KJFK")
	w.SetString("condition", "IFR")
	w.SetFloat("wind_speed", 25.0)
	w.SetFloat("visibility", 2.0)
	w.SetFloat("temperature", -5.0)
	w.SetTimestamp("observed_at", time.Date(2024, 1, 15, 9, 0, 0, 0, time.UTC))
	eng.AddNode(w)

	assert.Equal(t, 6, eng.Size())
	assert.Equal(t, 1, eng.TypeCount(TypeFlight))
	assert.Equal(t, 2, eng.TypeCount(TypeAirport))
	assert.Equal(t, 1, eng.TypeCount(TypeAircraft))
	assert.Equal(t, 1, eng.TypeCount(TypeAirline))
	assert.Equal(t, 1, eng.TypeCount(TypeWeather))

	// Build relationships
	assert.True(t, eng.AddEdge("al-UA", "flight-1", RelOperates))
	assert.True(t, eng.AddEdge("flight-1", "apt-jfk", RelDepartsFrom))
	assert.True(t, eng.AddEdge("flight-1", "apt-lax", RelArrivesAt))
	assert.True(t, eng.AddEdge("flight-1", "wx-jfk-1", RelAffectedBy))
	assert.Equal(t, 4, eng.EdgeCount())

	// Verify index lookups
	got, ok := eng.GetFlightByID("UA100")
	require.True(t, ok)
	assert.Equal(t, "flight-1", got.ID)

	got, ok = eng.GetAirportByCode("JFK")
	require.True(t, ok)
	name, _ := got.GetString("name")
	assert.Equal(t, "John F Kennedy International", name)

	got, ok = eng.GetAirportByCode("LAX")
	require.True(t, ok)
	assert.Equal(t, "apt-lax", got.ID)

	// Traverse from flight
	var depAirports []string
	var arrAirports []string
	var weatherStations []string
	eng.ForEachEdgeFrom("flight-1", func(rel RelationType, neighbor *Node) bool {
		switch rel {
		case RelDepartsFrom:
			n, _ := neighbor.GetString("name")
			depAirports = append(depAirports, n)
		case RelArrivesAt:
			n, _ := neighbor.GetString("name")
			arrAirports = append(arrAirports, n)
		case RelAffectedBy:
			s, _ := neighbor.GetString("station")
			weatherStations = append(weatherStations, s)
		}
		return true
	})
	assert.Equal(t, []string{"John F Kennedy International"}, depAirports)
	assert.Equal(t, []string{"Los Angeles International"}, arrAirports)
	assert.Equal(t, []string{"KJFK"}, weatherStations)

	// Traverse incoming to airline
	var operatedFlights []string
	eng.ForEachEdgeFrom("al-UA", func(rel RelationType, neighbor *Node) bool {
		if rel == RelOperates {
			cs, _ := neighbor.GetString("callsign")
			operatedFlights = append(operatedFlights, cs)
		}
		return true
	})
	assert.Equal(t, []string{"UAL100"}, operatedFlights)
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

func TestConcurrentAccess(t *testing.T) {
	eng := New()

	// Prepopulate
	for i := 0; i < 100; i++ {
		n := Node{ID: fmt.Sprintf("f%d", i), Type: TypeFlight}
		n.SetString("flight_id", fmt.Sprintf("FL%d", i))
		eng.AddNode(n)
	}

	var wg sync.WaitGroup

	// Concurrent readers
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				eng.GetNode(fmt.Sprintf("f%d", j%100))
				eng.GetFlightByID(fmt.Sprintf("FL%d", j%100))
				eng.FindByType(TypeFlight)
				eng.Prop(fmt.Sprintf("f%d", j%100), "flight_id")
				eng.ForEachNodeOfType(TypeFlight, func(n *Node) bool {
					return true
				})
			}
		}()
	}

	// Concurrent writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 100; j < 200; j++ {
			n := Node{ID: fmt.Sprintf("f%d", j), Type: TypeFlight}
			n.SetString("flight_id", fmt.Sprintf("FL%d", j))
			eng.AddNode(n)
		}
	}()

	wg.Wait()
	assert.Equal(t, 200, eng.Size())
}

// ---------------------------------------------------------------------------
// Deep-copy isolation
// ---------------------------------------------------------------------------

func TestGetNodeReturnsIndependentCopy(t *testing.T) {
	eng := New()

	n := Node{ID: "f1", Type: TypeFlight}
	n.SetString("callsign", "AAA")
	eng.AddNode(n)

	got, _ := eng.GetNode("f1")
	got.SetString("callsign", "MUTATED")

	original, _ := eng.GetNode("f1")
	v, _ := original.GetString("callsign")
	assert.Equal(t, "AAA", v, "mutation of returned copy must not affect engine")
}
