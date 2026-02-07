package ontology

import (
	"sort"
	"time"
)

// ---------------------------------------------------------------------------
// Node types
// ---------------------------------------------------------------------------

// NodeType identifies the category of an ontology node.
type NodeType uint8

const (
	TypeFlight   NodeType = iota
	TypeAirport
	TypeAircraft
	TypeAirline
	TypeWeather
	nodeTypeCount // must be last
)

var nodeTypeNames = [nodeTypeCount]string{
	TypeFlight:   "flight",
	TypeAirport:  "airport",
	TypeAircraft: "aircraft",
	TypeAirline:  "airline",
	TypeWeather:  "weather",
}

func (t NodeType) String() string {
	if t < nodeTypeCount {
		return nodeTypeNames[t]
	}
	return "unknown"
}

// ParseNodeType converts a string like "flight" to its NodeType constant.
func ParseNodeType(s string) (NodeType, bool) {
	for i, name := range nodeTypeNames {
		if name == s {
			return NodeType(i), true
		}
	}
	return 0, false
}

// ---------------------------------------------------------------------------
// Relationship types
// ---------------------------------------------------------------------------

// RelationType identifies the kind of directed relationship between nodes.
type RelationType uint8

const (
	RelOperates    RelationType = iota // airline -> flight
	RelDepartsFrom                     // flight -> airport
	RelArrivesAt                       // flight -> airport
	RelAffectedBy                      // flight -> weather
	relTypeCount
)

var relTypeNames = [relTypeCount]string{
	RelOperates:    "operates",
	RelDepartsFrom: "departs_from",
	RelArrivesAt:   "arrives_at",
	RelAffectedBy:  "affected_by",
}

func (r RelationType) String() string {
	if r < relTypeCount {
		return relTypeNames[r]
	}
	return "unknown"
}

// ParseRelationType converts a string like "operates" to its RelationType.
func ParseRelationType(s string) (RelationType, bool) {
	for i, name := range relTypeNames {
		if name == s {
			return RelationType(i), true
		}
	}
	return 0, false
}

// ---------------------------------------------------------------------------
// Typed property system
// ---------------------------------------------------------------------------

// PropType identifies the data type stored in a PropVal.
type PropType uint8

const (
	PropString    PropType = iota
	PropFloat
	PropTimestamp // stored as unix seconds (int64)
	PropLocation  // lat/lon pair
	PropBool
)

// PropVal is a value-type container for typed property data.
// Returned by value so reads never allocate on the heap.
type PropVal struct {
	Str  string  // PropString
	Num  float64 // PropFloat; latitude for PropLocation
	Num2 float64 // longitude for PropLocation
	Ts   int64   // unix seconds for PropTimestamp
	Type PropType
	Flag bool // PropBool
}

// Value constructors --------------------------------------------------------

func StringVal(s string) PropVal      { return PropVal{Type: PropString, Str: s} }
func FloatVal(f float64) PropVal      { return PropVal{Type: PropFloat, Num: f} }
func BoolVal(b bool) PropVal          { return PropVal{Type: PropBool, Flag: b} }
func TimestampVal(t time.Time) PropVal { return PropVal{Type: PropTimestamp, Ts: t.Unix()} }
func LocationVal(lat, lon float64) PropVal {
	return PropVal{Type: PropLocation, Num: lat, Num2: lon}
}

// Accessors -----------------------------------------------------------------

func (v PropVal) AsString() string                 { return v.Str }
func (v PropVal) AsFloat() float64                 { return v.Num }
func (v PropVal) AsBool() bool                     { return v.Flag }
func (v PropVal) AsTimestamp() time.Time            { return time.Unix(v.Ts, 0) }
func (v PropVal) AsLocation() (lat, lon float64)   { return v.Num, v.Num2 }

// Property is a named, typed attribute stored on a node.
type Property struct {
	Key string
	Val PropVal
}

// ---------------------------------------------------------------------------
// Graph primitives
// ---------------------------------------------------------------------------

// halfEdge is the compact internal representation in an adjacency list.
// 5 bytes effective (uint32 + uint8), padded to 8 by the compiler.
type halfEdge struct {
	target uint32
	rel    RelationType
}

// Edge is the external representation of a directed relationship.
type Edge struct {
	FromID   string
	ToID     string
	Relation RelationType
}

// ---------------------------------------------------------------------------
// Node
// ---------------------------------------------------------------------------

// Node is a vertex in the ontology graph.
// Properties are kept sorted by Key so lookups use binary search (O(log n),
// zero-allocation since PropVal is returned by value).
type Node struct {
	ID   string
	Type NodeType
	Props []Property // sorted by Key
}

// Get returns the value for a property key via binary search. Zero-allocation.
func (n *Node) Get(key string) (PropVal, bool) {
	i := n.propSearch(key)
	if i < 0 {
		return PropVal{}, false
	}
	return n.Props[i].Val, true
}

func (n *Node) GetString(key string) (string, bool) {
	v, ok := n.Get(key)
	return v.Str, ok
}

func (n *Node) GetFloat(key string) (float64, bool) {
	v, ok := n.Get(key)
	return v.Num, ok
}

func (n *Node) GetBool(key string) (bool, bool) {
	v, ok := n.Get(key)
	return v.Flag, ok
}

func (n *Node) GetTimestamp(key string) (time.Time, bool) {
	v, ok := n.Get(key)
	if !ok {
		return time.Time{}, false
	}
	return time.Unix(v.Ts, 0), true
}

func (n *Node) GetLocation(key string) (lat, lon float64, ok bool) {
	v, found := n.Get(key)
	if !found {
		return 0, 0, false
	}
	return v.Num, v.Num2, true
}

// Set inserts or updates a typed property, maintaining sorted order.
func (n *Node) Set(key string, val PropVal) {
	i := sort.Search(len(n.Props), func(i int) bool {
		return n.Props[i].Key >= key
	})
	if i < len(n.Props) && n.Props[i].Key == key {
		n.Props[i].Val = val
		return
	}
	n.Props = append(n.Props, Property{})
	copy(n.Props[i+1:], n.Props[i:])
	n.Props[i] = Property{Key: key, Val: val}
}

// Convenience setters -------------------------------------------------------

func (n *Node) SetString(key, val string)              { n.Set(key, StringVal(val)) }
func (n *Node) SetFloat(key string, val float64)       { n.Set(key, FloatVal(val)) }
func (n *Node) SetBool(key string, val bool)           { n.Set(key, BoolVal(val)) }
func (n *Node) SetTimestamp(key string, t time.Time)   { n.Set(key, TimestampVal(t)) }
func (n *Node) SetLocation(key string, lat, lon float64) {
	n.Set(key, LocationVal(lat, lon))
}

// propSearch returns the index of the property with the given key, or -1.
func (n *Node) propSearch(key string) int {
	i := sort.Search(len(n.Props), func(i int) bool {
		return n.Props[i].Key >= key
	})
	if i < len(n.Props) && n.Props[i].Key == key {
		return i
	}
	return -1
}
