package ontology

import "sync"

const defaultSlabCap = 4096

// slot is the internal storage unit: the public Node data plus adjacency
// lists and a liveness flag for the free-list allocator.
type slot struct {
	Node
	out   []halfEdge
	in    []halfEdge
	alive bool
}

// Engine is the thread-safe ontology graph engine.
//
// Storage layout:
//   - Dense slot slice with free-list recycling (cache-friendly, <512 MB at 300K nodes)
//   - Per-node adjacency lists (out/in halfEdge slices) for O(degree) traversal
//
// Indexes:
//   - idIdx:      node ID      → slot index   (primary, all types)
//   - typeIdx:    NodeType      → []slot index  (one list per type)
//   - flightIdx:  "flight_id"  → slot index   (Flight nodes only)
//   - airportIdx: "code"       → slot index   (Airport nodes only)
//
// Concurrency: sync.RWMutex — mutations take a write lock, queries take a
// read lock.  Zero-allocation query paths (Prop, ForEach*) never copy node
// data; they return values or pass internal pointers valid only within the
// callback / read-lock scope.
type Engine struct {
	mu sync.RWMutex

	slots []slot
	free  []uint32

	// Primary index
	idIdx map[string]uint32

	// Type index — fixed-size array avoids a map lookup
	typeIdx [nodeTypeCount][]uint32

	// Secondary indexes for sub-50ms lookups
	flightIdx  map[string]uint32 // flight_id prop → slot
	airportIdx map[string]uint32 // code prop → slot

	// Callbacks for node lifecycle (optional, for edge integration)
	onNodeAdded   func(id string)
	onNodeRemoved func(id string)
}

// EngineOption configures an Engine instance.
type EngineOption func(*Engine)

// WithCapacity sets the initial capacity for nodes and secondary indexes.
func WithCapacity(nodeCap, flightCap, airportCap int) EngineOption {
	return func(e *Engine) {
		e.slots = make([]slot, 0, nodeCap)
		e.idIdx = make(map[string]uint32, nodeCap)
		e.flightIdx = make(map[string]uint32, flightCap)
		e.airportIdx = make(map[string]uint32, airportCap)
	}
}

// WithNodeAddedCallback sets a callback invoked when a node is added.
func WithNodeAddedCallback(fn func(id string)) EngineOption {
	return func(e *Engine) {
		e.onNodeAdded = fn
	}
}

// WithNodeRemovedCallback sets a callback invoked when a node is removed.
func WithNodeRemovedCallback(fn func(id string)) EngineOption {
	return func(e *Engine) {
		e.onNodeRemoved = fn
	}
}

// New creates an empty Engine with pre-allocated backing storage.
func New(opts ...EngineOption) *Engine {
	e := &Engine{
		slots:      make([]slot, 0, defaultSlabCap),
		idIdx:      make(map[string]uint32, defaultSlabCap),
		flightIdx:  make(map[string]uint32, 1024),
		airportIdx: make(map[string]uint32, 1024),
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// =========================================================================
// Mutation (write-locked)
// =========================================================================

// AddNode inserts a node into the graph. If a node with the same ID already
// exists it is replaced in-place (edges are cleared).
func (e *Engine) AddNode(n Node) {
	var callback func(string)
	var nodeID string

	e.mu.Lock()
	if prev, exists := e.idIdx[n.ID]; exists {
		e.removeSecondaryIndexesForSlot(prev)
		e.removeFromTypeIndex(e.slots[prev].Node.Type, prev)
		e.slots[prev].Node = n
		e.slots[prev].out = e.slots[prev].out[:0]
		e.slots[prev].in = e.slots[prev].in[:0]
		e.typeIdx[n.Type] = append(e.typeIdx[n.Type], prev)
		e.addSecondaryIndexesForSlot(prev)
		e.mu.Unlock()
		return
	}

	idx := e.alloc()
	e.slots[idx].Node = n
	e.slots[idx].alive = true
	e.idIdx[n.ID] = idx
	e.typeIdx[n.Type] = append(e.typeIdx[n.Type], idx)
	e.addSecondaryIndexesForSlot(idx)

	// Capture callback for after unlock
	callback = e.onNodeAdded
	nodeID = n.ID
	e.mu.Unlock()

	// Notify callback outside lock to prevent deadlock
	if callback != nil {
		callback(nodeID)
	}
}

// RemoveNode deletes a node and all its edges from the graph.
func (e *Engine) RemoveNode(id string) bool {
	var callback func(string)

	e.mu.Lock()
	idx, ok := e.idIdx[id]
	if !ok {
		e.mu.Unlock()
		return false
	}

	e.removeEdgesFor(idx)
	e.removeSecondaryIndexesForSlot(idx)
	e.removeFromTypeIndex(e.slots[idx].Node.Type, idx)
	delete(e.idIdx, id)

	e.slots[idx] = slot{}
	e.free = append(e.free, idx)

	callback = e.onNodeRemoved
	e.mu.Unlock()

	// Notify callback outside lock
	if callback != nil {
		callback(id)
	}
	return true
}

// AddEdge creates a directed relationship between two nodes identified by ID.
func (e *Engine) AddEdge(fromID, toID string, rel RelationType) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	src, ok1 := e.idIdx[fromID]
	dst, ok2 := e.idIdx[toID]
	if !ok1 || !ok2 {
		return false
	}

	e.slots[src].out = append(e.slots[src].out, halfEdge{target: dst, rel: rel})
	e.slots[dst].in = append(e.slots[dst].in, halfEdge{target: src, rel: rel})
	return true
}

// SetProp updates a single property on an existing node, maintaining secondary
// indexes.
func (e *Engine) SetProp(id, key string, val PropVal) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	idx, ok := e.idIdx[id]
	if !ok {
		return false
	}

	s := &e.slots[idx]

	// Remove old secondary index entry for this key if indexed.
	if s.Node.Type == TypeFlight && key == "flight_id" {
		if old, ok := s.Node.GetString("flight_id"); ok {
			delete(e.flightIdx, old)
		}
	}
	if s.Node.Type == TypeAirport && key == "code" {
		if old, ok := s.Node.GetString("code"); ok {
			delete(e.airportIdx, old)
		}
	}

	s.Node.Set(key, val)

	// Add new secondary index entry.
	if s.Node.Type == TypeFlight && key == "flight_id" && val.Type == PropString {
		e.flightIdx[val.Str] = idx
	}
	if s.Node.Type == TypeAirport && key == "code" && val.Type == PropString {
		e.airportIdx[val.Str] = idx
	}

	return true
}

// =========================================================================
// Query — copy-based (read-locked, safe to use after return)
// =========================================================================

// GetNode returns a deep copy of the node with the given ID.
func (e *Engine) GetNode(id string) (Node, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	idx, ok := e.idIdx[id]
	if !ok {
		return Node{}, false
	}
	return e.cloneNode(idx), true
}

// GetFlightByID looks up a flight node by its "flight_id" property value.
// O(1) via the secondary index.
func (e *Engine) GetFlightByID(flightID string) (Node, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	idx, ok := e.flightIdx[flightID]
	if !ok {
		return Node{}, false
	}
	return e.cloneNode(idx), true
}

// GetAirportByCode looks up an airport node by its "code" property value.
// O(1) via the secondary index.
func (e *Engine) GetAirportByCode(code string) (Node, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	idx, ok := e.airportIdx[code]
	if !ok {
		return Node{}, false
	}
	return e.cloneNode(idx), true
}

// FindByType returns deep copies of all nodes of the given type.
func (e *Engine) FindByType(ntype NodeType) []Node {
	e.mu.RLock()
	defer e.mu.RUnlock()

	indices := e.typeIdx[ntype]
	result := make([]Node, 0, len(indices))
	for _, idx := range indices {
		if e.slots[idx].alive {
			result = append(result, e.cloneNode(idx))
		}
	}
	return result
}

// EdgesFrom returns all outgoing edges from a node (allocates a slice).
func (e *Engine) EdgesFrom(id string) []Edge {
	e.mu.RLock()
	defer e.mu.RUnlock()

	idx, ok := e.idIdx[id]
	if !ok {
		return nil
	}
	s := &e.slots[idx]
	edges := make([]Edge, 0, len(s.out))
	for _, he := range s.out {
		edges = append(edges, Edge{
			FromID:   id,
			ToID:     e.slots[he.target].Node.ID,
			Relation: he.rel,
		})
	}
	return edges
}

// EdgesTo returns all incoming edges to a node (allocates a slice).
func (e *Engine) EdgesTo(id string) []Edge {
	e.mu.RLock()
	defer e.mu.RUnlock()

	idx, ok := e.idIdx[id]
	if !ok {
		return nil
	}
	s := &e.slots[idx]
	edges := make([]Edge, 0, len(s.in))
	for _, he := range s.in {
		edges = append(edges, Edge{
			FromID:   e.slots[he.target].Node.ID,
			ToID:     id,
			Relation: he.rel,
		})
	}
	return edges
}

// =========================================================================
// Query — zero-allocation (read-locked, pointer valid only in callback)
// =========================================================================

// Prop returns a single property value without copying the full node.
// This is the fastest query path: map lookup + binary search, zero heap
// allocations.
func (e *Engine) Prop(id, key string) (PropVal, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	idx, ok := e.idIdx[id]
	if !ok {
		return PropVal{}, false
	}
	return e.slots[idx].Node.Get(key)
}

// ForEachNode calls fn for every live node. Return false to stop early.
// The *Node pointer is valid only for the duration of the callback.
func (e *Engine) ForEachNode(fn func(n *Node) bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for i := range e.slots {
		if e.slots[i].alive {
			if !fn(&e.slots[i].Node) {
				return
			}
		}
	}
}

// ForEachNodeOfType calls fn for every live node of the given type, driven by
// the type index (no full scan). Return false to stop early.
func (e *Engine) ForEachNodeOfType(ntype NodeType, fn func(n *Node) bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, idx := range e.typeIdx[ntype] {
		if e.slots[idx].alive {
			if !fn(&e.slots[idx].Node) {
				return
			}
		}
	}
}

// ForEachEdgeFrom calls fn for every outgoing edge. Zero-allocation.
func (e *Engine) ForEachEdgeFrom(id string, fn func(rel RelationType, neighbor *Node) bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	idx, ok := e.idIdx[id]
	if !ok {
		return
	}
	for _, he := range e.slots[idx].out {
		if !fn(he.rel, &e.slots[he.target].Node) {
			return
		}
	}
}

// ForEachEdgeTo calls fn for every incoming edge. Zero-allocation.
func (e *Engine) ForEachEdgeTo(id string, fn func(rel RelationType, neighbor *Node) bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	idx, ok := e.idIdx[id]
	if !ok {
		return
	}
	for _, he := range e.slots[idx].in {
		if !fn(he.rel, &e.slots[he.target].Node) {
			return
		}
	}
}

// =========================================================================
// Stats
// =========================================================================

// Size returns the number of live nodes.
func (e *Engine) Size() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.idIdx)
}

// EdgeCount returns the total number of directed edges.
func (e *Engine) EdgeCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()

	count := 0
	for i := range e.slots {
		if e.slots[i].alive {
			count += len(e.slots[i].out)
		}
	}
	return count
}

// TypeCount returns the number of live nodes of a given type.
func (e *Engine) TypeCount(ntype NodeType) int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.typeIdx[ntype])
}

// =========================================================================
// Internal helpers
// =========================================================================

// alloc returns a slot index, reusing freed slots when available.
func (e *Engine) alloc() uint32 {
	if n := len(e.free); n > 0 {
		idx := e.free[n-1]
		e.free = e.free[:n-1]
		return idx
	}
	idx := uint32(len(e.slots))
	e.slots = append(e.slots, slot{})
	return idx
}

// cloneNode returns a deep copy of the node at idx (copies Props slice).
func (e *Engine) cloneNode(idx uint32) Node {
	src := &e.slots[idx].Node
	n := Node{
		ID:   src.ID,
		Type: src.Type,
	}
	if len(src.Props) > 0 {
		n.Props = make([]Property, len(src.Props))
		copy(n.Props, src.Props)
	}
	return n
}

// addSecondaryIndexesForSlot indexes the relevant properties of the node.
func (e *Engine) addSecondaryIndexesForSlot(idx uint32) {
	s := &e.slots[idx]
	if s.Node.Type == TypeFlight {
		if v, ok := s.Node.GetString("flight_id"); ok {
			e.flightIdx[v] = idx
		}
	}
	if s.Node.Type == TypeAirport {
		if v, ok := s.Node.GetString("code"); ok {
			e.airportIdx[v] = idx
		}
	}
}

// removeSecondaryIndexesForSlot removes secondary index entries for the node.
func (e *Engine) removeSecondaryIndexesForSlot(idx uint32) {
	s := &e.slots[idx]
	if s.Node.Type == TypeFlight {
		if v, ok := s.Node.GetString("flight_id"); ok {
			if e.flightIdx[v] == idx {
				delete(e.flightIdx, v)
			}
		}
	}
	if s.Node.Type == TypeAirport {
		if v, ok := s.Node.GetString("code"); ok {
			if e.airportIdx[v] == idx {
				delete(e.airportIdx, v)
			}
		}
	}
}

// removeFromTypeIndex removes idx from the type index for ntype (swap-remove).
func (e *Engine) removeFromTypeIndex(ntype NodeType, idx uint32) {
	s := e.typeIdx[ntype]
	for i, v := range s {
		if v == idx {
			s[i] = s[len(s)-1]
			e.typeIdx[ntype] = s[:len(s)-1]
			return
		}
	}
}

// removeEdgesFor clears all adjacency-list entries involving the given slot.
func (e *Engine) removeEdgesFor(idx uint32) {
	s := &e.slots[idx]
	for _, he := range s.out {
		peer := &e.slots[he.target]
		peer.in = filterOutHalfEdge(peer.in, idx)
	}
	for _, he := range s.in {
		peer := &e.slots[he.target]
		peer.out = filterOutHalfEdge(peer.out, idx)
	}
	s.out = nil
	s.in = nil
}

// filterOutHalfEdge removes all half-edges targeting idx (in-place, no alloc).
func filterOutHalfEdge(edges []halfEdge, target uint32) []halfEdge {
	j := 0
	for _, he := range edges {
		if he.target != target {
			edges[j] = he
			j++
		}
	}
	return edges[:j]
}
