package api

import (
	"sync"
	"time"

	"github.com/rfratto/croissant/id"
	"github.com/rfratto/croissant/internal/idconv"
)

// State is the state of a node used for routing messages. Methods against State
// are goroutine state.
type State struct {
	mut sync.Mutex

	// The node this State is for.
	Node Descriptor

	// The nodes immediately before and after this node in the
	// hash ring.
	Predecessors, Successors *DescriptorSet

	// Size is the bit length of IDs to store for rounding, and
	// Base is the power-of-two base to represent them as.
	Size, Base int

	// Routing is the table used for routing.
	//
	// There is one row per digit in the ID, and Base number of columns.
	Routing [][]*Descriptor

	// Neighbors are geographically close peers in the cluster.
	Neighbors *DescriptorSet

	// Statuses maps a Descriptor's ID to its health state.
	Statuses map[Descriptor]Health

	// LastUpdated is the last time this State was updated. Used to ID it
	// between previous iterations of the State.
	LastUpdated time.Time
}

// NewState creates a new State for a node.
func NewState(
	node Descriptor,
	numLeaves, numNeighbors int,
	size, base int,
) *State {

	if numLeaves%2 != 0 {
		panic("numLeaves must be a multiple of 2")
	}

	s := &State{
		Node: node,

		// Predecessors <= Node <= Successors, which means we should keep
		// the biggest numbers closest to Node for the Predecessors.
		Predecessors: &DescriptorSet{Size: numLeaves / 2, KeepBiggest: true},
		Successors:   &DescriptorSet{Size: numLeaves / 2, KeepBiggest: false},

		Size: size,
		Base: base,

		Neighbors:   &DescriptorSet{Size: numNeighbors},
		Statuses:    make(map[Descriptor]Health),
		LastUpdated: time.Now(),
	}

	s.reset()
	return s
}

// IsNewer returns true if State has been modified since t.
func (s *State) IsNewer(t time.Time) bool {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.LastUpdated.After(t)
}

// resets the state, removing all peers.
func (s *State) reset() {
	s.Predecessors.Descriptors = s.Predecessors.Descriptors[:0]
	s.Successors.Descriptors = s.Successors.Descriptors[:0]

	s.Routing = NewRoutingTable(
		s.Base,                        // One column per possible value in a digit.
		idconv.Digits(s.Size, s.Base), // One row per digit.
	)

	s.Neighbors.Descriptors = s.Neighbors.Descriptors[:0]

	s.Statuses = make(map[Descriptor]Health)
	s.LastUpdated = time.Now()

	// Add ourselves into the routing table at every row.
	digits := s.Node.ID.Digits(s.Size, s.Base)
	for i, dig := range digits {
		s.Routing[i][dig] = &s.Node
	}
}

// Calculate initializes the state based on a set of hellos.
// hellos is expected to be ordered from seed to peer.
func (s *State) Calculate(hellos []Hello) {
	s.mut.Lock()
	defer s.mut.Unlock()

	s.reset()

	for i, h := range hellos {
		// Take neighbors from the seed node, which is intended to be
		// geographically close node to us.
		if i == 0 {
			s.mixinNeighbors(h.State)
		}

		// The final node is our peer, so take leaves from it.
		if i == len(hellos)-1 {
			s.mixinLeaves(h.State)
		}

		// Take routes from all nodes, since each peer along the join should
		// have at least one entry we can use.
		s.mixinRoutes(h.State)
	}
}

// MixinState takes the routes, neighbors, and leaves from the
// peer and mixes them all into s.
func (s *State) MixinState(peer *State) (updatedRoutes, updatedNeighbors, updatedLeaves bool) {
	s.mut.Lock()
	defer s.mut.Unlock()

	updatedRoutes = s.mixinRoutes(peer)
	updatedNeighbors = s.mixinNeighbors(peer)
	updatedLeaves = s.mixinLeaves(peer)

	return
}

// Clone returns a copy of s.
func (s *State) Clone() *State {
	s.mut.Lock()
	defer s.mut.Unlock()

	var clone State
	clone.Node = s.Node

	clone.Predecessors = s.Predecessors.Clone()
	clone.Successors = s.Successors.Clone()

	clone.Size = s.Size
	clone.Base = s.Base

	clone.Routing = make([][]*Descriptor, len(s.Routing))
	for row := 0; row < len(s.Routing); row++ {
		clone.Routing[row] = make([]*Descriptor, len(s.Routing[row]))
		for col := 0; col < len(s.Routing[row]); col++ {
			if s.Routing[row][col] == nil {
				continue
			}
			cp := *s.Routing[row][col]
			clone.Routing[row][col] = &cp
		}
	}

	clone.Neighbors = s.Neighbors.Clone()

	clone.Statuses = make(map[Descriptor]Health, len(s.Statuses))
	for k, v := range s.Statuses {
		clone.Statuses[k] = v
	}

	clone.LastUpdated = s.LastUpdated
	return &clone
}

// Leaves returns the full set of leaves. If all is true, known unhealthy
// leaves will also be returned.
func (s *State) Leaves(all bool) []Descriptor {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.leaves(all)
}

func (s *State) leaves(all bool) []Descriptor {
	leaves := make([]Descriptor, 0, len(s.Predecessors.Descriptors)+len(s.Successors.Descriptors))
	for _, p := range s.Predecessors.Descriptors {
		if all || s.Statuses[p] == Healthy {
			leaves = append(leaves, p)
		}
	}
	for _, p := range s.Successors.Descriptors {
		if all || s.Statuses[p] == Healthy {
			leaves = append(leaves, p)
		}
	}
	return leaves
}

// Peers returns the unique set of peers in s. Return order not guaranteed.
// If all is true, known unhealthy peers will also be returned.
func (s *State) Peers(all bool) []Descriptor {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.peers(all)
}

func (s *State) peers(all bool) []Descriptor {
	pMap := make(map[Descriptor]struct{})
	for _, p := range s.Predecessors.Descriptors {
		pMap[p] = struct{}{}
	}
	for _, p := range s.Successors.Descriptors {
		pMap[p] = struct{}{}
	}
	for _, row := range s.Routing {
		for _, p := range row {
			if p == nil || *p == s.Node {
				continue
			}
			pMap[*p] = struct{}{}
		}
	}
	for _, p := range s.Neighbors.Descriptors {
		pMap[p] = struct{}{}
	}

	pSet := make([]Descriptor, 0, len(pMap))
	for p := range pMap {
		if !all && s.Statuses[p] != Healthy {
			continue
		}
		pSet = append(pSet, p)
	}
	return pSet
}

// mixinRoutes takes routes from peer and incorporates each into
// s. Fails if the two state tables do not have the same size and base.
// Ignores node that s or peer do not find healthy.
func (s *State) mixinRoutes(peer *State) (updated bool) {
	// TODO(rfratto): relax restriction that State tables must be of same size.
	// To relax this, we'd need to iterate over the entire table for peer instead
	// of just one row.
	if s.Base != peer.Base || s.Size != peer.Size {
		return false
	}

	// Find row with relevant entries and incorporate.
	overlap := Prefix(
		s.Node.ID.Digits(s.Size, s.Base),
		peer.Node.ID.Digits(s.Size, s.Base),
	)
	for _, ent := range peer.Routing[overlap] {
		if ent == nil {
			continue
		}
		d := *ent
		if s.Statuses[d] != Healthy || peer.Statuses[d] != Healthy {
			continue
		}
		if s.addRoute(d) {
			updated = true
		}
	}

	return updated
}

func (s *State) addRoute(d Descriptor) bool {
	if s.Node.ID == d.ID {
		return false
	}

	var (
		other = d.ID.Digits(s.Size, s.Base)
		local = s.Node.ID.Digits(s.Size, s.Base)
	)

	// Find position in table. Rows all have the same prefix of digits, and the
	// column is the first non-matching digit.
	var (
		row = Prefix(other, local)
		col = other[row]
	)

	// Don't override an existing entry that is healthy.
	if exist := s.Routing[row][col]; exist != nil && s.Statuses[*exist] == Healthy {
		// TODO(rfratto): proximity optimization
		return false
	}

	s.Routing[row][col] = &d
	s.LastUpdated = time.Now()
	return true
}

// mixinNeighbors mixes in neighbors from peer. Returns true when neighbor set
// was updated.
func (s *State) mixinNeighbors(peer *State) (updated bool) {
	if s.addNeighbor(peer.Node) {
		updated = true
	}

	for _, d := range peer.Neighbors.Descriptors {
		if peer.Statuses[d] != Healthy || s.Node == d {
			continue
		}
		if s.addNeighbor(d) {
			updated = true
		}
	}

	return updated
}

func (s *State) addNeighbor(d Descriptor) (updated bool) {
	if s.Statuses[d] != Healthy {
		return false
	}

	updated = s.Neighbors.Push(d)
	if updated {
		s.LastUpdated = time.Now()
	}
	return
}

// IsLeaf returns true if d is a leaf node.
func (s *State) IsLeaf(d Descriptor) bool {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.Predecessors.Contains(d) || s.Successors.Contains(d)
}

// ReplaceLeaf will replace d with a leaf from peer.
// d must exist as a leaf and be unhealthy, otherwise
// nothing happens.
func (s *State) ReplaceLeaf(d Descriptor, peer *State) (changed bool) {
	s.mut.Lock()
	defer s.mut.Unlock()

	isPredecessor := s.Predecessors.Contains(d)
	isSuccessor := s.Successors.Contains(d)

	if s.Statuses[d] == Healthy || (!isPredecessor && !isSuccessor) {
		return false
	}

	s.Predecessors.Remove(d)
	s.Successors.Remove(d)
	s.LastUpdated = time.Now().UTC()
	changed = true // Changed is always true because we at least removed the leaf

	if peer == nil {
		return
	}

NextLeaf:
	for _, l := range peer.Leaves(false) {
		if s.Statuses[l] != Healthy {
			continue
		}

		switch {
		case isPredecessor:
			// Only replace predecessor with another predecessor
			if id.Compare(l.ID, s.Node.ID) >= 0 {
				continue NextLeaf
			}
			if s.Predecessors.Insert(l) {
				return
			}
		case isSuccessor:
			// Only replace successor with another successor
			if id.Compare(l.ID, s.Node.ID) <= 0 {
				continue NextLeaf
			}
			if s.Successors.Insert(l) {
				return
			}
		}
	}

	return
}

// ReplacePredecessor will replace d with a leaf from peer.
// d must exist as a Predecessor and be unhealthy, otherwise
// nothing happens.
func (s *State) ReplacePredecessor(d Descriptor, peer *State) (changed bool) {
	s.mut.Lock()
	defer s.mut.Unlock()

	if s.Statuses[d] == Healthy || !s.Predecessors.Contains(d) {
		return false
	}

	s.Predecessors.Remove(d)
	s.LastUpdated = time.Now().UTC()

	if peer == nil {
		return true
	}

	// Only replace with a value < s.Node
	for _, l := range peer.Leaves(false) {
		if s.Statuses[l] != Healthy {
			continue
		}
		if id.Compare(l.ID, s.Node.ID) >= 0 { // l.ID >= s.Node.ID
			continue
		}
		if s.Predecessors.Insert(l) {
			return true
		}
	}
	return true
}

// ReplaceSuccessor will replace d with a leaf from peer.
// d must exist as a Successors and be unhealthy, otherwise
// nothing happens.
func (s *State) ReplaceSuccessor(d Descriptor, peer *State) (changed bool) {
	s.mut.Lock()
	defer s.mut.Unlock()

	if s.Statuses[d] == Healthy || !s.Successors.Contains(d) {
		return false
	}

	s.Successors.Remove(d)
	s.LastUpdated = time.Now().UTC()

	if peer == nil {
		return true
	}

	// Only replace with a value > s.Node
	for _, l := range peer.Leaves(false) {
		if s.Statuses[l] != Healthy {
			continue
		}
		if id.Compare(l.ID, s.Node.ID) <= 0 { // l.ID <= s.Node.ID
			continue
		}
		if s.Successors.Insert(l) {
			return true
		}
	}
	return true
}

// ReplaceRoute will replace d in the routing table with an entry from peer.
// d must exist as a route and be unhealthy, otherwise returns ok==false.
func (s *State) ReplaceRoute(d Descriptor, peer *State) (replaced, ok bool) {
	s.mut.Lock()
	defer s.mut.Unlock()

	if peer != nil && (peer.Base != s.Base || peer.Size != s.Size) {
		return
	}

	row, col := s.routeIndex(d)
	if row < 0 || col < 0 {
		return
	}

	if s.Statuses[d] == Healthy || s.Routing[row][col] == nil || *s.Routing[row][col] != d {
		return
	}
	ok = true
	s.LastUpdated = time.Now().UTC()

	if peer == nil {
		s.Routing[row][col] = nil
		return
	}

	candidate := peer.Routing[row][col]
	if candidate != nil && s.Statuses[*candidate] == Healthy && peer.Statuses[*candidate] == Healthy {
		s.Routing[row][col] = candidate
		replaced = true
	}
	return
}

// RouteIndex returns the index in the routing table for d.
// d must not be s.Node, otherwise erturns -1, -1.
func (s *State) RouteIndex(d Descriptor) (row, col int) {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.routeIndex(d)
}

func (s *State) routeIndex(d Descriptor) (row, col int) {
	if s.Node == d {
		return -1, -1
	}

	var (
		dDigits = d.ID.Digits(s.Size, s.Base)
		sDigits = s.Node.ID.Digits(s.Size, s.Base)
	)

	row = Prefix(sDigits, dDigits)
	col = int(dDigits[row])
	return
}

// ReplaceNeighbor will replace d with a neighbor from peer.
// d must exist as a neighbor and be unhealthy, otherwise returns
// ok==false.
func (s *State) ReplaceNeighbor(d Descriptor, peer *State) (changed, ok bool) {
	s.mut.Lock()
	defer s.mut.Unlock()

	if s.Statuses[d] == Healthy || !s.Neighbors.Contains(d) {
		return
	}
	ok = true

	s.Neighbors.Remove(d)
	s.LastUpdated = time.Now().UTC()

	if peer == nil {
		return
	}

	for _, n := range peer.Neighbors.Descriptors {
		if peer.Statuses[n] != Healthy || s.Statuses[n] != Healthy {
			continue
		}
		if s.addNeighbor(n) {
			changed = true
			break
		}
	}

	return
}

// MixinLeaves will take leaves from a peer and incorporate them into
// s. The peer itself will also be considered a leaf.
//
// Ignores nodes that s or peer do not find healthy.
//
// Returns true when updated.
func (s *State) MixinLeaves(peer *State) (updated bool) {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.mixinLeaves(peer)
}

func (s *State) mixinLeaves(peer *State) (updated bool) {
	if s.addLeaf(peer.Node) {
		updated = true
	}

	for _, l := range peer.leaves(false) {
		if l == s.Node {
			continue
		}
		if s.addLeaf(l) {
			updated = true
		}
	}

	return updated
}

func (s *State) addLeaf(d Descriptor) (updated bool) {
	if s.Statuses[d] != Healthy {
		return false
	}

	defer func() {
		if updated {
			s.LastUpdated = time.Now()
		}
	}()

	switch id.Compare(d.ID, s.Node.ID) {
	case -1: // d.ID < s.Node.ID
		updated = s.Predecessors.Insert(d)
	case 1: // d.ID > s.Node.ID
		updated = s.Successors.Insert(d)
	}

	return
}

// SetHealth updates the health of p. Note that updating the health only
// affects routing; callers must manually remove unhealthy peers.
func (s *State) SetHealth(p Descriptor, h Health) (changed bool) {
	s.mut.Lock()
	defer s.mut.Unlock()

	old, ok := s.Statuses[p]
	s.Statuses[p] = h

	// This is considered a state change iif:
	// 1. It didn't exist and h != Healthy
	// 2. It did exist and old != h
	if !ok && h != Healthy {
		changed = true
	} else if ok && old != h {
		changed = true
	}
	if changed {
		s.LastUpdated = time.Now()
	}
	return
}

// Untrack removes p from s' health tracking. If p is a peer, Untrack
// will do nothing. This prevents untracking an unhealthy peer that is
// still being used for routing.
func (s *State) Untrack(p Descriptor) (changed bool) {
	s.mut.Lock()
	defer s.mut.Unlock()

	for _, o := range s.peers(true) {
		if o == p {
			return false
		}
	}

	_, ok := s.Statuses[p]
	delete(s.Statuses, p)
	return ok
}

// Prefix returns the first index where a and b differ. Returns
// len(a) if they are equal. Returns -1 if a and b have different
// lengths.
func Prefix(a, b id.Digits) int {
	if len(a) != len(b) {
		return -1
	}

	l := 0
	for i := range a {
		if a[i] != b[i] {
			break
		}
		l++
	}
	return l
}
