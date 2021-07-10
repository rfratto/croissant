package api

import (
	"github.com/rfratto/croissant/id"
)

type RoutingTable [][]*Descriptor

// NewRoutingTable creates a new RoutingTable with the given size.
func NewRoutingTable(width, height int) RoutingTable {
	rt := make(RoutingTable, height)
	for i, _ := range rt {
		rt[i] = make([]*Descriptor, width)
	}
	return rt
}

// NextHop looks up the next hop for a key for the given State s.
// May return s.Node if it is the closest node. Returning ok==false
// indicates a routing failure, likely due to a bug.
func NextHop(s *State, key id.ID) (next Descriptor, ok bool) {
	s.mut.Lock()
	defer s.mut.Unlock()

	if inLeafRange(s, key) {
		// Send to the lowest leaf node. Seed with ourselves so the local node can
		// be a candidate.
		var (
			lowestDist = s.distance(s.Node.ID, key)
			lowestPeer = s.Node
		)

		for _, n := range s.leaves(true) {
			// TODO(rfratto): if n is closest but unhealthy, should we return some
			// kind of error the caller can act on?
			if s.Statuses[n] != Healthy {
				continue
			}

			dist := s.distance(n.ID, key)
			if id.Compare(dist, lowestDist) < 0 {
				lowestDist = dist
				lowestPeer = n
			}
		}

		return lowestPeer, true
	}

	// Not in leaf range. See if the routing table has a node that has a shared prefixLen
	// with key.
	var (
		ourDigits = s.Node.ID.Digits(s.Size, s.Base)
		keyDigits = key.Digits(s.Size, s.Base)

		prefixLen = Prefix(ourDigits, keyDigits)
	)
	if ent := s.Routing[prefixLen][keyDigits[prefixLen]]; ent != nil && s.Statuses[*ent] == Healthy {
		return *ent, true
	}

	// Rare case: look for any node at all with a shared prefix greater than ours
	// that is also closer to it in the keyspace.
	var (
		localDistance  = s.distance(s.Node.ID, key)
		lowestDistance = localDistance
	)

	for _, p := range s.peers(false) {
		// Ignore any candidate who has less digits in common
		var (
			candidateDigits = p.ID.Digits(s.Size, s.Base)
			candidatePrefix = Prefix(candidateDigits, keyDigits)
		)
		if candidatePrefix < prefixLen {
			continue
		}

		dist := s.distance(p.ID, key)
		if id.Compare(dist, lowestDistance) < 0 {
			lowestDistance = dist
			next = p
			ok = true
		}
	}

	return
}

// inLeafRange returns true if the key is in the range of the leaf nodes.
func inLeafRange(s *State, key id.ID) bool {
	// If we're not full then the leaves contain all nodes in the cluster.
	if !s.Predecessors.IsFull() || !s.Successors.IsFull() {
		return true
	}

	// Get a set of descriptors that includes predecessors, ourselves, and
	// successors. Because of wraparound, this set might not be sorted and
	// nodes may appear twice.
	set := make([]Descriptor, 0, len(s.Predecessors.Descriptors)+len(s.Successors.Descriptors)+1)
	set = append(set, s.Predecessors.Descriptors...)
	set = append(set, s.Node)
	set = append(set, s.Successors.Descriptors...)

	// Break up each descriptors into pairs and see if key falls in between them.
	for i := 0; i < len(set); i++ {
		if i+1 >= len(set) {
			break
		}

		var (
			from = set[i+0].ID
			to   = set[i+1].ID
		)

		var inRange bool

		// If from > to then there's wraparound in the ring.
		if id.Compare(from, to) > 0 {
			// from <= key || key <= to
			inRange = (id.Compare(from, key) <= 0) || (id.Compare(key, to) <= 0)
		} else {
			// from <= key && key <= to
			inRange = id.Compare(from, key) <= 0 && id.Compare(key, to) <= 0
		}

		if inRange {
			return true
		}
	}

	return false
}
