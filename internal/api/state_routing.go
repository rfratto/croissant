package api

import (
	"math/bits"

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
			lowestDist = idDistance(s.Node.ID, key)
			lowestPeer = s.Node
		)

		for _, n := range s.leaves(true) {
			// TODO(rfratto): if n is closest but unhealthy, should we return some
			// kind of error the caller can act on?
			if s.Statuses[n] != Healthy {
				continue
			}

			dist := idDistance(n.ID, key)
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
		localDistance  = idDistance(s.Node.ID, key)
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

		dist := idDistance(p.ID, key)
		if id.Compare(dist, lowestDistance) < 0 {
			lowestDistance = dist
			next = p
			ok = true
		}
	}

	return
}

// inLeafRange returns true if the key is in the range of the leaf nodes.
// If the leaf set isn't full, returns true, since a non-full leaf set
// means the entire range of keys falls within the known leaf nodes.
func inLeafRange(s *State, key id.ID) bool {
	if !s.Predecessors.IsFull() || !s.Successors.IsFull() {
		return true
	}

	var (
		lowest  = s.Predecessors.Descriptors[0]
		highest = s.Successors.Descriptors[len(s.Successors.Descriptors)-1]
	)

	// lowest <= key <= highest
	return id.Compare(lowest.ID, key) <= 0 && id.Compare(key, highest.ID) <= 0
}

// idDistance calculates |a - b|.
func idDistance(a, b id.ID) id.ID {
	cmp := id.Compare(a, b)
	switch {
	case cmp < 0: // a < b
		return idSub(b, a)
	case cmp == 0: // a == b
		return id.Zero
	case cmp > 0: // a > b
		return idSub(a, b)
	default:
		panic("impossible case")
	}
}

// sub :: v - o
func idSub(v, o id.ID) id.ID {
	low, borrow := bits.Sub64(v.Low, o.Low, 0)
	high, borrow := bits.Sub64(v.High, o.High, borrow)
	return id.ID{High: high, Low: low}
}
