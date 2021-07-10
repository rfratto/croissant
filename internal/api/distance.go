package api

import (
	"math/bits"

	"github.com/rfratto/croissant/id"
)

// idDistance calculates the distance of a and b accounting
// for wraparound using max.
//
// Wraparound means that a may be closer to b if they traveled
// through max. The lowest value of the following is returned:
//
//   |a - b|
//   max - a + b + 1
//   max - b + a + 1
//
// Expressions that evaluate to be larger than max are ignored
// to prevent overflowing.
func idDistance(a, b id.ID, max id.ID) id.ID {
	// Wrap distance will always be smaller when a > b so
	// swap the two if that doesn't hold.
	if id.Compare(a, b) < 0 {
		return idDistance(b, a, max)
	}

	var (
		one = id.ID{Low: 1}

		directDist = absSub(b, a)
		maxDist    = idSub(max, a)
	)

	// Don't wrap around if b+1 or (max-a)+b+1 would overflow.
	if addOverflows(b, one, max) || addOverflows(maxDist, idAdd(b, one), max) {
		return directDist
	}

	wraparoundDist := idAdd(maxDist, idAdd(b, one))

	// Return the smaller of direct and wraparound distance.
	if id.Compare(wraparoundDist, directDist) < 0 {
		return wraparoundDist
	}
	return directDist
}

// absSub :: | a - b |
func absSub(a, b id.ID) id.ID {
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

// Returns true when v + o would overflow max.
func addOverflows(v, o, max id.ID) bool {
	// o overflows when (max - v) < o
	maxDist := idSub(max, v)
	return id.Compare(maxDist, o) < 0
}

// sub :: v - o
func idSub(v, o id.ID) id.ID {
	low, borrow := bits.Sub64(v.Low, o.Low, 0)
	high, borrow := bits.Sub64(v.High, o.High, borrow)
	return id.ID{High: high, Low: low}
}

// idAdd :: v + o
func idAdd(v, o id.ID) id.ID {
	low, borrow := bits.Add64(v.Low, o.Low, 0)
	high, _ := bits.Add64(v.High, o.High, borrow)
	return id.ID{High: high, Low: low}
}
