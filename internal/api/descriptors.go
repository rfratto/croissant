package api

import (
	"sort"

	"github.com/rfratto/croissant/id"
)

// DescriptorSet is an ordered set of descriptors.
type DescriptorSet struct {
	// Descriptors is the inner set of descriptors.
	Descriptors []Descriptor

	// Size is the maximum size of the set. Operations against the DescriptorSet
	// will keep it bound to this.
	Size int

	// KeepBiggest will keep the biggest elements when doing an Insert.
	// If the set is IDs (0, 1, 2), size=3, then an insert of 5
	// with KeepBiggest=true will change the set to (1, 2, 5), while
	// KeepBiggest=false will keep at (0, 1, 2).
	//
	// KeepBiggest being true corresponds to the set of predecessors.
	KeepBiggest bool

	// SearchFunc should return true when i is >= j.
	SearchFunc func(i, j Descriptor) bool
}

// Clone returns a copy of DescriptorSet.
func (dset *DescriptorSet) Clone() *DescriptorSet {
	var clone DescriptorSet
	clone.Descriptors = make([]Descriptor, len(dset.Descriptors))
	clone.Size = dset.Size
	clone.KeepBiggest = dset.KeepBiggest
	clone.SearchFunc = dset.SearchFunc

	for i := 0; i < len(dset.Descriptors); i++ {
		clone.Descriptors[i] = dset.Descriptors[i]
	}

	return &clone
}

// Contains returns true if d is in dset.
func (dset *DescriptorSet) Contains(d Descriptor) bool {
	i := dset.indexOf(d)
	return i < len(dset.Descriptors) && dset.Descriptors[i] == d
}

// Remove removes d from dset. Returns true if the element was removed.
func (dset *DescriptorSet) Remove(d Descriptor) bool {
	i := dset.indexOf(d)
	if i == len(dset.Descriptors) || dset.Descriptors[i] != d {
		return false
	}

	dset.Descriptors = append(dset.Descriptors[:i], dset.Descriptors[i+1:]...)
	return true
}

func (dset *DescriptorSet) indexOf(d Descriptor) int {
	sf := dset.SearchFunc
	if sf == nil {
		sf = DefaultSearchFunc
	}

	return sort.Search(len(dset.Descriptors), func(i int) bool {
		return sf(dset.Descriptors[i], d)
	})
}

// DefaultSearchFunc returns true when i >= j.
func DefaultSearchFunc(i, j Descriptor) bool {
	return id.Compare(i.ID, j.ID) >= 0
}

// WraparoundSearchFunc sorts IDs in two tiers: IDs that are larger than wraparound
// and IDs that are smaller than wraparound. This allows for wraparound semantics
// in the ring.
//
// Example: Given a set (0, 100, 200, 300) and wrapping around 150, numbers
// will be sorted as (200, 300, 0, 100).
func WraparoundSearchFunc(wraparound id.ID) func(i, j Descriptor) bool {
	return func(i, j Descriptor) bool {
		var (
			iBigger = id.Compare(i.ID, wraparound) >= 0
			jBigger = id.Compare(j.ID, wraparound) >= 0
		)
		switch {
		// Compare normally if neither or both are bigger than wraparound.
		case (!iBigger && !jBigger) || (iBigger && jBigger):
			return id.Compare(i.ID, j.ID) >= 0
		// Otherwise, i >= j iff i is bigger than the wraparound.
		default:
			return !iBigger
		}
	}
}

// Push pushes d into dset. If d already exists in dset or dset is full, Push
// is a no-op. Returns true if dset was modified.
func (dset *DescriptorSet) Push(d Descriptor) bool {
	return dset.inject(d, false)
}

// Insert inserts d into dset, removing other elements if dset is full. If d
// already exists in dset, Insert is a no-op.
func (dset *DescriptorSet) Insert(d Descriptor) bool {
	return dset.inject(d, true)
}

// IsFull returns true if dset is at capacity.
func (dset *DescriptorSet) IsFull() bool {
	return len(dset.Descriptors) == dset.Size
}

func (dset *DescriptorSet) inject(d Descriptor, insert bool) bool {
	if !insert && dset.IsFull() {
		return false
	}

	i := dset.indexOf(d)
	if i != len(dset.Descriptors) && dset.Descriptors[i] == d {
		return false
	}

	if i == len(dset.Descriptors) {
		dset.Descriptors = append(dset.Descriptors, d)
	} else {
		injected := append([]Descriptor{}, dset.Descriptors[:i]...)
		injected = append(injected, d)
		injected = append(injected, dset.Descriptors[i:]...)
		dset.Descriptors = injected
	}

	// Remove extraneous elements past limit
	for len(dset.Descriptors) > dset.Size {
		if dset.KeepBiggest {
			dset.Descriptors = dset.Descriptors[1:]
		} else {
			dset.Descriptors = dset.Descriptors[:len(dset.Descriptors)-1]
		}
	}

	return true
}

// Descriptor describes a node in a cluster.
type Descriptor struct {
	// ID for routing.
	ID id.ID
	// Addr for connecting.
	Addr string
}

// ReverseDescriptors reverses ds.
func ReverseDescriptors(ds []Descriptor) []Descriptor {
	reversed := make([]Descriptor, len(ds))
	for i := 0; i < len(ds); i++ {
		reversed[len(ds)-i-1] = ds[i]
	}
	return reversed
}
