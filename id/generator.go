package id

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"math"
	"sync"
)

// Generator generates IDs based on an input string.
type Generator interface {
	Get(s string) ID
}

// NewGenerator returns an ID generator where IDs will be generated from a hash
// of size (must be one of 8, 16, 32, 64, 128).
func NewGenerator(size int) Generator {
	switch size {
	case 8:
		var g gen8
		g.p.New = func() interface{} { return fnv.New32a() }
		return &g
	case 16:
		var g gen16
		g.p.New = func() interface{} { return fnv.New32a() }
		return &g
	case 32:
		var g gen32
		g.p.New = func() interface{} { return fnv.New32a() }
		return &g
	case 64:
		var g gen64
		g.p.New = func() interface{} { return fnv.New64a() }
		return &g
	case 128:
		var g gen128
		g.p.New = func() interface{} { return fnv.New128a() }
		return &g
	default:
		panic("invalid size")
	}
}

type gen8 struct{ p sync.Pool }

func (g *gen8) Get(s string) ID {
	h := g.p.New().(hash.Hash)
	defer g.p.Put(h)

	h.Reset()
	fmt.Fprint(h, s)

	sum := binary.BigEndian.Uint32(h.Sum(nil))
	return ID{Low: uint64(sum & math.MaxUint8)}
}

type gen16 struct{ p sync.Pool }

func (g *gen16) Get(s string) ID {
	h := g.p.New().(hash.Hash)
	defer g.p.Put(h)

	h.Reset()
	fmt.Fprint(h, s)

	sum := binary.BigEndian.Uint32(h.Sum(nil))
	return ID{Low: uint64(sum & math.MaxUint16)}
}

type gen32 struct{ p sync.Pool }

func (g *gen32) Get(s string) ID {
	h := g.p.New().(hash.Hash)
	defer g.p.Put(h)

	h.Reset()
	fmt.Fprint(h, s)

	sum := binary.BigEndian.Uint32(h.Sum(nil))
	return ID{Low: uint64(sum)}
}

type gen64 struct{ p sync.Pool }

func (g *gen64) Get(s string) ID {
	h := g.p.New().(hash.Hash)
	defer g.p.Put(h)

	h.Reset()
	fmt.Fprint(h, s)

	sum := binary.BigEndian.Uint64(h.Sum(nil))
	return ID{Low: sum}
}

type gen128 struct{ p sync.Pool }

func (g *gen128) Get(s string) ID {
	h := g.p.New().(hash.Hash)
	defer g.p.Put(h)

	h.Reset()
	fmt.Fprint(h, s)

	sum := h.Sum(nil)
	return ID{
		Low:  binary.BigEndian.Uint64(sum[8:]),
		High: binary.BigEndian.Uint64(sum[:8]),
	}
}
