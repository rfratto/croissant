package id

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"hash"
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
		g.max = MaxForSize(size).Low
		g.p.New = func() interface{} { return md5.New() }
		return &g
	case 16:
		var g gen16
		g.max = MaxForSize(size).Low
		g.p.New = func() interface{} { return md5.New() }
		return &g
	case 32:
		var g gen32
		g.max = MaxForSize(size).Low
		g.p.New = func() interface{} { return md5.New() }
		return &g
	case 64:
		var g gen64
		g.p.New = func() interface{} { return md5.New() }
		return &g
	case 128:
		var g gen128
		g.p.New = func() interface{} { return md5.New() }
		return &g
	default:
		panic("invalid size")
	}
}

type gen8 struct {
	max uint64
	p   sync.Pool
}

func (g *gen8) Get(s string) ID {
	h := g.p.New().(hash.Hash)
	defer g.p.Put(h)

	h.Reset()
	fmt.Fprint(h, s)

	sum := h.Sum(nil)
	var (
		low  = binary.BigEndian.Uint64(sum[8:])
		high = binary.BigEndian.Uint64(sum[:8])
	)
	return ID{Low: (high ^ low) % g.max}
}

type gen16 struct {
	max uint64
	p   sync.Pool
}

func (g *gen16) Get(s string) ID {
	h := g.p.New().(hash.Hash)
	defer g.p.Put(h)

	h.Reset()
	fmt.Fprint(h, s)

	sum := h.Sum(nil)
	var (
		low  = binary.BigEndian.Uint64(sum[8:])
		high = binary.BigEndian.Uint64(sum[:8])
	)
	return ID{Low: (high ^ low) % g.max}
}

type gen32 struct {
	max uint64
	p   sync.Pool
}

func (g *gen32) Get(s string) ID {
	h := g.p.New().(hash.Hash)
	defer g.p.Put(h)

	h.Reset()
	fmt.Fprint(h, s)

	sum := h.Sum(nil)
	var (
		low  = binary.BigEndian.Uint64(sum[8:])
		high = binary.BigEndian.Uint64(sum[:8])
	)
	return ID{Low: (high ^ low) % g.max}
}

type gen64 struct{ p sync.Pool }

func (g *gen64) Get(s string) ID {
	h := g.p.New().(hash.Hash)
	defer g.p.Put(h)

	h.Reset()
	fmt.Fprint(h, s)

	sum := h.Sum(nil)
	var (
		low  = binary.BigEndian.Uint64(sum[8:])
		high = binary.BigEndian.Uint64(sum[:8])
	)
	return ID{Low: high ^ low}
}

type gen128 struct{ p sync.Pool }

func (g *gen128) Get(s string) ID {
	h := g.p.New().(hash.Hash)
	defer g.p.Put(h)

	h.Reset()
	fmt.Fprint(h, s)

	sum := h.Sum(nil)
	var (
		low  = binary.BigEndian.Uint64(sum[8:])
		high = binary.BigEndian.Uint64(sum[:8])
	)
	return ID{High: high, Low: low}
}
