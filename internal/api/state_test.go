package api

import (
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/rfratto/croissant/id"
	"github.com/stretchr/testify/require"
)

func TestPrefix(t *testing.T) {
	tt := []struct {
		a, b   id.ID
		size   int
		base   int
		expect int
	}{
		{
			a:      id.ID{Low: 0xDEADBEEF},
			b:      id.ID{Low: 0xDEADFEED},
			size:   32,
			base:   16,
			expect: 4,
		},
		{
			a:      id.ID{Low: 0xFFFF},
			b:      id.ID{Low: 0xFFFF},
			size:   16,
			base:   16,
			expect: 4,
		},
		{
			a:      id.ID{Low: 0xFF},
			b:      id.ID{Low: 0o70},
			size:   8,
			base:   16,
			expect: 0,
		},
	}

	for _, tc := range tt {
		aDig := tc.a.Digits(tc.size, tc.base)
		bDig := tc.b.Digits(tc.size, tc.base)
		actual := Prefix(aDig, bDig)
		require.Equal(t, tc.expect, actual)
	}
}

func TestState(t *testing.T) {
	r := rand.New(rand.NewSource(0))

	key := id.NewGenerator(16).Get("Croissant")

	s := NewState(
		Descriptor{ID: key, Addr: "127.0.0.1:90905"},
		8, 8,
		16, 4, // bitsize=16, base=4
	)

	// Generate random IDs and insert them into the table.
	for i := 0; i < 500; i++ {
		s.addRoute(Descriptor{
			ID:   generateRandomID(t, r),
			Addr: "fake-node",
		})
	}

	// Ensure that every single row contains ourselves exactly
	// once.
	for rowNum, row := range s.Routing {
		var selfCount int
		for _, ent := range row {
			if ent == nil || *ent != s.Node {
				continue
			}
			selfCount++
		}
		require.Equal(t, 1, selfCount, "no self in row %d", rowNum)
	}
}

func generateRandomID(t *testing.T, r *rand.Rand) id.ID {
	t.Helper()

	buf := make([]byte, 2)
	_, err := r.Read(buf)
	require.NoError(t, err)

	val := binary.BigEndian.Uint16(buf)
	return id.ID{Low: uint64(val)}
}

func TestState_MixLeaf(t *testing.T) {
	idFrom := func(val int) id.ID {
		return id.ID{Low: uint64(val)}
	}

	descFrom := func(val int) Descriptor {
		return Descriptor{ID: idFrom(val)}
	}

	buildState := func(n Descriptor, leaves []int) *State {
		s := NewState(n, 8, 0, 16, 4)
		for _, l := range leaves {
			s.addLeaf(descFrom(l))
		}
		return s
	}

	tt := []struct {
		left, right *State
		expect      []Descriptor
	}{{
		left: buildState(
			descFrom(5000),
			[]int{
				1000,
				2000,
				3000,
				4000,
				// 5000
				6000,
				7000,
				8000,
				9000,
			},
		),
		right: buildState(
			descFrom(5050),
			[]int{
				4050,
				4060,
				4070,
				5000,
				// 5050
				7000,
				7050,
				8050,
				9050,
			},
		),
		expect: []Descriptor{
			descFrom(4000),
			descFrom(4050),
			descFrom(4060),
			descFrom(4070),
			// 0x5000,
			descFrom(5050),
			descFrom(6000),
			descFrom(7000),
			descFrom(7050),
		},
	}}

	for _, tc := range tt {
		updated := tc.left.MixinLeaves(tc.right)
		require.True(t, updated)

		var gotLeaves []Descriptor
		gotLeaves = append(gotLeaves, tc.left.Predecessors.Descriptors...)
		gotLeaves = append(gotLeaves, tc.left.Successors.Descriptors...)
		require.Equal(t, tc.expect, gotLeaves)
	}
}
