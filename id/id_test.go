package id

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestID_Strings(t *testing.T) {
	nums := []ID{
		Zero,
		{Low: 101010},
		{High: 0xABCDEF, Low: 0xFFFF},
		Max,
	}
	for _, n := range nums {
		// Assert that String == Parse
		parsed, err := Parse(n.String())
		require.NoError(t, err, "failed to parse %s", n.String())
		require.Equal(t, n, parsed)
	}
}

// TestID_String_Parse_Many generates a bunch of random numbers and ensures
// String == Parse.
func TestID_String_Parse_Many(t *testing.T) {
	r := rand.New(rand.NewSource(0))

	for i := 0; i < 1_000_000; i++ {
		id := ID{High: r.Uint64(), Low: r.Uint64()}

		// Sometimes generate a number that's just a uint64.
		if r.Int()%7 == 0 {
			id.High = 0
		}

		parsed, err := Parse(id.String())
		require.NoError(t, err)
		require.Equal(t, id, parsed)
	}
}

func TestID_Digits(t *testing.T) {
	tt := []struct {
		id     ID
		size   int
		base   int
		expect string
	}{
		{
			id:     ID{Low: 0b1101_1111},
			size:   8,
			base:   2,
			expect: "11011111",
		},
		{
			id:     ID{Low: 0b1001_1110},
			size:   8,
			base:   4,
			expect: "2132",
		},
		{
			id:     ID{Low: 0o325},
			size:   8,
			base:   8,
			expect: "325",
		},
		{
			id:     ID{Low: 0xF1F3},
			size:   16,
			base:   16,
			expect: "f1f3",
		},
		{
			id:     ID{Low: 0xDEADBEEF},
			size:   32,
			base:   16,
			expect: "deadbeef",
		},
		{
			id:     ID{Low: 0xDEADBEEF_DEADBEEF},
			size:   64,
			base:   16,
			expect: "deadbeefdeadbeef",
		},
		{
			id:     ID{High: 0xDEADBEEF_DEADFEED, Low: 0xDEADBEEF_DEADFEED},
			size:   128,
			base:   16,
			expect: "deadbeefdeadfeeddeadbeefdeadfeed",
		},
	}

	for _, tc := range tt {
		actual := tc.id.Digits(tc.size, tc.base).String()
		assert.Equal(t, tc.expect, actual)
	}
}

func BenchmarkDigits(b *testing.B) {
	r := rand.New(rand.NewSource(0))

	bases := []int{2, 4, 8, 16}

	for i := 0; i < b.N; i++ {
		id := ID{Low: r.Uint64()}
		_ = id.Digits(64, bases[r.Intn(len(bases))])
	}
}
