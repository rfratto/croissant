// ID implements a 128-bit pastry ID. 128-bit is larger than most applications
// will need, so utility functions exist to convert into smaller representations.
//
// IDs are converted into a power-of-two base, up to hexadecimal, for message
// routing.
package id

import (
	"fmt"
	"math"
	"strings"

	"github.com/rfratto/croissant/internal/idconv"
)

// Lowest and highest values
var (
	Zero = ID{}
	Max  = ID{
		High: math.MaxUint64,
		Low:  math.MaxUint64,
	}
)

// MaxForSize returns the max ID for the given size in bits. size must be
// multiple of 2 and between 8 and 128 inclusive.
func MaxForSize(size int) ID {
	if size == 0 || size < 8 || size > 128 || !powerOfTwo(size) {
		panic("invalid size")
	}
	switch size {
	case 8:
		return ID{Low: math.MaxUint8}
	case 16:
		return ID{Low: math.MaxUint16}
	case 32:
		return ID{Low: math.MaxUint32}
	case 64:
		return ID{Low: math.MaxUint64}
	case 128:
		return Max
	default:
		panic("impossible case")
	}
}

// Parse parses a string into an ID.
func Parse(s string) (ID, error) {
	if s == "" || s == "0" {
		return Zero, nil
	}

	var res ID

	cutoff := div64(Max, 10)

	for _, c := range []byte(s) {
		if c <= '0' && c >= '9' {
			return Zero, fmt.Errorf("unexpected digit %s", string(c))
		}
		dig := uint64(c - '0')

		// If multiplying by 10 would overflow, stop early
		if Compare(res, cutoff) > 0 {
			return Zero, fmt.Errorf("id overflow")
		}

		n := mul64(res, 10)
		n = add64(n, uint64(dig))

		if Compare(n, res) <= 0 {
			return Zero, fmt.Errorf("id overflow")
		}

		res = n
	}

	return res, nil
}

// ID is an unsigned 128-bit number used to identify nodes and assign ownership
// to resources.
type ID struct {
	High, Low uint64
}

// String returns the base-10 representation of the ID.
func (id ID) String() string {
	// Borrowed from lukechampine.com/uint128
	if id == Zero {
		return "0"
	}
	buf := []byte("0000000000000000000000000000000000000000")
	for i := len(buf); ; i -= 19 {
		q, r := quoRem64(id, 1e19)
		var n int
		for ; r != 0; r /= 10 {
			n++
			buf[i-n] += byte(r % 10)
		}
		if q == Zero {
			return string(buf[i-n:])
		}
		id = q
	}
}

// Digits converts ID into individual digits of a power-of-two base, up to 16.
// size allows for representing the 128-bit ID as a smaller uint size.
//
// Returns nil if id is too big for size.
func (id ID) Digits(size, base int) Digits {
	if size == 0 || size < 8 || size > 128 || !powerOfTwo(size) {
		panic("invalid size")
	}
	if base == 0 || base > 16 || !powerOfTwo(base) {
		panic("invalid base")
	}
	if Compare(id, MaxForSize(size)) > 0 {
		return nil
	}

	exp := idconv.Log2(base)
	bits := (size + exp - 1) - (size+exp-1)%exp
	buf := make([]byte, bits/exp)

	for i := range buf {
		if bits < exp*(i+1) {
			buf[i] = 0
		}

		// Digit is (id >> (bits - exp*(n+1))) & (1 << exp - 1)
		shifted := shr(id, bits-exp*(i+1))
		buf[i] = byte(and64(shifted, 1<<exp-1).Low)
	}

	return buf
}

// Digits is a set of individual digits of an ID after converting it into
// a different base.
type Digits []byte

// String returns the string representation of digits.
func (d Digits) String() string {
	var sw strings.Builder
	for _, v := range d {
		if v > 0xf {
			// The maximum digit that can be represented by an ID is 15.
			panic("unexpected digit")
		}
		fmt.Fprintf(&sw, "%x", v)
	}
	return sw.String()
}

func powerOfTwo(n int) bool {
	return (n & (n - 1)) == 0
}

// Compare returns an integer comparing two IDs. The result will be 0 if a==b,
// -1 if a < b, and +1 if a > b.
func Compare(a, b ID) int {
	switch {
	case a.High == b.High && a.Low == b.Low:
		return 0
	case a.High < b.High || (a.High == b.High && a.Low < b.Low):
		return -1
	default:
		return 1
	}
}
