// Package idconv implements utility methods for converting between a base-10
// 128-bit ID to a number of a different base and size.
package idconv

// Digits will the the number of digits for an integer of bitsize size and
// base base.
func Digits(size, base int) int {
	// Number of digits is full number of bits divided by log of the base.
	// Bits are rounded up to the next multiple of log of the base.
	exp := Log2(base)
	bits := (size + exp - 1) - (size+exp-1)%exp
	return bits / exp
}

// Log2 gets the base-2 log of n, if n is a power of 2 and <= 16.
func Log2(n int) int {
	res, ok := logLookup[n]
	if !ok {
		panic("unexpected n")
	}
	return res
}

var logLookup = map[int]int{
	// keys are result of 2^value
	1:  0,
	2:  1,
	4:  2,
	8:  3,
	16: 4,
}
