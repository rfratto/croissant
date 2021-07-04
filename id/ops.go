package id

import "math/bits"

// add64 :: v + o. May overflow.
func add64(v ID, o uint64) ID {
	lo, carry := bits.Add64(v.Low, o, 0)
	hi, _ := bits.Add64(v.High, 0, carry)
	return ID{High: hi, Low: lo}
}

// mul64 :: v * o. May overflow.
func mul64(v ID, o uint64) ID {
	hi, lo := bits.Mul64(v.Low, o)
	_, p1 := bits.Mul64(v.High, o)
	hi, _ = bits.Add64(hi, p1, 0)
	return ID{High: hi, Low: lo}
}

// div64 :: v / o
func div64(v ID, o uint64) ID {
	q, _ := quoRem64(v, o)
	return q
}

// quoRem64 :: v / o, v % o
func quoRem64(v ID, o uint64) (q ID, r uint64) {
	if v.High < o {
		q.Low, r = bits.Div64(v.High, v.Low, o)
	} else {
		q.High, r = bits.Div64(0, v.High, o)
		q.Low, r = bits.Div64(r, v.Low, o)
	}
	return
}

// shr :: v >> n
func shr(v ID, n int) ID {
	if n > 64 {
		return ID{Low: v.High >> (n - 64)}
	}

	return ID{
		Low:  v.Low>>n | v.High<<(64-n),
		High: v.High >> n,
	}
}

// and64 :: v & n
func and64(v ID, n uint64) ID {
	return ID{Low: v.Low & n}
}
