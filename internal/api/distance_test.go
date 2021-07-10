package api

import (
	"fmt"
	"testing"

	"github.com/rfratto/croissant/id"
	"github.com/stretchr/testify/require"
)

func Test_idDistanceWrap(t *testing.T) {
	newID := func(v uint64) id.ID { return id.ID{Low: v} }

	type testCase struct {
		src, dest, max id.ID
		expect         id.ID
	}
	tt := []testCase{
		{
			src:    newID(900),
			dest:   newID(50),
			max:    newID(1000),
			expect: newID(151), // wraps around
		},
		{
			src:    newID(0),
			dest:   newID(500),
			max:    newID(1000),
			expect: newID(500), // does not wrap aronud
		},
		{
			src:    newID(0),
			dest:   newID(1000),
			max:    newID(1000),
			expect: newID(1), // wraps around
		},

		// Same number tests
		{
			src:    newID(800),
			dest:   newID(800),
			max:    newID(1000),
			expect: newID(0), // does not wrap around
		},
		{
			src:    newID(0),
			dest:   newID(0),
			max:    newID(1000),
			expect: newID(0), // does not wrap around
		},
		{
			src:    newID(1000),
			dest:   newID(1000),
			max:    newID(1000),
			expect: newID(0), // does not wrap around
		},
	}

	// Swapped values for dest and src shouldn't affect the outcome of the test
	// since wrapping around works in both directions.
	for _, tc := range tt {
		// If src == dest we don't need to duplicate the test.
		if tc.src == tc.dest {
			continue
		}

		tt = append(tt, testCase{
			src:    tc.dest,
			dest:   tc.src,
			max:    tc.max,
			expect: tc.expect,
		})
	}

	for _, tc := range tt {
		src := tc.src
		dest := tc.dest

		name := fmt.Sprintf("src=%s,dest=%s,max=%s", src, dest, tc.max)
		t.Run(name, func(t *testing.T) {
			actual := idDistance(src, dest, tc.max)
			require.Equal(t, tc.expect, actual,
				"Distance of %s to %s (max %s) should be %s, but got %s",
				src, dest, tc.max, tc.expect, actual)
		})
	}
}
