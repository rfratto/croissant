package id

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerator(t *testing.T) {
	tt := []struct {
		input       string
		size        int
		expect      string
		expectPanic bool
	}{
		{
			input:  "Never gonna give you up",
			size:   8,
			expect: "6b",
		},
		{
			input:  "Never gonna let you down",
			size:   16,
			expect: "d902",
		},
		{
			input:  "Never gonna run around and desert you",
			size:   32,
			expect: "10e347ff",
		},
		{
			input:  "Never gonna make you cry",
			size:   64,
			expect: "ae605ab8ac8c86a5",
		},
		{
			input:  "Never gonna say goodbye",
			size:   128,
			expect: "a99afa90034a46f79e4470156d21b474",
		},
		{
			input: "Never gonna tell a lie and hurt you",
			size:  256,

			expectPanic: true,
		},
	}

	for _, tc := range tt {
		t.Run(fmt.Sprintf("Size%d", tc.size), func(t *testing.T) {
			if tc.expectPanic {
				require.Panics(t, func() {
					NewGenerator(tc.size)
				})
				return
			}
			id := NewGenerator(tc.size).Get(tc.input)
			assert.Equal(t, tc.expect, id.Digits(tc.size, 16).String())
		})
	}
}
