package api

import (
	"testing"

	"github.com/rfratto/croissant/id"
	"github.com/stretchr/testify/require"
)

func TestDescriptorSet_Push(t *testing.T) {
	tt := []struct {
		name   string
		dset   DescriptorSet
		inputs []int
		expect []int
	}{
		{
			name:   "keep smallest",
			dset:   DescriptorSet{Size: 4, KeepBiggest: false},
			inputs: []int{1, 10, 2, 5, 3, 6},
			expect: []int{1, 2, 5, 10},
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			for _, in := range tc.inputs {
				tc.dset.Push(Descriptor{
					ID: id.ID{Low: uint64(in)},
				})
			}

			res := []int{}
			for _, r := range tc.dset.Descriptors {
				res = append(res, int(r.ID.Low))
			}

			require.Equal(t, tc.expect, res)
		})
	}
}

func TestDescriptorSet_Insert(t *testing.T) {
	tt := []struct {
		name   string
		dset   DescriptorSet
		inputs []int
		expect []int
	}{
		{
			name:   "keep smallest",
			dset:   DescriptorSet{Size: 4, KeepBiggest: false},
			inputs: []int{1, 10, 2, 5, 3, 6},
			expect: []int{1, 2, 3, 5},
		},
		{
			name:   "keep biggest",
			dset:   DescriptorSet{Size: 4, KeepBiggest: true},
			inputs: []int{1, 10, 2, 5, 3, 6},
			expect: []int{3, 5, 6, 10},
		},
		{
			name:   "keep smallest push past limit",
			dset:   DescriptorSet{Size: 3, KeepBiggest: false},
			inputs: []int{0, 1, 2 /* push */, 5},
			expect: []int{0, 1, 2},
		},
		{
			name:   "keep biggest push past limit",
			dset:   DescriptorSet{Size: 3, KeepBiggest: true},
			inputs: []int{0, 1, 2 /* push */, 5},
			expect: []int{1, 2, 5},
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			for _, in := range tc.inputs {
				tc.dset.Insert(Descriptor{
					ID: id.ID{Low: uint64(in)},
				})
			}

			res := []int{}
			for _, r := range tc.dset.Descriptors {
				res = append(res, int(r.ID.Low))
			}

			require.Equal(t, tc.expect, res)
		})
	}
}

func TestDescriptorSet_Insert_Wraparound(t *testing.T) {
	tt := []struct {
		name   string
		dset   DescriptorSet
		anchor int
		inputs []int
		expect []int
	}{
		{
			name:   "anchor",
			dset:   DescriptorSet{Size: 4, KeepBiggest: false},
			anchor: 150,
			inputs: []int{0, 100, 200, 300},
			expect: []int{200, 300, 0, 100},
		},
		{
			name:   "anchor extra element",
			dset:   DescriptorSet{Size: 4, KeepBiggest: false},
			anchor: 150,
			inputs: []int{0, 100, 200, 300, 350},
			expect: []int{200, 300, 350, 0},
		},
		{
			name:   "anchor keep biggest",
			dset:   DescriptorSet{Size: 4, KeepBiggest: true},
			anchor: 150,
			inputs: []int{0, 100, 200, 300},
			expect: []int{200, 300, 0, 100},
		},
		{
			name:   "anchor keep biggest extra element",
			dset:   DescriptorSet{Size: 4, KeepBiggest: true},
			anchor: 150,
			inputs: []int{0, 100, 200, 300, 350},
			expect: []int{300, 350, 0, 100},
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			tc.dset.SearchFunc = WraparoundSearchFunc(id.ID{
				Low: uint64(tc.anchor),
			})

			for _, in := range tc.inputs {
				tc.dset.Insert(Descriptor{
					ID: id.ID{Low: uint64(in)},
				})
			}

			res := []int{}
			for _, r := range tc.dset.Descriptors {
				res = append(res, int(r.ID.Low))
			}

			require.Equal(t, tc.expect, res)
		})
	}
}

func TestDescriptorSet_Remove(t *testing.T) {
	tt := []struct {
		name   string
		dset   DescriptorSet
		inputs []int
		remove int
		expect []int
	}{
		{
			name:   "middle",
			dset:   DescriptorSet{},
			inputs: []int{1, 2, 3, 5},
			remove: 3,
			expect: []int{1, 2, 5},
		},
		{
			name:   "index 0",
			dset:   DescriptorSet{},
			inputs: []int{1, 2, 3, 5},
			remove: 1,
			expect: []int{2, 3, 5},
		},
		{
			name:   "end",
			dset:   DescriptorSet{},
			inputs: []int{1, 2, 3, 5},
			remove: 5,
			expect: []int{1, 2, 3},
		},
		{
			name:   "not found",
			dset:   DescriptorSet{},
			inputs: []int{1, 2, 3, 5},
			remove: 9,
			expect: []int{1, 2, 3, 5},
		},
		{
			name:   "single element",
			dset:   DescriptorSet{},
			inputs: []int{1},
			remove: 0,
			expect: []int{1},
		},
	}

	for _, tc := range tt {
		for _, in := range tc.inputs {
			tc.dset.Descriptors = append(tc.dset.Descriptors, Descriptor{
				ID: id.ID{Low: uint64(in)},
			})
		}

		tc.dset.Remove(Descriptor{
			ID: id.ID{Low: uint64(tc.remove)},
		})

		res := []int{}
		for _, r := range tc.dset.Descriptors {
			res = append(res, int(r.ID.Low))
		}

		require.Equal(t, tc.expect, res)
	}
}

func TestDescriptorSet_Remove_Wraparound(t *testing.T) {
	tt := []struct {
		name   string
		dset   DescriptorSet
		anchor int
		inputs []int
		remove int
		expect []int
	}{
		{
			name:   "middle",
			dset:   DescriptorSet{},
			anchor: 150,
			inputs: []int{200, 300, 0, 100},
			remove: 0,
			expect: []int{200, 300, 100},
		},
		{
			name:   "start",
			dset:   DescriptorSet{},
			anchor: 150,
			inputs: []int{200, 300, 0, 100},
			remove: 200,
			expect: []int{300, 0, 100},
		},
		{
			name:   "end",
			dset:   DescriptorSet{},
			anchor: 150,
			inputs: []int{200, 300, 0, 100},
			remove: 100,
			expect: []int{200, 300, 0},
		},
		{
			name:   "doesn't exist < anchor",
			dset:   DescriptorSet{},
			anchor: 150,
			inputs: []int{200, 300, 0, 100},
			remove: 80,
			expect: []int{200, 300, 0, 100},
		},
		{
			name:   "doesn't exist > anchor",
			dset:   DescriptorSet{},
			anchor: 150,
			inputs: []int{200, 300, 0, 100},
			remove: 500,
			expect: []int{200, 300, 0, 100},
		},
	}

	for _, tc := range tt {
		tc.dset.SearchFunc = WraparoundSearchFunc(id.ID{
			Low: uint64(tc.anchor),
		})

		for _, in := range tc.inputs {
			tc.dset.Descriptors = append(tc.dset.Descriptors, Descriptor{
				ID: id.ID{Low: uint64(in)},
			})
		}

		tc.dset.Remove(Descriptor{
			ID: id.ID{Low: uint64(tc.remove)},
		})

		res := []int{}
		for _, r := range tc.dset.Descriptors {
			res = append(res, int(r.ID.Low))
		}

		require.Equal(t, tc.expect, res)
	}
}
