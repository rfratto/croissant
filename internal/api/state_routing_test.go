package api

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/rfratto/croissant/id"
	"github.com/stretchr/testify/require"
)

func TestNextRoute(t *testing.T) {
	newDesc := func(key int) Descriptor {
		return Descriptor{
			ID: id.ID{Low: uint64(key)},
		}
	}

	tt := []struct {
		name   string
		input  *State
		key    int
		expect int
	}{
		{
			name: "exact match",
			input: NewState(
				newDesc(0o1000),
				4, 4,
				16, 8,
			),
			key:    0o1000,
			expect: 0o1000,
		},
		{
			name: "empty table",
			input: NewState(
				newDesc(0o1000),
				4, 4,
				16, 8,
			),
			key:    0o5000,
			expect: 0o1000,
		},
		{
			name: "incomplete leaf",
			input: func() *State {
				s := NewState(
					newDesc(0o1000),
					4, 4,
					16, 8,
				)

				p := NewState(
					newDesc(0o2000),
					4, 4,
					16, 8,
				)

				updated := s.MixinLeaves(p)
				require.True(t, updated)
				return s
			}(),
			key:    0o5000,
			expect: 0o2000,
		},
		{
			name: "full leaf",
			input: func() *State {
				s := NewState(newDesc(0o300), 4, 4, 16, 8)

				peers := []*State{
					NewState(newDesc(0o100), 4, 4, 16, 8),
					NewState(newDesc(0o200), 4, 4, 16, 8),
					NewState(newDesc(0o400), 4, 4, 16, 8),
					NewState(newDesc(0o500), 4, 4, 16, 8),
				}
				for _, p := range peers {
					updated := s.MixinLeaves(p)
					require.True(t, updated)
				}

				require.True(t, s.Predecessors.IsFull())
				require.True(t, s.Successors.IsFull())
				return s
			}(),
			key:    0o150,
			expect: 0o200,
		},
		{
			name: "routing lookup",
			input: func() *State {
				s := NewState(newDesc(0o1000), 4, 4, 16, 8)

				// Routing lookup requires a full leaf table.
				peers := []*State{
					NewState(newDesc(0o0776), 4, 4, 16, 8),
					NewState(newDesc(0o0777), 4, 4, 16, 8),
					NewState(newDesc(0o1001), 4, 4, 16, 8),
					NewState(newDesc(0o1002), 4, 4, 16, 8),
				}
				for _, p := range peers {
					updated := s.MixinLeaves(p)
					require.True(t, updated)
				}

				peer := NewState(newDesc(0o3000), 4, 4, 16, 8)
				updated := s.mixinRoutes(peer)
				require.True(t, updated)

				return s
			}(),
			key:    0o3123,
			expect: 0o3000,
		},
		{
			// In case the routing table doesn't have an entry, the closest
			// key amongst the leaves + routing + neighborhood should be used.
			name: "fallback to leaf",
			input: func() *State {
				s := NewState(newDesc(0o300), 4, 4, 16, 8)

				peers := []*State{
					NewState(newDesc(0o100), 4, 4, 16, 8),
					NewState(newDesc(0o200), 4, 4, 16, 8),
					NewState(newDesc(0o400), 4, 4, 16, 8),
					NewState(newDesc(0o500), 4, 4, 16, 8),
				}
				for _, p := range peers {
					updated := s.MixinLeaves(p)
					require.True(t, updated)
				}

				require.True(t, s.Predecessors.IsFull())
				require.True(t, s.Successors.IsFull())
				return s
			}(),
			key:    0o1000,
			expect: 0o500,
		},
		{
			// In case the routing table doesn't have an entry, the closest
			// key amongst the leaves + routing + neighborhood should be used.
			name: "fallback to neighbor",
			input: func() *State {
				s := NewState(newDesc(0o300), 4, 4, 16, 8)

				peers := []*State{
					NewState(newDesc(0o100), 4, 4, 16, 8),
					NewState(newDesc(0o200), 4, 4, 16, 8),
					NewState(newDesc(0o400), 4, 4, 16, 8),
					NewState(newDesc(0o500), 4, 4, 16, 8),
				}
				for _, p := range peers {
					updated := s.MixinLeaves(p)
					require.True(t, updated)
				}

				s.addNeighbor(newDesc(0o1050))
				return s
			}(),
			key:    0o1000,
			expect: 0o1050,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			actual, ok := NextHop(tc.input, id.ID{
				Low: uint64(tc.key),
			})
			require.True(t, ok)
			require.Equal(t, tc.expect, int(actual.ID.Low))
		})
	}
}

// TestSimulateCluster will simlate a cluster and verify that random nodes can
// be reached from arbitrary entrypoints.
func TestSimulateCluster(t *testing.T) {
	rnd := rand.New(rand.NewSource(0))

	var (
		numNodes = 10_000
	)

	nodes, states := createTestCluster(t, rnd, numNodes)

	// Assert that the set of leaves of each node is correct.
	t.Run("leaves", func(t *testing.T) {
		for i, n := range nodes {
			for p := len(n.Predecessors.Descriptors) - 1; p >= 0; p-- {
				actual := n.Predecessors.Descriptors[p]

				// Wraparound
				expectIdx := i - (len(n.Predecessors.Descriptors) - p)
				if expectIdx < 0 {
					expectIdx = len(nodes) - (-expectIdx)
				}
				expect := nodes[expectIdx].Node

				require.Equal(t, expect, actual,
					"node %s (index=%d) has wrong predecessors. expected predecessor %d to be %s, founud %s",
					n.Node.ID.Digits(32, 8),
					i,
					p,
					expect.ID.Digits(32, 8),
					actual.ID.Digits(32, 8),
				)
			}

			for s := 0; s < len(n.Successors.Descriptors); s++ {
				actual := n.Successors.Descriptors[s]

				// Wraparound
				expectIdx := i + s + 1
				if expectIdx >= len(nodes) {
					expectIdx = expectIdx - len(nodes)
				}
				expect := nodes[expectIdx].Node

				require.Equal(t, expect, actual,
					"node %s (index=%d) has wrong successors. expected successor %d to be %s, found %s",
					n.Node.ID.Digits(32, 8),
					i,
					s,
					expect.ID.Digits(32, 8),
					actual.ID.Digits(32, 8),
				)
			}
		}
	})

	// Test routing keys and ensure that their final destination is the correct node.
	t.Run("routing", func(t *testing.T) {
		var numKeys = 1_000_000

		for i := 0; i < numKeys; i++ {
			key := id.ID{Low: uint64(rnd.Uint32())}

			// Pick a random node to start the request from
			seed := nodes[rnd.Intn(len(nodes))]
			dest := fakeRoute(t, seed, key, states)

			// Find the ~10 nodes closest to our key to make sure none of them are
			// closer. This lets us do a ton of lookups very quickly even though
			// it looks funky.
			closest := sort.Search(len(nodes), func(i int) bool {
				return id.Compare(nodes[i].Node.ID, key) >= 0
			})
			start := closest - 5
			end := closest + 5
			if start < 0 {
				start = 0
			}
			if end >= len(nodes) {
				end = len(nodes) - 1
			}

			dist := idDistance(dest.ID, key, id.MaxForSize(32))
			for _, s := range nodes[start:end] {
				altDist := idDistance(s.Node.ID, key, id.MaxForSize(32))
				if id.Compare(altDist, dist) < 0 {
					require.Fail(t, "found routing to wrong node", "got distance %s but found closer distance %s", dist, altDist)
				}
			}
		}
	})

}

// TestSimulateCluster_Distribution tests the load distribution based on
// a small number of nodes and a ton of requests.
func TestSimulateCluster_Distribution(t *testing.T) {
	rnd := rand.New(rand.NewSource(0))

	var (
		numNodes = 5
		numKeys  = 1_000_000
	)

	nodes, states := createTestCluster(t, rnd, numNodes)

	reqs := map[id.ID]int{}
	for _, n := range nodes {
		reqs[n.Node.ID] = 0
	}

	// Simulate a bunch of requests and keep track of how many nodes end
	// up responsible for how many requests.
	for i := 0; i < numKeys; i++ {
		key := id.ID{Low: uint64(rnd.Uint32())}

		// Pick a random node to start the request from.
		seed := nodes[rnd.Intn(len(nodes))]

		dest := fakeRoute(t, seed, key, states)
		reqs[dest.ID]++
	}

	idealDist := 100.0 / float64(numNodes)
	fmt.Printf("Ideal Distribution:  %0.2f%%\n\n", idealDist)
	for _, count := range reqs {
		dist := 100 * float64(count) / float64(numKeys)
		offset := dist - idealDist
		fmt.Printf("Actual Distribution: %-6.2f%% (offset %-6.2f%%)\n", 100*float64(count)/float64(numKeys), offset)
	}

	t.Run("Simulated VNodes", func(t *testing.T) {
		rnd := rand.New(rand.NewSource(0))

		var (
			numVNodes = 128
		)

		nodes, states := createTestCluster(t, rnd, numNodes*numVNodes)

		vnodes := map[id.ID]id.ID{}
		for i := 0; i < numNodes; i++ {
			vNodeID := nodes[numVNodes*i].Node.ID
			for j := 0; j < numVNodes; j++ {
				nodeID := nodes[numVNodes*i+j].Node.ID
				vnodes[nodeID] = vNodeID
			}
		}

		reqs := map[id.ID]int{}
		for _, n := range vnodes {
			reqs[n] = 0
		}

		// Simulate a bunch of requests and keep track of how many nodes end
		// up responsible for how many requests.
		for i := 0; i < numKeys; i++ {
			key := id.ID{Low: uint64(rnd.Uint32())}

			// Pick a random node to start the request from.
			seed := nodes[rnd.Intn(len(nodes))]

			dest := fakeRoute(t, seed, key, states)
			reqs[vnodes[dest.ID]]++
		}

		idealDist := 100.0 / float64(numNodes)
		fmt.Printf("Ideal Distribution:  %0.2f%%\n\n", idealDist)
		for _, count := range reqs {
			dist := 100 * float64(count) / float64(numKeys)
			offset := dist - idealDist
			fmt.Printf("Actual Distribution: %-6.2f%% (offset %-6.2f%%)\n", 100*float64(count)/float64(numKeys), offset)
		}
	})
}

func fakeRoute(t *testing.T, seed *State, key id.ID, states map[id.ID]*State) Descriptor {
	next := seed
	dest := seed.Node

	hops := []Descriptor{}

	for next != nil {
		cur := next

		for _, h := range hops {
			if h == cur.Node {
				require.Failf(t, "detected routing cycle", "cycle to %s", h.ID)
			}
		}
		hops = append(hops, cur.Node)

		nextDesc, ok := NextHop(cur, key)
		require.True(t, ok, "routing error")

		dest = nextDesc

		if nextDesc == cur.Node {
			next = nil
		} else {
			next = states[nextDesc.ID]
		}

	}

	return dest
}

func createTestCluster(t *testing.T, rnd *rand.Rand, size int) ([]*State, map[id.ID]*State) {
	t.Helper()

	states := make(map[id.ID]*State, size)
	nodes := make([]*State, 0, size)

	// Bootstrap the cluster, having each node join.
	for i := 0; i < size; i++ {
		var (
			nodeID id.ID
			exist  = true
		)
		for exist {
			nodeID = id.ID{Low: uint64(rnd.Uint32())}
			_, exist = states[nodeID]
		}

		s := NewState(Descriptor{ID: nodeID, Addr: "test"}, 8, 8, 32, 8)
		if len(nodes) == 0 {
			states[nodeID] = s
			nodes = append(nodes, s)
			continue
		}

		// Simulate a join using a seed node.
		hellos := []Hello{}
		seed := nodes[rnd.Intn(len(nodes))]

		next := seed
		for next != nil {
			cur := next

			nextDesc, ok := NextHop(cur, nodeID)
			require.True(t, ok, "detected routing failure")

			for _, prev := range hellos {
				if prev.Initiator == next.Node {
					require.Failf(t, "detected routing cycle", "cycle to %s (iteration %d)", next.Node.ID, i)
				}
			}

			next = states[nextDesc.ID]
			if next.Node == cur.Node {
				next = nil
			}
			hellos = append(hellos, Hello{State: cur})
		}

		// Calculate the new node's state and then share it with all the peers.
		s.Calculate(hellos)
		for _, p := range s.Peers(true) {
			states[p.ID].MixinState(s)
		}

		states[nodeID] = s
		nodes = append(nodes, s)
	}

	// Sort nodes so we can do fast searches.
	sort.Slice(nodes, func(i, j int) bool {
		return id.Compare(nodes[i].Node.ID, nodes[j].Node.ID) < 0
	})

	return nodes, states
}
