package node

import (
	"context"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/rfratto/croissant/internal/api"
	"github.com/rfratto/croissant/internal/connpool"
	"github.com/rfratto/croissant/internal/nodepb"
)

func (c *controller) NodeGoodbye(ctx context.Context, leaver api.Descriptor) error {
	level.Info(c.log).Log("msg", "informed of node leaving, treating as dead", "node", leaver.Addr)
	if err := c.health.SetHealth(leaver, api.Dead); err != nil {
		level.Warn(c.log).Log("msg", "leaving node is not in set of peers", "node", leaver.Addr)
	}
	return nil
}

func (c *controller) HealthChanged(d api.Descriptor, h api.Health) {
	// Allow a minute for state recovery.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	level.Info(c.log).Log("msg", "changing health of peer", "peer", d.Addr, "health", h)
	c.state.SetHealth(d, h)

	if h != api.Dead {
		// Unless the node dies, there's nothing else to do here; Healthy restores
		// connectivity to a node and Unhealthy just stops routing.
		return
	}

	// Stop tracking the health of the node after we're done replacing it.
	// If we don't do this, dead nodes will leak in the state table.
	defer c.state.Untrack(d)
	defer c.pool.Remove(d.Addr)

	// Save the state so we can freely perform recovery without worrying
	// about race conditions.
	saved := c.state.Clone()

	// Determine what roles the dead node fills.
	var (
		isLeaf     bool = saved.IsLeaf(d)
		isNeighbor bool = saved.Neighbors.Contains(d)
	)

	// Determine if it's in the routing table
	var (
		routingRow, routingCol = saved.RouteIndex(d)
	)
	if ent := saved.Routing[routingRow][routingCol]; ent == nil || *ent != d {
		routingRow = -1
		routingCol = -1
	}

	if isLeaf {
		leaves := saved.Leaves(false)

		// Predecessors should be replaced by contacting the
		// smallest live predecessor, since it's the most likely to have a
		// replacement. If it's a successor, we need to do the opposite,
		// so invert the array.
		if saved.Successors.Contains(d) {
			leaves = api.ReverseDescriptors(leaves)
		}

		for _, l := range leaves {
			state, err := getPeerState(ctx, c.pool, l.Addr)
			if err != nil {
				level.Warn(c.log).Log("msg", "could not get state from peer candidate", "err", err)
				c.health.SetHealth(l, api.Unhealthy)
				continue
			}

			c.state.ReplaceLeaf(d, state)
			break
		}

		// Forcibly remove the entry in case there weren't any candiates to check from.
		c.state.ReplaceLeaf(d, nil)
	}

	// When a routing node fails, get the state from live nodes from the same
	// row. See if they have a replacement entry. If none of them do, ask other
	// nodes in lower rows if they have an entry for the row instead.
	if routingCol >= 0 && routingRow >= 0 {
		saved.Routing[routingRow][routingCol] = nil

	RoutingFix:
		for row := routingCol; row < len(saved.Routing); row++ {
			for col := 0; col < len(saved.Routing[row]); col++ {
				ent := saved.Routing[row][col]
				if ent == nil || saved.Statuses[*ent] != api.Healthy {
					continue
				}

				state, err := getPeerState(ctx, c.pool, ent.Addr)
				if err != nil {
					level.Warn(c.log).Log("msg", "could not get state from healthy routing row", "err", err)
					c.health.SetHealth(*ent, api.Unhealthy)
					continue
				}

				replaced, ok := c.state.ReplaceRoute(d, state)
				if replaced || !ok {
					break RoutingFix
				}
			}
		}

		// Forcibly remove the entry in case there weren't any candiates to check from.
		c.state.ReplaceRoute(d, nil)
	}

	// Neighbors should be replaced by contacting every other neighbor
	// and finding a replacement neighbor.
	if isNeighbor {
		for _, n := range saved.Neighbors.Descriptors {
			if saved.Statuses[n] != api.Healthy {
				continue
			}

			peerState, err := getPeerState(ctx, c.pool, n.Addr)
			if err != nil {
				level.Warn(c.log).Log("msg", "could not get state from peer candidate", "err", err)
				continue
			}

			changed, ok := c.state.ReplaceNeighbor(d, peerState)
			if !ok || changed {
				break
			}
		}

		// Forcibly remove the entry in case there weren't any candiates to check from.
		c.state.ReplaceNeighbor(d, nil)
	}

	// After updating the state, refresh health checker jobs.
	if isLeaf {
		c.app.PeersChanged(getPeers(c.state))
	}
	c.health.CheckNodes(c.state.Peers(true))
}

func getPeerState(ctx context.Context, p *connpool.Pool, addr string) (*api.State, error) {
	cc, err := p.Get(addr)
	if err != nil {
		return nil, err
	}
	return nodepb.ToAPI(nodepb.NewNodeClient(cc)).GetState(ctx)
}
