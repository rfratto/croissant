package node

import (
	"context"
	"errors"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/rfratto/croissant/internal/api"
	"github.com/rfratto/croissant/internal/nodepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errSelfJoin = errors.New("can't join self")

// Bootstrap joins the cluster using seed node seed. Only one Bootstrap
// call may be running concurrently.
func (c *controller) Bootstrap(ctx context.Context, seed string) error {
	c.joinMtx.Lock()
	defer c.joinMtx.Unlock()

	c.joining.Store(true)
	defer c.joining.Store(false)

	cc, err := c.pool.Get(seed)
	if err != nil {
		return err
	}
	cli := nodepb.ToAPI(nodepb.NewNodeClient(cc))

	// Get the nodes state first so we know what advertise address it's using.
	s, err := cli.GetState(ctx)
	if err != nil {
		return err
	}
	if s.Node == c.state.Node {
		// Don't allow a join to try to join itself. This may happen
		// if the seed was found using some kind of discovery process and
		// a node happened to find its own registration.
		return errSelfJoin
	}

	c.helloMut.Lock()
	c.nextHello = s.Node.Addr
	c.helloMut.Unlock()

	// Now send it a join.
	level.Info(c.log).Log("msg", "sending join to node", "addr", seed)
	err = cli.Join(ctx, c.state.Clone().Node)
	if err != nil {
		return err
	}

	// Wait for NodeHello to receive the finally hello in the chain, starting
	// from seed.
	select {
	case err := <-c.joinRes:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *controller) Join(ctx context.Context, joiner api.Descriptor) error {
	if joiner.Addr == "" {
		return status.Errorf(codes.InvalidArgument, "no cluster address received")
	} else if joiner == c.state.Node {
		return status.Errorf(codes.InvalidArgument, "node can't join itself")
	}

	level.Info(c.log).Log("msg", "received join request", "peer", joiner.Addr, "id", joiner.ID.String())

Retry:
	state := c.state.Clone()

	next, ok := api.NextHop(state, joiner.ID)
	if !ok {
		level.Error(c.log).Log("msg", "routing error: could not find next node for join request")
		return status.Errorf(codes.Internal, "routing error: can not find next node for join request")
	}

	// A ID can only be re-used if the address is also the same, which may
	// happen if a node attempts to re-join a cluster.
	if next.ID == joiner.ID && next.Addr != joiner.Addr {
		return status.Errorf(codes.InvalidArgument, "ID already in use")
	}

	hello := api.Hello{Initiator: state.Node, State: state}
	if next != state.Node && next != joiner {
		hello.Next = &next
	}

	cc, err := c.pool.Get(joiner.Addr)
	if err != nil {
		level.Warn(c.log).Log("msg", "failed to say hello to joining peer", "peer", joiner.Addr, "err", err)
		return err
	}

	cli := nodepb.ToAPI(nodepb.NewNodeClient(cc))

	helloCtx := nodepb.WithCallOptions(ctx, grpc.WaitForReady(true))
	err = cli.NodeHello(helloCtx, hello)
	if err != nil {
		level.Warn(c.log).Log("msg", "failed to say hello to joining peer", "peer", joiner.Addr, "err", err)
		return err
	}

	// If there's no more nodes to propagate to (or if joiner is re-joining the
	// cluster), we can stop.
	if next == state.Node || next == joiner {
		return nil
	}

	level.Info(c.log).Log("msg", "propagating join", "peer", joiner.Addr, "next", next.Addr)

	cc, err = c.pool.Get(next.Addr)
	if err != nil {
		if err := c.health.SetHealth(next, api.Unhealthy); err != nil {
			// This can happen if we already removed the node, but log the warning
			// anyway.
			level.Warn(c.log).Log("msg", "could not mark node unhealthy", "err", err)
		}
		goto Retry
	}

	cli = nodepb.ToAPI(nodepb.NewNodeClient(cc))
	err = cli.Join(ctx, joiner)
	if s := status.Convert(err); s != nil && s.Code() == codes.Unavailable {
		// If the call failed because the node was unavailble, taint it and try again.
		if err := c.health.SetHealth(next, api.Unhealthy); err != nil {
			// This can happen if we already removed the node, but log the warning
			// anyway.
			level.Warn(c.log).Log("msg", "could not mark node unhealthy", "err", err)
		}
		goto Retry
	}

	// Other errors may be valid, ie key already in use, so don't taint the
	// node if something unexpected happens.
	return err
}

func (c *controller) NodeHello(ctx context.Context, h api.Hello) error {
	// TODO(rfratto): In the future, allow for lazily adding jobs to the health
	// checker. This will allow us to only maintain a health check against a
	// neighbors and only spin up extra health checks when routing happens to
	// fail.
	defer func() {
		c.health.CheckNodes(c.state.Peers(true))
	}()

	// Don't even consider the hello at all if their state was based off of an
	// outdated version of ours.
	if !h.StateAck.IsZero() && c.state.IsNewer(h.StateAck) {
		level.Debug(c.log).Log("msg", "outdated ack", "received", h.StateAck, "expected", c.state.LastUpdated)
		return api.ErrStateChanged{NewState: c.state.Clone()}
	}

	level.Info(c.log).Log("msg", "got hello from peer", "peer", h.Initiator.Addr, "peer_id", h.Initiator.ID)

	if c.joining.Load() {
		return c.handleJoiningHello(ctx, h)
	}

	// We're not joining, just mixin the state.
	_, _, newLeaves := c.state.MixinState(h.State)
	if newLeaves {
		c.app.PeersChanged(getPeers(c.state))
	}
	return nil
}

// handleJoiningHello is called from NodeHello when controller is joining the
// cluster, since receiving a Hello while joining is handled separately.
func (c *controller) handleJoiningHello(ctx context.Context, h api.Hello) error {
	c.helloMut.Lock()
	defer c.helloMut.Unlock()

	// Hellos from unexpected nodes should be ignored. However, it's possible
	// that the previous node re-sent its hello after failing to propagate to
	// Next.
	if h.Initiator.Addr != c.nextHello {
		var (
			prev     *api.Hello
			prevAddr string
		)
		if len(c.hellos) > 0 {
			prev = &c.hellos[len(c.hellos)-1]
			prevAddr = prev.Initiator.Addr
		}

		if prev != nil && prev.Initiator == h.Initiator {
			c.hellos[len(c.hellos)-1] = h
		} else {
			level.Info(c.log).Log("msg", "ignoring unexpected hello during join", "expect", c.nextHello, "prev", prevAddr)
			return nil
		}
	} else {
		c.hellos = append(c.hellos, h)
	}

	if h.Next != nil {
		// NOTE(rfratto): It's possible for a malicious node to build a cyclical
		// hello and prevent us from ever joining. For now, we're assuming a trusted
		// environment.
		c.nextHello = h.Next.Addr
		return nil
	}

	level.Info(c.log).Log("msg", "completing cluster join")

	var joinErr error
Join:
	// Initialize our state based on all the Hellos.
	c.state.Calculate(c.hellos)

	// Tell every peer about our state.
	sendState := c.state.Clone()
	for _, p := range c.state.Peers(false) {
		// Check to see if we have state from this node. This allows us to
		// inform it that its state has changed.
		var (
			ackID    time.Time
			helloIdx = -1
		)
		for i, h := range c.hellos {
			if h.Initiator == p {
				ackID = h.State.LastUpdated
				helloIdx = i
				break
			}
		}

		level.Info(c.log).Log("msg", "sending join state to peer", "peer", p.Addr)

		cc, err := c.pool.Get(p.Addr)
		if err != nil {
			level.Error(c.log).Log("msg", "failed to inform peer of join", "peer", p.Addr, "err", err)
			joinErr = status.Errorf(codes.Aborted, "aboring join because communication with peer %s failed: %s", p.Addr, err)
			break
		}

		cli := nodepb.ToAPI(nodepb.NewNodeClient(cc))
		err = cli.NodeHello(ctx, api.Hello{
			Initiator: sendState.Node,
			State:     sendState,
			StateAck:  ackID,
		})
		if scErr := (api.ErrStateChanged{}); errors.As(err, &scErr) && helloIdx >= 0 {
			level.Info(c.log).Log("msg", "peer state changed since join, restarting join", "peer", p.Addr)
			// Store the updated hello and restart from the top. If a bunch of nodes
			// have started at once, we may have to do this a few times.
			c.hellos[helloIdx].State = scErr.NewState
			goto Join
		}

		// This can be relaxed in the future, but currently an error while finishing the
		// join is fatal.
		if err != nil {
			level.Error(c.log).Log("msg", "failed to inform peer of join", "peer", p.Addr, "err", err)
			joinErr = status.Errorf(codes.Aborted, "aboring join because communication with peer %s failed: %s", p.Addr, err)
			break
		}
	}

	c.joining.Store(false)
	c.joinRes <- joinErr
	return nil
}
