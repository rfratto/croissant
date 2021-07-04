package node

// TODO(rfratto): Known issues:
//
// 1. The hash ring isn't really a "ring" and doesn't loop around. While
//    messages will still always find their way to the correct node, it will
//    take a few extra hops, and replication can't be implemented until the
//    ring is actually a ring.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rfratto/croissant/id"
	"github.com/rfratto/croissant/internal/api"
	"github.com/rfratto/croissant/internal/connpool"
	"github.com/rfratto/croissant/internal/health"
	"github.com/rfratto/croissant/internal/nodepb"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
)

// Config controls how a node is initialized.
type Config struct {
	// ID represents the server. Must be specified.
	ID id.ID

	// BroadcastAddr is the address to share with peers when joining.
	// Must bet set.
	BroadcastAddr string

	// Number of leaves to track. Will be divisible by 2. Defaults to 8 if
	// unset.
	NumLeaves int
	// Number of neighbors to track for locality. Defaults to 8 if unset.
	NumNeighbors int

	// Log will be used for logging messages.
	Log log.Logger
}

// Node is a node within a Croissant cluster.
type Node struct {
	cfg Config

	controller *controller
}

// New creates a new Node and registers it against the given gRPC server. The
// provided set of DialOptions are used when communicating with a cluster peer.
func New(cfg Config, app Application, dial ...grpc.DialOption) (*Node, error) {
	if cfg.Log == nil {
		cfg.Log = log.NewNopLogger()
	}
	if cfg.ID == id.Zero {
		return nil, fmt.Errorf("ID must be set")
	}
	if cfg.BroadcastAddr == "" {
		return nil, fmt.Errorf("BroadcastAddr must be set")
	}

	if cfg.NumLeaves == 0 {
		cfg.NumLeaves = 8
	}
	if cfg.NumNeighbors == 0 {
		cfg.NumNeighbors = 8
	}
	if cfg.NumLeaves%2 != 0 {
		return nil, fmt.Errorf("leaves must be divisible by 2")
	}

	desc := api.Descriptor{
		ID:   cfg.ID,
		Addr: cfg.BroadcastAddr,
	}
	state := api.NewState(
		desc,
		cfg.NumLeaves,
		cfg.NumNeighbors,
		32,
		16,
	)

	return &Node{
		cfg:        cfg,
		controller: newController(cfg.Log, state, app, dial),
	}, nil
}

// Register registers the cluster API to gRPC. Must be called before Join,
// otherwise other nodes will be unable to connect to this node.
func (n *Node) Register(s grpc.ServiceRegistrar) {
	nodepb.RegisterNodeServer(s, nodepb.FromAPI(n.controller))
}

// Join joins the cluster. Calling this more than once will attempt to re-join
// the cluster.
func (n *Node) Join(ctx context.Context, addrs []string) error {
	var failed bool

	for _, seed := range addrs {
		err := n.controller.Bootstrap(ctx, seed)
		if err == nil {
			return nil
		}
		// Silently ignore trying to join ourselves. If there are other
		// candidates, they'll be tried, otherwise we'll bootstrap a
		// single-node cluster.
		if errors.Is(err, errSelfJoin) {
			continue
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		level.Warn(n.cfg.Log).Log("msg", "failed to join node", "addr", seed, "err", err)
		failed = true
	}

	if failed {
		return fmt.Errorf("failed to join every peer from join addrs")
	}

	// There were no nodes to join; start up as a single-node cluster.
	return nil
}

// NextPeer returns the next peer in the routing chain for a given key.
// self will be true if next is the node itself.
//
// This allows applications to implement special routing methods; e.g.,
// batch routing.
func (n *Node) NextPeer(key id.ID) (next Peer, self bool, err error) {
	return n.controller.NextPeer(key)
}

// Close leaves the cluster.
func (n *Node) Close() error {
	return n.controller.Close()
}

// controller implements health.Watcher and api.Node.
type controller struct {
	log log.Logger

	health *health.Checker
	pool   *connpool.Pool
	app    Application

	// Used to stop run loop by Close.
	quit chan struct{}

	joinMtx sync.Mutex   // Only allow one concurrent join.
	joinRes chan error   // Channel for receiving result of join.
	joining *atomic.Bool // Flag indicating joining.

	helloMut  sync.Mutex  // Protect hellos/nextHello from being changed concurrently.
	hellos    []api.Hello // Hello messages when joining.
	nextHello string      // Next expected hello.

	state *api.State
}

func newController(log log.Logger, state *api.State, app Application, dial []grpc.DialOption) *controller {
	// TODO(rfratto): change 250 to total # peers * 1/2
	pool := connpool.New(250, dial...)

	ctrl := &controller{
		log:  log,
		pool: pool,
		app:  app,

		quit: make(chan struct{}),

		joinRes: make(chan error, 1),
		joining: atomic.NewBool(false),

		state: state,
	}

	ctrl.health = health.NewChecker(health.Config{
		CheckFrequency: 5 * time.Second,
		CheckTimeout:   250 * time.Millisecond,
		MaxFailures:    3,
		Log:            log,
		Registerer:     prometheus.NewRegistry(),
	}, pool, ctrl)

	return ctrl
}

func (c *controller) run() {
	helloTicker := time.NewTicker(time.Minute)

	for {
		select {
		case <-c.quit:
			return
		case <-helloTicker.C:
			c.greetLeaves()
		}
	}
}

func (c *controller) greetLeaves() {
	level.Info(c.log).Log("msg", "pinging all leaves")
	defer level.Info(c.log).Log("msg", "done pinging leaves")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	state := c.state.Clone()

	for _, l := range c.state.Leaves(false) {
		cc, err := c.pool.Get(l.Addr)
		if err != nil {
			level.Warn(c.log).Log("msg", "pinging leaf failed", "leaf", l.Addr, "err", err)
			if err := c.health.SetHealth(l, api.Unhealthy); err != nil {
				level.Warn(c.log).Log("msg", "could not update health of leaf", "leaf", l.Addr, "err", err)
			}
			continue
		}

		cli := nodepb.ToAPI(nodepb.NewNodeClient(cc))
		err = cli.NodeHello(ctx, api.Hello{
			Initiator: state.Node,
			State:     state,
		})
		if err != nil {
			level.Warn(c.log).Log("msg", "pinging leaf failed", "leaf", l.Addr, "err", err)
			if err := c.health.SetHealth(l, api.Unhealthy); err != nil {
				level.Warn(c.log).Log("msg", "could not update health of leaf", "leaf", l.Addr, "err", err)
			}
		}
	}
}

func (c *controller) GetState(ctx context.Context) (*api.State, error) {
	return c.state.Clone(), nil
}

func (c *controller) NextPeer(key id.ID) (next Peer, self bool, err error) {
	hop, ok := api.NextHop(c.state, key)
	if !ok {
		err = fmt.Errorf("routing failure: unable to find any node able to accept key %s. THIS IS A BUG!", key.String())
		return
	}

	self = (hop == c.state.Node)
	next = Peer{ID: hop.ID, Addr: hop.Addr}
	return
}

func (c *controller) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	var firstErr error

	firstErr = c.health.Close()

	// Tell all healthy peers about us leaving.
	for _, p := range c.state.Peers(false) {
		cc, err := c.pool.Get(p.Addr)
		if err != nil {
			level.Warn(c.log).Log("msg", "failed to inform peer of leaving", "peer", p.Addr, "err", err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}

		cli := nodepb.ToAPI(nodepb.NewNodeClient(cc))
		err = cli.NodeGoodbye(ctx, c.state.Node)
		if err != nil {
			level.Warn(c.log).Log("msg", "failed to inform peer of leaving", "peer", p.Addr, "err", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	close(c.quit)
	return firstErr
}
