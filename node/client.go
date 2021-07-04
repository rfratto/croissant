package node

import (
	"context"
	"errors"

	"github.com/go-kit/kit/log/level"
	"github.com/rfratto/croissant/internal/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
)

var ErrSelfRouting = errors.New("route to self")

// ClientOption modifies a Client.
type ClientOption func(c *Client)

// WithAllowSelfRouting enables routing to self. Default is true.
func WithAllowSelfRouting(allow bool) ClientOption {
	return func(c *Client) {
		c.allowSelf = allow
	}
}

// WithForwardHook allows to hook into the forwarding functionality.
// Hooks may change the address of where data is sent or modify the
// message prior to sending.
func WithForwardHook(hook func(Peer) (Peer, error)) ClientOption {
	return func(c *Client) {
		c.forwardHook = hook
	}
}

// Client is a transparent gRPC client interface to the cluster.
// If a request context is missing a key from WithClientKey,
// requests will fail with InvalidArgument.
type Client struct {
	ctrl        *controller
	allowSelf   bool
	forwardHook func(Peer) (Peer, error)
}

// NewClient creates a new server Client using the node for routing.
// If allowSelfRouting is false, requests will fail with ErrSelfRouting
// if a node would connect to itself.
func NewClient(n *Node, opts ...ClientOption) *Client {
	c := &Client{
		ctrl:      n.controller,
		allowSelf: true,
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Invoke makes a request against the cluster, routing the request to the
// appropriate node. ctx must have a ClientKey set (via WithClientKey)
// or the request will fail.
func (c *Client) Invoke(ctx context.Context, method string, args interface{}, reply interface{}, opts ...grpc.CallOption) error {
	key, err := ExtractClientKey(ctx)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "missing or invalid routing key: %s", err.Error())
	}

Retry:
	next, ok := api.NextHop(c.ctrl.state, key)
	if !ok {
		return status.Errorf(codes.Internal, "routing error: unable to find any node for key %s", key)
	}

	if c.forwardHook != nil {
		p := Peer{ID: next.ID, Addr: next.Addr}
		p, err := c.forwardHook(p)
		if err != nil {
			return err
		}
		next = api.Descriptor{ID: p.ID, Addr: p.Addr}
	}

	if next == c.ctrl.state.Node && !c.allowSelf {
		return ErrSelfRouting
	}

	cc, err := c.ctrl.pool.Get(next.Addr)
	if err != nil {
		level.Info(c.ctrl.log).Log("msg", "failed to get conn to peer for routing", "peer", next.Addr, "err", err)
		_ = c.ctrl.health.SetHealth(next, api.Unhealthy)
		goto Retry
	}

	err = cc.Invoke(ctx, method, args, reply, opts...)
	if s := status.Convert(err); s != nil && s.Code() == codes.Unavailable && cc.GetState() == connectivity.TransientFailure {
		level.Info(c.ctrl.log).Log("msg", "failed to request forward to peer", "peer", next.Addr, "err", err)
		_ = c.ctrl.health.SetHealth(next, api.Unhealthy)
		goto Retry
	}

	return err
}

// NewStream makes a request against the cluster, routing the request to the
// appropriate node. ctx must have a ClientKey set (via WithClientKey)
// or the request will fail.
func (c *Client) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	key, err := ExtractClientKey(ctx)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "missing or invalid routing key: %s", err.Error())
	}

Retry:
	next, ok := api.NextHop(c.ctrl.state, key)
	if !ok {
		return nil, status.Errorf(codes.Internal, "routing error: unable to find any node for key %s", key)
	}

	if next == c.ctrl.state.Node && !c.allowSelf {
		return nil, ErrSelfRouting
	}

	cc, err := c.ctrl.pool.Get(next.Addr)
	if err != nil {
		level.Info(c.ctrl.log).Log("msg", "failed to get conn to peer for routing", "peer", next.Addr, "err", err)
		_ = c.ctrl.health.SetHealth(next, api.Unhealthy)
		goto Retry
	}

	cs, err := cc.NewStream(ctx, desc, method, opts...)
	if s := status.Convert(err); s != nil && s.Code() == codes.Unavailable && cc.GetState() == connectivity.TransientFailure {
		level.Info(c.ctrl.log).Log("msg", "failed to request forward to peer", "peer", next.Addr, "err", err)
		_ = c.ctrl.health.SetHealth(next, api.Unhealthy)
		goto Retry
	}

	return cs, err
}
