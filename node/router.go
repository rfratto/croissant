package node

import (
	"context"
	"errors"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// Router supplies a set of gRPC server interceptors that can route requests
// through the cluster. A Node must be set with SetNode for the Router to work.
// set with SetNode
type Router struct {
	mut  sync.Mutex
	node *Node
}

// Unary returns a grpc.UnaryServerInterceptor.
func (r *Router) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		r.mut.Lock()
		node := r.node
		r.mut.Unlock()

		if node == nil {
			return nil, status.Errorf(codes.Unavailable, "not connected to cluster")
		}

		return r.node.controller.ForwardUnary(ctx, req, info, handler)
	}
}

// Stream returns grpc.StreamServerInterceptor.
func (r *Router) Stream() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		r.mut.Lock()
		node := r.node
		r.mut.Unlock()

		if node == nil {
			return status.Errorf(codes.Unavailable, "not connected to cluster")
		}

		return r.node.controller.ForwardStream(srv, ss, info, handler)
	}
}

// SetNode sets the node to be used for routing requests.
func (r *Router) SetNode(n *Node) {
	r.mut.Lock()
	defer r.mut.Unlock()
	r.node = n
}

// ForwardUnary implements grpc.UnaryServerInterceptor and will propagate
// a request or call handler if it is owned by the local node. Node errors
// are resolved immediately and requests will be re-tried until there is a
// node that can handle it.
func (c *controller) ForwardUnary(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	_, err = ExtractClientKey(ctx)
	if errors.Is(err, ErrNoKey) {
		return handler(ctx, req)
	} else if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid key: %s", err)
	}

	cc := &Client{ctrl: c, allowSelf: false}

	var m anypb.Any
	err = cc.Invoke(ctx, info.FullMethod, req, &m)
	if errors.Is(err, ErrSelfRouting) {
		return handler(ctx, req)
	}
	return &m, err
}

// ForwardStream implements grpc.StreamServerInterceptor and will propagate
// a request or call handler if it is owned by the local node. Node errors
// are resolved immediately and requests will be re-tried until there is a
// node that can handle it.
func (c *controller) ForwardStream(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	_, err := ExtractClientKey(ss.Context())
	if errors.Is(err, ErrNoKey) {
		return handler(srv, ss)
	} else if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid key: %s", err)
	}

	// TODO(rfratto): implement
	return status.Errorf(codes.Unimplemented, "unable to route stream requests")
}
