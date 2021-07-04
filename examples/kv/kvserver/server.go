package kvserver

import (
	"context"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/rfratto/croissant/examples/kv/kvproto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements an example KV store. Don't use this for production.
type Server struct {
	kvproto.UnimplementedKVServer

	mut  sync.Mutex
	data map[string]string

	l log.Logger
}

func New(l log.Logger) *Server {
	if l == nil {
		l = log.NewNopLogger()
	}
	return &Server{
		data: make(map[string]string),
		l:    l,
	}
}

func (s *Server) Get(ctx context.Context, req *kvproto.GetRequest) (*kvproto.GetResponse, error) {
	s.mut.Lock()
	defer s.mut.Unlock()

	level.Info(s.l).Log("msg", "getting value for key", "key", req.Key)

	val, ok := s.data[req.GetKey()]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "key %s not found", req.GetKey())
	}
	return &kvproto.GetResponse{Value: val}, nil
}

func (s *Server) Set(ctx context.Context, req *kvproto.SetRequest) (*kvproto.SetResponse, error) {
	s.mut.Lock()
	defer s.mut.Unlock()

	level.Info(s.l).Log("msg", "setting key", "key", req.Key, "value", req.Value)

	s.data[req.GetKey()] = req.GetValue()
	return &kvproto.SetResponse{}, nil
}

// Func is a function-based server.
type Func struct {
	kvproto.UnimplementedKVServer

	GetFunc func(context.Context, *kvproto.GetRequest) (*kvproto.GetResponse, error)
	SetFunc func(context.Context, *kvproto.SetRequest) (*kvproto.SetResponse, error)
}

func (f *Func) Get(ctx context.Context, req *kvproto.GetRequest) (*kvproto.GetResponse, error) {
	if f.GetFunc == nil {
		return f.UnimplementedKVServer.Get(ctx, req)
	}
	return f.GetFunc(ctx, req)
}

func (f *Func) Set(ctx context.Context, req *kvproto.SetRequest) (*kvproto.SetResponse, error) {
	if f.SetFunc == nil {
		return f.UnimplementedKVServer.Set(ctx, req)
	}
	return f.SetFunc(ctx, req)
}
