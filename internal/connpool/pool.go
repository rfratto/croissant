// Package connpool implements a gRPC connection pool.
package connpool

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"
)

// Pool implements a connection Pool to nodes in the cluster. All
// connections share the same set of DialOptions.
//
// The Pool has a maximum number of connections, and the oldest
// unused connections will be closed and removed when opening a
// new one. Dead nodes will be automatically removed from the
// Pool.
type Pool struct {
	mut sync.RWMutex

	opts []grpc.DialOption

	maxConns   int
	conns      map[string]*poolConn
	connLookup map[*grpc.ClientConn]*poolConn
}

type poolConn struct {
	Conn     *grpc.ClientConn
	LastUsed time.Time
}

// New creates a new connection pool.
func New(maxConns int, opts ...grpc.DialOption) *Pool {
	p := &Pool{
		conns:      make(map[string]*poolConn, maxConns),
		connLookup: make(map[*grpc.ClientConn]*poolConn, maxConns),
		maxConns:   maxConns,
	}

	fullOpts := []grpc.DialOption{
		grpc.WithChainStreamInterceptor(p.streamRefreshConn),
		grpc.WithChainUnaryInterceptor(p.unaryRefreshConn),
	}
	fullOpts = append(fullOpts, opts...)

	p.opts = fullOpts
	return p
}

func (p *Pool) streamRefreshConn(
	ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn,
	method string, streamer grpc.Streamer, opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	p.mut.Lock()
	if pc, ok := p.connLookup[cc]; ok {
		pc.LastUsed = time.Now()
	}
	p.mut.Unlock()

	return streamer(ctx, desc, cc, method, opts...)
}

// refreshConn is invoked as a UnaryClientInterceptor that will refresh the
// last used time of the underlying connection.
func (p *Pool) unaryRefreshConn(
	ctx context.Context, method string, req, reply interface{},
	cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
) error {
	p.mut.Lock()
	if pc, ok := p.connLookup[cc]; ok {
		pc.LastUsed = time.Now()
	}
	p.mut.Unlock()

	return invoker(ctx, method, req, reply, cc, opts...)
}

// Get retrieves a cached addr or creates a new connection.
func (p *Pool) Get(addr string) (*grpc.ClientConn, error) {
	p.mut.Lock()
	defer p.mut.Unlock()

	if c, ok := p.conns[addr]; ok && c != nil {
		c.LastUsed = time.Now()
		return c.Conn, nil
	}

	conn, err := grpc.Dial(addr, p.opts...)
	if err != nil {
		return nil, err
	}
	p.conns[addr] = &poolConn{
		Conn:     conn,
		LastUsed: time.Now(),
	}
	p.connLookup[conn] = p.conns[addr]

	if len(p.conns) > p.maxConns {
		p.cleanupOldest()
	}

	return conn, err
}

// cleanupOldest should only be called when the mutex is held.
func (p *Pool) cleanupOldest() {
	var (
		oldest     = time.Now().Add(time.Hour * 24 * 365)
		oldestAddr *string
	)
	for addr, conn := range p.conns {
		if conn.LastUsed.Before(oldest) {
			oldest = conn.LastUsed
			oldestAddr = &addr
		}
	}
	if oldestAddr != nil {
		_ = p.conns[*oldestAddr].Conn.Close()
		delete(p.connLookup, p.conns[*oldestAddr].Conn)
		delete(p.conns, *oldestAddr)
	}
}

// Remove deletes a conn from the pool.
func (p *Pool) Remove(addr string) {
	if c, ok := p.conns[addr]; ok {
		// TODO(rfratto): will this let the existing conn be re-used?
		_ = c.Conn.Close()
		delete(p.connLookup, c.Conn)
		delete(p.conns, addr)
	}
}
