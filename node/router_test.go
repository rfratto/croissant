package node

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/rfratto/croissant/examples/kv/kvproto"
	"github.com/rfratto/croissant/examples/kv/kvserver"
	"github.com/rfratto/croissant/id"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TODO(rfratto): simulate a cluster with 1,000 nodes and make sure each
// request goes to the right one.

func TestClientRouting(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*3)
	defer cancel()

	l := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))

	seedSrv, seedNode := makeTestNode(t, log.With(l, "node", "seed"), func(s *grpc.Server) {
		kvproto.RegisterKVServer(s, echoKVServer(t, "seed"))
	})
	err := seedNode.Join(ctx, nil)
	require.NoError(t, err)
	_ = seedSrv

	_, peerNode := makeTestNode(t, log.With(l, "node", "peer"), func(s *grpc.Server) {
		kvproto.RegisterKVServer(s, echoKVServer(t, "peer"))
	})
	err = peerNode.Join(ctx, []string{seedNode.cfg.BroadcastAddr})
	require.NoError(t, err)

	// Dial into the seed as a client.
	clusterCC, err := grpc.Dial(seedNode.cfg.BroadcastAddr, grpc.WithInsecure())
	require.NoError(t, err)
	clusterClient := kvproto.NewKVClient(clusterCC)

	// Try routing to self
	resp, err := clusterClient.Get(
		WithClientKey(ctx, seedNode.cfg.ID),
		&kvproto.GetRequest{Key: "seed"},
	)
	require.NoError(t, err, "failed to route to seed")
	require.Equal(t, "seed", resp.GetValue(), "unexpected response from seed")

	// Try routing to peer
	resp, err = clusterClient.Get(
		WithClientKey(ctx, peerNode.cfg.ID),
		&kvproto.GetRequest{Key: "peer"},
	)
	require.NoError(t, err, "failed to route to peer")
	require.Equal(t, "peer", resp.GetValue(), "expected response from peer")
}

func makeTestNode(t *testing.T, l log.Logger, reg func(s *grpc.Server)) (*grpc.Server, *Node) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	var router Router
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(router.Unary()),
		grpc.ChainStreamInterceptor(router.Stream()),
	)
	if reg != nil {
		reg(srv)
	}

	n, err := New(Config{
		ID:            id.NewGenerator(32).Get(lis.Addr().String()),
		BroadcastAddr: lis.Addr().String(),
		NumLeaves:     8,
		NumNeighbors:  8,
		Log:           l,
	}, noopApplication{}, grpc.WithInsecure())
	n.Register(srv)
	require.NoError(t, err)

	router.SetNode(n)

	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	time.Sleep(50 * time.Millisecond)
	return srv, n
}

func echoKVServer(t *testing.T, expect string) *kvserver.Func {
	var kvFunc kvserver.Func
	kvFunc.GetFunc = func(c context.Context, gr *kvproto.GetRequest) (*kvproto.GetResponse, error) {
		if gr.Key != expect {
			return nil, status.Errorf(codes.InvalidArgument, "unexpected key")
		}
		return &kvproto.GetResponse{Value: expect}, nil
	}
	return &kvFunc
}

type noopApplication struct{}

func (noopApplication) PeersChanged(ps []Peer) {}
