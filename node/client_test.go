package node

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/rfratto/croissant/examples/kv/kvproto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestClient(t *testing.T) {
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

	// Create a cluster client that uses the node.
	clusterClient := kvproto.NewKVClient(NewClient(seedNode))

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
