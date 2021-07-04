package health

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/rfratto/croissant/id"
	"github.com/rfratto/croissant/internal/api"
	"github.com/rfratto/croissant/internal/connpool"
	"github.com/rfratto/croissant/internal/nodepb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestChecker(t *testing.T) {
	lis, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)

	srv := grpc.NewServer()
	defer srv.Stop()

	checkedCh := make(chan struct{})
	svc := &fakeService{
		OnGetState: func(ctx context.Context, req *nodepb.GetStateRequest) (*nodepb.GetStateResponse, error) {
			checkedCh <- struct{}{}
			return &nodepb.GetStateResponse{}, nil
		},
	}
	nodepb.RegisterNodeServer(srv, svc)

	go func() {
		err := srv.Serve(lis)
		require.NoError(t, err)
	}()

	d := api.Descriptor{
		ID:   id.Zero,
		Addr: lis.Addr().String(),
	}

	checker := NewChecker(Config{
		CheckFrequency: time.Second,
		CheckTimeout:   time.Second,
		MaxFailures:    0,
	}, connpool.New(100, grpc.WithInsecure()), &fakeWatcher{})

	err = checker.CheckNodes([]api.Descriptor{d})
	require.NoError(t, err)

	// Wait for our server to be checked
	select {
	case <-checkedCh:
	case <-time.After(5 * time.Second):
		require.Fail(t, "expected check to be run")
	}

	checker.CheckNodes([]api.Descriptor{})

	// Ensure we're not checked again
	select {
	case <-checkedCh:
		require.Fail(t, "expected check to not run again")
	case <-time.After(2 * time.Second):
	}
}
