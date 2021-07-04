package health

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/rfratto/croissant/id"
	"github.com/rfratto/croissant/internal/api"
	"github.com/rfratto/croissant/internal/connpool"
	"github.com/rfratto/croissant/internal/nodepb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestJob_Pass(t *testing.T) {
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

	doneCh := make(chan struct{})
	defer func() { <-doneCh }()

	j := newJob(jobConfig{
		Pool:    connpool.New(5, grpc.WithInsecure()),
		Node:    d,
		Log:     log.NewNopLogger(),
		Metrics: newMetrics(nil),
		CheckConfig: Config{
			CheckFrequency: time.Second,
			CheckTimeout:   time.Second,
			MaxFailures:    0,
		},
		Watcher: &fakeWatcher{
			OnHealthChanged: func(d api.Descriptor, h api.Health) {},
		},
		OnDone: func() { close(doneCh) },
	})
	defer j.Stop()

	select {
	case <-checkedCh:
		// Pass
	case <-time.After(5 * time.Second):
		require.Fail(t, "expected check to be called within 5 seconds")
	}
}

func TestJob_Timeout(t *testing.T) {
	lis, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)

	srv := grpc.NewServer()
	defer srv.Stop()

	svc := &fakeService{
		OnGetState: func(ctx context.Context, req *nodepb.GetStateRequest) (*nodepb.GetStateResponse, error) {
			time.Sleep(5 * time.Second)
			return &nodepb.GetStateResponse{}, nil
		},
	}
	nodepb.RegisterNodeServer(srv, svc)

	go func() {
		err := srv.Serve(lis)
		require.NoError(t, err)
	}()

	healthCh := make(chan api.Health)
	w := &fakeWatcher{
		OnHealthChanged: func(d api.Descriptor, h api.Health) {
			healthCh <- h
		},
	}

	d := api.Descriptor{
		ID:   id.Zero,
		Addr: lis.Addr().String(),
	}
	newJob(jobConfig{
		Pool:    connpool.New(5, grpc.WithInsecure()),
		Node:    d,
		Log:     log.NewNopLogger(),
		Metrics: newMetrics(nil),
		CheckConfig: Config{
			CheckFrequency: time.Second,
			CheckTimeout:   time.Second,
			MaxFailures:    1,
		},
		Watcher: w,
		OnDone:  func() {},
	})

	select {
	case h := <-healthCh:
		require.Equal(t, api.Unhealthy, h)
	case <-time.After(5 * time.Second):
		require.Fail(t, "expected health to have changed within 5 seconds")
	}
}

func TestJob_Fail(t *testing.T) {
	d := api.Descriptor{
		ID:   id.Zero,
		Addr: "google.com:80",
	}

	healthCh := make(chan api.Health)

	j := newJob(jobConfig{
		Pool:    connpool.New(5, grpc.WithInsecure()),
		Node:    d,
		Log:     log.NewNopLogger(),
		Metrics: newMetrics(nil),
		CheckConfig: Config{
			CheckFrequency: time.Second,
			CheckTimeout:   time.Second,
			MaxFailures:    1,
		},
		Watcher: &fakeWatcher{
			OnHealthChanged: func(d api.Descriptor, h api.Health) {
				healthCh <- h
			},
		},
		OnDone: func() {},
	})
	defer j.Stop()

	select {
	case h := <-healthCh:
		require.Equal(t, api.Unhealthy, h)
	case <-time.After(5 * time.Second):
		require.Fail(t, "expected health to have changed within 5 seconds")
	}
}

func TestJob_Transitions(t *testing.T) {
	health := api.Healthy
	watcher := &fakeWatcher{
		OnHealthChanged: func(d api.Descriptor, h api.Health) {
			health = h
		},
	}

	j := &job{
		cfg: jobConfig{
			Pool:    connpool.New(5, grpc.WithInsecure()),
			Node:    api.Descriptor{Addr: "localhost:12345"},
			Log:     log.NewNopLogger(),
			Metrics: newMetrics(nil),
			CheckConfig: Config{
				CheckFrequency: time.Second,
				CheckTimeout:   time.Second,
				MaxFailures:    4,
			},
			Watcher: watcher,
			OnDone:  func() {},
		},
	}

	tt := []struct {
		success bool
		health  api.Health
	}{
		{true, api.Healthy},
		{false, api.Unhealthy}, // 1
		{false, api.Unhealthy}, // 2
		{false, api.Unhealthy}, // 3
		{false, api.Unhealthy}, // 4
		{false, api.Dead},
		{false, api.Dead},
		{true, api.Healthy},
		// Ensure failure count resets
		{false, api.Unhealthy},
	}

	for _, tc := range tt {
		j.processCheckResult(tc.success)
		time.Sleep(100 * time.Millisecond)
		require.Equal(t, tc.health, health)
	}
}

type fakeService struct {
	nodepb.UnimplementedNodeServer
	OnGetState func(ctx context.Context, req *nodepb.GetStateRequest) (*nodepb.GetStateResponse, error)
}

func (f *fakeService) GetState(ctx context.Context, req *nodepb.GetStateRequest) (*nodepb.GetStateResponse, error) {
	return f.OnGetState(ctx, req)
}

type fakeWatcher struct {
	OnHealthChanged func(d api.Descriptor, h api.Health)
}

func (f *fakeWatcher) HealthChanged(d api.Descriptor, h api.Health) {
	if f.OnHealthChanged != nil {
		f.OnHealthChanged(d, h)
	}
}
