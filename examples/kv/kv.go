// Command kv implements an example distributed key-value store.
package main

import (
	"context"
	"flag"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gorilla/mux"
	"github.com/rfratto/croissant/examples/kv/kvproto"
	"github.com/rfratto/croissant/examples/kv/kvserver"
	"github.com/rfratto/croissant/id"
	"github.com/rfratto/croissant/node"
	"google.golang.org/grpc"

	_ "net/http/pprof"
)

func main() {
	var (
		name string

		httpListenAddr string
		grpcListenAddr string
		config         node.Config
		joinAddr       string
	)

	config.Log = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))

	hn, err := os.Hostname()
	if err != nil {
		level.Error(config.Log).Log("msg", "could not get hostname", "err", err)
		os.Exit(1)
	}

	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	fs.StringVar(&httpListenAddr, "http-listen-addr", "0.0.0.0:8080", "address to respond to HTTP requests on")
	fs.StringVar(&grpcListenAddr, "grpc-listen-addr", "0.0.0.0:9095", "address to response to gRPC requests on")
	fs.StringVar(&name, "cluster-id", hn, "string to use to generate name of server. Defaults to using hostname")
	fs.StringVar(&config.BroadcastAddr, "advertise-addr", "127.0.0.1:9095", "address to broadcast to peers for connecting.")
	fs.StringVar(&joinAddr, "join-addr", "", "If non empty, joins the cluster of the given address.")

	if err := fs.Parse(os.Args[1:]); err != nil {
		level.Error(config.Log).Log("msg", "invalid args", "err", err)
		os.Exit(1)
	}

	config.ID = id.NewGenerator(32).Get(name)

	var joinAddrs []string
	if joinAddr != "" {
		joinAddrs = append(joinAddrs, joinAddr)
	}

	var lb node.Router

	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(lb.Unary()))

	// Register the node
	n, err := node.New(config, fakeApp{}, grpc.WithInsecure())
	if err != nil {
		level.Error(config.Log).Log("msg", "failed to create http listener", "err", err)
		os.Exit(1)
	}
	n.Register(srv)
	lb.SetNode(n)

	// Create our KV server
	kvproto.RegisterKVServer(srv, kvserver.New(config.Log))

	httpLis, err := net.Listen("tcp", httpListenAddr)
	if err != nil {
		level.Error(config.Log).Log("msg", "failed to create http listener", "err", err)
		os.Exit(1)
	}
	grpcLis, err := net.Listen("tcp", grpcListenAddr)
	if err != nil {
		level.Error(config.Log).Log("msg", "failed to create grpc listener", "err", err)
		os.Exit(1)
	}

	r := mux.NewRouter()
	r.HandleFunc("/-/cluster", func(rw http.ResponseWriter, r *http.Request) {
		node.WriteHTTPState(config.Log, rw, n)
	})
	r.PathPrefix("/debug/pprof").Handler(http.DefaultServeMux)

	// Start the gRPC server and give 200ms for it to start up before we join
	// the cluster.
	go srv.Serve(grpcLis)
	time.Sleep(200 * time.Millisecond)

	if err := n.Join(context.Background(), joinAddrs); err != nil {
		level.Error(config.Log).Log("msg", "failed to join cluster", "err", err)
		os.Exit(1)
	}

	level.Info(config.Log).Log("msg", "now serving", "http", httpLis.Addr(), "grpc", grpcLis.Addr())
	http.Serve(httpLis, r)
}

type fakeApp struct{}

func (fa fakeApp) PeersChanged(ps []node.Peer) {}
