package node

import (
	"github.com/rfratto/croissant/id"
	"github.com/rfratto/croissant/internal/api"
)

// Application represents the application using the cluster. Methods will be
// invoked by the node depending on the state of the cluster.
type Application interface {
	// PeersChanged is invoked when the set of peers changes.
	PeersChanged(ps []Peer)
}

// Peer is a peer in the cluster.
type Peer struct {
	ID   id.ID
	Addr string
}

func getPeers(s *api.State) []Peer {
	leaves := s.Leaves(true)
	peers := make([]Peer, len(leaves))
	for i, l := range leaves {
		peers[i] = Peer{ID: l.ID, Addr: l.Addr}
	}
	return peers
}
