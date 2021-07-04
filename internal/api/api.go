// Package api implements a higher-level abstraction over the cluster compared to nodepb.
package api

import (
	"context"
	"fmt"
	"time"
)

// Node is a node in the cluster.
type Node interface {
	// Join informs a node that joiner wishes to join the cluster. Joins will
	// be propagated and routed through the cluster based on the joiner ID.
	// Each peer along the routing path will send a NodeHello to the joiner.
	Join(ctx context.Context, joiner Descriptor) error

	// NodeHello is used for nodes to share state. If h.StateAck is
	// set and the state has changed, NodeHello should reply with an
	// ErrStateChanged.
	NodeHello(ctx context.Context, h Hello) error

	// NodeGoodbye informs a node that the leaver is leaving the cluster.
	NodeGoodbye(ctx context.Context, leaver Descriptor) error

	// GetState gets the current state of a node.
	GetState(ctx context.Context) (*State, error)
}

// Hello is a state sharing message.
type Hello struct {
	// Initiator is the node that initiated the Hello.
	Initiator Descriptor

	// The next node (if any) that will also send a Hello.
	Next *Descriptor

	// State of the initiator.
	State *State

	// StateAck is used to verify the state for the initiator of a previous Hello
	// hasn't changed. Set to the value of State.LastUpdated from a previous Hello.
	StateAck time.Time
}

// ErrStateChanged is the error of a Hello if a node's state has changed since
// StateAck.
type ErrStateChanged struct {
	NewState *State
}

// Error returns the error string.
func (e ErrStateChanged) Error() string {
	return fmt.Sprintf("state out of date")
}
