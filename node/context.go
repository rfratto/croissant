package node

import (
	"context"
	"errors"

	"github.com/rfratto/croissant/id"
	"google.golang.org/grpc/metadata"
)

type clientContextKey int

const (
	idContextKey clientContextKey = iota
)

const (
	requestIdHeader = "croissant-request-id"
)

// ErrNoKey is returned when a key is missing.
var ErrNoKey = errors.New("no key in context")

// ExtractClientKey will extract the client key from ctx. Returns
// ErrNoKey when no key was found.
func ExtractClientKey(ctx context.Context) (id.ID, error) {
	// Try context value first.
	found, ok := ctx.Value(idContextKey).(id.ID)
	if ok {
		return found, nil
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return id.Zero, ErrNoKey
	}
	keys := md.Get(requestIdHeader)
	if len(keys) == 0 {
		return id.Zero, ErrNoKey
	}
	return id.Parse(keys[0])
}

// WithClientKey injects the ID into the context to be used for request
// routing.
func WithClientKey(ctx context.Context, id id.ID) context.Context {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.New(map[string]string{})
	} else {
		md = md.Copy()
	}

	md.Set(requestIdHeader, id.String())

	return metadata.NewOutgoingContext(
		context.WithValue(ctx, idContextKey, id),
		md,
	)
}
