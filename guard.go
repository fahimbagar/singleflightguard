package singleflightguard

import (
	"sync"

	"golang.org/x/sync/singleflight"
)

// Guard wraps a *singleflight.Group for a single named logical operation,
// adding collision detection, drift detection, and call metrics on top of
// unmodified singleflight coordination.
//
// Construct one Guard per logical operation and share it across every call
// site for that operation, the same way a *singleflight.Group already is.
// The identity type I is fixed per operation: it's whatever structured
// value uniquely identifies a request for that operation (e.g. the
// tenant/entity/version tuple the key is derived from).
//
// The underlying *singleflight.Group is held privately, not embedded: Do
// is the only way in, so there's no path that bypasses Guard's tracking
// the way an exported, promoted Group field would allow.
//
// A *Guard is safe for concurrent use.
type Guard[I comparable] struct {
	group *singleflight.Group

	op string

	mu           sync.Mutex
	seenIdentity map[string]I
}

// New creates a Guard for the named operation, with its own dedicated
// *singleflight.Group. op is used as a label in metrics and log/callback
// output. It should identify the logical operation (e.g.
// "fetchModelDescriptor"), not the specific key.
//
// Guard owns its Group exclusively, and there's no way to reach it except
// through Guard.Do: there's deliberately no way to hand New an existing
// Group, and the Group isn't exposed as a field. Any other path to that
// Group's own Do/DoChan would create exactly the blind spot Guard exists
// to catch: calls Guard can't see, colliding with calls it can.
func New[I comparable](op string, opts ...Option[I]) *Guard[I] {
	g := &Guard[I]{
		group:        &singleflight.Group{},
		op:           op,
		seenIdentity: make(map[string]I),
	}
	for _, opt := range opts {
		opt(g)
	}
	return g
}
