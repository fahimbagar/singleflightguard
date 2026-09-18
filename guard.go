package singleflightguard

import (
	"fmt"
	"sync"
	"time"

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
// The underlying *singleflight.Group is held privately, not embedded: Do,
// DoChan, and Forget are the only ways in, so there's no path that
// bypasses Guard's tracking the way an exported, promoted Group field
// would allow.
//
// A *Guard is safe for concurrent use.
type Guard[I comparable] struct {
	group *singleflight.Group

	op string

	recorder          Recorder
	onCollision       CollisionFunc[I]
	onDrift           DriftFunc
	refuseOnCollision bool
	driftDetection    bool
	keyShape          KeyShapeFunc

	mu           sync.Mutex
	seenIdentity map[string]I
	baseShape    string
	haveShape    bool
}

// New creates a Guard for the named operation, with its own dedicated
// *singleflight.Group. op is used as a label in metrics and log/callback
// output. It should identify the logical operation (e.g.
// "fetchModelDescriptor"), not the specific key.
//
// Guard owns its Group exclusively, and there's no way to reach it except
// through Guard.Do/DoChan/Forget: there's deliberately no way to hand New
// an existing Group, and the Group isn't exposed as a field. Any other
// path to that Group's own Do/DoChan would create exactly the blind spot
// Guard exists to catch: calls Guard can't see, colliding with calls it
// can.
func New[I comparable](op string, opts ...Option[I]) *Guard[I] {
	g := &Guard[I]{
		group:          &singleflight.Group{},
		op:             op,
		recorder:       NoopRecorder{},
		driftDetection: true,
		keyShape:       DefaultKeyShape,
		seenIdentity:   make(map[string]I),
	}
	for _, opt := range opts {
		opt(g)
	}
	return g
}

// Do runs fn under singleflight coordination for key, after checking key
// against the identity history recorded for this operation.
//
// The return shape matches singleflight.Group.Do exactly: the value, any
// error from fn, and whether the result was shared with another in-flight
// caller. Existing code calling sf.Do(key, fn) can switch to
// guard.Do(key, identity, fn) with no other changes.
func (g *Guard[I]) Do(key string, identity I, fn func() (any, error)) (v any, err error, shared bool) {
	start := time.Now()
	g.recorder.IncCall(g.op)

	effectiveKey := g.check(key, identity)

	// singleflight.Group only ever invokes the leader caller's fn for a
	// given in-flight key; every other caller's fn argument is discarded
	// unused. executed therefore tells us, for *this* call specifically,
	// whether it did the upstream work or was suppressed in favor of an
	// in-flight call. That's different from the shared return value below,
	// which is true for every member of a shared group, leader included.
	var executed bool
	v, err, shared = g.group.Do(effectiveKey, func() (any, error) {
		executed = true
		return fn()
	})

	g.recorder.ObserveDuration(g.op, time.Since(start))
	if !executed {
		g.recorder.IncSuppressed(g.op)
	}
	return v, err, shared
}

// DoChan is like Do, but returns a channel that receives the result once
// it's ready, mirroring singleflight.Group.DoChan. It applies the same
// collision, drift, and metrics tracking Do does.
//
// Unlike Do, singleflight.Group.DoChan always runs fn in a new goroutine
// (even for the leader), so the metrics recorded here can only happen once
// the result actually arrives, not when DoChan returns. DoChan here
// spawns one relay goroutine per call to bridge that gap; there's no way
// to observe completion without one, since the underlying channel is the
// only completion signal available.
func (g *Guard[I]) DoChan(key string, identity I, fn func() (any, error)) <-chan singleflight.Result {
	start := time.Now()
	g.recorder.IncCall(g.op)

	effectiveKey := g.check(key, identity)

	var executed bool
	inner := g.group.DoChan(effectiveKey, func() (any, error) {
		executed = true
		return fn()
	})

	out := make(chan singleflight.Result, 1)
	go func() {
		res := <-inner
		g.recorder.ObserveDuration(g.op, time.Since(start))
		if !executed {
			g.recorder.IncSuppressed(g.op)
		}
		out <- res
	}()
	return out
}

// Forget tells the underlying singleflight.Group to forget key, exactly
// like singleflight.Group.Forget: a Do/DoChan call for key made after
// Forget runs fn itself instead of waiting on a still-in-flight call.
//
// Forget also clears the identity Guard has on record for key, but not
// the operation-wide drift shape baseline (that's not tied to any one
// key). Without clearing the identity, a legitimate "forget this call and
// retry with corrected params" flow would look identical to a collision
// to check(): the retry's new identity would be compared against the
// forgotten call's old one and flagged as a mismatch, even though Forget
// is the caller's explicit signal that the old call is being superseded,
// not collided with.
func (g *Guard[I]) Forget(key string) {
	g.mu.Lock()
	delete(g.seenIdentity, key)
	g.mu.Unlock()

	g.group.Forget(key)
}

// check records/validates identity and key shape under lock, returning the
// key that should actually be passed to the underlying singleflight.Group.
// It's the same key unless a collision was detected and refuse-on-collision
// is enabled, in which case a derived key is returned so the mismatched
// caller doesn't share the original caller's in-flight result.
func (g *Guard[I]) check(key string, identity I) string {
	g.mu.Lock()
	defer g.mu.Unlock()

	effectiveKey := key

	if prev, ok := g.seenIdentity[key]; ok {
		if prev != identity {
			g.recorder.IncCollision(g.op)
			if g.onCollision != nil {
				g.onCollision(g.op, key, prev, identity)
			}
			if g.refuseOnCollision {
				effectiveKey = fmt.Sprintf("%s\x00collision\x00%v", key, identity)
			}
		}
	} else {
		g.seenIdentity[key] = identity
	}

	if g.driftDetection {
		shape := g.keyShape(key)
		if !g.haveShape {
			g.baseShape = shape
			g.haveShape = true
		} else if shape != g.baseShape {
			g.recorder.IncDrift(g.op)
			if g.onDrift != nil {
				g.onDrift(g.op, key, g.baseShape, shape)
			}
		}
	}

	return effectiveKey
}
