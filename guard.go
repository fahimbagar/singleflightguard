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
// Collision detection is scoped to calls that are actually in flight at
// the same time: Guard remembers a key's identity only for as long as at
// least one Do/DoChan call for it hasn't returned yet, then forgets it.
// Two calls for the same key with different identities are caught when
// they overlap, which is the scenario the package exists for (see doc.go);
// the same mismatch across two calls that never overlap in time is not.
// That bound is what keeps Guard's own memory footprint independent of how
// many distinct keys an operation has seen over its lifetime, rather than
// growing forever.
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
	keyShape          KeyShapeFunc // nil means DefaultKeyShape

	mu            sync.Mutex
	seenIdentity  map[string]*trackedIdentity[I]
	baseKey       string
	baseShapeHash uint64
	haveShape     bool
}

// trackedIdentity is the bookkeeping Guard keeps for one key while calls
// for it are in flight: the identity first seen for the key, how many
// current callers are relying on that entry, and (only once a collision
// under WithRefuseOnCollision actually happens) the derived keys already
// handed out to other identities colliding with it.
type trackedIdentity[I comparable] struct {
	identity I
	refs     int

	derived map[I]string
	nextID  int
}

// deriveKey returns the singleflight key a colliding identity should use
// instead of key, so it triggers its own upstream call rather than sharing
// t's original caller's result. The same identity always gets back the
// same derived key, so repeat calls from that identity still coalesce with
// each other; different identities never collide with each other on the
// derived key, since the lookup is keyed by I's own equality rather than
// by formatting identity into a string (which isn't guaranteed injective
// for arbitrary struct fields).
func (t *trackedIdentity[I]) deriveKey(key string, identity I) string {
	if derived, ok := t.derived[identity]; ok {
		return derived
	}
	if t.derived == nil {
		t.derived = make(map[I]string)
	}
	t.nextID++
	derived := fmt.Sprintf("%s\x00collision\x00%d", key, t.nextID)
	t.derived[identity] = derived
	return derived
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
		seenIdentity:   make(map[string]*trackedIdentity[I]),
	}
	for _, opt := range opts {
		opt(g)
	}
	return g
}

// Do runs fn under singleflight coordination for key, after checking key
// against the identity and shape history recorded for this operation.
//
// The return shape matches singleflight.Group.Do exactly: the value, any
// error from fn, and whether the result was shared with another in-flight
// caller. Existing code calling sf.Do(key, fn) can switch to
// guard.Do(key, identity, fn) with no other changes.
func (g *Guard[I]) Do(key string, identity I, fn func() (any, error)) (v any, err error, shared bool) {
	start := time.Now()
	g.recorder.IncCall(g.op)

	effectiveKey := g.check(key, identity)
	defer g.release(key)

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
		g.release(key)
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
// Forget does not touch the identity Guard has on record for key. It
// doesn't need to: that record is already released as soon as the call(s)
// that created it return (see the Guard doc comment), so there's nothing
// left over by the time a caller would think to call Forget for a
// completed call. Deleting it here as well, for a call that's still in
// flight, would race with that same call's own cleanup and could delete a
// different, unrelated call's entry that happens to reuse key afterwards.
func (g *Guard[I]) Forget(key string) {
	g.group.Forget(key)
}

// check records/validates identity and key shape under lock, returning the
// key that should actually be passed to the underlying singleflight.Group.
// It's the same key unless a collision was detected and refuse-on-collision
// is enabled, in which case a derived key is returned so the mismatched
// caller doesn't share the original caller's in-flight result.
//
// Every call to check must be paired with exactly one later call to
// release for the same key, once the caller's Do/DoChan call completes;
// that pairing is what keeps seenIdentity bounded to in-flight calls (see
// the Guard doc comment) instead of growing for the life of the Guard.
func (g *Guard[I]) check(key string, identity I) string {
	g.mu.Lock()
	defer g.mu.Unlock()

	effectiveKey := key

	t, seen := g.seenIdentity[key]
	if !seen {
		t = &trackedIdentity[I]{identity: identity}
		g.seenIdentity[key] = t
	}
	t.refs++

	if seen && t.identity != identity {
		g.recorder.IncCollision(g.op)
		if g.onCollision != nil {
			g.onCollision(g.op, key, t.identity, identity)
		}
		if g.refuseOnCollision {
			effectiveKey = t.deriveKey(key, identity)
		}
	}

	if g.driftDetection {
		hash := g.shapeHash(key)
		if !g.haveShape {
			g.baseKey = key
			g.baseShapeHash = hash
			g.haveShape = true
		} else if hash != g.baseShapeHash {
			g.recorder.IncDrift(g.op)
			if g.onDrift != nil {
				g.onDrift(g.op, key, g.shapeOf(g.baseKey), g.shapeOf(key))
			}
		}
	}

	return effectiveKey
}

// release marks one caller's use of key's tracked identity as done. Once
// every caller that saw key via check has released it, the entry is
// dropped, which is what keeps seenIdentity's size bounded by current
// in-flight concurrency rather than by every key an operation has ever
// seen.
func (g *Guard[I]) release(key string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	t, ok := g.seenIdentity[key]
	if !ok {
		return
	}
	t.refs--
	if t.refs <= 0 {
		delete(g.seenIdentity, key)
	}
}

// shapeHash returns a cheap, fixed-size fingerprint of key's shape,
// suitable for the equality check on every Do/DoChan call. shapeOf
// returns the same shape as a human-readable string, for the rarer path
// (an actual drift report) where one is needed.
func (g *Guard[I]) shapeHash(key string) uint64 {
	if g.keyShape == nil {
		return defaultKeyShapeHash(key)
	}
	return hashString(g.keyShape(key))
}

func (g *Guard[I]) shapeOf(key string) string {
	if g.keyShape == nil {
		return DefaultKeyShape(key)
	}
	return g.keyShape(key)
}
