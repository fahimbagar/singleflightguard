package singleflightguard

// Option configures a Guard[I] at construction time.
type Option[I comparable] func(*Guard[I])

// CollisionFunc is invoked when two Do calls for the same operation share a
// key but carry different identities. That combination is the signal that
// the key formula is not injective for at least these two inputs. Set one
// via WithOnCollision.
type CollisionFunc[I comparable] func(op, key string, prev, cur I)

// WithOnCollision sets the callback invoked when a key collision is
// detected (two calls, same key, different identities). Use it to log the
// colliding identities with enough detail to fix the key formula.
//
// f runs while the Guard holds its internal per-operation mutex (see
// Recorder.IncCollision), so keep it cheap: a slow callback blocks every
// other Do call for that operation for as long as it runs.
func WithOnCollision[I comparable](f CollisionFunc[I]) Option[I] {
	return func(g *Guard[I]) { g.onCollision = f }
}

// WithRefuseOnCollision controls what happens to the call whose identity
// collided with a previously-seen identity for the same key. When enabled,
// that call is routed to a derived key so it triggers its own upstream
// call instead of sharing the original caller's (provably wrong-for-it)
// result. When disabled (the default), the collision is still reported,
// but the call still shares the original result, trading correctness for
// dedup efficiency on that one call.
func WithRefuseOnCollision[I comparable](refuse bool) Option[I] {
	return func(g *Guard[I]) { g.refuseOnCollision = refuse }
}

// DriftFunc is invoked when a Do call's key structural fingerprint differs
// from the fingerprint previously established for the operation. That
// difference is the signal that call sites are building keys
// inconsistently. Set one via WithOnDrift.
type DriftFunc func(op, key, prevShape, curShape string)

// WithOnDrift sets the callback invoked when key drift is detected (a
// call's key shape differs from the operation's established shape).
//
// Same locking caveat as WithOnCollision: f runs under the Guard's
// internal mutex, so keep it cheap.
func WithOnDrift[I comparable](f DriftFunc) Option[I] {
	return func(g *Guard[I]) { g.onDrift = f }
}

// WithKeyShape overrides the structural fingerprint function used for
// drift detection. Defaults to DefaultKeyShape.
func WithKeyShape[I comparable](fn KeyShapeFunc) Option[I] {
	return func(g *Guard[I]) { g.keyShape = fn }
}

// WithDriftDetection enables or disables drift detection. Enabled by
// default; disable it for operations where key shape is expected to vary
// legitimately (e.g. keys built from a variable-length identity list) and
// where DefaultKeyShape's heuristic would otherwise produce false
// positives you don't want to tune away with WithKeyShape.
func WithDriftDetection[I comparable](enabled bool) Option[I] {
	return func(g *Guard[I]) { g.driftDetection = enabled }
}

// WithRecorder sets the Recorder that receives call/collision/drift
// metrics. Defaults to NoopRecorder.
func WithRecorder[I comparable](r Recorder) Option[I] {
	return func(g *Guard[I]) { g.recorder = r }
}
