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
// f runs while the Guard holds its internal per-operation mutex, so keep
// it cheap: a slow callback blocks every other Do call for that operation
// for as long as it runs.
func WithOnCollision[I comparable](f CollisionFunc[I]) Option[I] {
	return func(g *Guard[I]) { g.onCollision = f }
}
