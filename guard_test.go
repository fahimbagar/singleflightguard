package singleflightguard

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type descriptorKey struct{ Tenant, Entity, Version string }

// countingRecorder tallies every event so tests can assert on counts.
type countingRecorder struct {
	calls, suppressed, collisions, drifts int64
}

func (r *countingRecorder) IncCall(string)                        { atomic.AddInt64(&r.calls, 1) }
func (r *countingRecorder) IncSuppressed(string)                  { atomic.AddInt64(&r.suppressed, 1) }
func (r *countingRecorder) IncCollision(string)                   { atomic.AddInt64(&r.collisions, 1) }
func (r *countingRecorder) IncDrift(string)                       { atomic.AddInt64(&r.drifts, 1) }
func (r *countingRecorder) ObserveDuration(string, time.Duration) {}

// TestCollisionDetected reproduces Cyoda/cyoda-go#595: two callers whose
// tenant+entity+version fields differ but whose naively-joined key formula
// produces the same string.
func TestCollisionDetected(t *testing.T) {
	t.Parallel()

	rec := &countingRecorder{}
	var gotPrev, gotCur descriptorKey
	var collisionCount int

	g := New[descriptorKey]("fetchModelDescriptor",
		WithRecorder[descriptorKey](rec),
		WithOnCollision(func(op, key string, prev, cur descriptorKey) {
			collisionCount++
			gotPrev, gotCur = prev, cur
		}),
	)

	const key = "t1|b|c|1" // both rows below join to this same string
	callerA := descriptorKey{Tenant: "t1", Entity: "b|c", Version: "1"}
	callerB := descriptorKey{Tenant: "t1", Entity: "b", Version: "c|1"}

	release := make(chan struct{})
	var startedA sync.WaitGroup
	startedA.Add(1)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _, _ = g.Do(key, callerA, func() (any, error) {
			startedA.Done()
			<-release
			return "A's descriptor", nil
		})
	}()

	startedA.Wait()
	go func() {
		defer wg.Done()
		_, _, _ = g.Do(key, callerB, func() (any, error) {
			return "B's descriptor", nil
		})
	}()

	// Give B a chance to register before releasing A.
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()

	if collisionCount != 1 {
		t.Fatalf("collision callback fired %d times, want 1", collisionCount)
	}
	if gotPrev != callerA || gotCur != callerB {
		t.Fatalf("collision identities = (%+v, %+v), want (%+v, %+v)", gotPrev, gotCur, callerA, callerB)
	}
	if atomic.LoadInt64(&rec.collisions) != 1 {
		t.Fatalf("recorder collisions = %d, want 1", rec.collisions)
	}
}

// TestNoCollisionForRepeatedIdentity ensures the same caller retrying the
// same key/identity pair never trips the collision detector.
func TestNoCollisionForRepeatedIdentity(t *testing.T) {
	t.Parallel()

	rec := &countingRecorder{}
	g := New[descriptorKey]("op",
		WithRecorder[descriptorKey](rec),
		WithOnCollision(func(op, key string, prev, cur descriptorKey) {
			t.Fatalf("unexpected collision: prev=%+v cur=%+v", prev, cur)
		}),
	)

	id := descriptorKey{Tenant: "t1", Entity: "b", Version: "1"}
	for i := 0; i < 5; i++ {
		g.Do("t1|b|1", id, func() (any, error) { return nil, nil })
	}
	if rec.collisions != 0 {
		t.Fatalf("collisions = %d, want 0", rec.collisions)
	}
}

func TestDoPropagatesError(t *testing.T) {
	t.Parallel()

	g := New[string]("op")
	wantErr := errors.New("upstream failed")
	_, err, _ := g.Do("k", "id", func() (any, error) { return nil, wantErr })
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
}
