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
		_, _, _ = g.Do("t1|b|1", id, func() (any, error) { return nil, nil })
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

// TestSuppressedCounted checks that concurrent calls with a consistent
// key+identity correctly coalesce, and that the suppressed counter tracks
// exactly the calls that didn't trigger their own fn.
func TestSuppressedCounted(t *testing.T) {
	t.Parallel()

	rec := &countingRecorder{}
	g := New[descriptorKey]("op", WithRecorder[descriptorKey](rec))

	id := descriptorKey{Tenant: "t1", Entity: "b", Version: "1"}
	release := make(chan struct{})
	started := make(chan struct{})

	const n = 5
	var wg sync.WaitGroup
	wg.Add(n)
	var fnCalls int64
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			_, _, _ = g.Do("t1|b|1", id, func() (any, error) {
				atomic.AddInt64(&fnCalls, 1)
				close(started)
				<-release
				return nil, nil
			})
		}()
	}

	<-started
	time.Sleep(30 * time.Millisecond)
	close(release)
	wg.Wait()

	if fnCalls != 1 {
		t.Fatalf("fn ran %d times, want exactly 1 (singleflight coalescing broken)", fnCalls)
	}
	if rec.calls != n {
		t.Fatalf("calls = %d, want %d", rec.calls, n)
	}
	if rec.suppressed != n-1 {
		t.Fatalf("suppressed = %d, want %d", rec.suppressed, n-1)
	}
}

// TestDriftDetected checks that call sites building structurally different
// keys for the same operation are flagged, while consistent call sites
// (same shape, different content) are not.
func TestDriftDetected(t *testing.T) {
	t.Parallel()

	rec := &countingRecorder{}
	var drifts []string
	g := New[string]("fetchProduct",
		WithRecorder[string](rec),
		WithOnDrift[string](func(op, key, prevShape, curShape string) {
			drifts = append(drifts, key)
		}),
	)

	// Consistent call sites: numeric product IDs only.
	mustDo(t, g, "12345", "id-12345")
	mustDo(t, g, "67890", "id-67890")
	if rec.drifts != 0 {
		t.Fatalf("drifts = %d after consistent keys, want 0", rec.drifts)
	}

	// A different call site builds a SKU-shaped key for the same op.
	mustDo(t, g, "sku-12345", "id-sku-12345")
	if rec.drifts != 1 {
		t.Fatalf("drifts = %d after inconsistent key, want 1", rec.drifts)
	}
	if len(drifts) != 1 || drifts[0] != "sku-12345" {
		t.Fatalf("drift callback saw %v, want [sku-12345]", drifts)
	}
}

func TestDriftDetectionCanBeDisabled(t *testing.T) {
	t.Parallel()

	rec := &countingRecorder{}
	g := New[string]("op", WithRecorder[string](rec), WithDriftDetection[string](false))

	mustDo(t, g, "12345", "a")
	mustDo(t, g, "sku-12345", "b")
	if rec.drifts != 0 {
		t.Fatalf("drifts = %d with detection disabled, want 0", rec.drifts)
	}
}

// TestWithKeyShape checks that a custom shape function actually replaces
// DefaultKeyShape, in both directions: it must suppress drift reports
// DefaultKeyShape would have raised, and still raise drift reports for
// keys the custom function itself considers different.
func TestWithKeyShape(t *testing.T) {
	t.Parallel()

	rec := &countingRecorder{}
	firstCharShape := func(key string) string { return key[:1] }

	g := New[string]("op",
		WithRecorder[string](rec),
		WithKeyShape[string](firstCharShape),
	)

	// DefaultKeyShape would flag "abc123" vs "aXYZ" as different shapes
	// (letters vs. letters+digits); firstCharShape treats them the same.
	mustDo(t, g, "abc123", "v1")
	mustDo(t, g, "aXYZ", "v2")
	if rec.drifts != 0 {
		t.Fatalf("drifts = %d, want 0 (custom shape treats these keys as the same shape)", rec.drifts)
	}

	// A different first character is still a different shape under
	// firstCharShape, proving the override is wired in, not ignored.
	mustDo(t, g, "zzz", "v3")
	if rec.drifts != 1 {
		t.Fatalf("drifts = %d, want 1 (custom shape should still catch this one)", rec.drifts)
	}
}

// TestRefuseOnCollision verifies that when enabled, the caller whose
// identity collides gets its own upstream call instead of sharing the
// original caller's result.
func TestRefuseOnCollision(t *testing.T) {
	t.Parallel()

	g := New[descriptorKey]("op", WithRefuseOnCollision[descriptorKey](true))

	callerA := descriptorKey{Tenant: "t1", Entity: "b|c", Version: "1"}
	callerB := descriptorKey{Tenant: "t1", Entity: "b", Version: "c|1"}
	const key = "t1|b|c|1"

	release := make(chan struct{})
	started := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(1)
	var resultA any
	go func() {
		defer wg.Done()
		resultA, _, _ = g.Do(key, callerA, func() (any, error) {
			close(started)
			<-release
			return "A", nil
		})
	}()

	<-started
	time.Sleep(20 * time.Millisecond)

	// B's call must not depend on A's in-flight call finishing: if
	// collision refusal is working, B gets a derived key and its own
	// upstream call, which returns immediately. Run it in a goroutine, and
	// give it a moment to reach check() while A is still in flight (same
	// idiom as <-started plus a sleep, above) before closing release,
	// rather than after B returns. A regression that makes B coalesce
	// onto A's call then fails this test cleanly on the assertions below
	// instead of deadlocking the whole test binary, since closing release
	// doesn't wait on B either way.
	doneB := make(chan struct{})
	var resultB any
	var sharedB bool
	go func() {
		defer close(doneB)
		resultB, _, sharedB = g.Do(key, callerB, func() (any, error) {
			return "B", nil
		})
	}()
	time.Sleep(20 * time.Millisecond)

	close(release)
	wg.Wait()

	select {
	case <-doneB:
	case <-time.After(time.Second):
		t.Fatal("B's call never returned; refuse-on-collision likely isn't diverting B to its own key")
	}

	if sharedB {
		t.Fatalf("B's call reported shared=true, want a fresh call after collision refusal")
	}
	if resultA != "A" || resultB != "B" {
		t.Fatalf("resultA=%v resultB=%v, want A and B kept separate", resultA, resultB)
	}
}

// TestDoChanTracksCollisionAndSuppression checks that DoChan applies the
// same collision detection and suppressed-call accounting as Do, since it
// has its own separate bookkeeping path (a relay goroutine, not a direct
// return) precisely because singleflight.Group.DoChan's result arrives
// asynchronously.
func TestDoChanTracksCollisionAndSuppression(t *testing.T) {
	t.Parallel()

	rec := &countingRecorder{}
	var collisionCount int

	g := New[descriptorKey]("op",
		WithRecorder[descriptorKey](rec),
		WithOnCollision(func(op, key string, prev, cur descriptorKey) {
			collisionCount++
		}),
	)

	callerA := descriptorKey{Tenant: "t1", Entity: "b|c", Version: "1"}
	callerB := descriptorKey{Tenant: "t1", Entity: "b", Version: "c|1"}
	const key = "t1|b|c|1"

	release := make(chan struct{})
	started := make(chan struct{})

	chA := g.DoChan(key, callerA, func() (any, error) {
		close(started)
		<-release
		return "A", nil
	})
	<-started
	time.Sleep(20 * time.Millisecond)

	chB := g.DoChan(key, callerB, func() (any, error) {
		return "B", nil
	})
	close(release)

	resA := <-chA
	resB := <-chB

	if collisionCount != 1 {
		t.Fatalf("collision callback fired %d times, want 1", collisionCount)
	}
	if resA.Val != "A" || resB.Val != "A" {
		t.Fatalf("resA=%v resB=%v, want both to share A's result (refuse-on-collision is off)", resA.Val, resB.Val)
	}
	if !resB.Shared {
		t.Fatalf("resB.Shared = false, want true (B coalesced onto A's in-flight call)")
	}
	if rec.calls != 2 {
		t.Fatalf("calls = %d, want 2", rec.calls)
	}
	if rec.suppressed != 1 {
		t.Fatalf("suppressed = %d, want 1 (B never ran its own fn)", rec.suppressed)
	}
}

// TestForget checks two things: Forget is a real passthrough to the
// underlying singleflight.Group (a call already in flight when Forget
// runs still completes normally and keeps sharing its result with anyone
// already waiting on it, exactly like singleflight.Group.Forget
// documents), and Forget clears Guard's own identity record for that key,
// so a legitimate retry with a different identity right after isn't
// mistaken for a collision.
func TestForget(t *testing.T) {
	t.Parallel()

	var collisionCount int
	g := New[string]("op",
		WithOnCollision(func(op, key string, prev, cur string) { collisionCount++ }),
	)

	release := make(chan struct{})
	started := make(chan struct{})

	ch := g.DoChan("k", "id-1", func() (any, error) {
		close(started)
		<-release
		return "v", nil
	})
	<-started
	g.Forget("k")
	close(release)

	res := <-ch
	if res.Val != "v" || res.Err != nil {
		t.Fatalf("res = %+v, want Val=v, Err=nil", res)
	}

	mustDo(t, g, "k", "id-2")
	if collisionCount != 0 {
		t.Fatalf("collisionCount = %d, want 0 (Forget should have cleared the old identity for this key)", collisionCount)
	}
}

func mustDo(t *testing.T, g *Guard[string], key, identity string) {
	t.Helper()
	if _, err, _ := g.Do(key, identity, func() (any, error) { return nil, nil }); err != nil {
		t.Fatalf("Do(%q) unexpected error: %v", key, err)
	}
}
