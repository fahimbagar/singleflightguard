package singleflightguard

import (
	"errors"
	"fmt"
	"strconv"
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

// TestDriftSkipsRehashForUnchangedKey checks that check() doesn't call the
// shape function at all for a key identical to the one that established
// the baseline: its shape can't have changed, so there's nothing to learn
// by recomputing it.
func TestDriftSkipsRehashForUnchangedKey(t *testing.T) {
	t.Parallel()

	var calls int64
	g := New[string]("op", WithKeyShape[string](func(key string) string {
		atomic.AddInt64(&calls, 1)
		return key
	}))

	for i := 0; i < 5; i++ {
		mustDo(t, g, "same-key", "id")
	}

	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Fatalf("shape func called %d times for 5 calls with the same key, want 1 (the baseline call)", got)
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

// TestOnCollisionDoesNotBlockOtherKeys checks that a slow onCollision
// callback only blocks the call it's reported on, not concurrent calls for
// an unrelated key: check() must release Guard's lock before invoking it,
// not hold the lock for the callback's duration.
func TestOnCollisionDoesNotBlockOtherKeys(t *testing.T) {
	t.Parallel()

	releaseLeader := make(chan struct{})
	startedLeader := make(chan struct{})
	blockCollision := make(chan struct{})
	inCollision := make(chan struct{})

	g := New[string]("op", WithOnCollision(func(op, key string, prev, cur string) {
		close(inCollision)
		<-blockCollision
	}))

	doneLeader := make(chan struct{})
	go func() {
		defer close(doneLeader)
		_, _, _ = g.Do("k1", "id-1", func() (any, error) {
			close(startedLeader)
			<-releaseLeader
			return nil, nil
		})
	}()
	<-startedLeader
	time.Sleep(20 * time.Millisecond)

	doneCollision := make(chan struct{})
	go func() {
		defer close(doneCollision)
		_, _, _ = g.Do("k1", "id-2", func() (any, error) { return nil, nil })
	}()

	select {
	case <-inCollision:
	case <-time.After(time.Second):
		t.Fatal("onCollision never started")
	}

	doneOther := make(chan struct{})
	errOther := make(chan error, 1)
	go func() {
		defer close(doneOther)
		_, err, _ := g.Do("k2", "id-3", func() (any, error) { return nil, nil })
		errOther <- err
	}()

	select {
	case <-doneOther:
		if err := <-errOther; err != nil {
			t.Fatalf("Do(k2) unexpected error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Do for an unrelated key blocked on a concurrent onCollision callback")
	}

	// Refuse-on-collision is off, so the colliding call is a follower on
	// the leader's still-in-flight call: it can't return until the leader
	// does, so release the leader first.
	close(blockCollision)
	close(releaseLeader)
	<-doneCollision
	<-doneLeader
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

// TestForget checks Forget's one real job: a call for key made after
// Forget gets its own upstream execution instead of waiting on a call for
// key that's still in flight, exactly like singleflight.Group.Forget
// documents. It uses the same identity both times, so a collision report
// would mean Forget (or the surrounding refcounting) mishandled the
// still-in-flight entry, not that the two calls' identities genuinely
// disagreed.
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

	doneB := make(chan struct{})
	var resultB any
	var sharedB bool
	go func() {
		defer close(doneB)
		resultB, _, sharedB = g.Do("k", "id-1", func() (any, error) { return "v2", nil })
	}()

	select {
	case <-doneB:
	case <-time.After(time.Second):
		t.Fatal("Do after Forget never returned; Forget likely isn't reaching the underlying Group")
	}
	if sharedB {
		t.Fatalf("sharedB = true, want false (Forget should have started a fresh call instead of coalescing with the still in-flight one)")
	}
	if resultB != "v2" {
		t.Fatalf("resultB = %v, want v2", resultB)
	}

	close(release)
	res := <-ch
	if res.Val != "v" || res.Err != nil {
		t.Fatalf("res = %+v, want Val=v, Err=nil", res)
	}
	if collisionCount != 0 {
		t.Fatalf("collisionCount = %d, want 0 (both calls used the same identity)", collisionCount)
	}
}

// TestSeenIdentityBoundedToInFlight checks that Guard's own bookkeeping
// doesn't grow with the number of distinct keys an operation has ever
// seen: once every call for a key has returned, nothing about that key is
// left behind.
func TestSeenIdentityBoundedToInFlight(t *testing.T) {
	t.Parallel()

	g := New[int]("op")

	for i := 0; i < 1000; i++ {
		key := "key-" + strconv.Itoa(i)
		if _, err, _ := g.Do(key, i, func() (any, error) { return nil, nil }); err != nil {
			t.Fatalf("Do(%q) unexpected error: %v", key, err)
		}
	}

	g.mu.Lock()
	n := len(g.seenIdentity)
	g.mu.Unlock()

	if n != 0 {
		t.Fatalf("seenIdentity has %d entries after all 1000 calls completed, want 0", n)
	}
}

// TestRefuseOnCollisionDerivedKeysAreUnique guards the fix for a real bug:
// deriving a collision key by formatting the identity with
// fmt.Sprintf("%v", ...) isn't injective for a struct with
// string fields, since %v joins fields with a plain space and doesn't
// escape a space already inside a field value. ID{A: "a b", B: "c"} and
// ID{A: "a", B: "b c"} format identically, so two genuinely different
// colliding identities could derive the same key and get coalesced with
// each other, defeating the whole point of WithRefuseOnCollision.
func TestRefuseOnCollisionDerivedKeysAreUnique(t *testing.T) {
	t.Parallel()

	type id struct{ A, B string }

	x := id{A: "a b", B: "c"}
	y := id{A: "a", B: "b c"}
	if fmt.Sprintf("%v", x) != fmt.Sprintf("%v", y) {
		t.Fatalf("test setup invalid: %+v and %+v must format identically via %%v for this test to be meaningful", x, y)
	}

	g := New[id]("op", WithRefuseOnCollision[id](true))
	leader := id{A: "leader", B: "leader"}
	const key = "k"

	release := make(chan struct{})
	started := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _, _ = g.Do(key, leader, func() (any, error) {
			close(started)
			<-release
			return "leader", nil
		})
	}()
	<-started
	time.Sleep(20 * time.Millisecond)

	// x must still be in flight on its own derived key when y arrives: two
	// calls only coalesce if they actually overlap, so a bug that makes x
	// and y derive the same key would go unexercised without this.
	releaseX := make(chan struct{})
	startedX := make(chan struct{})
	doneX := make(chan struct{})
	var xVal any
	var xShared bool
	go func() {
		defer close(doneX)
		xVal, _, xShared = g.Do(key, x, func() (any, error) {
			close(startedX)
			<-releaseX
			return "x", nil
		})
	}()
	<-startedX
	time.Sleep(20 * time.Millisecond)

	doneY := make(chan struct{})
	var yVal any
	var yShared bool
	go func() {
		defer close(doneY)
		yVal, _, yShared = g.Do(key, y, func() (any, error) { return "y", nil })
	}()
	time.Sleep(20 * time.Millisecond)

	close(releaseX)
	close(release)
	wg.Wait()

	for _, done := range []chan struct{}{doneX, doneY} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("a colliding call never returned; refuse-on-collision likely isn't diverting it to its own key")
		}
	}

	if xShared || yShared {
		t.Fatalf("xShared=%v yShared=%v, want both false: x and y must not coalesce with each other or the leader", xShared, yShared)
	}
	if xVal != "x" || yVal != "y" {
		t.Fatalf("xVal=%v yVal=%v, want x and y kept separate despite formatting identically via %%v", xVal, yVal)
	}
}

func mustDo(t *testing.T, g *Guard[string], key, identity string) {
	t.Helper()
	if _, err, _ := g.Do(key, identity, func() (any, error) { return nil, nil }); err != nil {
		t.Fatalf("Do(%q) unexpected error: %v", key, err)
	}
}
