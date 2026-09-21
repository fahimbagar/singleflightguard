package singleflightguard_test

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/fahimbagar/singleflightguard"
)

// benchRecorder does the same shape of work a real metrics client would
// (an atomic increment per event) so BenchmarkGuard_SharedKeyRecording
// measures actual recording cost instead of NoopRecorder's no-op cost,
// which the other benchmarks already exercise.
type benchRecorder struct {
	calls, suppressed, collisions, drifts atomic.Int64
	durationNanos                         atomic.Int64
}

func (r *benchRecorder) IncCall(string)       { r.calls.Add(1) }
func (r *benchRecorder) IncSuppressed(string) { r.suppressed.Add(1) }
func (r *benchRecorder) IncCollision(string)  { r.collisions.Add(1) }
func (r *benchRecorder) IncDrift(string)      { r.drifts.Add(1) }
func (r *benchRecorder) ObserveDuration(_ string, d time.Duration) {
	r.durationNanos.Add(int64(d))
}

// sinkV, sinkErr, and sinkShared store the sequential benchmarks' loop
// results, so the compiler can't prove them unused and elide the
// benchmarked call ("A note on compiler optimisations":
// https://dave.cheney.net/2013/06/30/how-to-write-benchmarks-in-go). The
// b.RunParallel benchmarks below skip this: each call already locks a
// mutex and mutates a map, effects no compiler can elide regardless of the
// return value, and sharing one sink across goroutines would add
// contention that isn't part of what's being measured.
var (
	sinkV      any
	sinkErr    error
	sinkShared bool
)

// BenchmarkSingleflightGroup_UniqueKeys and BenchmarkGuard_UniqueKeys measure
// per-call overhead when every key is distinct, so singleflight never
// coalesces anything and Guard's identity/shape bookkeeping runs on every
// call with nothing to dedupe against.
func BenchmarkSingleflightGroup_UniqueKeys(b *testing.B) {
	var sf singleflight.Group
	b.ReportAllocs()
	var v any
	var err error
	var shared bool
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		v, err, shared = sf.Do(key, func() (any, error) { return i, nil })
	}
	sinkV, sinkErr, sinkShared = v, err, shared
}

func BenchmarkGuard_UniqueKeys(b *testing.B) {
	g := singleflightguard.New[int]("op")
	b.ReportAllocs()
	var v any
	var err error
	var shared bool
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		v, err, shared = g.Do(key, i, func() (any, error) { return i, nil })
	}
	sinkV, sinkErr, sinkShared = v, err, shared
}

// BenchmarkSingleflightGroup_SharedKey and BenchmarkGuard_SharedKey measure
// per-call overhead under the case singleflight exists for: many concurrent
// callers hitting the same key, most of them coalescing into one fn call.
func BenchmarkSingleflightGroup_SharedKey(b *testing.B) {
	var sf singleflight.Group
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _, _ = sf.Do("shared-key", func() (any, error) { return 42, nil })
		}
	})
}

func BenchmarkGuard_SharedKey(b *testing.B) {
	g := singleflightguard.New[int]("op")
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _, _ = g.Do("shared-key", 42, func() (any, error) { return 42, nil })
		}
	})
}

// BenchmarkGuard_SharedKeyRecording repeats the shared-key case with a
// Recorder that does real atomic-counter work, plus both callbacks set, to
// show the cost of the observability path on top of the bookkeeping
// measured by BenchmarkGuard_SharedKey above.
func BenchmarkGuard_SharedKeyRecording(b *testing.B) {
	g := singleflightguard.New[int]("op",
		singleflightguard.WithRecorder[int](&benchRecorder{}),
		singleflightguard.WithOnCollision(func(op, key string, prev, cur int) {}),
		singleflightguard.WithOnDrift[int](func(op, key, prevShape, curShape string) {}),
	)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _, _ = g.Do("shared-key", 42, func() (any, error) { return 42, nil })
		}
	})
}

// BenchmarkGuard_CollisionEveryCall and BenchmarkGuard_CollisionEveryCallRefuse
// measure the worst case for collision detection, as opposed to the happy
// path measured by BenchmarkGuard_SharedKey above: many concurrent callers
// share one key, but every caller carries a distinct identity, so nearly
// every call detects a collision instead of none. The Refuse variant also
// enables WithRefuseOnCollision, exercising the derived-key path. Each
// reports its actual collision rate as a custom metric, to confirm the
// scenario really is close to 100% collisions and not an accident of
// timing (Guard's collision detection only fires between calls that
// overlap in flight, so a sequential loop could never trigger this case).
func BenchmarkGuard_CollisionEveryCall(b *testing.B) {
	rec := &benchRecorder{}
	g := singleflightguard.New[int64]("op", singleflightguard.WithRecorder[int64](rec))
	b.ReportAllocs()
	var n int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := atomic.AddInt64(&n, 1)
			_, _, _ = g.Do("shared-key", id, func() (any, error) { return 42, nil })
		}
	})
	b.ReportMetric(float64(rec.collisions.Load())/float64(b.N)*100, "collision-%")
}

func BenchmarkGuard_CollisionEveryCallRefuse(b *testing.B) {
	rec := &benchRecorder{}
	g := singleflightguard.New[int64]("op",
		singleflightguard.WithRecorder[int64](rec),
		singleflightguard.WithRefuseOnCollision[int64](true),
	)
	b.ReportAllocs()
	var n int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := atomic.AddInt64(&n, 1)
			_, _, _ = g.Do("shared-key", id, func() (any, error) { return 42, nil })
		}
	})
	b.ReportMetric(float64(rec.collisions.Load())/float64(b.N)*100, "collision-%")
}

// BenchmarkGuard_DriftEveryCall measures the worst case for drift
// detection, as opposed to the happy path (every other benchmark here uses
// a single consistent key shape, so drift never fires): the first call
// establishes one shape as the operation's baseline, then every later call
// uses a different shape, so onDrift fires on nearly every call instead of
// never. Keys are still unique per call, like BenchmarkGuard_UniqueKeys, so
// this isolates drift-detection cost from coalescing. onDrift is set (a
// no-op, as in BenchmarkGuard_SharedKeyRecording) because check() only
// materializes the human-readable shape strings when onDrift is non-nil;
// without one, this would measure counting a drift, not reporting it.
func BenchmarkGuard_DriftEveryCall(b *testing.B) {
	rec := &benchRecorder{}
	g := singleflightguard.New[int]("op",
		singleflightguard.WithRecorder[int](rec),
		singleflightguard.WithOnDrift[int](func(op, key, prevShape, curShape string) {}),
	)
	b.ReportAllocs()
	var v any
	var err error
	var shared bool
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i) // shape "a-9"
		if i == 0 {
			key = fmt.Sprintf("%d", i) // shape "9", becomes the baseline
		}
		v, err, shared = g.Do(key, i, func() (any, error) { return i, nil })
	}
	sinkV, sinkErr, sinkShared = v, err, shared
	b.ReportMetric(float64(rec.drifts.Load())/float64(b.N)*100, "drift-%")
}
