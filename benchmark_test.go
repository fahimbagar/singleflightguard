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
