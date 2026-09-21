package singleflightguard

import "time"

// Recorder receives observability events from a Guard. Implementations are
// expected to be safe for concurrent use, since Do may call them from
// multiple goroutines.
//
// The package intentionally does not depend on any specific metrics client
// (e.g. Prometheus). Implement Recorder against whatever your application
// already uses, or adapt an existing metrics client to this interface.
type Recorder interface {
	// IncCall is invoked once per Do call, before coalescing is resolved.
	IncCall(op string)
	// IncSuppressed is invoked when a call was coalesced into an
	// in-flight call rather than triggering its own fn execution.
	IncSuppressed(op string)
	// ObserveDuration reports how long Do took to return, from the
	// caller's perspective (in-flight wait time included for suppressed
	// calls).
	ObserveDuration(op string, d time.Duration)
	// IncCollision is invoked when two calls shared a key but carried
	// different identities.
	IncCollision(op string)
	// IncDrift is invoked when a call's key structural fingerprint
	// differs from the fingerprint previously established for the
	// operation.
	IncDrift(op string)
}

// NoopRecorder is a Recorder that discards every event. It is the default
// used by New when no Recorder is configured via WithRecorder.
type NoopRecorder struct{}

func (NoopRecorder) IncCall(string)                        {}
func (NoopRecorder) IncSuppressed(string)                  {}
func (NoopRecorder) ObserveDuration(string, time.Duration) {}
func (NoopRecorder) IncCollision(string)                   {}
func (NoopRecorder) IncDrift(string)                       {}

var _ Recorder = NoopRecorder{}
