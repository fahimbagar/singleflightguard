package singleflightguard

// Option configures a Guard[I] at construction time.
type Option[I comparable] func(*Guard[I])
