package singleflightguard

import (
	"unicode"
	"unicode/utf8"
)

// KeyShapeFunc reduces a singleflight key to a structural fingerprint used
// for drift detection: two keys built by consistent call sites for the same
// operation should reduce to the same shape even though their literal
// content differs, while keys built by inconsistent call sites (extra
// field, different separator, different field order) should not.
type KeyShapeFunc func(key string) string

// DefaultKeyShape collapses runs of letters to 'a' and runs of digits to
// '9', leaving every other rune (separators, punctuation) untouched. It's a
// cheap, dependency-free heuristic that's good enough to catch the common
// drift cases (a field added/dropped, a different separator) without
// knowing anything about the operation's key format.
//
// It is not a substitute for a use-case-specific shape function; pass one
// via WithKeyShape when the default heuristic is too coarse or too
// sensitive for a given operation.
func DefaultKeyShape(key string) string {
	var out []rune
	var lastClass rune // 0, 'a', or '9'

	flush := func(class rune) {
		if class != lastClass {
			out = append(out, class)
			lastClass = class
		}
	}

	for _, r := range key {
		switch {
		case unicode.IsLetter(r):
			flush('a')
		case unicode.IsDigit(r):
			flush('9')
		default:
			out = append(out, r)
			lastClass = 0
		}
	}
	return string(out)
}

// fnvOffset64 and fnvPrime64 are the 64-bit FNV-1a constants (see
// hash/fnv). defaultKeyShapeHash and hashString below inline the algorithm
// instead of using hash/fnv's hash.Hash64, so hashing a shape doesn't cost
// a heap-allocated hash.Hash64 on every Guard.Do/DoChan call.
const (
	fnvOffset64 uint64 = 14695981039346656037
	fnvPrime64  uint64 = 1099511628211
)

// defaultKeyShapeHash computes the same letter/digit-collapsing shape as
// DefaultKeyShape, but folds it directly into a fixed-size hash instead of
// building a []rune/string. Guard's hot path only ever needs to compare
// one call's shape against the operation's baseline shape; it doesn't need
// the human-readable form unless a drift is actually about to be reported,
// which is DefaultKeyShape's job, kept separate so this stays allocation
// free even though the two walk the same collapsing rule.
//
// Like any fixed-size hash, two different shapes can in principle collide
// to the same value, which would miss a drift report. At 64 bits and the
// small number of distinct shapes any real operation produces, that's not
// worth trading away allocation-free comparisons for.
func defaultKeyShapeHash(key string) uint64 {
	h := fnvOffset64
	var lastClass rune // 0, 'a', or '9'

	write := func(r rune) {
		var buf [utf8.UTFMax]byte
		n := utf8.EncodeRune(buf[:], r)
		for _, b := range buf[:n] {
			h ^= uint64(b)
			h *= fnvPrime64
		}
	}

	for _, r := range key {
		switch {
		case unicode.IsLetter(r):
			if lastClass != 'a' {
				write('a')
				lastClass = 'a'
			}
		case unicode.IsDigit(r):
			if lastClass != '9' {
				write('9')
				lastClass = '9'
			}
		default:
			write(r)
			lastClass = 0
		}
	}
	return h
}

// hashString is a generic allocation-free FNV-1a hash over a string's
// bytes, used both to compare a custom KeyShapeFunc's result the same way
// defaultKeyShapeHash compares the default shape, and to route a key to
// one of Guard's identity shards (see shardFor in guard.go).
func hashString(s string) uint64 {
	h := fnvOffset64
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= fnvPrime64
	}
	return h
}
