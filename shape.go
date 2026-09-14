package singleflightguard

import "unicode"

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
