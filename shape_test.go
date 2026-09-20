package singleflightguard

import "testing"

// TestDefaultKeyShape pins down DefaultKeyShape's exact output rather than
// only whether two keys' shapes are equal. A test that checks relative
// equality alone can pass even when the function computes the wrong value
// on both sides: a mutation testing run found DefaultKeyShape collapsing
// to a constant empty string still left "12345" and "67890" equal to each
// other, and "sku-12345" still different from both, purely by coincidence.
// Asserting literal output closes that gap.
func TestDefaultKeyShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		key  string
		want string
	}{
		{"", ""},
		{"12345", "9"},
		{"abc", "a"},
		{"abc123", "a9"},
		{"sku-12345", "a-9"},
		{"t1|b|c|1", "a9|a|a|9"},
	}

	for _, tt := range tests {
		if got := DefaultKeyShape(tt.key); got != tt.want {
			t.Errorf("DefaultKeyShape(%q) = %q, want %q", tt.key, got, tt.want)
		}
	}
}
