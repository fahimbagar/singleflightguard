package singleflightguard

// KeyShapeFunc reduces a singleflight key to a structural fingerprint used
// for drift detection: two keys built by consistent call sites for the same
// operation should reduce to the same shape even though their literal
// content differs, while keys built by inconsistent call sites (extra
// field, different separator, different field order) should not.
type KeyShapeFunc func(key string) string
