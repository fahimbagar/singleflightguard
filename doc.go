// Package singleflightguard wraps golang.org/x/sync/singleflight with two
// independent runtime checks and an observability layer, targeting two
// distinct failure modes seen in production use of singleflight:
//
//   - Collision (over-coalescing): a key-construction formula that isn't
//     injective lets two genuinely different requests share one key, so
//     singleflight coalesces them and one caller silently receives the
//     other's result. Guard catches this by requiring callers to also pass
//     a structured identity alongside the key; if two calls share a key but
//     carry different identities, that's a collision, reported the moment
//     it happens.
//
//   - Drift (under-coalescing): different call sites for what's supposed to
//     be the same logical operation build keys with inconsistent shape
//     (e.g. one site includes a field another omits), so calls that should
//     coalesce into one upstream fetch don't. Guard tracks a structural
//     fingerprint of the keys seen for an operation and flags when that
//     fingerprint changes.
//
// Guard never changes singleflight's coordination semantics: it holds its
// own *singleflight.Group and delegates every call to it unchanged (aside
// from the optional refuse-on-collision key rewrite, which only affects the
// one call whose identity provably collided). It only observes and
// reports. The Group is private, not embedded, so Do/DoChan/Forget on
// Guard are the only way to reach it; there's no exported field a caller
// could use to bypass Guard's tracking.
package singleflightguard
