# singleflightguard

[![License](https://img.shields.io/github/license/fahimbagar/singleflightguard)](LICENSE)
[![Issues](https://img.shields.io/github/issues/fahimbagar/singleflightguard)](https://github.com/fahimbagar/singleflightguard/issues)
[![static analysis](https://github.com/fahimbagar/singleflightguard/actions/workflows/static-analysis.yml/badge.svg)](https://github.com/fahimbagar/singleflightguard/actions/workflows/static-analysis.yml)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Ffahimbagar%2Fsingleflightguard.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Ffahimbagar%2Fsingleflightguard?ref=badge_shield)

`singleflightguard` wraps `golang.org/x/sync/singleflight` with two runtime
checks it doesn't have on its own: it tells you when two different requests
got coalesced under the same key (a correctness bug), and when call sites
that are supposed to share a key don't (a cost problem). Vanilla
`singleflight` has no signal for either.

## Why this exists

I hit the drift failure mode myself, in production, before I'd even heard
of the collision one. The collision failure mode turned out to be a real,
publicly filed bug on a completely different codebase (see
[Collision](#collision) below), same root cause: a key formula that
looked fine until real traffic disagreed. Running into that twice, on two
unrelated projects, is why this is public. The failure mode is easy to
reintroduce anywhere `singleflight` is used, and checking for it at
runtime beats assuming a formula is injective because it looks that way
on paper.

## What `singleflight` already does

`singleflight` (created by
[Brad Fitzpatrick](https://en.wikipedia.org/wiki/Brad_Fitzpatrick))
provides `Group.Do(key, fn)`, which runs `fn` at most once per key at a
time; concurrent callers using the same key wait for the in-flight call
and share its result instead of each triggering their own. It's the
standard fix for a
[thundering herd](https://en.wikipedia.org/wiki/Thundering_herd_problem):
many concurrent requests for the same resource collapsing into one
upstream call.

## The problem: the key is the only thing deciding what gets shared

`singleflight` trusts the caller-supplied key completely, with no way to
check it, and that's intentional: Go proverb, ["clear is better than
clever"](https://www.youtube.com/watch?v=PAAkCSZUG1c&t=14m35s). But two
ways that trust goes wrong.

### Collision

A key formula that isn't injective lets two different requests share a key
by accident. `tenant + "|" + entity + "|" + version` looks fine until
`entity="b|c", version="1"` and `entity="b", version="c|1"` both produce
the same string. `singleflight` coalesces them, and one caller silently
gets the other's data back. This happened for real:
[Cyoda/cyoda-go#595](https://github.com/Cyoda/cyoda-go/issues/595).

### Drift

Different call sites for the same logical operation build the key
differently (one includes a field another omits), so calls that should
coalesce don't. Not a correctness bug, but every uncoalesced call is a
duplicate upstream fetch that didn't need to happen.

`singleflightguard.Guard` is a drop-in over `*singleflight.Group` that
catches both, at the moment they happen.

## What this is not

[`samber/go-singleflightx`](https://github.com/samber/go-singleflightx),
[`tarndt/shardedsingleflight`](https://github.com/tarndt/shardedsingleflight),
and [`janos/singleflight`](https://github.com/janos/singleflight) each add
something `Guard` doesn't: batched multi-key fetches, sharded groups for
high-contention workloads, context-aware cancellation. None of them check
whether the key was safe for the coalescing that already happened. `Guard`
only does that one job. It holds its own `*singleflight.Group` privately
and delegates every call to it unmodified, so adopting it doesn't change
coordination behavior, it adds one argument (the structured identity behind
the key) and a set of checks running alongside the existing call.

## Install

```sh
go get github.com/fahimbagar/singleflightguard
```

Requires Go 1.21+. The only dependency is `golang.org/x/sync`, for
`singleflight` itself.

## Quick example

```go
type descriptorKey struct{ Tenant, Entity, Version string }

g := singleflightguard.New[descriptorKey]("fetchModelDescriptor",
    singleflightguard.WithOnCollision(func(op, key string, prev, cur descriptorKey) {
        log.Printf("collision on %s key=%q: %+v vs %+v", op, key, prev, cur)
    }),
)

key := tenant + "|" + entity + "|" + version
v, err, shared := g.Do(key,
    descriptorKey{Tenant: tenant, Entity: entity, Version: version},
    func() (any, error) { return fetchModelDescriptor(tenant, entity, version) },
)
```

Two goroutines calling this concurrently with the colliding tenant/entity/
version pair from above now produce a `collision on fetchModelDescriptor
...` log line the moment it happens, instead of one of them silently
getting the wrong descriptor back.

## Features

- `WithOnCollision`/`WithRefuseOnCollision` flag two in-flight calls
  sharing a key with different identities, optionally routing the
  mismatched call to a derived key instead of just reporting it, trading a
  little dedup efficiency for correctness on that one call.
- `WithOnDrift`/`WithKeyShape`/`WithDriftDetection` track the structural
  shape of the keys seen per operation and flag when it changes across
  call sites.
- `WithRecorder` reports call counts, suppression counts, collision/drift
  counts, and in-flight duration to whatever metrics backend is already in
  place.
- `Guard` holds a private `*singleflight.Group` and delegates every
  `Do`/`DoChan`/`Forget` call to it unmodified, so coordination semantics
  never change.

Full API on
[pkg.go.dev](https://pkg.go.dev/github.com/fahimbagar/singleflightguard).

## Caveats

`Guard[I comparable]` requires the identity type to satisfy Go's
`comparable` constraint, so its fields must be comparable too: no slices,
maps, or funcs. An identity that's naturally dynamic, a
`map[string]string` with a variable field set, doesn't fit. There's no
`reflect.DeepEqual` or `Equal`-method fallback right now; normalize a
dynamic identity into a comparable shape, a sorted and joined string
works, before it reaches `Do`.

## Benchmark

Local numbers (`go test -bench=. -benchmem`), not a promise for your
hardware:

| Benchmark | ns/op | B/op | allocs/op |
|---|---|---|---|
| bare `singleflight.Group`, shared key | 388.6 | 79 | 0 |
| `Guard`, shared key (happy path) | 663.8 | 77 | 0 |
| `Guard`, every call collides (worst case) | 689.6 | 75 | 0 |
| `Guard`, every call drifts (worst case) | 1081 | 167 | 7 |

Collision detection barely moves the number past the happy path; it's the
same identity compare `Guard` already runs on every call. Drift's worst
case costs more because reporting an actual drift needs the
human-readable shape string, not just the hash comparison every other
call uses.

Machine: Intel(R) Core(TM) i7-6500U CPU @ 2.50GHz, GOMAXPROCS=4.

## TODO

- [ ] Example: Prometheus + Grafana, metrics on suppression rate,
      collisions, and drift
- [ ] Example: multi-tenant cache proxy running several `Guard`s in one
      service
- [ ] Example: incident replay reproducing both failure modes under
      concurrent load, a bare `singleflight.Group` alongside for contrast

## License

MIT, see [LICENSE](LICENSE).
