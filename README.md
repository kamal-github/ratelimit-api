# Rate-Limited Demo API

A small Go HTTP API with two endpoints, `GET /foo` and `GET /bar`, each
enforcing a **different, hand-rolled rate limiting algorithm**, with rate
limit state pluggable between **two storage strategies** — in-memory and
Redis — behind the same interfaces.

## Contents

- [Architecture](#architecture)
- [Algorithms](#algorithms)
- [Storage strategies](#storage-strategies)
- [Authentication](#authentication)
- [Configuration](#configuration)
- [Running it](#running-it)
- [Demo script](#demo-script-two-clients-both-endpoints-both-backends)
- [Testing](#testing)
- [Deployment](#deployment)
- [Design notes and tradeoffs](#design-notes-and-tradeoffs)
- [What I'd add next](#what-id-add-next)

## Architecture

```
cmd/server/main.go        process entrypoint: flags, signal handling, graceful shutdown
internal/
  app/                    wires config -> storage -> limiters -> HTTP mux (testable, no net listener)
  config/                 JSON config loading, env var overrides, validation
  client/                 client registry: id validation + per-algorithm config maps
  ratelimit/              the algorithms themselves — Limiter interface, TokenBucketLimiter, SlidingWindowLimiter
  store/
    memory/               in-memory backend (mutex + map, janitor goroutine for expiry)
    redisstore/           Redis backend (Lua scripts for atomic CAS / increment)
  middleware/             Auth (Authorization: bearer <client-id>) and RateLimit HTTP middleware
  handler/                the actual /foo, /bar, /healthz handlers (trivial by design — see below)
```

The dependency direction is deliberate:

- `handler` and `middleware` depend only on `ratelimit.Limiter` — an
  interface with one method, `Allow(ctx, key) (Decision, error)`. They have
  no idea which algorithm or backend is behind it.
- Each algorithm (`TokenBucketLimiter`, `SlidingWindowLimiter`) depends only
  on a small, purpose-built storage interface (`BucketStore`,
  `CounterStore`) — not a single fat `Store` interface. This keeps each
  backend's job explicit: a `BucketStore` only needs to support
  load-and-compare-and-swap; a `CounterStore` only needs atomic
  increment-with-expiry. Adding a third algorithm later doesn't force every
  existing backend to grow new, unrelated methods.
- Both storage backends (`memory`, `redisstore`) implement both interfaces
  independently, with the exact same atomicity contract, so an algorithm
  behaves identically regardless of which one it's wired to.

Because of this, `internal/handler` ended up close to empty — by the time a
request reaches the handler, `Auth` has already authenticated the client and
`RateLimit` has already decided the request is allowed. That's the intended
outcome of putting cross-cutting concerns in middleware rather than
threading them through every endpoint.

## Algorithms

**`/foo` — Token Bucket** (`internal/ratelimit/tokenbucket.go`)

Each client has a bucket with a fixed `capacity` that refills continuously
at `refill_per_second` tokens/sec. Every allowed request costs one token.
This permits short bursts up to `capacity` while enforcing a long-run
average rate — the classic choice for "let occasional spikes through, but
not sustained abuse."

**`/bar` — Sliding Window Counter** (`internal/ratelimit/slidingwindow.go`)

Time is divided into fixed windows, but instead of hard-resetting the count
at each boundary (which lets a naive fixed-window counter allow 2x the
limit across a boundary), the previous window's count is carried forward,
weighted by how much it still overlaps the trailing window ending *now*:

```
estimated_count = previous_window_count * overlap_fraction + current_window_count
```

It's an approximation (it assumes even distribution of
requests within a window), not an exact sliding log, but it's O(1) storage
per client and needs only two cheap store primitives (atomic increment,
plain read) — genuinely different machinery from the token bucket, not the
same idea with renamed fields.

Both algorithms are implemented from scratch against `time.Now()` / basic
arithmetic — no third-party rate-limiting library.

## Storage strategies

**In-memory** (`internal/store/memory`) — a mutex-protected map per store
type, with a background janitor goroutine sweeping expired entries every
minute so idle clients don't leak memory. Zero dependencies, not shared
across instances, lost on restart.

**Redis** (`internal/store/redisstore`) — the same interfaces, backed by
Redis, so state is shared across every instance of the API. Each operation
an algorithm needs — "atomically compare-and-swap this bucket" or
"atomically increment this counter and set its expiry the first time it's
created" — is a single Lua script (`EVAL`), so it's a genuine one-round-trip
atomic operation, not a check-then-act race waiting to happen under
concurrent load.

Which one is active is controlled by `storage.type` in `config.json` (or the
`STORAGE_TYPE` env var) — see [Configuration](#configuration). Swapping it
is the whole point of the exercise: same algorithms, same client configs,
different persistence underneath.

**Why Redis for the "persistent" strategy specifically:** the assignment
calls for a persistent storage option alongside the in-memory one, and Redis
is the natural fit for what "persistent" needs to mean for a rate limiter —
state that outlives a single process and is shared by every instance behind
a load balancer, so a client can't dodge its limit by hitting a different
instance, and a restart or deploy doesn't hand every client a fresh bucket.
It's not persistence in the sense of *permanent* retention, though: every
key this app writes to Redis carries a TTL (`PX`/`PEXPIRE` in the Lua
scripts, mirrored by `expiresAt` in the in-memory store) and is left to
expire once a client goes idle. Rate limit state is inherently ephemeral —
there's no reason to keep a bucket around forever — so "persistent" here
means *durable and shared across instances*, not *permanent*.

## Authentication

Every request to `/foo` and `/bar` requires:

```
Authorization: bearer <client-id>
```

The scheme is matched case-insensitively per RFC 6750. An unrecognized or
missing client ID gets `401` before any rate limit check runs — rate limits
are a resource allocated to known clients, not a stand-in for
authentication. `GET /healthz` is intentionally unauthenticated (it's for
load balancer / orchestrator health checks, not API clients).

## Configuration

`config.json` (JSON, not YAML — see [Design notes](#design-notes-and-tradeoffs) for why):

```json
{
  "server": { "port": 8080, "read_timeout": "5s", "write_timeout": "10s", "idle_timeout": "60s" },
  "storage": {
    "type": "memory",
    "redis": { "addr": "localhost:6379", "password": "", "db": 0 }
  },
  "clients": [
    { "id": "client-a", "foo": { "capacity": 3,  "refill_per_second": 0.5 }, "bar": { "limit": 5,  "window": "10s" } },
    { "id": "client-b", "foo": { "capacity": 10, "refill_per_second": 2   }, "bar": { "limit": 20, "window": "10s" } }
  ]
}
```

Env var overrides (for deploying the same file unmodified across
environments — the standard 12-factor pattern): `SERVER_PORT`,
`STORAGE_TYPE` (`memory` | `redis`), `REDIS_ADDR`, `REDIS_PASSWORD`. Client
rate limits are **not** env-overridable on purpose — they're a reviewable
business decision that belongs in one file, not scattered across
deployment configs.

Config is validated at startup (`internal/config/config.go`): unknown
storage types, missing Redis address when `type: redis`, duplicate or
empty client IDs, and zero/negative limits all fail fast with a clear
error instead of misbehaving on a client's first request in production.

## Running it

Requires Go 1.22+.

**In-memory backend** (no external dependencies):

```bash
make run
# or directly:
go run ./cmd/server -config config.json
```

**Redis backend**, option A — local Redis:

```bash
redis-server --daemonize yes          # or however you normally run Redis
make run-redis
# or directly:
STORAGE_TYPE=redis REDIS_ADDR=localhost:6379 go run ./cmd/server -config config.json
```

**Redis backend**, option B — Docker Compose (brings up Redis *and* the API):

```bash
make docker-up
# or: docker compose up --build
```

## Demo script (two clients, both endpoints, both backends)

With the server running (either backend — behavior is identical from the
outside), using the sample `config.json` limits above:

```bash
# health check (no auth)
curl localhost:8080/healthz

# no Authorization header -> 401
curl -i localhost:8080/foo

# unknown client -> 401
curl -i -H "Authorization: bearer nobody" localhost:8080/foo

# client-a on /foo: capacity 3 -> first 3 succeed, 4th is throttled
for i in 1 2 3 4; do curl -s -w " -> %{http_code}\n" -H "Authorization: bearer client-a" localhost:8080/foo; done
# {"success":true} -> 200   (3 times)
# {"error":"rate limit exceeded"} -> 429

# client-b on /foo is a completely independent bucket (capacity 10) —
# unaffected by client-a just having been throttled
curl -s -w " -> %{http_code}\n" -H "Authorization: bearer client-b" localhost:8080/foo
# {"success":true} -> 200

# Please see config.json for config set.
# client-a on /bar: limit 5 per 10s window -> first 5 succeed, 6th is throttled
for i in 1 2 3 4 5 6; do curl -s -w " -> %{http_code}\n" -H "Authorization: bearer client-a" localhost:8080/bar; done
# {"success":true} -> 200   (5 times)
# {"error":"rate limit exceeded"} -> 429   (Retry-After header included)
```

This exact sequence was run against both backends while building this
(see [Testing](#testing)) — the memory backend and the Redis backend both
produce identical HTTP-level behavior.

## Testing

```bash
go test ./...              # unit + integration; Redis tests auto-skip if no Redis is reachable
go test ./... -race        # same, with the race detector
```

What's covered:

- **`internal/ratelimit`** — both algorithms in isolation, against
  lightweight in-package fakes (not the real stores, to keep these true
  unit tests): burst-to-capacity behavior, refill-over-time behavior
  (via an injectable clock, no real sleeping), the sliding window's
  weighted carry-over across a window boundary, and a concurrency test
  that fires 200 goroutines at a capacity-10 bucket and asserts exactly
  10 are allowed — proving the CAS retry loop holds under real
  contention, not just in a single-threaded read.
- **`internal/store/memory`** — CAS success/failure semantics and TTL
  expiry directly against the store.
- **`internal/store/redisstore`** — the same CAS/expiry contract, plus a
  concurrent-increment test, run against a **real Redis instance** (not
  mocked) via `go-redis`. These tests call `t.Skip` if no Redis is
  reachable at `localhost:6379` (override with `REDIS_ADDR`), so `go test
  ./...` still passes cleanly in an environment with no Redis — but they
  do run for real wherever Redis is available, including in CI
  (`.github/workflows/ci.yml` starts a Redis service container).
- **`internal/app`** — full-stack HTTP integration tests via `httptest`:
  401 for missing/unknown auth, 200→429 transitions for both clients on
  both endpoints, the `Retry-After` header on a 429, and `/healthz` being
  reachable without auth.

## Deployment

`Dockerfile` is a multi-stage build: compiles a static binary
(`CGO_ENABLED=0`), then ships it on `distroless/static-debian12:nonroot` —
no shell, no package manager, runs as a non-root UID by default. `docker
compose up --build` brings up Redis + the API wired together for the
persistent-storage demo.

**I have not personally deployed this to a cloud provider** — The image is deploy-ready; the fastest paths I'd suggest, roughly in
order of "least setup":

- **Fly.io / Render / Railway** — all three can build directly from this
  `Dockerfile` with a `git push` or repo connection, and all three offer a
  managed Redis/KeyValue add-on you can point `REDIS_ADDR` at via their env
  var UI. This is a 10–15 minute path from empty account to a public URL.
- **AWS App Runner** or **ECS Fargate** — build the image, push to ECR,
  point App Runner at it; pair with **ElastiCache for Redis** for the
  persistent backend. More setup, more control, closer to what a real prod
  deployment on AWS would look like.

Whichever you pick, the only things it needs from you: build the
`Dockerfile`, set `STORAGE_TYPE`/`REDIS_ADDR` env vars if using the Redis
backend, and expose port 8080.

## Design notes and tradeoffs

- **JSON config instead of YAML.** The obvious default for Go config is
  often YAML, but that pulls in a dependency (`gopkg.in/yaml.v3`) for a
  config shape that's small and flat enough not to need YAML's extra
  expressiveness. `encoding/json` is stdlib — one fewer thing to vendor,
  one fewer thing that can drift from `go.sum`.
- **Compare-and-swap, not a mutex around the whole algorithm.** Both
  `BucketStore.Save` and `CounterStore.IncrementWindow` are built so the
  *storage* layer owns atomicity, not the algorithm. That's what lets the
  exact same `TokenBucketLimiter`/`SlidingWindowLimiter` code run correctly
  whether it's backed by an in-process mutex or a Redis Lua script — the
  algorithm never needs to know how "atomic" is achieved.
- **Fail closed on storage errors.** If the rate limiter's backing store is
  unreachable, `middleware.RateLimit` returns `503`, not "let it through."
  A rate limiter that can't count isn't a rate limiter, and letting traffic
  through unlimited during a Redis outage is the worst possible failure
  mode for the thing whose entire job is protecting the service.
- **One storage backend per running instance, not per-request.** The
  assignment's "two storage strategies" is a deployment-time choice
  (`storage.type`), not something a client selects — that matches how this
  would actually be operated (you don't let callers choose whether your
  rate limiter is distributed).

## What I'd add next

Being upfront about what a 4-hour scoped exercise doesn't cover, roughly in
priority order for taking this further:

- `X-RateLimit-Limit` / `X-RateLimit-Remaining` / `X-RateLimit-Reset`
  response headers on successful requests, not just `Retry-After` on 429s.
- Structured metrics (requests allowed/denied per client/endpoint) and
  tracing — right now there's only structured JSON logging via `log/slog`.
- Redis Cluster / Sentinel support in `redisstore` (currently a single
  `redis.Client`) for actual HA in production.
- Config hot-reload (currently requires a restart to pick up a client
  list change) and a proper secrets manager for `REDIS_PASSWORD` instead
  of a plain env var.
- Per-client, per-endpoint algorithm selection (right now the algorithm is
  fixed per endpoint, which matches the assignment's wording, but a real
  system might want to let a client choose token-bucket-style bursting on
  an endpoint that's fixed-window today).
