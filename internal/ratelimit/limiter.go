// Package ratelimit contains the rate limiting algorithms used by the API.
//
// Design:
//   - Limiter is the algorithm-facing interface every endpoint depends on.
//     Handlers and middleware only ever see a Limiter; they don't know or
//     care whether it's a token bucket, a sliding window, or something else.
//   - Each algorithm depends on a small, purpose-built storage interface
//     instead of a single fat "Store" interface, so a backend only has to
//     implement the primitives one algorithm actually needs.
package ratelimit

import "context"

// Decision is the outcome of a rate limit check.
type Decision struct {
	// Allowed reports whether the request should proceed.
	Allowed bool
	// RetryAfterSeconds is a best-effort hint for how long the client
	// should wait before retrying. It is advisory only: 0 means "no
	// specific guidance", not "retry immediately".
	RetryAfterSeconds int
}

// Limiter decides whether a request from a given key (a client ID, an IP,
// an API key — whatever the caller chooses to partition by) is allowed to
// proceed right now. Implementations must be safe for concurrent use.
type Limiter interface {
	Allow(ctx context.Context, key string) (Decision, error)
}
