package ratelimit

import (
	"context"
	"time"
)

// BucketState is the persisted business state of a single token bucket.
type BucketState struct {
	// Tokens is the number of tokens currently available.
	Tokens float64
	// LastRefill is the unix-milliseconds timestamp the bucket was last
	// refilled at. Milliseconds (not nanoseconds) specifically: the Redis
	// backend serializes this through Lua's double-precision number type,
	// which loses precision on 19-digit nanosecond values. See
	// tokenbucket.go for the full explanation.
	LastRefill int64
}

// BucketStore persists token bucket state, keyed by an arbitrary string
// (in this app, the client ID). Implementations must provide compare-and-
// swap semantics on Save so that concurrent requests for the same key never
// silently lose an update — the classic read-modify-write race that makes
// naive rate limiters leak requests under load.
//
// The CAS token is the bucket's own LastRefill timestamp: Save only applies
// if the stored LastRefill still matches expectedLastRefill (0 meaning "the
// caller believes this key doesn't exist yet"). A CAS failure is not an
// error: it means another goroutine (or, for the Redis implementation,
// another process/replica) won the race for this key in the same instant.
// The caller is expected to reload and retry.
type BucketStore interface {
	// Load returns the current state for key. ok is false if the key has
	// never been written (the caller should treat this as a fresh, full
	// bucket).
	Load(ctx context.Context, key string) (state BucketState, ok bool, err error)

	// Save writes newState for key, succeeding only if the key's current
	// LastRefill still equals expectedLastRefill. ttl bounds how long an
	// idle client's bucket state is retained; it is refreshed on every
	// successful write.
	Save(ctx context.Context, key string, newState BucketState, expectedLastRefill int64, ttl time.Duration) (swapped bool, err error)
}

// CounterStore supports atomic, expiring counters, which is exactly what a
// fixed/sliding-window-counter rate limiter needs: "increment the count for
// this window, creating it with a TTL if it doesn't exist yet, and tell me
// the new value" — as a single atomic step.
type CounterStore interface {
	// IncrementWindow atomically increments the counter identified by key.
	// If the key doesn't exist, it is created with value 1 and the given
	// ttl. If it does exist, ttl is NOT reset (this is what makes it a
	// fixed window rather than a rolling idle timeout). Returns the count
	// after incrementing.
	IncrementWindow(ctx context.Context, key string, ttl time.Duration) (count int64, err error)

	// GetWindow returns the current count for key without modifying it.
	// ok is false if the key doesn't exist (i.e. that window has seen no
	// requests, or has already expired). Used by the sliding window
	// counter to read the previous window's count for interpolation.
	GetWindow(ctx context.Context, key string) (count int64, ok bool, err error)
}
