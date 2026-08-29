package ratelimit

import (
	"context"
	"fmt"
	"math"
	"time"
)

// TokenBucketConfig configures a single client's token bucket.
type TokenBucketConfig struct {
	// Capacity is the maximum number of tokens the bucket can hold, i.e.
	// the size of the burst a client is allowed to send in one go.
	Capacity float64
	// RefillPerSecond is how many tokens are added back per second.
	// Capacity / RefillPerSecond is, informally, the sustained
	// requests-per-second rate once the initial burst is spent.
	RefillPerSecond float64
}

// clock lets tests substitute a deterministic time source. Defaults to
// time.Now via NewTokenBucketLimiter.
type clock func() time.Time

// TokenBucketLimiter implements the token bucket algorithm: each client has
// a bucket that refills continuously at a fixed rate and is drained by one
// token per allowed request. It permits short bursts up to Capacity while
// enforcing a long-run average rate of RefillPerSecond.
//
// State is kept entirely in the injected BucketStore, so the limiter itself
// is stateless and safe to construct once and share across goroutines (and,
// with the Redis-backed store, across process instances).
type TokenBucketLimiter struct {
	store   BucketStore
	configs map[string]TokenBucketConfig
	// ttl bounds how long a client's bucket state survives with no
	// traffic. It must comfortably exceed the time it takes to refill
	// from empty to full, or an idle client would come back to a bucket
	// that was evicted mid-refill and unfairly reset to full early (which
	// is harmless) or — if set too short relative to bursts — churn keys
	// unnecessarily. We default it generously in NewTokenBucketLimiter.
	ttl        time.Duration
	now        clock
	maxRetries int
}

// NewTokenBucketLimiter builds a limiter from per-client configs. ttl is the
// idle expiry applied to stored bucket state; pass 0 to use a sensible
// default (10x the time needed to refill an empty bucket to full, capped at
// 24h).
func NewTokenBucketLimiter(store BucketStore, configs map[string]TokenBucketConfig) *TokenBucketLimiter {
	return &TokenBucketLimiter{
		store:      store,
		configs:    configs,
		now:        time.Now,
		maxRetries: 5,
	}
}

func (l *TokenBucketLimiter) ttlFor(cfg TokenBucketConfig) time.Duration {
	if l.ttl > 0 {
		return l.ttl
	}
	if cfg.RefillPerSecond <= 0 {
		return 24 * time.Hour
	}
	secondsToFill := cfg.Capacity / cfg.RefillPerSecond
	ttl := time.Duration(secondsToFill*10) * time.Second
	if ttl > 24*time.Hour || ttl <= 0 {
		return 24 * time.Hour
	}
	return ttl
}

// Allow implements Limiter.
func (l *TokenBucketLimiter) Allow(ctx context.Context, key string) (Decision, error) {
	cfg, ok := l.configs[key]
	if !ok {
		return Decision{}, fmt.Errorf("ratelimit: no token bucket config for key %q", key)
	}

	ttl := l.ttlFor(cfg)

	for attempt := 0; attempt < l.maxRetries; attempt++ {
		now := l.now()

		state := BucketState{Tokens: cfg.Capacity, LastRefill: now.UnixMilli()}
		expectedLastRefill := int64(0)
		loaded, found, err := l.store.Load(ctx, key)
		if err != nil {
			return Decision{}, fmt.Errorf("ratelimit: loading bucket state: %w", err)
		}
		if found {
			state = loaded
			expectedLastRefill = loaded.LastRefill
		}

		// Refill based on elapsed time since the last observed refill.
		// Timestamps are millisecond-resolution, not nanosecond: the Redis
		// backend round-trips this value through Lua's double-precision
		// number type via cjson, which cannot represent a nanosecond unix
		// timestamp (19 digits) exactly — it silently loses precision and
		// gets serialized in scientific notation, which then fails to
		// decode back into an int64. Milliseconds (13 digits) are well
		// within a double's exact-integer range and are far finer
		// resolution than any real rate limit needs.
		elapsedSeconds := float64(now.UnixMilli()-state.LastRefill) / 1000.0
		if elapsedSeconds < 0 {
			elapsedSeconds = 0 // clock skew guard; never rewind tokens
		}
		tokens := math.Min(cfg.Capacity, state.Tokens+elapsedSeconds*cfg.RefillPerSecond)

		var (
			allowed  bool
			newState BucketState
		)
		if tokens >= 1 {
			allowed = true
			newState = BucketState{Tokens: tokens - 1, LastRefill: now.UnixMilli()}
		} else {
			allowed = false
			newState = BucketState{Tokens: tokens, LastRefill: now.UnixMilli()}
		}

		swapped, err := l.store.Save(ctx, key, newState, expectedLastRefill, ttl)
		if err != nil {
			return Decision{}, fmt.Errorf("ratelimit: saving bucket state: %w", err)
		}
		if !swapped {
			// Someone else updated this key between our Load and Save.
			// Reload and retry rather than risk handing out (or denying)
			// a token based on stale state.
			continue
		}

		if allowed {
			return Decision{Allowed: true}, nil
		}
		deficit := 1 - tokens
		retryAfter := 0
		if cfg.RefillPerSecond > 0 {
			retryAfter = int(math.Ceil(deficit / cfg.RefillPerSecond))
		}
		return Decision{Allowed: false, RetryAfterSeconds: retryAfter}, nil
	}

	return Decision{}, fmt.Errorf("ratelimit: too much contention updating bucket for key %q", key)
}
