package ratelimit

import (
	"context"
	"fmt"
	"math"
	"time"
)

// SlidingWindowConfig configures a single client's sliding window counter.
type SlidingWindowConfig struct {
	// Limit is the maximum number of requests allowed per Window.
	Limit int64
	// Window is the size of the rate limiting window, e.g. 10 * time.Second.
	Window time.Duration
}

// SlidingWindowLimiter implements the "sliding window counter" algorithm
// (the same approach used by, e.g., Cloudflare's public rate limiter):
// time is divided into fixed windows, but instead of hard-resetting the
// count at each window boundary (which lets a client burst 2x the limit
// across a boundary), the count from the previous window is carried
// forward, weighted by how much of it still overlaps the trailing "window"
// of time ending now.
//
//	estimated_count = previous_window_count * overlap_fraction + current_window_count
//
// This is an approximation, not an exact sliding log — it assumes requests
// are evenly distributed within each window — but it's O(1) in storage per
// client and needs only two cheap primitives from the store (an atomic
// increment and a plain read), which is what makes it a meaningfully
// different implementation from the token bucket rather than the same idea
// with different variable names.
type SlidingWindowLimiter struct {
	store   CounterStore
	configs map[string]SlidingWindowConfig
	now     clock
}

// NewSlidingWindowLimiter builds a limiter from per-client configs.
func NewSlidingWindowLimiter(store CounterStore, configs map[string]SlidingWindowConfig) *SlidingWindowLimiter {
	return &SlidingWindowLimiter{
		store:   store,
		configs: configs,
		now:     time.Now,
	}
}

// Allow implements Limiter.
func (l *SlidingWindowLimiter) Allow(ctx context.Context, key string) (Decision, error) {
	cfg, ok := l.configs[key]
	if !ok {
		return Decision{}, fmt.Errorf("ratelimit: no sliding window config for key %q", key)
	}
	if cfg.Window <= 0 {
		return Decision{}, fmt.Errorf("ratelimit: invalid window for key %q", key)
	}

	now := l.now()
	windowSeconds := cfg.Window.Seconds()
	windowID := now.UnixNano() / cfg.Window.Nanoseconds()
	windowStart := time.Unix(0, windowID*cfg.Window.Nanoseconds())
	elapsedInWindow := now.Sub(windowStart).Seconds()

	currentKey := fmt.Sprintf("%s:%d", key, windowID)
	previousKey := fmt.Sprintf("%s:%d", key, windowID-1)

	// Retain each window's counter for two full windows: long enough for
	// it to still be readable as "the previous window" right up until the
	// next window closes, then it's garbage.
	ttl := cfg.Window * 2

	// Increment first: the request has happened, so it counts, whether or
	// not we end up allowing it through.
	currentCount, err := l.store.IncrementWindow(ctx, currentKey, ttl)
	if err != nil {
		return Decision{}, fmt.Errorf("ratelimit: incrementing window: %w", err)
	}

	previousCount, _, err := l.store.GetWindow(ctx, previousKey)
	if err != nil {
		return Decision{}, fmt.Errorf("ratelimit: reading previous window: %w", err)
	}

	overlap := (windowSeconds - elapsedInWindow) / windowSeconds
	if overlap < 0 {
		overlap = 0
	}
	estimated := float64(previousCount)*overlap + float64(currentCount)

	if estimated <= float64(cfg.Limit) {
		return Decision{Allowed: true}, nil
	}

	retryAfter := int(math.Ceil(cfg.Window.Seconds() - elapsedInWindow))
	if retryAfter < 0 {
		retryAfter = 0
	}
	return Decision{Allowed: false, RetryAfterSeconds: retryAfter}, nil
}
