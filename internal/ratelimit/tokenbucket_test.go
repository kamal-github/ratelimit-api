package ratelimit

import (
	"context"
	"testing"
	"time"
)

func TestTokenBucketLimiter_AllowsBurstUpToCapacity(t *testing.T) {
	store := newFakeBucketStore()

	limiter := NewTokenBucketLimiter(store, map[string]TokenBucketConfig{
		"client-a": {Capacity: 3, RefillPerSecond: 1},
	})
	fixedNow := time.Now()
	limiter.now = func() time.Time { return fixedNow }

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		d, err := limiter.Allow(ctx, "client-a")
		if err != nil {
			t.Fatalf("request %d: unexpected error: %v", i, err)
		}
		if !d.Allowed {
			t.Fatalf("request %d: expected allowed, got denied", i)
		}
	}

	// The 4th request in the same instant should exhaust the bucket.
	d, err := limiter.Allow(ctx, "client-a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d.Allowed {
		t.Fatalf("expected 4th request to be denied, capacity was 3")
	}
	if d.RetryAfterSeconds <= 0 {
		t.Errorf("expected a positive RetryAfterSeconds hint, got %d", d.RetryAfterSeconds)
	}
}

func TestTokenBucketLimiter_RefillsOverTime(t *testing.T) {
	store := newFakeBucketStore()

	limiter := NewTokenBucketLimiter(store, map[string]TokenBucketConfig{
		"client-a": {Capacity: 1, RefillPerSecond: 1},
	})
	current := time.Now()
	limiter.now = func() time.Time { return current }

	ctx := context.Background()

	d, err := limiter.Allow(ctx, "client-a")
	if err != nil || !d.Allowed {
		t.Fatalf("expected first request allowed, got allowed=%v err=%v", d.Allowed, err)
	}

	d, err = limiter.Allow(ctx, "client-a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d.Allowed {
		t.Fatalf("expected immediate second request to be denied (bucket just emptied)")
	}

	// Advance the fake clock by exactly one refill interval; the bucket
	// should now have exactly one token again.
	current = current.Add(1 * time.Second)
	d, err = limiter.Allow(ctx, "client-a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !d.Allowed {
		t.Fatalf("expected request to be allowed after refill interval elapsed")
	}
}

func TestTokenBucketLimiter_UnknownClient(t *testing.T) {
	store := newFakeBucketStore()
	limiter := NewTokenBucketLimiter(store, map[string]TokenBucketConfig{
		"client-a": {Capacity: 1, RefillPerSecond: 1},
	})

	if _, err := limiter.Allow(context.Background(), "no-such-client"); err == nil {
		t.Fatal("expected an error for an unconfigured client, got nil")
	}
}
