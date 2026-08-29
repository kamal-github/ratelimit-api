package ratelimit

import (
	"context"
	"sync"
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

func TestTokenBucketLimiter_ConcurrentRequestsNeverExceedCapacity(t *testing.T) {
	// Fires far more concurrent requests than the bucket's capacity and
	// asserts that the number allowed never exceeds capacity, exercising
	// the store's compare-and-swap path under real contention.
	store := newFakeBucketStore()

	const capacity = 10
	limiter := NewTokenBucketLimiter(store, map[string]TokenBucketConfig{
		"client-a": {Capacity: capacity, RefillPerSecond: 0}, // no refill: isolates the burst check
	})
	fixedNow := time.Now()
	limiter.now = func() time.Time { return fixedNow }

	const attempts = 200
	var wg sync.WaitGroup
	var mu sync.Mutex
	allowedCount := 0

	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d, err := limiter.Allow(context.Background(), "client-a")
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if d.Allowed {
				mu.Lock()
				allowedCount++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if allowedCount != capacity {
		t.Fatalf("expected exactly %d requests allowed under contention, got %d", capacity, allowedCount)
	}
}
