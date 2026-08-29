package ratelimit

import (
	"context"
	"testing"
	"time"
)

func TestSlidingWindowLimiter_AllowsUpToLimitWithinWindow(t *testing.T) {
	store := newFakeCounterStore()

	limiter := NewSlidingWindowLimiter(store, map[string]SlidingWindowConfig{
		"client-a": {Limit: 3, Window: 10 * time.Second},
	})
	// Anchor "now" mid-window so the previous window's weight is nonzero
	// and deterministic, rather than landing exactly on a boundary.
	fixedNow := time.Unix(1000, 0)
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

	d, err := limiter.Allow(ctx, "client-a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d.Allowed {
		t.Fatalf("expected 4th request in same window to be denied, limit was 3")
	}
	if d.RetryAfterSeconds <= 0 {
		t.Errorf("expected a positive RetryAfterSeconds hint, got %d", d.RetryAfterSeconds)
	}
}

func TestSlidingWindowLimiter_CarriesWeightedCountAcrossBoundary(t *testing.T) {
	store := newFakeCounterStore()

	limiter := NewSlidingWindowLimiter(store, map[string]SlidingWindowConfig{
		"client-a": {Limit: 5, Window: 10 * time.Second},
	})

	current := time.Unix(1000, 0) // window boundary: 1000 is divisible by 10
	limiter.now = func() time.Time { return current }

	ctx := context.Background()
	// Use up the entire limit in the window starting at t=1000.
	for i := 0; i < 5; i++ {
		d, err := limiter.Allow(ctx, "client-a")
		if err != nil || !d.Allowed {
			t.Fatalf("setup request %d: expected allowed, got allowed=%v err=%v", i, d.Allowed, err)
		}
	}

	// Move to 1 second into the *next* window (90% overlap with the
	// previous, saturated window). The weighted estimate should still be
	// close to 5 and deny the request.
	current = time.Unix(1011, 0)
	d, err := limiter.Allow(ctx, "client-a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d.Allowed {
		t.Fatalf("expected request just after the boundary to be denied due to carried-over weight")
	}

	// Move to 9 seconds into the next window (10% overlap remaining) —
	// the carried-over weight should have decayed enough to allow traffic
	// again well before the naive fixed-window reset would.
	current = time.Unix(1019, 0)
	d, err = limiter.Allow(ctx, "client-a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !d.Allowed {
		t.Fatalf("expected request near the end of the next window to be allowed as the weight decayed")
	}
}

func TestSlidingWindowLimiter_UnknownClient(t *testing.T) {
	store := newFakeCounterStore()
	limiter := NewSlidingWindowLimiter(store, map[string]SlidingWindowConfig{
		"client-a": {Limit: 1, Window: time.Second},
	})

	if _, err := limiter.Allow(context.Background(), "no-such-client"); err == nil {
		t.Fatal("expected an error for an unconfigured client, got nil")
	}
}
