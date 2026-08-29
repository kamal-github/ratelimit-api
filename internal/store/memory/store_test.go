package memory

import (
	"context"
	"testing"
	"time"

	"github.com/kamal/ratelimit-api/internal/ratelimit"
)

func TestBucketStore_SaveFailsCASOnMismatch(t *testing.T) {
	s := NewBucketStore()
	defer s.Close()
	ctx := context.Background()

	// First write for a fresh key must present expectedLastRefill == 0.
	swapped, err := s.Save(ctx, "k1", ratelimit.BucketState{Tokens: 5, LastRefill: 100}, 0, time.Minute)
	if err != nil || !swapped {
		t.Fatalf("expected first write to succeed, got swapped=%v err=%v", swapped, err)
	}

	// A second write claiming expectedLastRefill is still 0 (stale view)
	// must be rejected — that's the whole point of CAS.
	swapped, err = s.Save(ctx, "k1", ratelimit.BucketState{Tokens: 4, LastRefill: 200}, 0, time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if swapped {
		t.Fatalf("expected CAS to fail on stale expectedLastRefill, but it succeeded")
	}

	// A write with the correct current LastRefill (100, from the first
	// write) succeeds.
	swapped, err = s.Save(ctx, "k1", ratelimit.BucketState{Tokens: 4, LastRefill: 200}, 100, time.Minute)
	if err != nil || !swapped {
		t.Fatalf("expected CAS with correct expectedLastRefill to succeed, got swapped=%v err=%v", swapped, err)
	}

	state, ok, err := s.Load(ctx, "k1")
	if err != nil || !ok {
		t.Fatalf("expected key to be loadable, ok=%v err=%v", ok, err)
	}
	if state.Tokens != 4 || state.LastRefill != 200 {
		t.Fatalf("unexpected state after CAS: %+v", state)
	}
}

func TestBucketStore_ExpiresAfterTTL(t *testing.T) {
	s := NewBucketStore()
	defer s.Close()
	ctx := context.Background()

	_, err := s.Save(ctx, "k1", ratelimit.BucketState{Tokens: 1, LastRefill: 1}, 0, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, ok, _ := s.Load(ctx, "k1"); !ok {
		t.Fatalf("expected key to exist immediately after write")
	}

	time.Sleep(30 * time.Millisecond)

	if _, ok, _ := s.Load(ctx, "k1"); ok {
		t.Fatalf("expected key to have expired")
	}
}

func TestCounterStore_IncrementsAndExpires(t *testing.T) {
	s := NewCounterStore()
	defer s.Close()
	ctx := context.Background()

	for i := int64(1); i <= 3; i++ {
		count, err := s.IncrementWindow(ctx, "k1", time.Minute)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if count != i {
			t.Fatalf("expected count %d, got %d", i, count)
		}
	}

	count, ok, err := s.GetWindow(ctx, "k1")
	if err != nil || !ok || count != 3 {
		t.Fatalf("expected count=3 ok=true, got count=%d ok=%v err=%v", count, ok, err)
	}

	// A fresh key with a short TTL should expire and reset to 1 on the
	// next increment after expiry, not keep counting from where it left off.
	if _, err := s.IncrementWindow(ctx, "k2", 10*time.Millisecond); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	count, err = s.IncrementWindow(ctx, "k2", time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected counter to reset to 1 after expiry, got %d", count)
	}
}
