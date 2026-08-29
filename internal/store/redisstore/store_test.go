// These tests exercise the Lua-script-based stores against a real Redis
// instance to prove the atomicity contract actually holds server-side, not
// just in a Go-level fake. They're integration tests, not unit tests, and
// are skipped automatically if no Redis is reachable (e.g. in a sandboxed
// CI step that hasn't started one) rather than failing the whole suite.
//
// Run locally with Redis available:
//
//	redis-server --daemonize yes
//	go test ./internal/store/redisstore/...
//
// Or point at a non-default instance:
//
//	REDIS_ADDR=localhost:6380 go test ./internal/store/redisstore/...
package redisstore

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kamal/ratelimit-api/internal/ratelimit"
)

func testClient(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	client := redis.NewClient(&redis.Options{Addr: addr})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("skipping: no Redis reachable at %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestRedisBucketStore_CASContract(t *testing.T) {
	client := testClient(t)
	store := NewBucketStore(client, "test:bucket:")
	ctx := context.Background()
	key := "cas-key"
	defer client.Del(ctx, "test:bucket:"+key)

	swapped, err := store.Save(ctx, key, ratelimit.BucketState{Tokens: 5, LastRefill: 100}, 0, time.Minute)
	if err != nil || !swapped {
		t.Fatalf("expected first write to succeed, got swapped=%v err=%v", swapped, err)
	}

	swapped, err = store.Save(ctx, key, ratelimit.BucketState{Tokens: 4, LastRefill: 200}, 0, time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if swapped {
		t.Fatalf("expected CAS to fail on stale expectedVersion, but it succeeded")
	}

	swapped, err = store.Save(ctx, key, ratelimit.BucketState{Tokens: 4, LastRefill: 200}, 1, time.Minute)
	if err != nil || !swapped {
		t.Fatalf("expected CAS with correct expectedVersion to succeed, got swapped=%v err=%v", swapped, err)
	}

	state, version, ok, err := store.Load(ctx, key)
	if err != nil || !ok {
		t.Fatalf("expected key to be loadable, ok=%v err=%v", ok, err)
	}
	if state.Tokens != 4 || state.LastRefill != 200 {
		t.Fatalf("unexpected state after CAS: %+v", state)
	}
	if version != 2 {
		t.Fatalf("expected version 2 after two successful writes, got %d", version)
	}
}

// TestRedisBucketStore_CASSurvivesIdenticalLastRefill is the Redis-backed
// counterpart to the same-named test in internal/store/memory: proves the
// version-based CAS in the Lua script rejects a stale write even when the
// business value (LastRefill) it's writing is bit-for-bit identical to
// what's already stored — which is exactly the scenario that silently
// defeated an earlier, LastRefill-keyed version of this script.
func TestRedisBucketStore_CASSurvivesIdenticalLastRefill(t *testing.T) {
	client := testClient(t)
	store := NewBucketStore(client, "test:bucket:")
	ctx := context.Background()
	key := "frozen-ts-key"
	defer client.Del(ctx, "test:bucket:"+key)

	const frozenTimestamp = int64(999999)

	swapped, err := store.Save(ctx, key, ratelimit.BucketState{Tokens: 5, LastRefill: frozenTimestamp}, 0, time.Minute)
	if err != nil || !swapped {
		t.Fatalf("expected first write to succeed, got swapped=%v err=%v", swapped, err)
	}

	swapped, err = store.Save(ctx, key, ratelimit.BucketState{Tokens: 4, LastRefill: frozenTimestamp}, 0, time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if swapped {
		t.Fatalf("expected stale write to be rejected even though LastRefill is unchanged, but it succeeded")
	}

	state, _, _, err := store.Load(ctx, key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.Tokens != 5 {
		t.Fatalf("expected the first writer's tokens (5) to still be in effect, got %v", state.Tokens)
	}
}

func TestRedisBucketStore_LoadMissingKey(t *testing.T) {
	client := testClient(t)
	store := NewBucketStore(client, "test:bucket:")

	_, _, ok, err := store.Load(context.Background(), "does-not-exist")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false for a missing key")
	}
}

func TestRedisCounterStore_IncrementAndExpire(t *testing.T) {
	client := testClient(t)
	store := NewCounterStore(client, "test:counter:")
	ctx := context.Background()
	key := "window-key"
	defer client.Del(ctx, "test:counter:"+key)

	for i := int64(1); i <= 3; i++ {
		count, err := store.IncrementWindow(ctx, key, time.Minute)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if count != i {
			t.Fatalf("expected count %d, got %d", i, count)
		}
	}

	count, ok, err := store.GetWindow(ctx, key)
	if err != nil || !ok || count != 3 {
		t.Fatalf("expected count=3 ok=true, got count=%d ok=%v err=%v", count, ok, err)
	}

	shortKey := "short-ttl-key"
	defer client.Del(ctx, "test:counter:"+shortKey)
	if _, err := store.IncrementWindow(ctx, shortKey, 50*time.Millisecond); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	if _, ok, err := store.GetWindow(ctx, shortKey); err != nil || ok {
		t.Fatalf("expected key to have expired in Redis, ok=%v err=%v", ok, err)
	}
}

// TestRedisStores_ConcurrentBucketWritesNeverExceedCapacity is the
// Redis-backed counterpart to
// TestTokenBucketLimiter_ConcurrentRequestsNeverExceedCapacity in
// internal/ratelimit — it drives the same CAS retry loop a real
// TokenBucketLimiter would, directly against the Redis Lua script, with
// many goroutines racing for the same bucket and a frozen LastRefill
// (no refill), and asserts exactly `capacity` writes succeed.
func TestRedisStores_ConcurrentBucketWritesNeverExceedCapacity(t *testing.T) {
	client := testClient(t)
	store := NewBucketStore(client, "test:bucket:")
	ctx := context.Background()
	key := "concurrent-bucket-key"
	defer client.Del(ctx, "test:bucket:"+key)

	const capacity = 10
	const frozenTimestamp = int64(42)
	const attempts = 100

	var wg sync.WaitGroup
	var mu sync.Mutex
	allowedCount := 0

	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for { // CAS retry loop, same shape as TokenBucketLimiter.Allow
				state, version, ok, err := store.Load(ctx, key)
				if err != nil {
					t.Errorf("unexpected error: %v", err)
					return
				}
				tokens := float64(capacity)
				expectedVersion := int64(0)
				if ok {
					tokens = state.Tokens
					expectedVersion = version
				}

				var newTokens float64
				allowed := tokens >= 1
				if allowed {
					newTokens = tokens - 1
				} else {
					newTokens = tokens
				}

				swapped, err := store.Save(ctx, key, ratelimit.BucketState{Tokens: newTokens, LastRefill: frozenTimestamp}, expectedVersion, time.Minute)
				if err != nil {
					t.Errorf("unexpected error: %v", err)
					return
				}
				if !swapped {
					continue // lost the race, reload and retry
				}
				if allowed {
					mu.Lock()
					allowedCount++
					mu.Unlock()
				}
				return
			}
		}()
	}
	wg.Wait()

	if allowedCount != capacity {
		t.Fatalf("expected exactly %d requests allowed under contention, got %d", capacity, allowedCount)
	}
}

// TestRedisStores_ConcurrentIncrementsAreAtomic fires many concurrent
// increments at the same window key across goroutines (each with its own
// connection from the pool) and checks the final count is exactly right —
// the property a naive GET-then-SET implementation would fail under load.
func TestRedisStores_ConcurrentIncrementsAreAtomic(t *testing.T) {
	client := testClient(t)
	store := NewCounterStore(client, "test:counter:")
	ctx := context.Background()
	key := "concurrent-key"
	defer client.Del(ctx, "test:counter:"+key)

	const n = 100
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			_, err := store.IncrementWindow(ctx, key, time.Minute)
			errCh <- err
		}()
	}
	for i := 0; i < n; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	count, ok, err := store.GetWindow(ctx, key)
	if err != nil || !ok {
		t.Fatalf("expected key to exist, ok=%v err=%v", ok, err)
	}
	if count != n {
		t.Fatalf("expected exactly %d after %d concurrent increments, got %d", n, n, count)
	}
}
