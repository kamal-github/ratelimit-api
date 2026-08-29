// Package redisstore implements ratelimit.BucketStore and
// ratelimit.CounterStore on top of Redis, so rate limit state is shared
// across every instance of the API rather than living in one process's
// memory. Both stores use server-side Lua scripts (EVAL) so that each
// operation the algorithms depend on — compare-and-swap a bucket, or
// atomically increment-and-maybe-set-expiry a counter — is a single
// round trip and genuinely atomic, immune to races between concurrent
// requests hitting different instances at once.
package redisstore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kamal/ratelimit-api/internal/ratelimit"
)

// bucketCASScript atomically compares the stored bucket's version field
// against ARGV[3] (0 meaning "I expect this key not to exist yet") and,
// only on a match, overwrites it with the new state, an incremented
// version, and a (re)set TTL. Returns 1 on success, 0 if another writer
// won the race.
//
// The version is a dedicated counter, not derived from last_refill or any
// other business value. An earlier revision of this script compared
// last_refill directly, on the assumption it always changes between
// writes — which breaks the instant two writes land within the same
// millisecond (a real, reproducible lost-update bug, not a theoretical
// one — see the BucketStore interface doc in internal/ratelimit/store.go).
// A counter that increments on every successful write regardless of what
// the business data says has no such degenerate case.
const bucketCASScript = `
local current = redis.call('GET', KEYS[1])
local currentVersion = 0
if current then
  local decoded = cjson.decode(current)
  currentVersion = decoded.version
end

if currentVersion ~= tonumber(ARGV[3]) then
  return 0
end

local newVal = cjson.encode({tokens = tonumber(ARGV[1]), last_refill = tonumber(ARGV[2]), version = currentVersion + 1})
redis.call('SET', KEYS[1], newVal, 'PX', tonumber(ARGV[4]))
return 1
`

// BucketStore is a Redis-backed ratelimit.BucketStore.
type BucketStore struct {
	client *redis.Client
	script *redis.Script
	prefix string
}

// NewBucketStore builds a BucketStore. keyPrefix is prepended to every key
// (e.g. "ratelimit:bucket:") to keep this app's keys from colliding with
// anything else sharing the same Redis instance.
func NewBucketStore(client *redis.Client, keyPrefix string) *BucketStore {
	return &BucketStore{
		client: client,
		script: redis.NewScript(bucketCASScript),
		prefix: keyPrefix,
	}
}

func (s *BucketStore) fullKey(key string) string {
	return s.prefix + key
}

// Load implements ratelimit.BucketStore.
func (s *BucketStore) Load(ctx context.Context, key string) (ratelimit.BucketState, int64, bool, error) {
	raw, err := s.client.Get(ctx, s.fullKey(key)).Result()
	if err == redis.Nil {
		return ratelimit.BucketState{}, 0, false, nil
	}
	if err != nil {
		return ratelimit.BucketState{}, 0, false, fmt.Errorf("redisstore: GET: %w", err)
	}

	var wire struct {
		Tokens     float64 `json:"tokens"`
		LastRefill int64   `json:"last_refill"`
		Version    int64   `json:"version"`
	}
	if err := json.Unmarshal([]byte(raw), &wire); err != nil {
		return ratelimit.BucketState{}, 0, false, fmt.Errorf("redisstore: decoding bucket state: %w", err)
	}
	return ratelimit.BucketState{Tokens: wire.Tokens, LastRefill: wire.LastRefill}, wire.Version, true, nil
}

// Save implements ratelimit.BucketStore.
func (s *BucketStore) Save(ctx context.Context, key string, newState ratelimit.BucketState, expectedVersion int64, ttl time.Duration) (bool, error) {
	res, err := s.script.Run(ctx, s.client, []string{s.fullKey(key)},
		newState.Tokens,
		newState.LastRefill,
		expectedVersion,
		ttl.Milliseconds(),
	).Result()
	if err != nil {
		return false, fmt.Errorf("redisstore: bucket CAS script: %w", err)
	}
	swapped, _ := res.(int64)
	return swapped == 1, nil
}
