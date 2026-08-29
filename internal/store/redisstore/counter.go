package redisstore

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// incrementWindowScript atomically increments a counter and, only the very
// first time it's created, attaches a TTL. Plain INCR + separate EXPIRE
// would leave a window until it gets a TTL, during which a crash or a
// concurrent request could leave it counting forever.
const incrementWindowScript = `
local count = redis.call('INCR', KEYS[1])
if count == 1 then
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
end
return count
`

// CounterStore is a Redis-backed ratelimit.CounterStore.
type CounterStore struct {
	client *redis.Client
	script *redis.Script
	prefix string
}

// NewCounterStore builds a CounterStore. keyPrefix is prepended to every key.
func NewCounterStore(client *redis.Client, keyPrefix string) *CounterStore {
	return &CounterStore{
		client: client,
		script: redis.NewScript(incrementWindowScript),
		prefix: keyPrefix,
	}
}

func (s *CounterStore) fullKey(key string) string {
	return s.prefix + key
}

// IncrementWindow implements ratelimit.CounterStore.
func (s *CounterStore) IncrementWindow(ctx context.Context, key string, ttl time.Duration) (int64, error) {
	res, err := s.script.Run(ctx, s.client, []string{s.fullKey(key)}, ttl.Milliseconds()).Result()
	if err != nil {
		return 0, fmt.Errorf("redisstore: increment window script: %w", err)
	}
	count, ok := res.(int64)
	if !ok {
		return 0, fmt.Errorf("redisstore: unexpected script result type %T", res)
	}
	return count, nil
}

// GetWindow implements ratelimit.CounterStore.
func (s *CounterStore) GetWindow(ctx context.Context, key string) (int64, bool, error) {
	count, err := s.client.Get(ctx, s.fullKey(key)).Int64()
	if err == redis.Nil {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("redisstore: GET: %w", err)
	}
	return count, true, nil
}
