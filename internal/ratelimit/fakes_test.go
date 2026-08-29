package ratelimit

import (
	"context"
	"sync"
	"time"
)

// fakeBucketStore is a minimal, mutex-protected BucketStore used to unit
// test TokenBucketLimiter in isolation from any real storage backend. Its
// CAS semantics mirror the contract every BucketStore implementation (in
// particular internal/store/memory and internal/store/redisstore) must
// honor — see store.go.
type fakeBucketStore struct {
	mu   sync.Mutex
	data map[string]BucketState
}

func newFakeBucketStore() *fakeBucketStore {
	return &fakeBucketStore{data: make(map[string]BucketState)}
}

func (s *fakeBucketStore) Load(_ context.Context, key string) (BucketState, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.data[key]
	return state, ok, nil
}

func (s *fakeBucketStore) Save(_ context.Context, key string, newState BucketState, expectedLastRefill int64, _ time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := int64(0)
	if existing, ok := s.data[key]; ok {
		current = existing.LastRefill
	}
	if current != expectedLastRefill {
		return false, nil
	}
	s.data[key] = newState
	return true, nil
}
