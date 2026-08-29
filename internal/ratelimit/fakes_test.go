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
// honor — see store.go. Deliberately version-based, not LastRefill-based:
// an earlier revision compared LastRefill directly, which is exactly the
// bug this test's frozen-clock scenario exposed (identical timestamps
// across concurrent writes silently defeat a CAS keyed on the timestamp).
type fakeBucketStore struct {
	mu   sync.Mutex
	data map[string]fakeBucketEntry
}

type fakeBucketEntry struct {
	state   BucketState
	version int64
}

func newFakeBucketStore() *fakeBucketStore {
	return &fakeBucketStore{data: make(map[string]fakeBucketEntry)}
}

func (s *fakeBucketStore) Load(_ context.Context, key string) (BucketState, int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.data[key]
	return entry.state, entry.version, ok, nil
}

func (s *fakeBucketStore) Save(_ context.Context, key string, newState BucketState, expectedVersion int64, _ time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := int64(0)
	if existing, ok := s.data[key]; ok {
		current = existing.version
	}
	if current != expectedVersion {
		return false, nil
	}
	s.data[key] = fakeBucketEntry{state: newState, version: current + 1}
	return true, nil
}

// fakeCounterStore is a minimal, mutex-protected CounterStore used to unit
// test SlidingWindowLimiter in isolation.
type fakeCounterStore struct {
	mu   sync.Mutex
	data map[string]int64
}

func newFakeCounterStore() *fakeCounterStore {
	return &fakeCounterStore{data: make(map[string]int64)}
}

func (s *fakeCounterStore) IncrementWindow(_ context.Context, key string, _ time.Duration) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key]++
	return s.data[key], nil
}

func (s *fakeCounterStore) GetWindow(_ context.Context, key string) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	count, ok := s.data[key]
	return count, ok, nil
}
