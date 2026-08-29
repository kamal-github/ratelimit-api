// Package memory implements ratelimit.BucketStore and ratelimit.CounterStore
// on top of a plain in-process map. It has zero external dependencies and
// no persistence: state is lost on restart and is not shared across
// instances. It exists for local development, tests, and single-instance
// deployments where that tradeoff is acceptable.
package memory

import (
	"context"
	"sync"
	"time"

	"github.com/kamal/ratelimit-api/internal/ratelimit"
)

type bucketEntry struct {
	state     ratelimit.BucketState
	version   int64
	expiresAt time.Time
}

// BucketStore is a mutex-protected, in-memory ratelimit.BucketStore.
type BucketStore struct {
	mu     sync.Mutex
	data   map[string]bucketEntry
	stopCh chan struct{}
}

// NewBucketStore constructs an empty BucketStore and starts a background
// janitor that periodically evicts expired entries so idle clients don't
// accumulate in memory forever. Call Close when done to stop the janitor.
func NewBucketStore() *BucketStore {
	s := &BucketStore{data: make(map[string]bucketEntry), stopCh: make(chan struct{})}
	go s.runJanitor(time.Minute)
	return s
}

func (s *BucketStore) runJanitor(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.sweep()
		case <-s.stopCh:
			return
		}
	}
}

func (s *BucketStore) sweep() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for k, v := range s.data {
		if now.After(v.expiresAt) {
			delete(s.data, k)
		}
	}
}

// Close stops the background janitor goroutine.
func (s *BucketStore) Close() {
	close(s.stopCh)
}

// Load implements ratelimit.BucketStore.
func (s *BucketStore) Load(_ context.Context, key string) (ratelimit.BucketState, int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.data[key]
	if !ok || time.Now().After(entry.expiresAt) {
		return ratelimit.BucketState{}, 0, false, nil
	}
	return entry.state, entry.version, true, nil
}

// Save implements ratelimit.BucketStore with compare-and-swap semantics on
// an explicit version counter (see the interface doc in
// internal/ratelimit/store.go for why it's a dedicated counter rather than,
// say, LastRefill). The CAS check is load-bearing here, not a formality:
// the mutex only makes each individual Load or Save call atomic — the
// algorithm calls them as two separate operations, so another goroutine can
// still run its own Load-decide-Save in between this call's Load and this
// call's Save. Without the version check, that interleaving is a silent
// lost update.
func (s *BucketStore) Save(_ context.Context, key string, newState ratelimit.BucketState, expectedVersion int64, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.data[key]
	currentVersion := int64(0)
	if ok && time.Now().Before(existing.expiresAt) {
		currentVersion = existing.version
	}

	if currentVersion != expectedVersion {
		return false, nil
	}

	s.data[key] = bucketEntry{state: newState, version: currentVersion + 1, expiresAt: time.Now().Add(ttl)}
	return true, nil
}

// Len reports the number of live (non-expired) entries. Exposed for tests.
func (s *BucketStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	now := time.Now()
	for _, e := range s.data {
		if now.Before(e.expiresAt) {
			n++
		}
	}
	return n
}
