package memory

import (
	"context"
	"sync"
	"time"
)

type counterEntry struct {
	count     int64
	expiresAt time.Time
}

// CounterStore is a mutex-protected, in-memory ratelimit.CounterStore.
type CounterStore struct {
	mu     sync.Mutex
	data   map[string]counterEntry
	stopCh chan struct{}
}

// NewCounterStore constructs an empty CounterStore and starts a background
// janitor that periodically evicts expired window entries.
func NewCounterStore() *CounterStore {
	s := &CounterStore{data: make(map[string]counterEntry), stopCh: make(chan struct{})}
	go s.runJanitor(time.Minute)
	return s
}

func (s *CounterStore) runJanitor(interval time.Duration) {
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

func (s *CounterStore) sweep() {
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
func (s *CounterStore) Close() {
	close(s.stopCh)
}

// IncrementWindow implements ratelimit.CounterStore.
func (s *CounterStore) IncrementWindow(_ context.Context, key string, ttl time.Duration) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	entry, ok := s.data[key]
	if !ok || now.After(entry.expiresAt) {
		entry = counterEntry{count: 0, expiresAt: now.Add(ttl)}
	}
	entry.count++
	s.data[key] = entry
	return entry.count, nil
}

// GetWindow implements ratelimit.CounterStore.
func (s *CounterStore) GetWindow(_ context.Context, key string) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.data[key]
	if !ok || time.Now().After(entry.expiresAt) {
		return 0, false, nil
	}
	return entry.count, true, nil
}

// Len reports the number of live (non-expired) entries. Exposed for tests.
func (s *CounterStore) Len() int {
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
