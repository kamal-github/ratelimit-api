// Package client turns the list of configured clients into the two things
// the rest of the app needs: a set to validate incoming client IDs against
// (middleware.ClientValidator), and per-algorithm config maps keyed by
// client ID (consumed by the ratelimit limiters).
package client

import (
	"github.com/kamal/ratelimit-api/internal/config"
	"github.com/kamal/ratelimit-api/internal/ratelimit"
)

// Registry holds the known clients and exposes them in the shapes each
// consumer needs.
type Registry struct {
	ids map[string]bool
	foo map[string]ratelimit.TokenBucketConfig
	bar map[string]ratelimit.SlidingWindowConfig
}

// NewRegistry builds a Registry from validated config. Config.Validate is
// assumed to have already run (via config.Load), so this does not
// re-validate — it trusts its input, same as any other internal
// constructor taking a value its caller is responsible for producing correctly.
func NewRegistry(clients []config.ClientConfig) *Registry {
	r := &Registry{
		ids: make(map[string]bool, len(clients)),
		foo: make(map[string]ratelimit.TokenBucketConfig, len(clients)),
		bar: make(map[string]ratelimit.SlidingWindowConfig, len(clients)),
	}
	for _, c := range clients {
		r.ids[c.ID] = true
		r.foo[c.ID] = ratelimit.TokenBucketConfig{
			Capacity:        c.Foo.Capacity,
			RefillPerSecond: c.Foo.RefillPerSecond,
		}
		r.bar[c.ID] = ratelimit.SlidingWindowConfig{
			Limit:  c.Bar.Limit,
			Window: c.Bar.Window.AsDuration(),
		}
	}
	return r
}

// IsValidClient implements middleware.ClientValidator.
func (r *Registry) IsValidClient(id string) bool {
	return r.ids[id]
}

// FooConfigs returns the per-client token bucket configs for /foo.
func (r *Registry) FooConfigs() map[string]ratelimit.TokenBucketConfig {
	return r.foo
}

// BarConfigs returns the per-client sliding window configs for /bar.
func (r *Registry) BarConfigs() map[string]ratelimit.SlidingWindowConfig {
	return r.bar
}
