// Package app wires configuration, storage, rate limiters, and HTTP
// handlers together into a servable mux. It's kept separate from
// cmd/server/main.go — which only owns process concerns (flags, signal
// handling, starting/stopping the listener) — specifically so this wiring
// can be exercised by tests via httptest without spinning up a real
// network listener or a real main().
package app

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/kamal/ratelimit-api/internal/client"
	"github.com/kamal/ratelimit-api/internal/config"
	"github.com/kamal/ratelimit-api/internal/handler"
	"github.com/kamal/ratelimit-api/internal/middleware"
	"github.com/kamal/ratelimit-api/internal/ratelimit"
	"github.com/kamal/ratelimit-api/internal/store/memory"
)

// New builds the application's http.Handler from cfg, along with a cleanup
// func the caller must invoke on shutdown to release storage backend
// resources (the in-memory janitor goroutines, or the Redis connection
// pool).
func New(cfg *config.Config, logger *slog.Logger) (http.Handler, func(), error) {
	registry := client.NewRegistry(cfg.Clients)

	fooLimiter, barLimiter, cleanup, err := buildLimiters(cfg, registry)
	if err != nil {
		return nil, nil, fmt.Errorf("building rate limiters: %w", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handler.Healthz)

	authMW := middleware.Auth(registry)
	mux.Handle("GET /foo", authMW(middleware.RateLimit(fooLimiter, logger)(http.HandlerFunc(handler.Foo))))
	mux.Handle("GET /bar", authMW(middleware.RateLimit(barLimiter, logger)(http.HandlerFunc(handler.Bar))))

	return mux, cleanup, nil
}

// buildLimiters constructs the two endpoint limiters on top of whichever
// storage backend cfg.Storage.Type selects.
func buildLimiters(cfg *config.Config, registry *client.Registry) (foo ratelimit.Limiter, bar ratelimit.Limiter, cleanup func(), err error) {
	switch cfg.Storage.Type {
	case config.StorageMemory:
		bucketStore := memory.NewBucketStore()
		counterStore := memory.NewCounterStore()
		cleanup = func() {
			bucketStore.Close()
			counterStore.Close()
		}
		foo = ratelimit.NewTokenBucketLimiter(bucketStore, registry.FooConfigs())
		bar = ratelimit.NewSlidingWindowLimiter(counterStore, registry.BarConfigs())
		return foo, bar, cleanup, nil

	default:
		return nil, nil, nil, fmt.Errorf("unknown or not-yet-supported storage type %q", cfg.Storage.Type)
	}
}
