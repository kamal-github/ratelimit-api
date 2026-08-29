// Package app wires configuration, storage, rate limiters, and HTTP
// handlers together into a servable mux. It's kept separate from
// cmd/server/main.go — which only owns process concerns (flags, signal
// handling, starting/stopping the listener) — specifically so this wiring
// can be exercised by tests via httptest without spinning up a real
// network listener or a real main().
package app

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kamal/ratelimit-api/internal/client"
	"github.com/kamal/ratelimit-api/internal/config"
	"github.com/kamal/ratelimit-api/internal/handler"
	"github.com/kamal/ratelimit-api/internal/middleware"
	"github.com/kamal/ratelimit-api/internal/ratelimit"
	"github.com/kamal/ratelimit-api/internal/store/memory"
	"github.com/kamal/ratelimit-api/internal/store/redisstore"
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
// storage backend cfg.Storage.Type selects. Both /foo and /bar always share
// the same backend in a given run of the server — swap cfg.Storage.Type (or
// the STORAGE_TYPE env var) to demonstrate the same algorithms and the same
// client configs running against either storage strategy.
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

	case config.StorageRedis:
		rdb := redis.NewClient(&redis.Options{
			Addr:     cfg.Storage.Redis.Addr,
			Password: cfg.Storage.Redis.Password,
			DB:       cfg.Storage.Redis.DB,
		})
		pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if pingErr := rdb.Ping(pingCtx).Err(); pingErr != nil {
			return nil, nil, nil, fmt.Errorf("connecting to redis at %s: %w", cfg.Storage.Redis.Addr, pingErr)
		}

		bucketStore := redisstore.NewBucketStore(rdb, "ratelimit:bucket:")
		counterStore := redisstore.NewCounterStore(rdb, "ratelimit:counter:")
		cleanup = func() { _ = rdb.Close() }

		foo = ratelimit.NewTokenBucketLimiter(bucketStore, registry.FooConfigs())
		bar = ratelimit.NewSlidingWindowLimiter(counterStore, registry.BarConfigs())
		return foo, bar, cleanup, nil

	default:
		return nil, nil, nil, fmt.Errorf("unknown storage type %q", cfg.Storage.Type)
	}
}
