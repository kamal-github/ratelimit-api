// Command server runs the rate-limited demo API.
//
// Usage:
//
//	go run ./cmd/server -config config.json
//
// See the README for full setup, configuration, and demo instructions.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kamal/ratelimit-api/internal/app"
	"github.com/kamal/ratelimit-api/internal/config"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "config.json", "path to config JSON file")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	mux, cleanup, err := app.New(cfg, logger)
	if err != nil {
		return fmt.Errorf("initializing app: %w", err)
	}
	defer cleanup()

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Server.Port),
		Handler:      mux,
		ReadTimeout:  cfg.Server.ReadTimeout.AsDuration(),
		WriteTimeout: cfg.Server.WriteTimeout.AsDuration(),
		IdleTimeout:  cfg.Server.IdleTimeout.AsDuration(),
	}

	logger.Info("starting server",
		"port", cfg.Server.Port,
		"storage", cfg.Storage.Type,
		"clients", len(cfg.Clients),
	)

	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-errCh:
		return fmt.Errorf("server error: %w", err)
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}
	logger.Info("server stopped cleanly")
	return nil
}
