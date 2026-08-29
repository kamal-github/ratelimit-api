package app_test

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kamal/ratelimit-api/internal/app"
	"github.com/kamal/ratelimit-api/internal/config"
)

func testConfig() *config.Config {
	tenSeconds := config.Duration(10 * time.Second)
	return &config.Config{
		Server: config.ServerConfig{Port: 0},
		Storage: config.StorageConfig{
			Type: config.StorageMemory,
		},
		Clients: []config.ClientConfig{
			{
				ID:  "client-a",
				Foo: config.FooLimitConfig{Capacity: 2, RefillPerSecond: 0.001},
				Bar: config.BarLimitConfig{Limit: 2, Window: tenSeconds},
			},
			{
				ID:  "client-b",
				Foo: config.FooLimitConfig{Capacity: 5, RefillPerSecond: 0.001},
				Bar: config.BarLimitConfig{Limit: 5, Window: tenSeconds},
			},
		},
	}
}

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mux, cleanup, err := app.New(testConfig(), logger)
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}
	t.Cleanup(cleanup)
	return httptest.NewServer(mux)
}

func doRequest(t *testing.T, srv *httptest.Server, method, path, bearer string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, srv.URL+path, nil)
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	if bearer != "" {
		req.Header.Set("Authorization", "bearer "+bearer)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("doing request: %v", err)
	}
	return resp
}

func TestUnauthenticatedRequest_Returns401(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()

	resp := doRequest(t, srv, "GET", "/foo", "")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
}

func TestUnknownClient_Returns401(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()

	resp := doRequest(t, srv, "GET", "/foo", "someone-not-configured")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
}

func TestFoo_AllowsThenRateLimits_PerClient(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()

	// client-a has capacity 2 on /foo: first two calls succeed, third is limited.
	for i := 0; i < 2; i++ {
		resp := doRequest(t, srv, "GET", "/foo", "client-a")
		assertSuccess(t, resp)
	}
	resp := doRequest(t, srv, "GET", "/foo", "client-a")
	assertRateLimited(t, resp)

	// client-b has its own, independent bucket (capacity 5) and is
	// unaffected by client-a having just been throttled.
	resp = doRequest(t, srv, "GET", "/foo", "client-b")
	assertSuccess(t, resp)
}

func TestBar_AllowsThenRateLimits_PerClient(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()

	// client-a has limit 2 on /bar within the window.
	for i := 0; i < 2; i++ {
		resp := doRequest(t, srv, "GET", "/bar", "client-a")
		assertSuccess(t, resp)
	}
	resp := doRequest(t, srv, "GET", "/bar", "client-a")
	assertRateLimited(t, resp)

	if retryAfter := resp.Header.Get("Retry-After"); retryAfter == "" {
		t.Errorf("expected a Retry-After header on a 429 response")
	}
}

func TestHealthz_IsUnauthenticated(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()

	resp := doRequest(t, srv, "GET", "/healthz", "")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from /healthz with no auth, got %d", resp.StatusCode)
	}
}

func assertSuccess(t *testing.T, resp *http.Response) {
	t.Helper()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var body struct {
		Success bool `json:"success"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decoding body: %v", err)
	}
	if !body.Success {
		t.Errorf("expected success:true in body")
	}
}

func assertRateLimited(t *testing.T, resp *http.Response) {
	t.Helper()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", resp.StatusCode)
	}
	var body struct {
		Error string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decoding body: %v", err)
	}
	if body.Error != "rate limit exceeded" {
		t.Errorf(`expected error "rate limit exceeded", got %q`, body.Error)
	}
}
