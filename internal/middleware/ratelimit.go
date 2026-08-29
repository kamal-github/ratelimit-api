package middleware

import (
	"log/slog"
	"net/http"
	"strconv"

	"github.com/kamal/ratelimit-api/internal/ratelimit"
)

// RateLimit returns middleware that enforces limiter against the
// authenticated client ID (set by Auth, which must run first). On a denied
// request it writes 429 with the exact body the API contract requires and
// stops the chain; on an error from the limiter itself (e.g. the storage
// backend is unreachable) it fails closed with 503 rather than silently
// letting unlimited traffic through — a rate limiter that can't count is
// not a rate limiter.
func RateLimit(limiter ratelimit.Limiter, logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			clientID, ok := ClientIDFromContext(r.Context())
			if !ok {
				// Programmer error: RateLimit used without Auth in front
				// of it. Fail closed rather than rate-limiting an empty key.
				writeJSONError(w, http.StatusInternalServerError, "internal server error")
				return
			}

			decision, err := limiter.Allow(r.Context(), clientID)
			if err != nil {
				logger.Error("rate limiter error", "client_id", clientID, "path", r.URL.Path, "error", err)
				writeJSONError(w, http.StatusServiceUnavailable, "rate limiter unavailable")
				return
			}

			if !decision.Allowed {
				if decision.RetryAfterSeconds > 0 {
					w.Header().Set("Retry-After", strconv.Itoa(decision.RetryAfterSeconds))
				}
				writeJSONError(w, http.StatusTooManyRequests, "rate limit exceeded")
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
