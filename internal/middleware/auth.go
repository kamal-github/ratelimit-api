// Package middleware contains HTTP middleware: authentication and rate
// limiting. Both are implemented as standard func(http.Handler) http.Handler
// wrappers so they compose with net/http's ServeMux (or any other router)
// with no framework lock-in.
package middleware

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
)

type contextKey int

const clientIDKey contextKey = iota

// ClientIDFromContext returns the authenticated client ID attached by Auth,
// and whether one was present. Handlers downstream of Auth can always
// expect ok == true; it's exposed as a bool anyway so misuse (calling it
// on a request that skipped the middleware) fails safely instead of
// returning a zero-value client ID that would silently match nothing.
func ClientIDFromContext(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(clientIDKey).(string)
	return id, ok
}

// ClientValidator reports whether id is a recognized, authorized client.
// Implemented by the client registry built from config; kept as a small
// interface here so auth middleware doesn't need to know how clients are
// stored or configured.
type ClientValidator interface {
	IsValidClient(id string) bool
}

// Auth returns middleware that requires a valid
// "Authorization: bearer <client-id>" header. Unauthenticated or unknown
// clients get 401 before touching any rate limiting or handler logic —
// rate limits are a resource allocated to known clients, not a substitute
// for authentication.
func Auth(validator ClientValidator) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			clientID, ok := extractClientID(r)
			if !ok || !validator.IsValidClient(clientID) {
				writeJSONError(w, http.StatusUnauthorized, "invalid or missing authorization")
				return
			}
			ctx := context.WithValue(r.Context(), clientIDKey, clientID)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// extractClientID parses "Authorization: bearer <client-id>". The scheme is
// matched case-insensitively per RFC 6750; the client ID itself is taken
// verbatim (client IDs are opaque tokens here, not further validated).
func extractClientID(r *http.Request) (string, bool) {
	header := r.Header.Get("Authorization")
	if header == "" {
		return "", false
	}
	parts := strings.SplitN(header, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") {
		return "", false
	}
	clientID := strings.TrimSpace(parts[1])
	if clientID == "" {
		return "", false
	}
	return clientID, true
}

func writeJSONError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}
