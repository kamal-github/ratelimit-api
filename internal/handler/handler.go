// Package handler contains the actual endpoint logic for /foo and /bar.
// By the time a request reaches these handlers, middleware.Auth has already
// authenticated the client and middleware.RateLimit has already confirmed
// the client is under its limit — so there is deliberately nothing left for
// these handlers to do except report success. That's the point of pushing
// cross-cutting concerns into middleware: the endpoint logic itself stays
// this simple even as more endpoints are added.
package handler

import (
	"encoding/json"
	"net/http"
)

type successResponse struct {
	Success bool `json:"success"`
}

func writeSuccess(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(successResponse{Success: true})
}

// Foo handles GET /foo.
func Foo(w http.ResponseWriter, r *http.Request) {
	writeSuccess(w)
}

// Bar handles GET /bar.
func Bar(w http.ResponseWriter, r *http.Request) {
	writeSuccess(w)
}

// Healthz is an unauthenticated liveness endpoint for load balancers and
// container orchestrators (Kubernetes liveness/readiness probes, cloud LB
// health checks, etc). It deliberately sits outside the auth/rate-limit
// middleware chain — a health checker is not a "client" of the API.
func Healthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}
