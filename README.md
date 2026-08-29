# Rate-Limited Demo API

A small Go HTTP API with two endpoints, `GET /foo` and `GET /bar`, each
enforcing a different rate limiting algorithm, with rate limit state
pluggable between two storage strategies (in-memory and Redis).

This is a work in progress. Plan:

- [ ] Config loading and validation
- [ ] Token bucket limiter (`/foo`)
- [ ] Sliding window limiter (`/bar`)
- [ ] In-memory storage backend
- [ ] Auth + rate limit middleware, HTTP wiring
- [ ] Redis storage backend
- [ ] Docker/CI
- [ ] Write up the full design notes here
