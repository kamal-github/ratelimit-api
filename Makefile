.PHONY: build test test-race run run-redis docker-up docker-down fmt vet

build:
	go build -o bin/server ./cmd/server

vet:
	go vet ./...

fmt:
	gofmt -l .

# Unit + integration tests. The Redis integration tests in
# internal/store/redisstore auto-skip if no Redis is reachable at
# localhost:6379 (or $REDIS_ADDR) — start one with `redis-server` or
# `make docker-up` (redis service only) to include them.
test:
	go test ./...

test-race:
	go test ./... -race

# Runs the server with the in-memory storage backend — no external
# dependencies required.
run: build
	./bin/server -config config.json

# Runs the server against a locally-installed Redis at localhost:6379.
# Start Redis first, e.g.: redis-server --daemonize yes
run-redis: build
	STORAGE_TYPE=redis REDIS_ADDR=localhost:6379 ./bin/server -config config.json

# Brings up Redis + the API (configured for the Redis backend) via Docker.
docker-up:
	docker compose up --build

docker-down:
	docker compose down
