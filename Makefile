.PHONY: build test test-race run fmt vet

build:
	go build -o bin/server ./cmd/server

vet:
	go vet ./...

fmt:
	gofmt -l .

test:
	go test ./...

test-race:
	go test ./... -race

# Runs the server with the in-memory storage backend — no external
# dependencies required.
run: build
	./bin/server -config config.json
