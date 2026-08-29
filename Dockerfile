# --- build stage ---
FROM golang:1.22-bookworm AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server

# --- runtime stage ---
# distroless:static has no shell, no package manager, no libc — a smaller
# attack surface than even a minimal Debian/Alpine base, and CGO_ENABLED=0
# above means the static binary doesn't need libc anyway.
FROM gcr.io/distroless/static-debian12:nonroot AS runtime
WORKDIR /app

COPY --from=build /out/server ./server
COPY config.json ./config.json

# distroless:nonroot already runs as a non-root UID by default (65532).
USER nonroot:nonroot

EXPOSE 8080
ENTRYPOINT ["./server", "-config", "config.json"]
