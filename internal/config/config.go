// Package config loads and validates the application's configuration:
// server settings, which storage backend to use, and the per-client rate
// limits for each endpoint.
//
// Config is plain JSON rather than YAML. That's a deliberate, small choice:
// encoding/json is in the standard library, so the config format doesn't
// pull in a dependency, doesn't need a lockfile entry, and can't drift from
// what's vendored. Given the small, flat shape of this config, JSON's extra
// punctuation is a fair trade for that.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// StorageType selects which ratelimit store backend the server uses for
// both endpoints.
type StorageType string

const (
	StorageMemory StorageType = "memory"
	StorageRedis  StorageType = "redis"
)

// Config is the fully parsed, validated application configuration.
type Config struct {
	Server  ServerConfig   `json:"server"`
	Storage StorageConfig  `json:"storage"`
	Clients []ClientConfig `json:"clients"`
}

// ServerConfig configures the HTTP server itself.
type ServerConfig struct {
	Port         int      `json:"port"`
	ReadTimeout  Duration `json:"read_timeout"`
	WriteTimeout Duration `json:"write_timeout"`
	IdleTimeout  Duration `json:"idle_timeout"`
}

// StorageConfig selects and configures the rate-limit counter backend.
type StorageConfig struct {
	Type  StorageType `json:"type"`
	Redis RedisConfig `json:"redis"`
}

// RedisConfig configures the Redis connection. Only used when
// Storage.Type == StorageRedis.
type RedisConfig struct {
	Addr     string `json:"addr"`
	Password string `json:"password"`
	DB       int    `json:"db"`
}

// ClientConfig is one authorized client's identity and its rate limits for
// each endpoint. The client ID is the bearer token value clients send in
// the Authorization header.
type ClientConfig struct {
	ID  string         `json:"id"`
	Foo FooLimitConfig `json:"foo"`
	Bar BarLimitConfig `json:"bar"`
}

// FooLimitConfig configures this client's token bucket for /foo.
type FooLimitConfig struct {
	Capacity        float64 `json:"capacity"`
	RefillPerSecond float64 `json:"refill_per_second"`
}

// BarLimitConfig configures this client's sliding window counter for /bar.
type BarLimitConfig struct {
	Limit  int64    `json:"limit"`
	Window Duration `json:"window"`
}

// Duration wraps time.Duration so config files can use human-friendly
// strings like "10s" or "500ms" instead of raw nanosecond integers.
type Duration time.Duration

func (d Duration) String() string {
	return time.Duration(d).String()
}

// AsDuration returns d as a standard time.Duration.
func (d Duration) AsDuration() time.Duration {
	return time.Duration(d)
}

// UnmarshalJSON accepts either a JSON string ("10s") or a plain number
// (nanoseconds), so config authors get Go's familiar duration syntax.
func (d *Duration) UnmarshalJSON(data []byte) error {
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	switch val := v.(type) {
	case string:
		parsed, err := time.ParseDuration(val)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", val, err)
		}
		*d = Duration(parsed)
	case float64:
		*d = Duration(time.Duration(val))
	default:
		return fmt.Errorf("invalid duration value: %v", v)
	}
	return nil
}

// Load reads and validates configuration from the JSON file at path, then
// applies environment variable overrides on top (see applyEnvOverrides).
// Env overrides exist so the same config.json can be deployed unmodified
// across environments, with only secrets/endpoints (like the Redis address)
// injected at runtime — the standard 12-factor pattern.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: reading %s: %w", path, err)
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("config: parsing %s: %w", path, err)
	}

	cfg.applyEnvOverrides()
	cfg.applyDefaults()

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	return &cfg, nil
}

// applyEnvOverrides lets deployment-specific values be injected without
// editing the checked-in config file. Only infrastructure knobs are
// overridable this way (not client rate limits) — those are a business
// decision that belongs in one reviewable file, not scattered env vars.
func (c *Config) applyEnvOverrides() {
	if v := os.Getenv("SERVER_PORT"); v != "" {
		var port int
		if _, err := fmt.Sscanf(v, "%d", &port); err == nil {
			c.Server.Port = port
		}
	}
	if v := os.Getenv("STORAGE_TYPE"); v != "" {
		c.Storage.Type = StorageType(v)
	}
	if v := os.Getenv("REDIS_ADDR"); v != "" {
		c.Storage.Redis.Addr = v
	}
	if v := os.Getenv("REDIS_PASSWORD"); v != "" {
		c.Storage.Redis.Password = v
	}
}

func (c *Config) applyDefaults() {
	if c.Server.Port == 0 {
		c.Server.Port = 8080
	}
	if c.Server.ReadTimeout == 0 {
		c.Server.ReadTimeout = Duration(5 * time.Second)
	}
	if c.Server.WriteTimeout == 0 {
		c.Server.WriteTimeout = Duration(10 * time.Second)
	}
	if c.Server.IdleTimeout == 0 {
		c.Server.IdleTimeout = Duration(60 * time.Second)
	}
	if c.Storage.Type == "" {
		c.Storage.Type = StorageMemory
	}
}

// Validate rejects configs that would cause confusing runtime behavior
// (an unknown client silently getting no rate limit, a client with a
// zero-value limit that would either always allow or always block, etc.)
// so misconfiguration fails fast at startup, not on a client's first
// request in production.
func (c *Config) Validate() error {
	if c.Storage.Type != StorageMemory && c.Storage.Type != StorageRedis {
		return fmt.Errorf("storage.type must be %q or %q, got %q", StorageMemory, StorageRedis, c.Storage.Type)
	}
	if c.Storage.Type == StorageRedis && c.Storage.Redis.Addr == "" {
		return fmt.Errorf("storage.redis.addr is required when storage.type is %q", StorageRedis)
	}
	if len(c.Clients) == 0 {
		return fmt.Errorf("at least one client must be configured")
	}

	seen := make(map[string]bool, len(c.Clients))
	for _, cl := range c.Clients {
		if cl.ID == "" {
			return fmt.Errorf("client with empty id")
		}
		if seen[cl.ID] {
			return fmt.Errorf("duplicate client id %q", cl.ID)
		}
		seen[cl.ID] = true

		if cl.Foo.Capacity <= 0 || cl.Foo.RefillPerSecond <= 0 {
			return fmt.Errorf("client %q: foo.capacity and foo.refill_per_second must be > 0", cl.ID)
		}
		if cl.Bar.Limit <= 0 || cl.Bar.Window <= 0 {
			return fmt.Errorf("client %q: bar.limit and bar.window must be > 0", cl.ID)
		}
	}
	return nil
}
