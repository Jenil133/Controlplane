package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/Jenil133/Controlplane/internal/notify"
)

// config is read from flags, falling back to CONTROLPLANE_* environment
// variables, falling back to defaults.
type config struct {
	GRPCAddr          string
	HTTPAddr          string
	Store             string
	DatabaseURL       string
	Migrate           bool
	RedisURL          string
	RedisChannel      string
	ReconcileInterval time.Duration
	ShutdownTimeout   time.Duration
	LogLevel          slog.Level
	LogFormat         string
}

func parseConfig(args []string) (config, error) {
	var (
		c        config
		logLevel string
	)
	fs := flag.NewFlagSet("controlplane", flag.ContinueOnError)
	fs.StringVar(&c.GRPCAddr, "grpc-addr", env("GRPC_ADDR", ":9090"), "gRPC listen address")
	fs.StringVar(&c.HTTPAddr, "http-addr", env("HTTP_ADDR", ":8080"), "health check listen address")
	fs.StringVar(&c.Store, "store", env("STORE", "postgres"), "storage backend: postgres or memory")
	fs.StringVar(&c.DatabaseURL, "database-url", env("DATABASE_URL", ""), "PostgreSQL connection URL")
	fs.BoolVar(&c.Migrate, "migrate", envBool("MIGRATE", true), "apply database migrations on startup")
	fs.StringVar(&c.RedisURL, "redis-url", env("REDIS_URL", ""), "Redis URL for cross-replica change events; empty runs a single replica")
	fs.StringVar(&c.RedisChannel, "redis-channel", env("REDIS_CHANNEL", notify.DefaultChannel), "Redis pub/sub channel")
	fs.DurationVar(&c.ReconcileInterval, "reconcile-interval", envDuration("RECONCILE_INTERVAL", 10*time.Second), "how often watcher state is checked against the database")
	fs.DurationVar(&c.ShutdownTimeout, "shutdown-timeout", envDuration("SHUTDOWN_TIMEOUT", 15*time.Second), "grace period for in-flight requests on shutdown")
	fs.StringVar(&logLevel, "log-level", env("LOG_LEVEL", "info"), "debug, info, warn or error")
	fs.StringVar(&c.LogFormat, "log-format", env("LOG_FORMAT", "json"), "json or text")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}

	if err := c.LogLevel.UnmarshalText([]byte(logLevel)); err != nil {
		return config{}, fmt.Errorf("--log-level: %w", err)
	}
	switch c.Store {
	case "postgres":
		if c.DatabaseURL == "" {
			return config{}, fmt.Errorf("--database-url (or CONTROLPLANE_DATABASE_URL) is required with --store postgres")
		}
	case "memory":
	default:
		return config{}, fmt.Errorf("--store must be postgres or memory, got %q", c.Store)
	}
	if c.LogFormat != "json" && c.LogFormat != "text" {
		return config{}, fmt.Errorf("--log-format must be json or text, got %q", c.LogFormat)
	}
	return c, nil
}

func env(key, def string) string {
	if v, ok := os.LookupEnv("CONTROLPLANE_" + key); ok {
		return v
	}
	return def
}

func envBool(key string, def bool) bool {
	if b, err := strconv.ParseBool(env(key, "")); err == nil {
		return b
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(env(key, "")); err == nil {
		return d
	}
	return def
}
