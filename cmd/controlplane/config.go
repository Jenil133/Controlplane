package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/Jenil133/Controlplane/internal/notify"
)

// envPrefix starts the environment variable of every flag: --grpc-addr is
// CONTROLPLANE_GRPC_ADDR. Deployments (deploy/k8s, deploy/docker-compose.yml)
// configure the server through these names.
const envPrefix = "CONTROLPLANE_"

// commandMigrate applies the database migrations and exits, e.g. in a job
// that runs before a release rolls out.
const commandMigrate = "migrate"

const usageHeader = `Usage:
  controlplane [flags]           run the server
  controlplane migrate [flags]   apply the database migrations to --database-url and exit

Every flag can also be set by the environment variable shown with it. Flags
win over the environment, and an empty variable counts as unset.

Flags:
`

// config is read from flags, falling back to CONTROLPLANE_* environment
// variables, falling back to defaults.
type config struct {
	// Command is empty to run the server, or commandMigrate.
	Command           string
	GRPCAddr          string
	HTTPAddr          string
	Store             string
	DatabaseURL       string
	Migrate           bool
	RedisURL          string
	RedisChannel      string
	AuthTokensFile    string
	ReconcileInterval time.Duration
	RolloutInterval   time.Duration
	ShutdownTimeout   time.Duration
	LogLevel          slog.Level
	LogFormat         string
}

func parseConfig(args []string) (config, error) {
	var (
		c        config
		logLevel string
	)
	if len(args) > 0 && args[0] == commandMigrate {
		c.Command, args = commandMigrate, args[1:]
	}
	fs := flag.NewFlagSet("controlplane", flag.ContinueOnError)
	fs.StringVar(&c.GRPCAddr, "grpc-addr", ":9090", "gRPC listen address")
	fs.StringVar(&c.HTTPAddr, "http-addr", ":8080", "HTTP listen address: JSON API, admin UI, /metrics, /healthz and /readyz")
	fs.StringVar(&c.Store, "store", "postgres", "storage backend: postgres or memory")
	fs.StringVar(&c.DatabaseURL, "database-url", "", "PostgreSQL connection URL")
	fs.BoolVar(&c.Migrate, "migrate", true, "apply database migrations on startup")
	fs.StringVar(&c.RedisURL, "redis-url", "", "Redis URL for cross-replica change events; empty runs a single replica")
	fs.StringVar(&c.RedisChannel, "redis-channel", notify.DefaultChannel, "Redis pub/sub channel")
	fs.StringVar(&c.AuthTokensFile, "auth-tokens-file", "", "JSON file of API token hashes and roles; setting it turns authentication on")
	fs.DurationVar(&c.ReconcileInterval, "reconcile-interval", 10*time.Second, "how often watcher state is checked against the database")
	fs.DurationVar(&c.RolloutInterval, "rollout-interval", 5*time.Second, "how often due rollout stages are advanced; 0 disables the rollout controller on this replica")
	fs.DurationVar(&c.ShutdownTimeout, "shutdown-timeout", 15*time.Second, "grace period for in-flight requests on shutdown, for gRPC and then again for HTTP")
	fs.StringVar(&logLevel, "log-level", "info", "debug, info, warn or error")
	fs.StringVar(&c.LogFormat, "log-format", "json", "json or text")
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), usageHeader)
		fs.PrintDefaults()
	}
	if err := bindEnv(fs); err != nil {
		return config{}, err
	}
	// The flag package would print a parse error itself, and main prints it
	// again: stay silent while parsing, and show the usage only when asked.
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fs.SetOutput(os.Stderr)
			fs.Usage()
			return config{}, err
		}
		return config{}, fmt.Errorf("%w (controlplane -h lists the flags)", err)
	}
	if fs.NArg() > 0 {
		return config{}, fmt.Errorf("unexpected argument %q (the only command is %s, given before any flag)", fs.Arg(0), commandMigrate)
	}

	if err := c.LogLevel.UnmarshalText([]byte(logLevel)); err != nil {
		return config{}, fmt.Errorf("--log-level: %w", err)
	}
	if c.LogFormat != "json" && c.LogFormat != "text" {
		return config{}, fmt.Errorf("--log-format must be json or text, got %q", c.LogFormat)
	}
	switch c.Store {
	case "postgres":
		if c.DatabaseURL == "" {
			return config{}, fmt.Errorf("--database-url (or %sDATABASE_URL) is required with --store postgres", envPrefix)
		}
	case "memory":
		if c.Command == commandMigrate {
			return config{}, fmt.Errorf("%s needs --store postgres: the memory store has no schema", commandMigrate)
		}
	default:
		return config{}, fmt.Errorf("--store must be postgres or memory, got %q", c.Store)
	}
	switch {
	case c.ReconcileInterval <= 0:
		return config{}, fmt.Errorf("--reconcile-interval must be positive, got %v", c.ReconcileInterval)
	case c.RolloutInterval < 0:
		return config{}, fmt.Errorf("--rollout-interval must not be negative (0 disables the rollout controller), got %v", c.RolloutInterval)
	case c.ShutdownTimeout < 0:
		return config{}, fmt.Errorf("--shutdown-timeout must not be negative, got %v", c.ShutdownTimeout)
	}
	return c, nil
}

// bindEnv names each flag's environment variable in its usage and sets the
// flag from the variable when that is not empty. The flag parses the value,
// so a malformed one is an error instead of quietly becoming the default: a
// shutdown timeout without its unit must not silently stretch past the
// Kubernetes grace period.
func bindEnv(fs *flag.FlagSet) error {
	var errs []error
	fs.VisitAll(func(f *flag.Flag) {
		name := envName(f.Name)
		f.Usage += " ($" + name + ")"
		if v := os.Getenv(name); v != "" {
			if err := f.Value.Set(v); err != nil {
				errs = append(errs, fmt.Errorf("invalid value %q for %s: %w", v, name, err))
			}
		}
	})
	return errors.Join(errs...)
}

// envName returns the environment variable of a flag.
func envName(flagName string) string {
	return envPrefix + strings.ToUpper(strings.ReplaceAll(flagName, "-", "_"))
}
