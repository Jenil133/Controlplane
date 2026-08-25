package main

import (
	"errors"
	"flag"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Jenil133/Controlplane/internal/notify"
)

// clearEnv hides the CONTROLPLANE_* settings of the environment the tests
// run in (an empty variable counts as unset), keeping CONTROLPLANE_TEST_*,
// which configure test infrastructure.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		if strings.HasPrefix(name, envPrefix) && !strings.HasPrefix(name, envPrefix+"TEST_") {
			t.Setenv(name, "")
		}
	}
}

func mustParse(t *testing.T, args ...string) config {
	t.Helper()
	c, err := parseConfig(args)
	if err != nil {
		t.Fatalf("parseConfig(%q): %v", args, err)
	}
	return c
}

func TestParseConfigDefaults(t *testing.T) {
	clearEnv(t)
	got := mustParse(t, "--database-url", "postgres://example")
	want := config{
		GRPCAddr:          ":9090",
		HTTPAddr:          ":8080",
		Store:             "postgres",
		DatabaseURL:       "postgres://example",
		Migrate:           true,
		RedisChannel:      notify.DefaultChannel,
		ReconcileInterval: 10 * time.Second,
		RolloutInterval:   5 * time.Second,
		ShutdownTimeout:   15 * time.Second,
		LogLevel:          slog.LevelInfo,
		LogFormat:         "json",
	}
	if got != want {
		t.Fatalf("config = %+v\nwant     %+v", got, want)
	}
}

// TestParseConfigEnv sets every variable by the exact name deployments use
// (deploy/k8s, deploy/docker-compose.yml).
func TestParseConfigEnv(t *testing.T) {
	clearEnv(t)
	for name, value := range map[string]string{
		"CONTROLPLANE_GRPC_ADDR":          ":19090",
		"CONTROLPLANE_HTTP_ADDR":          ":18080",
		"CONTROLPLANE_STORE":              "postgres",
		"CONTROLPLANE_DATABASE_URL":       "postgres://db/controlplane",
		"CONTROLPLANE_MIGRATE":            "false",
		"CONTROLPLANE_REDIS_URL":          "redis://redis:6379/0",
		"CONTROLPLANE_REDIS_CHANNEL":      "changes",
		"CONTROLPLANE_AUTH_TOKENS_FILE":   "/etc/controlplane/tokens.json",
		"CONTROLPLANE_RECONCILE_INTERVAL": "3s",
		"CONTROLPLANE_ROLLOUT_INTERVAL":   "250ms",
		"CONTROLPLANE_SHUTDOWN_TIMEOUT":   "10s",
		"CONTROLPLANE_LOG_LEVEL":          "debug",
		"CONTROLPLANE_LOG_FORMAT":         "text",
	} {
		t.Setenv(name, value)
	}
	got := mustParse(t)
	want := config{
		GRPCAddr:          ":19090",
		HTTPAddr:          ":18080",
		Store:             "postgres",
		DatabaseURL:       "postgres://db/controlplane",
		Migrate:           false,
		RedisURL:          "redis://redis:6379/0",
		RedisChannel:      "changes",
		AuthTokensFile:    "/etc/controlplane/tokens.json",
		ReconcileInterval: 3 * time.Second,
		RolloutInterval:   250 * time.Millisecond,
		ShutdownTimeout:   10 * time.Second,
		LogLevel:          slog.LevelDebug,
		LogFormat:         "text",
	}
	if got != want {
		t.Fatalf("config = %+v\nwant     %+v", got, want)
	}
}

func TestParseConfigFlagsBeatEnv(t *testing.T) {
	clearEnv(t)
	t.Setenv("CONTROLPLANE_GRPC_ADDR", ":1111")
	t.Setenv("CONTROLPLANE_ROLLOUT_INTERVAL", "1m")
	t.Setenv("CONTROLPLANE_AUTH_TOKENS_FILE", "/env/tokens.json")
	c := mustParse(t, "--store", "memory", "--grpc-addr", ":2222", "--log-level", "debug",
		"--rollout-interval", "0", "--auth-tokens-file", "/flag/tokens.json")
	if c.GRPCAddr != ":2222" || c.LogLevel != slog.LevelDebug || c.RolloutInterval != 0 || c.AuthTokensFile != "/flag/tokens.json" {
		t.Fatalf("config = %+v", c)
	}
}

func TestParseConfigEmptyEnvIsUnset(t *testing.T) {
	clearEnv(t)
	for _, name := range []string{"CONTROLPLANE_GRPC_ADDR", "CONTROLPLANE_ROLLOUT_INTERVAL", "CONTROLPLANE_MIGRATE"} {
		t.Setenv(name, "")
	}
	c := mustParse(t, "--store", "memory")
	if c.GRPCAddr != ":9090" || c.RolloutInterval != 5*time.Second || !c.Migrate {
		t.Fatalf("empty variables were not ignored: %+v", c)
	}
}

func TestParseConfigMigrateCommand(t *testing.T) {
	clearEnv(t)
	t.Setenv("CONTROLPLANE_DATABASE_URL", "postgres://db/controlplane")
	c := mustParse(t, "migrate", "--log-format", "text")
	if c.Command != commandMigrate || c.DatabaseURL != "postgres://db/controlplane" || c.LogFormat != "text" {
		t.Fatalf("config = %+v", c)
	}
	if c := mustParse(t); c.Command != "" {
		t.Fatalf("without a command: Command = %q, want empty (serve)", c.Command)
	}
}

func TestParseConfigRejectsBadInput(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  map[string]string
		args []string
		// want is a substring of the error, when it matters.
		want string
	}{
		{name: "postgres without database url", args: []string{"--store", "postgres"}},
		{name: "unknown store", args: []string{"--store", "sqlite"}},
		{name: "unknown log format", args: []string{"--store", "memory", "--log-format", "xml"}},
		{name: "unknown log level", args: []string{"--store", "memory", "--log-level", "loud"}},
		{name: "negative rollout interval", args: []string{"--store", "memory", "--rollout-interval", "-1s"}},
		{name: "zero reconcile interval", args: []string{"--store", "memory", "--reconcile-interval", "0"}},
		{name: "negative shutdown timeout", args: []string{"--store", "memory", "--shutdown-timeout", "-5s"}},
		{name: "migrate the memory store", args: []string{"migrate", "--store", "memory"}},
		{name: "unknown flag", args: []string{"--nope"}, want: "flag provided but not defined: -nope"},
		{name: "duration without unit in flag", args: []string{"--store", "memory", "--rollout-interval", "5"}, want: "rollout-interval"},
		{name: "command after flags", args: []string{"--store", "memory", "migrate"}, want: `unexpected argument "migrate"`},
		{name: "unknown command", args: []string{"serve", "--store", "memory"}, want: `unexpected argument "serve"`},
		{
			name: "duration without unit in env",
			env:  map[string]string{"CONTROLPLANE_SHUTDOWN_TIMEOUT": "10"},
			args: []string{"--store", "memory"},
			want: "CONTROLPLANE_SHUTDOWN_TIMEOUT",
		},
		{
			name: "malformed bool in env",
			env:  map[string]string{"CONTROLPLANE_MIGRATE": "yes please"},
			args: []string{"--store", "memory"},
			want: "CONTROLPLANE_MIGRATE",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearEnv(t)
			for name, value := range tc.env {
				t.Setenv(name, value)
			}
			_, err := parseConfig(tc.args)
			if err == nil {
				t.Fatalf("parseConfig(%q) succeeded, want an error", tc.args)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("parseConfig(%q) = %v, want an error mentioning %q", tc.args, err, tc.want)
			}
		})
	}
}

func TestParseConfigHelp(t *testing.T) {
	clearEnv(t)
	if _, err := parseConfig([]string{"-h"}); !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("parseConfig(-h) = %v, want flag.ErrHelp", err)
	}
}
