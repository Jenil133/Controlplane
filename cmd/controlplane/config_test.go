package main

import (
	"log/slog"
	"testing"
	"time"
)

func TestParseConfigDefaultsAndEnv(t *testing.T) {
	t.Setenv("CONTROLPLANE_DATABASE_URL", "postgres://example")
	t.Setenv("CONTROLPLANE_RECONCILE_INTERVAL", "3s")
	t.Setenv("CONTROLPLANE_MIGRATE", "false")

	c, err := parseConfig(nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.GRPCAddr != ":9090" || c.Store != "postgres" || c.DatabaseURL != "postgres://example" {
		t.Fatalf("config = %+v", c)
	}
	if c.ReconcileInterval != 3*time.Second || c.Migrate {
		t.Fatalf("env overrides not applied: %+v", c)
	}
	if c.LogLevel != slog.LevelInfo {
		t.Fatalf("log level = %v", c.LogLevel)
	}
}

func TestParseConfigFlagsBeatEnv(t *testing.T) {
	t.Setenv("CONTROLPLANE_GRPC_ADDR", ":1111")
	c, err := parseConfig([]string{"--store", "memory", "--grpc-addr", ":2222", "--log-level", "debug"})
	if err != nil {
		t.Fatal(err)
	}
	if c.GRPCAddr != ":2222" || c.LogLevel != slog.LevelDebug {
		t.Fatalf("config = %+v", c)
	}
}

func TestParseConfigRejectsBadInput(t *testing.T) {
	t.Setenv("CONTROLPLANE_DATABASE_URL", "")
	for _, args := range [][]string{
		{"--store", "postgres"},
		{"--store", "sqlite"},
		{"--store", "memory", "--log-format", "xml"},
		{"--store", "memory", "--log-level", "loud"},
	} {
		if _, err := parseConfig(args); err == nil {
			t.Errorf("parseConfig(%v) succeeded, want error", args)
		}
	}
}
