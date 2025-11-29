package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds runtime configuration for the hivemoji service.
type Config struct {
	HiveRPCURL                string
	PostgresDSN               string
	StartBlock                int64
	PollInterval              time.Duration
	CatchupPollInterval       time.Duration
	IncompleteChunkTTL        time.Duration
	IncompleteCleanupInterval time.Duration
	ServerAddr                string
}

// Load reads environment variables and applies defaults.
func Load() (Config, error) {
	cfg := Config{
		HiveRPCURL:                envOr("HIVE_RPC_URL", "https://api.hive.blog"),
		PostgresDSN:               os.Getenv("POSTGRES_DSN"),
		ServerAddr:                envOr("SERVER_ADDR", ":8080"),
		PollInterval:              3 * time.Second,
		CatchupPollInterval:       500 * time.Millisecond,
		IncompleteChunkTTL:        1 * time.Hour,
		IncompleteCleanupInterval: 10 * time.Minute,
		StartBlock:                0,
	}

	if v := os.Getenv("HIVE_POLL_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return cfg, fmt.Errorf("invalid HIVE_POLL_INTERVAL: %w", err)
		}
		cfg.PollInterval = d
	}

	if v := os.Getenv("HIVE_CATCHUP_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return cfg, fmt.Errorf("invalid HIVE_CATCHUP_INTERVAL: %w", err)
		}
		cfg.CatchupPollInterval = d
	}

	if v := os.Getenv("HIVE_INCOMPLETE_TTL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return cfg, fmt.Errorf("invalid HIVE_INCOMPLETE_TTL: %w", err)
		}
		cfg.IncompleteChunkTTL = d
	}

	if v := os.Getenv("HIVE_INCOMPLETE_CLEANUP_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return cfg, fmt.Errorf("invalid HIVE_INCOMPLETE_CLEANUP_INTERVAL: %w", err)
		}
		cfg.IncompleteCleanupInterval = d
	}

	if v := os.Getenv("HIVE_START_BLOCK"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return cfg, fmt.Errorf("invalid HIVE_START_BLOCK: %w", err)
		}
		cfg.StartBlock = n
	}

	if cfg.PostgresDSN == "" {
		return cfg, fmt.Errorf("POSTGRES_DSN is required")
	}

	return cfg, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
