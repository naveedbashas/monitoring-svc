package config

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"
)

const (
	defaultListenAddr            = ":8080"
	defaultShutdownTimeout       = 10 * time.Second
	defaultMaxOutstandingMessage = 100
	defaultRedisAddr             = "localhost:6379"
	defaultRedisChannel          = "monitoring:relay"
	defaultRedisDB               = 0
)

// Config captures runtime configuration for the monitoring service.
type Config struct {
	Addr                   string
	ProjectID              string
	SubscriptionID         string
	ShutdownTimeout        time.Duration
	MaxOutstandingMessages int
	Redis                  RedisConfig
}

// RedisConfig describes the Redis connection used for cross-instance fan-out.
type RedisConfig struct {
	Addr     string
	Channel  string
	Password string
	DB       int
}

// Load parses configuration from environment variables and command-line flags.
func Load() (Config, error) {
	cfg := Config{
		Addr:                   getEnv("LISTEN_ADDR", defaultListenAddr),
		ProjectID:              os.Getenv("PUBSUB_PROJECT_ID"),
		SubscriptionID:         os.Getenv("PUBSUB_SUBSCRIPTION_ID"),
		ShutdownTimeout:        getEnvDuration("SHUTDOWN_TIMEOUT", defaultShutdownTimeout),
		MaxOutstandingMessages: getEnvInt("PUBSUB_MAX_OUTSTANDING_MESSAGES", defaultMaxOutstandingMessage),
		Redis: RedisConfig{
			Addr:     getEnv("REDIS_ADDR", defaultRedisAddr),
			Channel:  getEnv("REDIS_CHANNEL", defaultRedisChannel),
			Password: os.Getenv("REDIS_PASSWORD"),
			DB:       getEnvInt("REDIS_DB", defaultRedisDB),
		},
	}

	fs := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	fs.StringVar(&cfg.Addr, "addr", cfg.Addr, "HTTP listen address")
	fs.StringVar(&cfg.ProjectID, "project", cfg.ProjectID, "Google Cloud project ID")
	fs.StringVar(&cfg.SubscriptionID, "subscription", cfg.SubscriptionID, "Pub/Sub subscription ID")
	fs.DurationVar(&cfg.ShutdownTimeout, "shutdown-timeout", cfg.ShutdownTimeout, "Graceful shutdown timeout")
	fs.IntVar(&cfg.MaxOutstandingMessages, "pubsub-max-outstanding", cfg.MaxOutstandingMessages, "Pub/Sub max outstanding messages")
	fs.StringVar(&cfg.Redis.Addr, "redis-addr", cfg.Redis.Addr, "Redis address (host:port)")
	fs.StringVar(&cfg.Redis.Channel, "redis-channel", cfg.Redis.Channel, "Redis pub/sub channel for relaying messages")
	fs.StringVar(&cfg.Redis.Password, "redis-password", cfg.Redis.Password, "Redis password")
	fs.IntVar(&cfg.Redis.DB, "redis-db", cfg.Redis.DB, "Redis database index")

	fs.SetOutput(os.Stderr)
	if err := fs.Parse(os.Args[1:]); err != nil {
		return Config{}, fmt.Errorf("parse flags: %w", err)
	}

	if cfg.ProjectID == "" {
		return Config{}, errors.New("missing project ID (set PUBSUB_PROJECT_ID or use --project)")
	}

	if cfg.SubscriptionID == "" {
		return Config{}, errors.New("missing subscription ID (set PUBSUB_SUBSCRIPTION_ID or use --subscription)")
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if value, ok := os.LookupEnv(key); ok {
		d, err := time.ParseDuration(value)
		if err == nil {
			return d
		}
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if value, ok := os.LookupEnv(key); ok {
		n, err := strconv.Atoi(value)
		if err == nil {
			return n
		}
	}
	return fallback
}
