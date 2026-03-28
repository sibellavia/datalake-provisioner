package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPPort          string
	ReadHeaderTimeout time.Duration
	ReadyTimeout      time.Duration

	LogFormat string
	LogLevel  string

	DatabaseURL string

	InternalToken string

	WorkerEnabled      bool
	WorkerPollInterval time.Duration
	WorkerStaleAfter   time.Duration
	WorkerMaxAttempts  int

	RGWEndpoint           string
	RGWAdminPath          string
	RGWRegion             string
	RGWAccessKeyID        string
	RGWSecretAccessKey    string
	RGWInsecureSkipVerify bool
}

func Load() Config {
	return Config{
		HTTPPort:              getEnv("HTTP_PORT", "8081"),
		ReadHeaderTimeout:     time.Duration(getEnvInt("READ_HEADER_TIMEOUT_SECONDS", 5)) * time.Second,
		ReadyTimeout:          time.Duration(getEnvInt("READY_TIMEOUT_SECONDS", 3)) * time.Second,
		LogFormat:             getEnv("LOG_FORMAT", "json"),
		LogLevel:              getEnv("LOG_LEVEL", "info"),
		DatabaseURL:           getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/datalake?sslmode=disable"),
		InternalToken:         getEnv("INTERNAL_TOKEN", "change-me"),
		WorkerEnabled:         getEnvBool("WORKER_ENABLED", true),
		WorkerPollInterval:    time.Duration(getEnvInt("WORKER_POLL_INTERVAL_SECONDS", 2)) * time.Second,
		WorkerStaleAfter:      time.Duration(getEnvInt("WORKER_STALE_AFTER_SECONDS", 120)) * time.Second,
		WorkerMaxAttempts:     getEnvInt("WORKER_MAX_ATTEMPTS", 3),
		RGWEndpoint:           getEnv("RGW_ENDPOINT", "http://localhost:8080"),
		RGWAdminPath:          getEnv("RGW_ADMIN_PATH", "/admin"),
		RGWRegion:             getEnv("RGW_REGION", "us-east-1"),
		RGWAccessKeyID:        getEnv("RGW_ACCESS_KEY_ID", ""),
		RGWSecretAccessKey:    getEnv("RGW_SECRET_ACCESS_KEY", ""),
		RGWInsecureSkipVerify: getEnvBool("RGW_INSECURE_SKIP_VERIFY", false),
	}
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	v := getEnv(key, "")
	if v == "" {
		return fallback
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return i
}

func getEnvBool(key string, fallback bool) bool {
	v := getEnv(key, "")
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}
