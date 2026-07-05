package config

import (
	"os"
	"strconv"
)

type Config struct {
	DatabaseURL              string
	APIPort                  string
	WorkerID                 string
	SchedulerIntervalMS      int
	WorkerPollIntervalMS     int
	LeaseDurationSeconds     int
	HeartbeatIntervalSeconds int
}

func Load() *Config {
	hostname, _ := os.Hostname()
	return &Config{
		DatabaseURL:              getEnv("DATABASE_URL", "postgres://orchestrator:orchestrator@localhost:5432/orchestrator?sslmode=disable"),
		APIPort:                  getEnv("API_PORT", "8080"),
		WorkerID:                 getEnv("WORKER_ID", hostname),
		SchedulerIntervalMS:      getEnvInt("SCHEDULER_INTERVAL_MS", 1000),
		WorkerPollIntervalMS:     getEnvInt("WORKER_POLL_INTERVAL_MS", 500),
		LeaseDurationSeconds:     getEnvInt("LEASE_DURATION_SECONDS", 30),
		HeartbeatIntervalSeconds: getEnvInt("HEARTBEAT_INTERVAL_SECONDS", 10),
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
