package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/shireenmeher/distributed-workflow-orchestrator/internal/config"
	"github.com/shireenmeher/distributed-workflow-orchestrator/internal/db"
	"github.com/shireenmeher/distributed-workflow-orchestrator/internal/metrics"
	"github.com/shireenmeher/distributed-workflow-orchestrator/internal/scheduler"
)

func main() {
	metrics.Register()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := config.Load()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	pool, err := db.Open(ctx, cfg.DatabaseURL)
	cancel()
	if err != nil {
		logger.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	store := db.NewStore(pool)
	sched := scheduler.New(store, cfg.SchedulerIntervalMS, logger)

	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go sched.Run(runCtx)
	if cfg.MetricsPort != "" {
		go metrics.Serve(runCtx, cfg.MetricsPort, logger)
	}

	<-quit
	logger.Info("shutting down scheduler")
	runCancel()
}
