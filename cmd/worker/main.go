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
	"github.com/shireenmeher/distributed-workflow-orchestrator/internal/worker"
)

func main() {
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
	w := worker.New(cfg.WorkerID, store, cfg.WorkerPollIntervalMS, cfg.LeaseDurationSeconds, cfg.HeartbeatIntervalSeconds, logger)

	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go w.Run(runCtx)

	<-quit
	logger.Info("shutting down worker")
	runCancel()
}
