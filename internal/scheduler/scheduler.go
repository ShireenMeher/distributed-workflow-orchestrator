package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/shireenmeher/distributed-workflow-orchestrator/internal/db"
	"github.com/shireenmeher/distributed-workflow-orchestrator/internal/metrics"
	"github.com/shireenmeher/distributed-workflow-orchestrator/internal/models"
)

type Scheduler struct {
	store    *db.Store
	interval time.Duration
	logger   *slog.Logger
}

func New(store *db.Store, intervalMS int, logger *slog.Logger) *Scheduler {
	return &Scheduler{
		store:    store,
		interval: time.Duration(intervalMS) * time.Millisecond,
		logger:   logger,
	}
}

func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	s.logger.Info("scheduler started", "interval_ms", s.interval.Milliseconds())

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("scheduler stopping")
			return
		case <-ticker.C:
			s.tick(ctx)
		}
	}
}

func (s *Scheduler) tick(ctx context.Context) {
	start := time.Now()
	defer func() {
		metrics.SchedulerLoopDuration.Observe(time.Since(start).Seconds())
	}()

	// Handle orphaned tasks first
	orphaned, err := s.store.FindOrphanedTasks(ctx)
	if err != nil {
		s.logger.Error("find orphaned tasks", "error", err)
	}
	metrics.TaskLeaseExpiredTotal.Add(float64(len(orphaned)))
	for _, tr := range orphaned {
		s.logger.Warn("requeueing orphaned task", "task_run_id", tr.ID, "task_id", tr.TaskID)
		if err := s.store.RequeueOrphanedTask(ctx, tr.ID); err != nil {
			s.logger.Error("requeue orphaned task", "task_run_id", tr.ID, "error", err)
			continue
		}
		_ = s.store.LogEvent(ctx, &models.TaskEvent{
			RunID:     tr.RunID,
			TaskRunID: &tr.ID,
			TaskID:    &tr.TaskID,
			EventType: "TASK_LEASE_EXPIRED",
			Message:   "lease expired, task requeued",
		})
	}

	// Re-queue retry-ready tasks
	retryReady, err := s.store.FindRetryReadyTasks(ctx)
	if err != nil {
		s.logger.Error("find retry ready tasks", "error", err)
	}
	for _, tr := range retryReady {
		s.logger.Info("requeueing retry task", "task_run_id", tr.ID, "task_id", tr.TaskID)
		if err := s.store.MarkTaskQueued(ctx, tr.ID); err != nil {
			s.logger.Error("mark retry task queued", "task_run_id", tr.ID, "error", err)
			continue
		}
		metrics.TaskRetriesTotal.WithLabelValues(string(tr.TaskType)).Inc()
		_ = s.store.LogEvent(ctx, &models.TaskEvent{
			RunID:     tr.RunID,
			TaskRunID: &tr.ID,
			TaskID:    &tr.TaskID,
			EventType: "TASK_RETRY_QUEUED",
			Message:   "retry delay elapsed, task requeued",
		})
	}

	// Process active runs
	runIDs, err := s.store.FindActiveRuns(ctx)
	if err != nil {
		s.logger.Error("find active runs", "error", err)
		return
	}

	for _, runID := range runIDs {
		s.processRun(ctx, runID)
	}

	// Update gauge metrics for queued and running tasks
	if queuedCount, err := s.store.CountTasksByStatus(ctx, models.TaskRunStatusQueued); err != nil {
		s.logger.Error("count queued tasks", "error", err)
	} else {
		metrics.QueuedTasksGauge.Set(float64(queuedCount))
	}
	if runningCount, err := s.store.CountTasksByStatus(ctx, models.TaskRunStatusRunning); err != nil {
		s.logger.Error("count running tasks", "error", err)
	} else {
		metrics.RunningTasksGauge.Set(float64(runningCount))
	}
}

func (s *Scheduler) processRun(ctx context.Context, runID string) {
	// Find tasks ready to run
	ready, err := s.store.FindReadyTasksForRun(ctx, runID)
	if err != nil {
		s.logger.Error("find ready tasks", "run_id", runID, "error", err)
		return
	}

	for _, tr := range ready {
		if err := s.store.MarkTaskQueued(ctx, tr.ID); err != nil {
			s.logger.Error("mark task queued", "task_run_id", tr.ID, "error", err)
			continue
		}
		s.logger.Info("task queued", "run_id", runID, "task_id", tr.TaskID, "task_run_id", tr.ID)
		_ = s.store.LogEvent(ctx, &models.TaskEvent{
			RunID:     runID,
			TaskRunID: &tr.ID,
			TaskID:    &tr.TaskID,
			EventType: "TASK_QUEUED",
			Message:   "task queued for execution",
		})
	}

	// Cascade failures to dependent tasks
	if err := s.store.CascadeFailureForRun(ctx, runID); err != nil {
		s.logger.Error("cascade failure", "run_id", runID, "error", err)
	}

	// Check if run is complete
	if err := s.store.CheckAndCompleteRun(ctx, runID); err != nil {
		s.logger.Error("check run completion", "run_id", runID, "error", err)
	}
}
