package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/shireenmeher/distributed-workflow-orchestrator/internal/db"
	"github.com/shireenmeher/distributed-workflow-orchestrator/internal/executor"
	"github.com/shireenmeher/distributed-workflow-orchestrator/internal/models"
)

type Worker struct {
	id        string
	store     *db.Store
	poll      time.Duration
	leaseSecs int
	hbSecs    int
	logger    *slog.Logger
}

func New(id string, store *db.Store, pollIntervalMS, leaseSecs, hbSecs int, logger *slog.Logger) *Worker {
	return &Worker{
		id:        id,
		store:     store,
		poll:      time.Duration(pollIntervalMS) * time.Millisecond,
		leaseSecs: leaseSecs,
		hbSecs:    hbSecs,
		logger:    logger,
	}
}

func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.poll)
	defer ticker.Stop()
	w.logger.Info("worker started", "worker_id", w.id)

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("worker stopping", "worker_id", w.id)
			return
		case <-ticker.C:
			w.processNext(ctx)
		}
	}
}

func (w *Worker) processNext(ctx context.Context) {
	taskRun, err := w.store.ClaimTask(ctx, w.id, w.leaseSecs)
	if err != nil {
		w.logger.Error("claim task", "error", err)
		return
	}
	if taskRun == nil {
		return // nothing to do
	}

	w.logger.Info("task claimed", "worker_id", w.id, "task_run_id", taskRun.ID, "task_id", taskRun.TaskID, "task_type", taskRun.TaskType)

	_ = w.store.LogEvent(ctx, &models.TaskEvent{
		RunID:     taskRun.RunID,
		TaskRunID: &taskRun.ID,
		TaskID:    &taskRun.TaskID,
		EventType: "TASK_STARTED",
		Message:   "worker " + w.id + " started execution",
	})

	// Start heartbeat
	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	go w.heartbeat(hbCtx, taskRun.ID)

	// Execute the task
	exec, err := executor.New(taskRun.TaskType)
	if err != nil {
		hbCancel()
		w.failTask(ctx, taskRun, err.Error())
		return
	}

	result := exec.Execute(ctx, taskRun.TaskConfig)
	hbCancel()

	if result.Err != nil {
		w.failTask(ctx, taskRun, result.Err.Error())
		return
	}

	if err := w.store.MarkTaskSucceeded(ctx, taskRun.ID, result.Output); err != nil {
		w.logger.Error("mark task succeeded", "task_run_id", taskRun.ID, "error", err)
		return
	}
	w.logger.Info("task succeeded", "task_run_id", taskRun.ID, "task_id", taskRun.TaskID)
	_ = w.store.LogEvent(ctx, &models.TaskEvent{
		RunID:     taskRun.RunID,
		TaskRunID: &taskRun.ID,
		TaskID:    &taskRun.TaskID,
		EventType: "TASK_SUCCEEDED",
		Message:   "task completed successfully",
	})
}

func (w *Worker) failTask(ctx context.Context, taskRun *models.TaskRun, errMsg string) {
	var policy *models.RetryPolicy
	if len(taskRun.RetryPolicyJSON) > 0 {
		var p models.RetryPolicy
		if err := json.Unmarshal(taskRun.RetryPolicyJSON, &p); err == nil {
			policy = &p
		}
	}

	if err := w.store.MarkTaskFailed(ctx, taskRun.ID, errMsg, policy); err != nil {
		w.logger.Error("mark task failed", "task_run_id", taskRun.ID, "error", err)
		return
	}

	w.logger.Warn("task failed", "task_run_id", taskRun.ID, "task_id", taskRun.TaskID, "error", errMsg)
	_ = w.store.LogEvent(ctx, &models.TaskEvent{
		RunID:     taskRun.RunID,
		TaskRunID: &taskRun.ID,
		TaskID:    &taskRun.TaskID,
		EventType: "TASK_FAILED",
		Message:   errMsg,
	})

	// Move permanently failed tasks (exhausted all attempts) to the dead letter queue.
	if taskRun.Attempt >= taskRun.MaxAttempts {
		if err := w.store.MoveToDeadLetterQueue(ctx, taskRun, errMsg); err != nil {
			w.logger.Error("move to dead letter queue", "task_run_id", taskRun.ID, "error", err)
		} else {
			w.logger.Warn("task moved to dead letter queue", "task_run_id", taskRun.ID, "task_id", taskRun.TaskID)
		}
	}
}

func (w *Worker) heartbeat(ctx context.Context, taskRunID string) {
	ticker := time.NewTicker(time.Duration(w.hbSecs) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.store.UpdateHeartbeat(ctx, taskRunID, w.id, w.leaseSecs); err != nil {
				w.logger.Error("heartbeat failed", "task_run_id", taskRunID, "error", err)
			}
		}
	}
}
