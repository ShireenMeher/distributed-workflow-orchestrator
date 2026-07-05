package db

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shireenmeher/distributed-workflow-orchestrator/internal/models"
)

var ErrNotFound = fmt.Errorf("not found")

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func scanTaskRun(row pgx.Row) (*models.TaskRun, error) {
	var tr models.TaskRun
	var taskConfig, retryPolicy, outputJSON []byte
	var scheduledAt, startedAt, completedAt, leaseExpiresAt, nextRetryAt *time.Time

	err := row.Scan(
		&tr.ID, &tr.RunID, &tr.TaskID, &tr.TaskType,
		&taskConfig, &tr.Status, &tr.Attempt, &tr.MaxAttempts,
		&retryPolicy, &scheduledAt, &startedAt, &completedAt,
		&tr.LeaseOwner, &leaseExpiresAt, &nextRetryAt,
		&tr.ErrorMessage, &outputJSON, &tr.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	tr.TaskConfig = taskConfig
	tr.RetryPolicyJSON = retryPolicy
	tr.OutputJSON = outputJSON
	tr.ScheduledAt = scheduledAt
	tr.StartedAt = startedAt
	tr.CompletedAt = completedAt
	tr.LeaseExpiresAt = leaseExpiresAt
	tr.NextRetryAt = nextRetryAt
	return &tr, nil
}

// CreateWorkflow inserts a new workflow definition and returns the created record.
func (s *Store) CreateWorkflow(ctx context.Context, def *models.WorkflowDefinition) (*models.Workflow, error) {
	defJSON, err := json.Marshal(def)
	if err != nil {
		return nil, fmt.Errorf("marshal definition: %w", err)
	}

	var wf models.Workflow
	var defBytes []byte
	err = s.pool.QueryRow(ctx, `
		INSERT INTO workflows (name, definition_json)
		VALUES ($1, $2)
		RETURNING workflow_id, name, version, definition_json, created_at, updated_at
	`, def.Name, defJSON).Scan(
		&wf.ID, &wf.Name, &wf.Version, &defBytes, &wf.CreatedAt, &wf.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("insert workflow: %w", err)
	}
	wf.DefinitionJSON = defBytes
	return &wf, nil
}

// GetWorkflow retrieves a workflow by ID.
func (s *Store) GetWorkflow(ctx context.Context, workflowID string) (*models.Workflow, error) {
	var wf models.Workflow
	var defBytes []byte
	err := s.pool.QueryRow(ctx, `
		SELECT workflow_id, name, version, definition_json, created_at, updated_at
		FROM workflows
		WHERE workflow_id = $1
	`, workflowID).Scan(
		&wf.ID, &wf.Name, &wf.Version, &defBytes, &wf.CreatedAt, &wf.UpdatedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get workflow: %w", err)
	}
	wf.DefinitionJSON = defBytes
	return &wf, nil
}

// CreateWorkflowRun creates a new workflow run in a transaction, seeding all task_runs and dependencies.
func (s *Store) CreateWorkflowRun(ctx context.Context, workflowID string) (*models.WorkflowRun, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// Get workflow definition
	var defBytes []byte
	err = tx.QueryRow(ctx, `
		SELECT definition_json FROM workflows WHERE workflow_id = $1
	`, workflowID).Scan(&defBytes)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get workflow definition: %w", err)
	}

	var def models.WorkflowDefinition
	if err := json.Unmarshal(defBytes, &def); err != nil {
		return nil, fmt.Errorf("unmarshal definition: %w", err)
	}

	// Insert workflow run
	var run models.WorkflowRun
	err = tx.QueryRow(ctx, `
		INSERT INTO workflow_runs (workflow_id, status, started_at, trigger_type)
		VALUES ($1, 'RUNNING', NOW(), 'MANUAL')
		RETURNING run_id, workflow_id, status, created_at, started_at, completed_at, trigger_type
	`, workflowID).Scan(
		&run.ID, &run.WorkflowID, &run.Status, &run.CreatedAt,
		&run.StartedAt, &run.CompletedAt, &run.TriggerType,
	)
	if err != nil {
		return nil, fmt.Errorf("insert workflow run: %w", err)
	}

	// Insert task runs
	for _, task := range def.Tasks {
		maxAttempts := task.MaxAttempts
		if maxAttempts == 0 {
			maxAttempts = 3
		}

		taskConfig := task.Config
		if len(taskConfig) == 0 {
			taskConfig = json.RawMessage("{}")
		}

		var retryPolicyJSON []byte
		if task.RetryPolicy != nil {
			retryPolicyJSON, err = json.Marshal(task.RetryPolicy)
			if err != nil {
				return nil, fmt.Errorf("marshal retry policy for task %s: %w", task.ID, err)
			}
		}

		if retryPolicyJSON != nil {
			_, err = tx.Exec(ctx, `
				INSERT INTO task_runs (run_id, task_id, task_type, task_config, status, max_attempts, retry_policy_json)
				VALUES ($1, $2, $3, $4, 'PENDING', $5, $6)
			`, run.ID, task.ID, string(task.Type), taskConfig, maxAttempts, retryPolicyJSON)
		} else {
			_, err = tx.Exec(ctx, `
				INSERT INTO task_runs (run_id, task_id, task_type, task_config, status, max_attempts)
				VALUES ($1, $2, $3, $4, 'PENDING', $5)
			`, run.ID, task.ID, string(task.Type), taskConfig, maxAttempts)
		}
		if err != nil {
			return nil, fmt.Errorf("insert task run for task %s: %w", task.ID, err)
		}

		// Insert dependencies
		for _, dep := range task.DependsOn {
			_, err = tx.Exec(ctx, `
				INSERT INTO task_dependencies (run_id, task_id, depends_on_task_id)
				VALUES ($1, $2, $3)
				ON CONFLICT DO NOTHING
			`, run.ID, task.ID, dep)
			if err != nil {
				return nil, fmt.Errorf("insert dependency %s -> %s: %w", task.ID, dep, err)
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}

	return &run, nil
}

// GetWorkflowRun retrieves a workflow run by ID.
func (s *Store) GetWorkflowRun(ctx context.Context, runID string) (*models.WorkflowRun, error) {
	var run models.WorkflowRun
	err := s.pool.QueryRow(ctx, `
		SELECT run_id, workflow_id, status, created_at, started_at, completed_at, trigger_type
		FROM workflow_runs
		WHERE run_id = $1
	`, runID).Scan(
		&run.ID, &run.WorkflowID, &run.Status, &run.CreatedAt,
		&run.StartedAt, &run.CompletedAt, &run.TriggerType,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get workflow run: %w", err)
	}
	return &run, nil
}

// GetTaskRunsForRun retrieves all task runs for a workflow run.
func (s *Store) GetTaskRunsForRun(ctx context.Context, runID string) ([]*models.TaskRun, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT task_run_id, run_id, task_id, task_type,
		       task_config, status, attempt, max_attempts,
		       retry_policy_json, scheduled_at, started_at, completed_at,
		       lease_owner, lease_expires_at, next_retry_at,
		       error_message, output_json, created_at
		FROM task_runs
		WHERE run_id = $1
		ORDER BY created_at ASC
	`, runID)
	if err != nil {
		return nil, fmt.Errorf("query task runs: %w", err)
	}
	defer rows.Close()

	var result []*models.TaskRun
	for rows.Next() {
		tr, err := scanTaskRun(rows)
		if err != nil {
			return nil, fmt.Errorf("scan task run: %w", err)
		}
		result = append(result, tr)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return result, nil
}

// GetTaskRun retrieves a single task run by ID.
func (s *Store) GetTaskRun(ctx context.Context, taskRunID string) (*models.TaskRun, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT task_run_id, run_id, task_id, task_type,
		       task_config, status, attempt, max_attempts,
		       retry_policy_json, scheduled_at, started_at, completed_at,
		       lease_owner, lease_expires_at, next_retry_at,
		       error_message, output_json, created_at
		FROM task_runs
		WHERE task_run_id = $1
	`, taskRunID)
	tr, err := scanTaskRun(row)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get task run: %w", err)
	}
	return tr, nil
}

// FindActiveRuns returns run IDs of all RUNNING workflow runs.
func (s *Store) FindActiveRuns(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT run_id FROM workflow_runs WHERE status = 'RUNNING'
	`)
	if err != nil {
		return nil, fmt.Errorf("query active runs: %w", err)
	}
	defer rows.Close()

	var runIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan run_id: %w", err)
		}
		runIDs = append(runIDs, id)
	}
	return runIDs, rows.Err()
}

// FindReadyTasksForRun finds PENDING task runs where all dependencies have SUCCEEDED.
func (s *Store) FindReadyTasksForRun(ctx context.Context, runID string) ([]*models.TaskRun, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT tr.task_run_id, tr.run_id, tr.task_id, tr.task_type, tr.task_config,
		       tr.status, tr.attempt, tr.max_attempts, tr.retry_policy_json,
		       tr.scheduled_at, tr.started_at, tr.completed_at,
		       tr.lease_owner, tr.lease_expires_at, tr.next_retry_at,
		       tr.error_message, tr.output_json, tr.created_at
		FROM task_runs tr
		WHERE tr.run_id = $1
		  AND tr.status = 'PENDING'
		  AND NOT EXISTS (
		    SELECT 1
		    FROM task_dependencies td
		    JOIN task_runs dep ON dep.run_id = td.run_id AND dep.task_id = td.depends_on_task_id
		    WHERE td.run_id = tr.run_id
		      AND td.task_id = tr.task_id
		      AND dep.status != 'SUCCEEDED'
		  )
	`, runID)
	if err != nil {
		return nil, fmt.Errorf("query ready tasks: %w", err)
	}
	defer rows.Close()

	var result []*models.TaskRun
	for rows.Next() {
		tr, err := scanTaskRun(rows)
		if err != nil {
			return nil, fmt.Errorf("scan task run: %w", err)
		}
		result = append(result, tr)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return result, nil
}

// MarkTaskQueued transitions a task from PENDING or RETRY_WAITING to QUEUED.
func (s *Store) MarkTaskQueued(ctx context.Context, taskRunID string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE task_runs
		SET status = 'QUEUED', scheduled_at = NOW()
		WHERE task_run_id = $1
		  AND status IN ('PENDING', 'RETRY_WAITING')
	`, taskRunID)
	return err
}

// FindRetryReadyTasks finds tasks in RETRY_WAITING state whose retry time has passed.
func (s *Store) FindRetryReadyTasks(ctx context.Context) ([]*models.TaskRun, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT task_run_id, run_id, task_id, task_type, task_config,
		       status, attempt, max_attempts, retry_policy_json,
		       scheduled_at, started_at, completed_at,
		       lease_owner, lease_expires_at, next_retry_at,
		       error_message, output_json, created_at
		FROM task_runs
		WHERE status = 'RETRY_WAITING'
		  AND next_retry_at <= NOW()
	`)
	if err != nil {
		return nil, fmt.Errorf("query retry ready tasks: %w", err)
	}
	defer rows.Close()

	var result []*models.TaskRun
	for rows.Next() {
		tr, err := scanTaskRun(rows)
		if err != nil {
			return nil, fmt.Errorf("scan task run: %w", err)
		}
		result = append(result, tr)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return result, nil
}

// FindOrphanedTasks finds tasks that are RUNNING but have expired leases.
func (s *Store) FindOrphanedTasks(ctx context.Context) ([]*models.TaskRun, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT task_run_id, run_id, task_id, task_type, task_config,
		       status, attempt, max_attempts, retry_policy_json,
		       scheduled_at, started_at, completed_at,
		       lease_owner, lease_expires_at, next_retry_at,
		       error_message, output_json, created_at
		FROM task_runs
		WHERE status = 'RUNNING'
		  AND lease_expires_at < NOW()
	`)
	if err != nil {
		return nil, fmt.Errorf("query orphaned tasks: %w", err)
	}
	defer rows.Close()

	var result []*models.TaskRun
	for rows.Next() {
		tr, err := scanTaskRun(rows)
		if err != nil {
			return nil, fmt.Errorf("scan task run: %w", err)
		}
		result = append(result, tr)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return result, nil
}

// RequeueOrphanedTask resets an orphaned RUNNING task back to QUEUED.
func (s *Store) RequeueOrphanedTask(ctx context.Context, taskRunID string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE task_runs
		SET status = 'QUEUED',
		    lease_owner = NULL,
		    lease_expires_at = NULL,
		    last_heartbeat_at = NULL
		WHERE task_run_id = $1
		  AND status = 'RUNNING'
		  AND lease_expires_at < NOW()
	`, taskRunID)
	return err
}

// CascadeFailureForRun marks dependent tasks as SKIPPED when their dependencies have FAILED.
func (s *Store) CascadeFailureForRun(ctx context.Context, runID string) error {
	for i := 0; i < 10; i++ {
		tag, err := s.pool.Exec(ctx, `
			UPDATE task_runs
			SET status = 'SKIPPED'
			WHERE run_id = $1
			  AND status IN ('PENDING', 'QUEUED')
			  AND EXISTS (
			    SELECT 1 FROM task_dependencies td
			    JOIN task_runs dep ON dep.run_id = td.run_id AND dep.task_id = td.depends_on_task_id
			    WHERE td.run_id = task_runs.run_id
			      AND td.task_id = task_runs.task_id
			      AND dep.status = 'FAILED'
			  )
		`, runID)
		if err != nil {
			return fmt.Errorf("cascade failure update: %w", err)
		}
		if tag.RowsAffected() == 0 {
			break
		}
	}
	return nil
}

// CheckAndCompleteRun finalizes a workflow run if all tasks have reached a terminal state.
func (s *Store) CheckAndCompleteRun(ctx context.Context, runID string) error {
	var nonTerminalCount int
	err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM task_runs
		WHERE run_id = $1
		  AND status NOT IN ('SUCCEEDED', 'FAILED', 'SKIPPED', 'CANCELLED')
	`, runID).Scan(&nonTerminalCount)
	if err != nil {
		return fmt.Errorf("count non-terminal tasks: %w", err)
	}
	if nonTerminalCount > 0 {
		return nil
	}

	var failedCount int
	err = s.pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM task_runs
		WHERE run_id = $1
		  AND status = 'FAILED'
	`, runID).Scan(&failedCount)
	if err != nil {
		return fmt.Errorf("count failed tasks: %w", err)
	}

	if failedCount > 0 {
		_, err = s.pool.Exec(ctx, `
			UPDATE workflow_runs
			SET status = 'FAILED', completed_at = NOW()
			WHERE run_id = $1
		`, runID)
	} else {
		_, err = s.pool.Exec(ctx, `
			UPDATE workflow_runs
			SET status = 'SUCCEEDED', completed_at = NOW()
			WHERE run_id = $1
		`, runID)
	}
	return err
}

// ClaimTask atomically claims the oldest QUEUED task for a worker.
func (s *Store) ClaimTask(ctx context.Context, workerID string, leaseDurationSecs int) (*models.TaskRun, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	var taskRunID string
	err = tx.QueryRow(ctx, `
		SELECT task_run_id
		FROM task_runs
		WHERE status = 'QUEUED'
		ORDER BY created_at ASC
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`).Scan(&taskRunID)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("select queued task: %w", err)
	}

	_, err = tx.Exec(ctx, `
		UPDATE task_runs
		SET status = 'RUNNING',
		    attempt = attempt + 1,
		    started_at = NOW(),
		    lease_owner = $2,
		    lease_expires_at = NOW() + ($3 * interval '1 second'),
		    last_heartbeat_at = NOW(),
		    idempotency_key = run_id || ':' || task_id || ':' || (attempt + 1)::text
		WHERE task_run_id = $1
	`, taskRunID, workerID, leaseDurationSecs)
	if err != nil {
		return nil, fmt.Errorf("update task to running: %w", err)
	}

	row := tx.QueryRow(ctx, `
		SELECT task_run_id, run_id, task_id, task_type,
		       task_config, status, attempt, max_attempts,
		       retry_policy_json, scheduled_at, started_at, completed_at,
		       lease_owner, lease_expires_at, next_retry_at,
		       error_message, output_json, created_at
		FROM task_runs
		WHERE task_run_id = $1
	`, taskRunID)

	tr, err := scanTaskRun(row)
	if err != nil {
		return nil, fmt.Errorf("scan claimed task: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}

	return tr, nil
}

// UpdateHeartbeat refreshes the lease on a running task.
func (s *Store) UpdateHeartbeat(ctx context.Context, taskRunID, workerID string, leaseDurationSecs int) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE task_runs
		SET last_heartbeat_at = NOW(),
		    lease_expires_at = NOW() + ($3 * interval '1 second')
		WHERE task_run_id = $1
		  AND lease_owner = $2
		  AND status = 'RUNNING'
	`, taskRunID, workerID, leaseDurationSecs)
	return err
}

// MarkTaskSucceeded marks a task run as SUCCEEDED with output.
func (s *Store) MarkTaskSucceeded(ctx context.Context, taskRunID string, output json.RawMessage) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE task_runs
		SET status = 'SUCCEEDED',
		    completed_at = NOW(),
		    output_json = $2
		WHERE task_run_id = $1
	`, taskRunID, output)
	return err
}

// MarkTaskFailed marks a task run as failed or schedules a retry.
func (s *Store) MarkTaskFailed(ctx context.Context, taskRunID string, errMsg string, policy *models.RetryPolicy) error {
	var attempt, maxAttempts int
	err := s.pool.QueryRow(ctx, `
		SELECT attempt, max_attempts FROM task_runs WHERE task_run_id = $1
	`, taskRunID).Scan(&attempt, &maxAttempts)
	if err != nil {
		return fmt.Errorf("get attempt info: %w", err)
	}

	if attempt < maxAttempts {
		nextRetryAt := models.ComputeNextRetryAt(attempt, policy)
		_, err = s.pool.Exec(ctx, `
			UPDATE task_runs
			SET status = 'RETRY_WAITING',
			    error_message = $2,
			    next_retry_at = $3,
			    lease_owner = NULL,
			    lease_expires_at = NULL
			WHERE task_run_id = $1
		`, taskRunID, errMsg, nextRetryAt)
	} else {
		_, err = s.pool.Exec(ctx, `
			UPDATE task_runs
			SET status = 'FAILED',
			    completed_at = NOW(),
			    error_message = $2,
			    lease_owner = NULL,
			    lease_expires_at = NULL
			WHERE task_run_id = $1
		`, taskRunID, errMsg)
	}
	return err
}

// CountTasksByStatus returns the count of task runs in the given status.
func (s *Store) CountTasksByStatus(ctx context.Context, status models.TaskRunStatus) (int, error) {
	var count int
	err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM task_runs WHERE status = $1`, string(status)).Scan(&count)
	return count, err
}

// IsTaskAlreadySucceeded checks if a task with the given idempotency key has already succeeded.
func (s *Store) IsTaskAlreadySucceeded(ctx context.Context, idempotencyKey string) (bool, error) {
	var count int
	err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM task_runs WHERE idempotency_key = $1 AND status = 'SUCCEEDED'
	`, idempotencyKey).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("check idempotency: %w", err)
	}
	return count > 0, nil
}

// MoveToDeadLetterQueue inserts a permanently failed task into the dead_letter_tasks table.
func (s *Store) MoveToDeadLetterQueue(ctx context.Context, tr *models.TaskRun, finalError string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO dead_letter_tasks (task_run_id, run_id, task_id, task_type, final_error, attempts, task_config)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, tr.ID, tr.RunID, tr.TaskID, string(tr.TaskType), finalError, tr.Attempt, tr.TaskConfig)
	if err != nil {
		return fmt.Errorf("insert dead letter task: %w", err)
	}
	return nil
}

// ListDeadLetterTasks returns all dead letter queue entries ordered by creation time descending.
func (s *Store) ListDeadLetterTasks(ctx context.Context) ([]*models.DeadLetterTask, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT dlq_id, task_run_id, run_id, task_id, task_type, final_error, attempts, task_config, created_at
		FROM dead_letter_tasks
		ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("query dead letter tasks: %w", err)
	}
	defer rows.Close()

	var result []*models.DeadLetterTask
	for rows.Next() {
		var d models.DeadLetterTask
		var taskConfig []byte
		if err := rows.Scan(
			&d.DLQID, &d.TaskRunID, &d.RunID, &d.TaskID, &d.TaskType,
			&d.FinalError, &d.Attempts, &taskConfig, &d.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan dead letter task: %w", err)
		}
		d.TaskConfig = taskConfig
		result = append(result, &d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return result, nil
}

// LogEvent inserts a task event record.
func (s *Store) LogEvent(ctx context.Context, event *models.TaskEvent) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO task_events (run_id, task_run_id, task_id, event_type, message)
		VALUES ($1, $2, $3, $4, $5)
	`, event.RunID, event.TaskRunID, event.TaskID, event.EventType, event.Message)
	return err
}
