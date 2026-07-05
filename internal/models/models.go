package models

import (
	"encoding/json"
	"math"
	"time"
)

type RunStatus string

const (
	RunStatusPending   RunStatus = "PENDING"
	RunStatusRunning   RunStatus = "RUNNING"
	RunStatusSucceeded RunStatus = "SUCCEEDED"
	RunStatusFailed    RunStatus = "FAILED"
	RunStatusCancelled RunStatus = "CANCELLED"
)

type TaskRunStatus string

const (
	TaskRunStatusPending      TaskRunStatus = "PENDING"
	TaskRunStatusQueued       TaskRunStatus = "QUEUED"
	TaskRunStatusRunning      TaskRunStatus = "RUNNING"
	TaskRunStatusSucceeded    TaskRunStatus = "SUCCEEDED"
	TaskRunStatusFailed       TaskRunStatus = "FAILED"
	TaskRunStatusRetryWaiting TaskRunStatus = "RETRY_WAITING"
	TaskRunStatusSkipped      TaskRunStatus = "SKIPPED"
	TaskRunStatusCancelled    TaskRunStatus = "CANCELLED"
)

type TaskType string

const (
	TaskTypeHTTP    TaskType = "HTTP"
	TaskTypeShell   TaskType = "SHELL"
	TaskTypeDelay   TaskType = "DELAY"
	TaskTypeWebhook TaskType = "WEBHOOK"
)

type RetryPolicyType string

const (
	RetryPolicyExponential RetryPolicyType = "EXPONENTIAL_BACKOFF"
	RetryPolicyFixed       RetryPolicyType = "FIXED"
)

type Workflow struct {
	ID             string          `json:"id"`
	Name           string          `json:"name"`
	Version        int             `json:"version"`
	DefinitionJSON json.RawMessage `json:"definition"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

type WorkflowDefinition struct {
	Name  string    `json:"name"`
	Tasks []TaskDef `json:"tasks"`
}

type TaskDef struct {
	ID          string          `json:"id"`
	Type        TaskType        `json:"type"`
	Config      json.RawMessage `json:"config"`
	DependsOn   []string        `json:"depends_on"`
	MaxAttempts int             `json:"max_attempts"`
	RetryPolicy *RetryPolicy    `json:"retry_policy"`
}

type RetryPolicy struct {
	Type                RetryPolicyType `json:"type"`
	InitialDelaySeconds int             `json:"initial_delay_seconds"`
	MaxDelaySeconds     int             `json:"max_delay_seconds"`
}

type WorkflowRun struct {
	ID          string     `json:"id"`
	WorkflowID  string     `json:"workflow_id"`
	Status      RunStatus  `json:"status"`
	CreatedAt   time.Time  `json:"created_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	TriggerType string     `json:"trigger_type"`
}

type TaskRun struct {
	ID              string          `json:"id"`
	RunID           string          `json:"run_id"`
	TaskID          string          `json:"task_id"`
	TaskType        TaskType        `json:"task_type"`
	TaskConfig      json.RawMessage `json:"task_config"`
	Status          TaskRunStatus   `json:"status"`
	Attempt         int             `json:"attempt"`
	MaxAttempts     int             `json:"max_attempts"`
	RetryPolicyJSON json.RawMessage `json:"retry_policy,omitempty"`
	ScheduledAt     *time.Time      `json:"scheduled_at,omitempty"`
	StartedAt       *time.Time      `json:"started_at,omitempty"`
	CompletedAt     *time.Time      `json:"completed_at,omitempty"`
	LeaseOwner      *string         `json:"lease_owner,omitempty"`
	LeaseExpiresAt  *time.Time      `json:"lease_expires_at,omitempty"`
	NextRetryAt     *time.Time      `json:"next_retry_at,omitempty"`
	ErrorMessage    *string         `json:"error_message,omitempty"`
	OutputJSON      json.RawMessage `json:"output,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
}

type TaskEvent struct {
	EventID   int64     `json:"event_id"`
	RunID     string    `json:"run_id"`
	TaskRunID *string   `json:"task_run_id,omitempty"`
	TaskID    *string   `json:"task_id,omitempty"`
	EventType string    `json:"event_type"`
	Message   string    `json:"message"`
	CreatedAt time.Time `json:"created_at"`
}

type DeadLetterTask struct {
	DLQID      string          `json:"dlq_id"`
	TaskRunID  string          `json:"task_run_id"`
	RunID      string          `json:"run_id"`
	TaskID     string          `json:"task_id"`
	TaskType   TaskType        `json:"task_type"`
	FinalError string          `json:"final_error"`
	Attempts   int             `json:"attempts"`
	TaskConfig json.RawMessage `json:"task_config,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
}

func ComputeNextRetryAt(attempt int, policy *RetryPolicy) time.Time {
	if policy == nil {
		delaySecs := attempt * 5
		if delaySecs > 60 {
			delaySecs = 60
		}
		return time.Now().Add(time.Duration(delaySecs) * time.Second)
	}
	switch policy.Type {
	case RetryPolicyExponential:
		delaySecs := float64(policy.InitialDelaySeconds) * math.Pow(2, float64(attempt-1))
		if delaySecs > float64(policy.MaxDelaySeconds) {
			delaySecs = float64(policy.MaxDelaySeconds)
		}
		return time.Now().Add(time.Duration(delaySecs) * time.Second)
	default:
		return time.Now().Add(time.Duration(policy.InitialDelaySeconds) * time.Second)
	}
}
