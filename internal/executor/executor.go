package executor

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/shireenmeher/distributed-workflow-orchestrator/internal/models"
)

type Result struct {
	Output json.RawMessage
	Err    error
}

type Executor interface {
	Execute(ctx context.Context, config json.RawMessage) Result
}

func New(taskType models.TaskType) (Executor, error) {
	switch taskType {
	case models.TaskTypeHTTP:
		return &HTTPExecutor{}, nil
	case models.TaskTypeShell:
		return &ShellExecutor{}, nil
	case models.TaskTypeDelay:
		return &DelayExecutor{}, nil
	case models.TaskTypeWebhook:
		return &WebhookExecutor{}, nil
	default:
		return nil, fmt.Errorf("unknown task type: %s", taskType)
	}
}
