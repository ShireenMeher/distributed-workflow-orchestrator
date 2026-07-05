package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type DelayExecutor struct{}

type delayConfig struct {
	DurationSeconds int `json:"duration_seconds"`
}

func (e *DelayExecutor) Execute(ctx context.Context, config json.RawMessage) Result {
	var cfg delayConfig
	if err := json.Unmarshal(config, &cfg); err != nil {
		return Result{Err: fmt.Errorf("invalid delay config: %w", err)}
	}

	select {
	case <-time.After(time.Duration(cfg.DurationSeconds) * time.Second):
		output, _ := json.Marshal(map[string]int{"slept_seconds": cfg.DurationSeconds})
		return Result{Output: output}
	case <-ctx.Done():
		return Result{Err: ctx.Err()}
	}
}
