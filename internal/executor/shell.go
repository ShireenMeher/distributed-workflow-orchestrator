package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"time"
)

type ShellExecutor struct{}

type shellConfig struct {
	Command        string `json:"command"`
	TimeoutSeconds int    `json:"timeout_seconds"`
}

func (e *ShellExecutor) Execute(ctx context.Context, config json.RawMessage) Result {
	var cfg shellConfig
	if err := json.Unmarshal(config, &cfg); err != nil {
		return Result{Err: fmt.Errorf("invalid shell config: %w", err)}
	}
	if cfg.TimeoutSeconds <= 0 {
		cfg.TimeoutSeconds = 60
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(cfg.TimeoutSeconds)*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "sh", "-c", cfg.Command)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return Result{Err: fmt.Errorf("command failed: %w, output: %s", err, string(out))}
	}

	output, _ := json.Marshal(map[string]string{"stdout": string(out)})
	return Result{Output: output}
}
