package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type WebhookExecutor struct{}

type webhookConfig struct {
	URL     string          `json:"url"`
	Payload json.RawMessage `json:"payload"`
}

func (e *WebhookExecutor) Execute(ctx context.Context, config json.RawMessage) Result {
	var cfg webhookConfig
	if err := json.Unmarshal(config, &cfg); err != nil {
		return Result{Err: fmt.Errorf("invalid webhook config: %w", err)}
	}

	var body []byte
	if len(cfg.Payload) > 0 {
		body = cfg.Payload
	} else {
		body = []byte("{}")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", cfg.URL, bytes.NewReader(body))
	if err != nil {
		return Result{Err: fmt.Errorf("create request: %w", err)}
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return Result{Err: fmt.Errorf("webhook request: %w", err)}
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Result{Err: fmt.Errorf("webhook http %d: %s", resp.StatusCode, string(respBody))}
	}

	output, _ := json.Marshal(map[string]any{"status_code": resp.StatusCode})
	return Result{Output: output}
}
