package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type HTTPExecutor struct{}

type httpConfig struct {
	Method    string            `json:"method"`
	URL       string            `json:"url"`
	Headers   map[string]string `json:"headers"`
	Body      json.RawMessage   `json:"body"`
	TimeoutMS int               `json:"timeout_ms"`
}

func (e *HTTPExecutor) Execute(ctx context.Context, config json.RawMessage) Result {
	var cfg httpConfig
	if err := json.Unmarshal(config, &cfg); err != nil {
		return Result{Err: fmt.Errorf("invalid HTTP config: %w", err)}
	}
	if cfg.Method == "" {
		cfg.Method = "GET"
	}
	if cfg.TimeoutMS <= 0 {
		cfg.TimeoutMS = 30000
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(cfg.TimeoutMS)*time.Millisecond)
	defer cancel()

	var bodyReader io.Reader
	if len(cfg.Body) > 0 && string(cfg.Body) != "null" {
		bodyReader = bytes.NewReader(cfg.Body)
	}

	req, err := http.NewRequestWithContext(ctx, cfg.Method, cfg.URL, bodyReader)
	if err != nil {
		return Result{Err: fmt.Errorf("create request: %w", err)}
	}
	if bodyReader != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range cfg.Headers {
		req.Header.Set(k, v)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return Result{Err: fmt.Errorf("http request: %w", err)}
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return Result{Err: fmt.Errorf("read response: %w", err)}
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Result{Err: fmt.Errorf("http %d: %s", resp.StatusCode, string(respBody))}
	}

	output, _ := json.Marshal(map[string]any{
		"status_code": resp.StatusCode,
		"body":        string(respBody),
	})
	return Result{Output: output}
}
