package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func NewRouter(h *Handler) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.RequestID)

	r.Post("/workflows", h.CreateWorkflow)
	r.Get("/workflows/{workflowID}", h.GetWorkflow)
	r.Post("/workflows/{workflowID}/runs", h.CreateWorkflowRun)
	r.Get("/runs/{runID}", h.GetWorkflowRun)
	r.Get("/runs/{runID}/tasks", h.GetTaskRunsForRun)
	r.Get("/tasks/{taskRunID}", h.GetTaskRun)
	r.Get("/dlq", h.ListDeadLetterTasks)
	r.Handle("/metrics", promhttp.Handler())

	return r
}
