package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/robfig/cron/v3"
	"github.com/shireenmeher/distributed-workflow-orchestrator/internal/db"
	"github.com/shireenmeher/distributed-workflow-orchestrator/internal/metrics"
	"github.com/shireenmeher/distributed-workflow-orchestrator/internal/models"
)

type Handler struct {
	store *db.Store
}

func NewHandler(store *db.Store) *Handler {
	return &Handler{store: store}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func (h *Handler) CreateWorkflow(w http.ResponseWriter, r *http.Request) {
	var def models.WorkflowDefinition
	if err := json.NewDecoder(r.Body).Decode(&def); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if err := validateDAG(&def); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	wf, err := h.store.CreateWorkflow(r.Context(), &def)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, wf)
}

func (h *Handler) GetWorkflow(w http.ResponseWriter, r *http.Request) {
	wfID := chi.URLParam(r, "workflowID")
	wf, err := h.store.GetWorkflow(r.Context(), wfID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			writeError(w, http.StatusNotFound, "workflow not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, wf)
}

func (h *Handler) CreateWorkflowRun(w http.ResponseWriter, r *http.Request) {
	wfID := chi.URLParam(r, "workflowID")
	run, err := h.store.CreateWorkflowRun(r.Context(), wfID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			writeError(w, http.StatusNotFound, "workflow not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	metrics.WorkflowRunsTotal.WithLabelValues(wfID).Inc()
	writeJSON(w, http.StatusCreated, run)
}

func (h *Handler) GetWorkflowRun(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "runID")
	run, err := h.store.GetWorkflowRun(r.Context(), runID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			writeError(w, http.StatusNotFound, "run not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (h *Handler) GetTaskRunsForRun(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "runID")
	tasks, err := h.store.GetTaskRunsForRun(r.Context(), runID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, tasks)
}

func (h *Handler) GetTaskRun(w http.ResponseWriter, r *http.Request) {
	taskRunID := chi.URLParam(r, "taskRunID")
	task, err := h.store.GetTaskRun(r.Context(), taskRunID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			writeError(w, http.StatusNotFound, "task run not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, task)
}

func (h *Handler) CreateSchedule(w http.ResponseWriter, r *http.Request) {
	var body struct {
		WorkflowID     string `json:"workflow_id"`
		CronExpression string `json:"cron_expression"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if body.WorkflowID == "" {
		writeError(w, http.StatusBadRequest, "workflow_id is required")
		return
	}
	if body.CronExpression == "" {
		writeError(w, http.StatusBadRequest, "cron_expression is required")
		return
	}

	// Validate cron expression
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := parser.Parse(body.CronExpression)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid cron_expression: "+err.Error())
		return
	}

	nextRunAt := schedule.Next(time.Now())
	sched, err := h.store.CreateSchedule(r.Context(), body.WorkflowID, body.CronExpression, nextRunAt)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, sched)
}

func (h *Handler) ListSchedules(w http.ResponseWriter, r *http.Request) {
	schedules, err := h.store.ListSchedules(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, schedules)
}

func (h *Handler) DeleteSchedule(w http.ResponseWriter, r *http.Request) {
	scheduleID := chi.URLParam(r, "scheduleID")
	if err := h.store.DeleteSchedule(r.Context(), scheduleID); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) ListDeadLetterTasks(w http.ResponseWriter, r *http.Request) {
	tasks, err := h.store.ListDeadLetterTasks(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, tasks)
}

// validateDAG validates the workflow definition: unique IDs, valid deps, no cycles
func validateDAG(def *models.WorkflowDefinition) error {
	if len(def.Tasks) == 0 {
		return fmt.Errorf("workflow must have at least one task")
	}

	taskSet := make(map[string]bool)
	for _, t := range def.Tasks {
		if t.ID == "" {
			return fmt.Errorf("task ID cannot be empty")
		}
		if taskSet[t.ID] {
			return fmt.Errorf("duplicate task ID: %s", t.ID)
		}
		taskSet[t.ID] = true
		if t.Type == "" {
			return fmt.Errorf("task %s: type cannot be empty", t.ID)
		}
	}

	for _, t := range def.Tasks {
		for _, dep := range t.DependsOn {
			if !taskSet[dep] {
				return fmt.Errorf("task %s: unknown dependency %s", t.ID, dep)
			}
		}
	}

	// cycle detection via DFS
	adj := make(map[string][]string)
	for _, t := range def.Tasks {
		adj[t.ID] = t.DependsOn
	}
	// 0=white, 1=gray, 2=black
	color := make(map[string]int)
	var dfs func(node string) error
	dfs = func(node string) error {
		color[node] = 1
		for _, dep := range adj[node] {
			if color[dep] == 1 {
				return fmt.Errorf("cycle detected involving task %s", node)
			}
			if color[dep] == 0 {
				if err := dfs(dep); err != nil {
					return err
				}
			}
		}
		color[node] = 2
		return nil
	}
	for _, t := range def.Tasks {
		if color[t.ID] == 0 {
			if err := dfs(t.ID); err != nil {
				return err
			}
		}
	}

	return nil
}
