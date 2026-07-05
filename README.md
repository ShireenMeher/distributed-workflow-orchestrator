# Distributed Workflow Orchestrator

A production-style workflow orchestration engine written in Go. Submit a workflow defined as a directed acyclic graph (DAG) of tasks, and the system schedules, distributes, executes, and monitors those tasks across multiple workers — handling retries, failures, crash recovery, and recurring schedules automatically.

Inspired by systems like Temporal, Airflow, and AWS Step Functions.

---

## Architecture

```
                        ┌──────────────────────┐
                        │    Workflow API       │
                        │  REST + /metrics      │
                        └──────────┬───────────┘
                                   │ write workflow + run
                                   ▼
                        ┌──────────────────────┐
                        │       Postgres        │
                        │   (source of truth)   │
                        │  workflows            │
                        │  workflow_runs        │
                        │  task_runs            │
                        │  task_dependencies    │
                        │  task_events          │
                        │  dead_letter_tasks    │
                        │  workflow_schedules   │
                        └──────┬───────┬────────┘
                               │       │
               reads/writes    │       │  SELECT FOR UPDATE
                               │       │  SKIP LOCKED
                    ┌──────────┘       └──────────────┐
                    ▼                                  ▼
       ┌────────────────────┐             ┌────────────────────┐
       │     Scheduler       │             │     Worker(s)       │
       │                    │             │                    │
       │ - cron scanner      │             │ - claim task       │
       │ - find ready tasks  │             │ - idempotency check│
       │ - detect orphans    │             │ - acquire lease    │
       │ - promote retries   │             │ - send heartbeats  │
       │ - cascade failures  │             │ - execute task     │
       │ - mark runs done    │             │ - retry / DLQ      │
       │ - emit metrics      │             │ - emit metrics     │
       └────────────────────┘             └────────────────────┘
```

Three independent binaries share one Postgres database. The database is the queue — no external message broker required.

---

## Core Features

**Scheduling**
- **DAG execution** — tasks run only when all upstream dependencies have succeeded; independent branches execute in parallel
- **Cron scheduling** — recurring workflows via standard cron expressions (`*/5 * * * *`), computed with `next_run_at` advancement

**Reliability**
- **Worker leases** — each task is exclusively owned by one worker for a configurable duration, preventing double-execution across concurrent workers
- **Heartbeats** — workers renew their lease every N seconds; the scheduler detects stale leases and requeues orphaned tasks automatically
- **Retry with exponential backoff** — failed tasks are retried up to `max_attempts` times with fixed or exponential delay policies
- **Cascade failure** — when a task permanently fails, all downstream dependents are marked `SKIPPED` so the run reaches a terminal state
- **Dead letter queue** — permanently failed tasks are written to `dead_letter_tasks` for inspection and replay
- **Idempotency keys** — before executing, a worker checks `run_id:task_id:attempt` to skip tasks that already succeeded, preventing duplicate side effects from crash recovery
- **Crash recovery** — the scheduler is fully stateless; on restart it resumes from the database without manual intervention

**Observability**
- **Prometheus metrics** — 11 instruments exposed on `GET /metrics`: counters, histograms, and gauges covering task lifecycle, scheduler performance, and queue depth
- **Structured JSON logs** — every service uses `slog` with `run_id`, `task_id`, `worker_id` in every log line
- **Append-only event log** — every state transition is recorded in `task_events` for debugging and audit

**Operations**
- **`SELECT FOR UPDATE SKIP LOCKED`** — concurrent workers claim tasks atomically without application-level locks or a separate broker
- **Auto-migration** — migrations run idempotently on startup; all three binaries can start in any order

---

## How It Works

### 1. Workflow Definition and Validation

A workflow is a named collection of tasks with typed configurations and declared dependencies. When `POST /workflows` is called, the API validates the DAG before persisting it:

- All task IDs are unique and non-empty
- All referenced dependencies exist in the task list
- No cycles exist (DFS with white/gray/black node coloring)

### 2. Triggering a Run

`POST /workflows/{id}/runs` seeds the run into Postgres in a single transaction:

1. Insert a `workflow_run` row (status `RUNNING`, `started_at = NOW()`)
2. Insert one `task_run` row per task (status `PENDING`, copy `max_attempts` and `retry_policy_json` from definition)
3. Insert one `task_dependency` row per dependency edge

### 3. Cron Scanner (runs at the start of every scheduler tick)

```
processCronSchedules():
  Find workflow_schedules WHERE enabled = TRUE AND next_run_at <= NOW()
  For each due schedule:
    CreateWorkflowRun(workflow_id)
    Parse cron expression → compute next_run_at
    UpdateScheduleNextRun(schedule_id, next_run_at)
```

### 4. Scheduler Loop (every 1 second)

```
tick():
  0. processCronSchedules()
  1. Find RUNNING tasks with expired leases → requeue (crash recovery)
     → emit task_lease_expired_total metric
  2. Find RETRY_WAITING tasks where next_retry_at <= now → requeue
     → emit task_retries_total metric
  3. For each RUNNING workflow run:
       a. Find PENDING tasks where all dependencies = SUCCEEDED → mark QUEUED
       b. Cascade: mark PENDING/QUEUED tasks whose any dep is FAILED → SKIPPED
       c. If all tasks terminal → mark run SUCCEEDED or FAILED
  4. Update queued_tasks_count and running_tasks_count gauges
  5. Observe scheduler_loop_duration_seconds histogram
```

The ready-task query uses a correlated `NOT EXISTS` subquery — a task is ready only when there is no dependency that is not yet `SUCCEEDED`.

### 5. Worker Loop (every 500 ms)

```
tick():
  1. BEGIN transaction
       SELECT task_run_id FROM task_runs
       WHERE status = 'QUEUED'
       ORDER BY created_at ASC LIMIT 1
       FOR UPDATE SKIP LOCKED
     UPDATE: status=RUNNING, attempt++, lease_expires_at=NOW()+30s,
             idempotency_key='run_id:task_id:attempt'
     COMMIT
  2. Check idempotency: if this key already SUCCEEDED → skip execution
  3. Start heartbeat goroutine (renews lease every 10s in background)
  4. Execute task via executor
  5. Cancel heartbeat goroutine
  6. On success:
       → MarkTaskSucceeded, emit task_runs_total{status=succeeded}
  7. On failure:
       → if attempt < max_attempts: RETRY_WAITING + next_retry_at
       → if attempt >= max_attempts: FAILED + insert into dead_letter_tasks
       → emit task_runs_total{status=failed}, dead_letter_tasks_total
```

### 6. Dead Letter Queue

When a task exhausts all retry attempts it is permanently failed and a record is inserted into `dead_letter_tasks` containing the task config, final error message, and total attempt count. Inspect via `GET /dlq`.

### 7. Idempotency

Each task claim generates an idempotency key: `run_id:task_id:attempt`. This is stored in `task_runs.idempotency_key` with a unique partial index. Before executing, the worker queries whether a row with this key already has `status = SUCCEEDED`. If so, it skips execution and marks the task done without re-running the task logic — preventing duplicate side effects when a worker restarts after a crash mid-execution.

### 8. Retry Policy

Each task can declare a retry policy:

```json
{
  "max_attempts": 3,
  "retry_policy": {
    "type": "EXPONENTIAL_BACKOFF",
    "initial_delay_seconds": 5,
    "max_delay_seconds": 60
  }
}
```

Delay schedule for `initial_delay_seconds: 5`:

| Attempt | Delay |
|---------|-------|
| 1 → 2   | 5s    |
| 2 → 3   | 10s   |
| 3 → 4   | 20s   |

`type: "FIXED"` uses `initial_delay_seconds` on every retry. Without a policy, the default is `attempt × 5s`, capped at 60s.

### 9. Task Status State Machine

```
PENDING ──► QUEUED ──► RUNNING ──► SUCCEEDED
                          │
                          ├──► RETRY_WAITING ──► QUEUED (loop)
                          │
                          └──► FAILED ──► dead_letter_tasks
                                    │
                                    └──► (cascade) ──► SKIPPED (dependents)
```

### 10. Run Status State Machine

```
RUNNING ──► SUCCEEDED  (all tasks SUCCEEDED or SKIPPED)
        └──► FAILED    (any task permanently FAILED)
```

---

## Data Model

### `workflows`
Stores workflow definitions as JSONB. Each definition is versioned.

| Column | Type | Notes |
|--------|------|-------|
| `workflow_id` | UUID | PK, `gen_random_uuid()` |
| `name` | VARCHAR | Human-readable name |
| `version` | INTEGER | Default 1 |
| `definition_json` | JSONB | Full task graph with configs and retry policies |
| `created_at`, `updated_at` | TIMESTAMPTZ | |

### `workflow_runs`
One row per execution of a workflow.

| Column | Type | Notes |
|--------|------|-------|
| `run_id` | UUID | PK |
| `workflow_id` | UUID | FK → workflows |
| `status` | VARCHAR | `RUNNING`, `SUCCEEDED`, `FAILED`, `CANCELLED` |
| `trigger_type` | VARCHAR | `MANUAL` or `CRON` |
| `started_at`, `completed_at` | TIMESTAMPTZ | |

### `task_runs`
One row per task per run. The scheduler and worker's primary working set.

| Column | Type | Notes |
|--------|------|-------|
| `task_run_id` | UUID | PK |
| `run_id` | UUID | FK → workflow_runs |
| `task_id` | VARCHAR | Matches the task ID in the definition |
| `task_type` | VARCHAR | `HTTP`, `SHELL`, `DELAY`, `WEBHOOK` |
| `task_config` | JSONB | Executor-specific configuration |
| `status` | VARCHAR | Full state machine above |
| `attempt` | INTEGER | Incremented atomically by the worker on each claim |
| `max_attempts` | INTEGER | From task definition, default 3 |
| `retry_policy_json` | JSONB | Retry policy copied from definition |
| `idempotency_key` | VARCHAR | `run_id:task_id:attempt`, unique partial index |
| `lease_owner` | VARCHAR | Worker ID holding the current lease |
| `lease_expires_at` | TIMESTAMPTZ | Scheduler requeues if `now > this` and `status = RUNNING` |
| `last_heartbeat_at` | TIMESTAMPTZ | Updated every heartbeat interval |
| `next_retry_at` | TIMESTAMPTZ | Scheduler requeues when `now >= this` |
| `error_message` | TEXT | Last failure reason |
| `output_json` | JSONB | Task output on success |

### `task_dependencies`
DAG edge list, scoped per run.

| Column | Notes |
|--------|-------|
| `run_id` | |
| `task_id` | The downstream task |
| `depends_on_task_id` | The upstream task that must SUCCEED first |

### `task_events`
Append-only audit log of every state transition.

| Event type | Trigger |
|------------|---------|
| `TASK_QUEUED` | Scheduler marks task ready |
| `TASK_STARTED` | Worker claims task |
| `TASK_SUCCEEDED` | Worker reports success |
| `TASK_FAILED` | Worker reports permanent failure |
| `TASK_RETRY_QUEUED` | Scheduler promotes retry-waiting task |
| `TASK_LEASE_EXPIRED` | Scheduler detects orphaned task and requeues |

### `dead_letter_tasks`
Permanent record of tasks that exhausted all retry attempts.

| Column | Type | Notes |
|--------|------|-------|
| `dlq_id` | UUID | PK |
| `task_run_id` | UUID | Original task run |
| `run_id` | UUID | Parent workflow run |
| `task_id` | VARCHAR | |
| `task_type` | VARCHAR | |
| `final_error` | TEXT | Last error message |
| `attempts` | INTEGER | Total attempts made |
| `task_config` | JSONB | Config at time of failure |

### `workflow_schedules`
Cron-based recurring workflow triggers.

| Column | Type | Notes |
|--------|------|-------|
| `schedule_id` | UUID | PK |
| `workflow_id` | UUID | FK → workflows |
| `cron_expression` | VARCHAR | Standard 5-field cron (`*/5 * * * *`) |
| `next_run_at` | TIMESTAMPTZ | Indexed; scheduler fires when `now >= this` |
| `enabled` | BOOLEAN | Disable without deleting |

---

## Task Types

### HTTP

Makes an HTTP request and returns status code and response body as output.

```json
{
  "id": "fetch_data",
  "type": "HTTP",
  "config": {
    "method": "POST",
    "url": "https://api.example.com/process",
    "headers": { "Authorization": "Bearer token" },
    "body": { "key": "value" },
    "timeout_ms": 5000
  }
}
```

### SHELL

Runs a shell command via `sh -c`. Returns combined stdout/stderr as output.

```json
{
  "id": "transform",
  "type": "SHELL",
  "config": {
    "command": "python3 scripts/transform.py --input /data/raw",
    "timeout_seconds": 120
  }
}
```

### DELAY

Sleeps for a fixed duration. Useful for rate limiting or simulating latency in tests.

```json
{
  "id": "wait",
  "type": "DELAY",
  "config": {
    "duration_seconds": 30
  }
}
```

### WEBHOOK

POSTs a JSON payload to a URL. Useful for notifications or triggering external systems.

```json
{
  "id": "notify",
  "type": "WEBHOOK",
  "config": {
    "url": "https://hooks.example.com/notify",
    "payload": { "message": "pipeline complete" }
  }
}
```

---

## API Reference

### Workflows

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/workflows` | Create a workflow (validates DAG) |
| `GET` | `/workflows/{workflowID}` | Get workflow definition |
| `POST` | `/workflows/{workflowID}/runs` | Trigger a run |

### Runs & Tasks

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/runs/{runID}` | Get run status |
| `GET` | `/runs/{runID}/tasks` | List all task runs for a run |
| `GET` | `/tasks/{taskRunID}` | Get a single task run |

### Schedules

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/schedules` | Create a cron schedule |
| `GET` | `/schedules` | List all schedules |
| `DELETE` | `/schedules/{scheduleID}` | Delete a schedule |

### Operations

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/dlq` | List dead letter tasks |
| `GET` | `/metrics` | Prometheus metrics |

---

### Create a workflow — example

```bash
curl -X POST http://localhost:8080/workflows \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "image_processing_pipeline",
    "tasks": [
      {
        "id": "download",
        "type": "HTTP",
        "config": { "method": "GET", "url": "https://example.com/image.jpg" },
        "depends_on": []
      },
      {
        "id": "resize",
        "type": "SHELL",
        "config": { "command": "python3 resize.py" },
        "depends_on": ["download"],
        "max_attempts": 3,
        "retry_policy": {
          "type": "EXPONENTIAL_BACKOFF",
          "initial_delay_seconds": 5,
          "max_delay_seconds": 60
        }
      },
      {
        "id": "notify",
        "type": "WEBHOOK",
        "config": { "url": "https://hooks.example.com/done" },
        "depends_on": ["resize"]
      }
    ]
  }'
```

### Create a cron schedule — example

```bash
curl -X POST http://localhost:8080/schedules \
  -H 'Content-Type: application/json' \
  -d '{
    "workflow_id": "<workflow-id>",
    "cron_expression": "0 * * * *"
  }'
```

Triggers the workflow at the top of every hour. The cron expression is validated on creation and `next_run_at` is computed immediately.

---

## Running Locally

**Prerequisites:** Docker and Docker Compose.

```bash
git clone https://github.com/ShireenMeher/distributed-workflow-orchestrator
cd distributed-workflow-orchestrator

docker compose up --build
```

This starts:
- Postgres on `localhost:5432`
- API server on `localhost:8080`
- One scheduler instance
- One worker instance

Migrations run automatically on startup. All three services are stateless and can be restarted independently.

**Scale workers:**
```bash
docker compose up --scale worker=3
```

---

## Full Example Walkthrough

```bash
# 1. Create a workflow: fan-out then join
WORKFLOW=$(curl -s -X POST http://localhost:8080/workflows \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "fan_out_demo",
    "tasks": [
      { "id": "root",    "type": "DELAY", "config": {"duration_seconds": 1}, "depends_on": [] },
      { "id": "branch1", "type": "DELAY", "config": {"duration_seconds": 2}, "depends_on": ["root"] },
      { "id": "branch2", "type": "DELAY", "config": {"duration_seconds": 2}, "depends_on": ["root"] },
      { "id": "join",    "type": "DELAY", "config": {"duration_seconds": 1}, "depends_on": ["branch1","branch2"] }
    ]
  }')

WORKFLOW_ID=$(echo $WORKFLOW | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
echo "Created workflow: $WORKFLOW_ID"

# 2. Trigger a run
RUN=$(curl -s -X POST http://localhost:8080/workflows/$WORKFLOW_ID/runs)
RUN_ID=$(echo $RUN | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
echo "Started run: $RUN_ID"

# 3. Poll run status
watch -n 1 "curl -s http://localhost:8080/runs/$RUN_ID/tasks | python3 -m json.tool"

# 4. Inspect Prometheus metrics
curl -s http://localhost:8080/metrics | grep workflow_runs_total

# 5. Set up a recurring schedule (every 5 minutes)
curl -X POST http://localhost:8080/schedules \
  -H 'Content-Type: application/json' \
  -d "{\"workflow_id\": \"$WORKFLOW_ID\", \"cron_expression\": \"*/5 * * * *\"}"

# 6. Inspect dead letter queue after a failure
curl -s http://localhost:8080/dlq | python3 -m json.tool
```

---

## Observability

### Prometheus Metrics

All metrics are exposed on `GET /metrics` (Prometheus text format).

| Metric | Type | Description |
|--------|------|-------------|
| `workflow_runs_total` | Counter | Runs triggered, labeled by `workflow_id` |
| `workflow_runs_succeeded_total` | Counter | Successful runs |
| `workflow_runs_failed_total` | Counter | Failed runs |
| `task_runs_total` | Counter | Task attempts, labeled by `task_type` and `status` |
| `task_duration_seconds` | Histogram | Execution time per task, labeled by `task_type` and `status` |
| `task_retries_total` | Counter | Retries promoted by scheduler, labeled by `task_type` |
| `task_lease_expired_total` | Counter | Orphaned tasks detected and requeued |
| `dead_letter_tasks_total` | Counter | Tasks sent to DLQ, labeled by `task_type` |
| `scheduler_loop_duration_seconds` | Histogram | Time for one full scheduler tick |
| `queued_tasks_count` | Gauge | Current tasks in `QUEUED` status |
| `running_tasks_count` | Gauge | Current tasks in `RUNNING` status |

---

## Load Testing

Load test scripts live in `load-tests/` using [k6](https://k6.io).

### Workflow load test

Tests throughput with three DAG shapes: linear pipeline, fan-out/fan-in, and parallel branches.

```bash
# Start stack with 3 workers
docker compose up --build --scale worker=3

# Run load test (requires k6 installed)
k6 run load-tests/workflow_load.js

# Or run k6 in Docker against the compose stack
docker compose -f docker-compose.yml -f load-tests/docker-compose.k6.yml \
  --profile load-test up --scale worker=3
```

**Configured thresholds:**

| Metric | Threshold |
|--------|-----------|
| Error rate | < 1% |
| Scheduling latency p95 | < 500 ms |
| Scheduling latency p99 | < 1000 ms |
| Run completion time p95 | < 15 s |
| HTTP request duration p99 | < 500 ms |

### Failure simulation

Tests crash recovery: creates 100 long-running workflows, then a worker is killed mid-task. The script observes that `attempt > 1` on recovered tasks, confirming the scheduler detected the expired lease and requeued the task.

```bash
# 1. Start with 3 workers
docker compose up --scale worker=3

# 2. In another terminal, start the simulation
k6 run load-tests/failure_simulation.js

# 3. While the test is running, kill one worker
docker compose kill worker

# Expected: orphan_recoveries_observed increases, runs_completed continues increasing
# Expected: 0 duplicate SUCCEEDED task executions (idempotency key prevents re-execution)
```

---

## Configuration

All configuration is via environment variables.

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgres://orchestrator:orchestrator@localhost:5432/orchestrator?sslmode=disable` | Postgres connection string |
| `API_PORT` | `8080` | Port the API server listens on |
| `WORKER_ID` | hostname | Unique identifier for this worker (used in leases and logs) |
| `SCHEDULER_INTERVAL_MS` | `1000` | How often the scheduler polls |
| `WORKER_POLL_INTERVAL_MS` | `500` | How often a worker polls for a task to claim |
| `LEASE_DURATION_SECONDS` | `30` | How long a worker exclusively owns a task |
| `HEARTBEAT_INTERVAL_SECONDS` | `10` | How often a worker renews its lease |

---

## Project Structure

```
.
├── cmd/
│   ├── api/           # API server binary
│   ├── scheduler/     # Scheduler binary
│   └── worker/        # Worker binary
├── internal/
│   ├── api/           # HTTP handlers and chi router
│   ├── config/        # Environment config loader
│   ├── db/            # pgxpool setup, auto-migration, Store (25 methods)
│   ├── executor/      # Executor interface + HTTP, Shell, Delay, Webhook impls
│   ├── metrics/       # Prometheus metric definitions and Register()
│   ├── models/        # Domain types, status enums, retry backoff logic
│   ├── scheduler/     # Scheduler loop + cron scanner
│   └── worker/        # Worker loop + heartbeat goroutine
├── load-tests/
│   ├── workflow_load.js        # k6 throughput test
│   ├── failure_simulation.js   # k6 crash recovery test
│   └── docker-compose.k6.yml  # Compose override for load testing
├── migrations/
│   └── 001_initial.sql
├── Dockerfile         # Multi-stage; SERVICE arg selects which binary to run
└── docker-compose.yml
```

---

## Roadmap

- [ ] `POST /runs/{id}/cancel` — cancel in-flight runs, mark PENDING/QUEUED tasks as CANCELLED
- [ ] `POST /tasks/{id}/retry` — manually replay a dead letter task
- [ ] Redis Streams queue — replace DB polling with stream consumer groups for higher throughput
- [ ] Grafana dashboard — visualise `task_duration_seconds`, `queued_tasks_count`, and failure rates
- [ ] gRPC API alongside REST
