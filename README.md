# Distributed Workflow Orchestrator

A production-style workflow orchestration engine written in Go. Submit a workflow defined as a directed acyclic graph (DAG) of tasks, and the system schedules, distributes, executes, and monitors those tasks across multiple workers — handling retries, failures, and crash recovery automatically.

Inspired by systems like Temporal, Airflow, and AWS Step Functions.

---

## Architecture

```
                        ┌──────────────────────┐
                        │    Workflow API       │
                        │  (REST on :8080)      │
                        └──────────┬───────────┘
                                   │ write workflow + run
                                   ▼
                        ┌──────────────────────┐
                        │   Postgres            │
                        │  (source of truth)    │
                        │  workflows            │
                        │  workflow_runs        │
                        │  task_runs            │
                        │  task_dependencies    │
                        │  task_events          │
                        └──────┬───────┬────────┘
                               │       │
               reads/writes    │       │  SELECT FOR UPDATE
                               │       │  SKIP LOCKED
                    ┌──────────┘       └──────────────┐
                    ▼                                  ▼
       ┌────────────────────┐             ┌────────────────────┐
       │    Scheduler        │             │    Worker(s)        │
       │                    │             │                    │
       │ - find ready tasks  │             │ - claim task       │
       │ - detect orphans    │             │ - acquire lease    │
       │ - promote retries   │             │ - send heartbeats  │
       │ - cascade failures  │             │ - execute task     │
       │ - mark runs done    │             │ - retry on failure │
       └────────────────────┘             └────────────────────┘
```

Three independent binaries share one Postgres database. The database is the queue — no external message broker required in v1.

---

## Core Features

- **DAG scheduling** — tasks execute only when all upstream dependencies have succeeded; independent branches run in parallel
- **Worker leases** — each task is exclusively owned by one worker for a configurable duration, preventing double-execution across concurrent workers
- **Heartbeats** — workers refresh their lease every N seconds; the scheduler detects stale leases and requeues orphaned tasks automatically
- **Retry with exponential backoff** — failed tasks are retried up to `max_attempts` times with configurable delay policies
- **Cascade failure** — when a task permanently fails, all downstream dependents are marked `SKIPPED` so the run can still reach a terminal state
- **Crash recovery** — the scheduler is fully stateless; on restart it resumes from the database without manual intervention
- **Structured event log** — every task state transition (`TASK_QUEUED`, `TASK_STARTED`, `TASK_SUCCEEDED`, `TASK_FAILED`, `TASK_RETRY_QUEUED`, `TASK_LEASE_EXPIRED`) is appended to `task_events` for debugging and audit
- **`SELECT FOR UPDATE SKIP LOCKED`** — concurrent workers claim tasks without races or distributed locks

---

## How It Works

### 1. Workflow Definition and Validation

A workflow is a named collection of tasks with typed configurations and declared dependencies. When `POST /workflows` is called, the API validates the DAG before persisting it:

- All task IDs are unique
- All referenced dependencies exist
- No cycles exist (DFS with white/gray/black node coloring)

### 2. Triggering a Run

`POST /workflows/{id}/runs` seeds the run into Postgres atomically:

1. Insert a `workflow_run` row (status `RUNNING`)
2. Insert one `task_run` row per task (status `PENDING`)
3. Insert one `task_dependency` row per dependency edge

### 3. Scheduler Loop (every 1 second)

```
tick():
  1. Find RUNNING tasks with expired leases → requeue them (crash recovery)
  2. Find RETRY_WAITING tasks where next_retry_at <= now → requeue them
  3. For each active run:
       a. Find PENDING tasks where all dependencies = SUCCEEDED → mark QUEUED
       b. Cascade: mark PENDING tasks whose dependency is FAILED → SKIPPED
       c. If all tasks are terminal → mark run SUCCEEDED or FAILED
```

The ready-task query uses a correlated `NOT EXISTS` subquery — a task is ready only if there is no dependency that is not yet `SUCCEEDED`.

### 4. Worker Loop (every 500 ms)

```
tick():
  1. BEGIN transaction
     SELECT task_run_id FROM task_runs
     WHERE status = 'QUEUED'
     ORDER BY created_at ASC
     LIMIT 1
     FOR UPDATE SKIP LOCKED
  2. Mark task RUNNING, increment attempt, set lease_expires_at = now + 30s
  3. COMMIT
  4. Start heartbeat goroutine (renews lease every 10s in background)
  5. Execute task via the appropriate executor
  6. Cancel heartbeat goroutine
  7. On success → SUCCEEDED
     On failure → RETRY_WAITING (if attempts remain) or FAILED (if exhausted)
```

### 5. Retry Policy

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

Without a policy, the default is `attempt × 5s`, capped at 60s.

### 6. Task Status State Machine

```
PENDING ──► QUEUED ──► RUNNING ──► SUCCEEDED
                          │
                          ├──► RETRY_WAITING ──► QUEUED (loop)
                          │
                          └──► FAILED ──► (cascade) ──► SKIPPED (dependents)
```

### 7. Run Status State Machine

```
RUNNING ──► SUCCEEDED  (all tasks SUCCEEDED)
        └──► FAILED    (any task permanently FAILED)
```

---

## Data Model

### `workflows`
Stores workflow definitions as JSONB. Each definition is versioned.

| Column | Type | Notes |
|--------|------|-------|
| `workflow_id` | UUID | Primary key, generated by Postgres |
| `name` | VARCHAR | Human-readable name |
| `version` | INTEGER | Schema version, default 1 |
| `definition_json` | JSONB | Full task graph including configs and retry policies |
| `created_at`, `updated_at` | TIMESTAMPTZ | |

### `workflow_runs`
One row per execution of a workflow.

| Column | Type | Notes |
|--------|------|-------|
| `run_id` | UUID | |
| `workflow_id` | UUID | FK → workflows |
| `status` | VARCHAR | `RUNNING`, `SUCCEEDED`, `FAILED`, `CANCELLED` |
| `trigger_type` | VARCHAR | `MANUAL` (cron planned for v2) |
| `started_at`, `completed_at` | TIMESTAMPTZ | |

### `task_runs`
One row per task per run. This is the scheduler's and worker's primary working set.

| Column | Type | Notes |
|--------|------|-------|
| `task_run_id` | UUID | |
| `run_id` | UUID | FK → workflow_runs |
| `task_id` | VARCHAR | Matches the task ID in the definition |
| `task_type` | VARCHAR | `HTTP`, `SHELL`, `DELAY`, `WEBHOOK` |
| `task_config` | JSONB | Executor-specific configuration |
| `status` | VARCHAR | Full state machine above |
| `attempt` | INTEGER | Incremented by the worker on each claim |
| `max_attempts` | INTEGER | From task definition, default 3 |
| `retry_policy_json` | JSONB | Retry policy copied from definition |
| `lease_owner` | VARCHAR | Worker ID holding the current lease |
| `lease_expires_at` | TIMESTAMPTZ | Scheduler requeues if now > this and status = RUNNING |
| `last_heartbeat_at` | TIMESTAMPTZ | Updated every heartbeat interval |
| `next_retry_at` | TIMESTAMPTZ | Scheduler requeues when now >= this |
| `error_message` | TEXT | Last failure reason |
| `output_json` | JSONB | Task output on success |

### `task_dependencies`
The DAG edge list, scoped per run.

| Column | Notes |
|--------|-------|
| `run_id` | |
| `task_id` | The dependent task |
| `depends_on_task_id` | The upstream task that must SUCCEED first |

### `task_events`
Append-only audit log of every task state transition.

| Event type | Trigger |
|------------|---------|
| `TASK_QUEUED` | Scheduler marks task ready |
| `TASK_STARTED` | Worker claims task |
| `TASK_SUCCEEDED` | Worker reports success |
| `TASK_FAILED` | Worker reports permanent failure |
| `TASK_RETRY_QUEUED` | Scheduler promotes retry-waiting task |
| `TASK_LEASE_EXPIRED` | Scheduler detects orphaned task |

---

## Task Types

### HTTP

Makes an HTTP request. Returns status code and response body as output.

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

Sleeps for a fixed duration. Useful for rate limiting or simulating latency.

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

### Create a workflow

```
POST /workflows
```

**Request body:**
```json
{
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
}
```

**Response:** `201 Created` with workflow object including `id`.

---

### Get a workflow

```
GET /workflows/{workflowID}
```

---

### Trigger a run

```
POST /workflows/{workflowID}/runs
```

**Response:** `201 Created` with run object including `id` and `status: "RUNNING"`.

---

### Get run status

```
GET /runs/{runID}
```

**Response:**
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "workflow_id": "...",
  "status": "SUCCEEDED",
  "created_at": "2024-01-15T10:00:00Z",
  "started_at": "2024-01-15T10:00:00Z",
  "completed_at": "2024-01-15T10:00:45Z",
  "trigger_type": "MANUAL"
}
```

---

### Get all task statuses for a run

```
GET /runs/{runID}/tasks
```

Returns an array of task run objects, each with `status`, `attempt`, `error_message`, `output`, timestamps, and lease info.

---

### Get a single task run

```
GET /tasks/{taskRunID}
```

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

The API server runs migrations automatically on startup. All three services are stateless and can be restarted independently.

**Scale workers:**
```bash
docker compose up --scale worker=3
```

---

## Example Walkthrough

```bash
# 1. Create a workflow with a two-step linear pipeline
WORKFLOW=$(curl -s -X POST http://localhost:8080/workflows \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "demo_pipeline",
    "tasks": [
      {
        "id": "step_one",
        "type": "DELAY",
        "config": { "duration_seconds": 3 },
        "depends_on": []
      },
      {
        "id": "step_two",
        "type": "SHELL",
        "config": { "command": "echo hello from step two" },
        "depends_on": ["step_one"]
      }
    ]
  }')

WORKFLOW_ID=$(echo $WORKFLOW | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
echo "Workflow: $WORKFLOW_ID"

# 2. Trigger a run
RUN=$(curl -s -X POST http://localhost:8080/workflows/$WORKFLOW_ID/runs)
RUN_ID=$(echo $RUN | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
echo "Run: $RUN_ID"

# 3. Poll run status
curl -s http://localhost:8080/runs/$RUN_ID | python3 -m json.tool

# 4. Check individual task statuses
curl -s http://localhost:8080/runs/$RUN_ID/tasks | python3 -m json.tool
```

Expected progression: `step_one` moves through `PENDING → QUEUED → RUNNING → SUCCEEDED`, which unblocks `step_two`, and the run ends as `SUCCEEDED`.

---

## Configuration

All configuration is via environment variables.

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgres://orchestrator:orchestrator@localhost:5432/orchestrator?sslmode=disable` | Postgres connection string |
| `API_PORT` | `8080` | Port the API server listens on |
| `WORKER_ID` | hostname | Unique identifier for this worker (used in leases) |
| `SCHEDULER_INTERVAL_MS` | `1000` | How often the scheduler polls for work |
| `WORKER_POLL_INTERVAL_MS` | `500` | How often a worker polls for a task to claim |
| `LEASE_DURATION_SECONDS` | `30` | How long a worker exclusively owns a task |
| `HEARTBEAT_INTERVAL_SECONDS` | `10` | How often a worker renews its lease |

---

## Project Structure

```
.
├── cmd/
│   ├── api/          # API server binary
│   ├── scheduler/    # Scheduler binary
│   └── worker/       # Worker binary
├── internal/
│   ├── api/          # HTTP handlers and router (chi)
│   ├── config/       # Environment config loader
│   ├── db/           # pgxpool setup, migrations, Store (17 methods)
│   ├── executor/     # Task executor interface + HTTP, Shell, Delay, Webhook impls
│   ├── models/       # Domain types, status enums, retry backoff logic
│   ├── scheduler/    # Scheduler loop
│   └── worker/       # Worker loop + heartbeat goroutine
├── migrations/
│   └── 001_initial.sql
├── Dockerfile        # Multi-stage build; SERVICE arg selects binary
└── docker-compose.yml
```

---

## Roadmap

**v2**
- [ ] `POST /runs/{id}/cancel` — cancel in-flight runs
- [ ] `POST /tasks/{id}/retry` — manually retry a permanently failed task
- [ ] Dead letter queue — move permanently failed tasks to a `dead_letter_tasks` table
- [ ] Redis Streams queue — replace DB polling in workers with stream consumer groups
- [ ] Idempotency keys — prevent duplicate execution on retry using `run_id + task_id + attempt`

**v3**
- [ ] Cron scheduling — recurring runs via `workflow_schedules` table
- [ ] Prometheus `/metrics` endpoint — task durations, retry rates, queue depth, scheduler latency
- [ ] Grafana dashboard
- [ ] k6 load tests with worker failure simulation
- [ ] gRPC API alongside REST
