import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

const runsCreated = new Counter('runs_created');
const runsCompleted = new Counter('runs_completed');
const runsFailed = new Counter('runs_failed');
const orphanRecoveries = new Counter('orphan_recoveries_observed');

/**
 * Failure simulation test.
 *
 * This test creates long-running workflows (DELAY tasks) and monitors
 * that the system recovers correctly when workers are killed mid-task.
 *
 * HOW TO RUN:
 *   1. Start the stack: docker compose up --scale worker=3
 *   2. In a separate terminal, run this script:
 *        k6 run load-tests/failure_simulation.js
 *   3. While the test is running, kill one worker:
 *        docker compose kill worker   (kills one instance)
 *   4. Observe in the output that orphan_recoveries_observed increases,
 *      and that runs_completed still increases (scheduler requeued tasks).
 *
 * EXPECTED OUTCOME:
 *   - Killed worker's in-flight tasks have expired leases after LEASE_DURATION_SECONDS
 *   - Scheduler detects expired leases and requeues tasks
 *   - Surviving workers pick up requeued tasks
 *   - All workflow runs eventually complete as SUCCEEDED
 *   - No duplicate SUCCEEDED task executions (idempotency key prevents re-execution)
 */
export const options = {
  scenarios: {
    simulation: {
      executor: 'shared-iterations',
      vus: 5,
      iterations: 100,
      maxDuration: '10m',
    },
  },
  thresholds: {
    runs_failed: ['count<5'],
  },
};

let workflowID = null;

export function setup() {
  // Create a workflow with a long-running task followed by a short one.
  // The long task is likely to be in-flight when a worker is killed.
  const def = {
    name: 'failure_simulation',
    tasks: [
      {
        id: 'long_task',
        type: 'DELAY',
        config: { duration_seconds: 25 },
        depends_on: [],
        max_attempts: 5,
        retry_policy: {
          type: 'FIXED',
          initial_delay_seconds: 2,
          max_delay_seconds: 10,
        },
      },
      {
        id: 'short_task',
        type: 'DELAY',
        config: { duration_seconds: 1 },
        depends_on: ['long_task'],
      },
    ],
  };

  const res = http.post(`${BASE_URL}/workflows`, JSON.stringify(def), {
    headers: { 'Content-Type': 'application/json' },
  });
  check(res, { 'workflow created': (r) => r.status === 201 });
  const wf = JSON.parse(res.body);
  return { workflowID: wf.id };
}

export default function (data) {
  const wfID = data.workflowID;

  const runRes = http.post(`${BASE_URL}/workflows/${wfID}/runs`, null, {
    headers: { 'Content-Type': 'application/json' },
  });
  if (!check(runRes, { 'run created': (r) => r.status === 201 })) return;

  runsCreated.add(1);
  const run = JSON.parse(runRes.body);
  const runID = run.id;

  // Poll for completion, up to 5 minutes
  for (let i = 0; i < 300; i++) {
    sleep(1);

    const statusRes = http.get(`${BASE_URL}/runs/${runID}`);
    if (statusRes.status !== 200) continue;

    const status = JSON.parse(statusRes.body);

    // Check if any task shows evidence of lease expiry recovery
    const tasksRes = http.get(`${BASE_URL}/runs/${runID}/tasks`);
    if (tasksRes.status === 200) {
      const tasks = JSON.parse(tasksRes.body);
      // A task with attempt > 1 that is RUNNING or SUCCEEDED indicates recovery from a crash
      const recovered = tasks.filter(t => t.attempt > 1);
      if (recovered.length > 0) {
        orphanRecoveries.add(recovered.length);
      }
    }

    if (status.status === 'SUCCEEDED') {
      runsCompleted.add(1);
      break;
    }
    if (status.status === 'FAILED') {
      runsFailed.add(1);
      break;
    }
  }
}
