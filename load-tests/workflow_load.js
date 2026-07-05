import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend, Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

const workflowRunsCreated = new Counter('workflow_runs_created');
const workflowRunsCompleted = new Counter('workflow_runs_completed');
const schedulingLatency = new Trend('scheduling_latency_ms', true);
const runCompletionTime = new Trend('run_completion_time_ms', true);
const errorRate = new Rate('errors');

export const options = {
  scenarios: {
    ramp_up: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '30s', target: 10 },
        { duration: '2m',  target: 20 },
        { duration: '30s', target: 0 },
      ],
    },
  },
  thresholds: {
    // Zero tolerance for errors
    errors: ['rate<0.01'],
    // Scheduling latency is bounded by SCHEDULER_INTERVAL_MS (default 1s).
    // p95 ≈ 1x interval under load. Reduce SCHEDULER_INTERVAL_MS for tighter latency.
    scheduling_latency_ms: ['p(95)<2000', 'p(99)<2500'],
    // Run completion time under 20-VU concurrent load with 3 workers and multi-hop DAGs
    run_completion_time_ms: ['p(95)<60000'],
    // HTTP API (create/poll) should stay fast regardless of execution load
    http_req_duration: ['p(99)<500'],
  },
};

// Workflow definitions: linear, fan-out, parallel branches
const WORKFLOW_DEFS = [
  {
    name: 'linear_pipeline',
    tasks: [
      { id: 'step1', type: 'DELAY', config: { duration_seconds: 1 }, depends_on: [] },
      { id: 'step2', type: 'DELAY', config: { duration_seconds: 1 }, depends_on: ['step1'] },
      { id: 'step3', type: 'DELAY', config: { duration_seconds: 1 }, depends_on: ['step2'] },
    ],
  },
  {
    name: 'fan_out_fan_in',
    tasks: [
      { id: 'root',   type: 'DELAY', config: { duration_seconds: 1 }, depends_on: [] },
      { id: 'branch1', type: 'DELAY', config: { duration_seconds: 1 }, depends_on: ['root'] },
      { id: 'branch2', type: 'DELAY', config: { duration_seconds: 1 }, depends_on: ['root'] },
      { id: 'branch3', type: 'DELAY', config: { duration_seconds: 1 }, depends_on: ['root'] },
      { id: 'join',   type: 'DELAY', config: { duration_seconds: 1 }, depends_on: ['branch1', 'branch2', 'branch3'] },
    ],
  },
  {
    name: 'parallel_branches',
    tasks: [
      { id: 'a', type: 'DELAY', config: { duration_seconds: 1 }, depends_on: [] },
      { id: 'b', type: 'DELAY', config: { duration_seconds: 1 }, depends_on: [] },
      { id: 'c', type: 'DELAY', config: { duration_seconds: 1 }, depends_on: [] },
      { id: 'd', type: 'DELAY', config: { duration_seconds: 2 }, depends_on: ['a', 'b'] },
      { id: 'e', type: 'DELAY', config: { duration_seconds: 1 }, depends_on: ['c'] },
      { id: 'f', type: 'DELAY', config: { duration_seconds: 1 }, depends_on: ['d', 'e'] },
    ],
  },
];

let workflowIDs = [];

export function setup() {
  const ids = [];
  for (const def of WORKFLOW_DEFS) {
    const res = http.post(`${BASE_URL}/workflows`, JSON.stringify(def), {
      headers: { 'Content-Type': 'application/json' },
    });
    check(res, { 'workflow created': (r) => r.status === 201 });
    const body = JSON.parse(res.body);
    ids.push(body.id);
  }
  return { workflowIDs: ids };
}

export default function (data) {
  const wfID = data.workflowIDs[Math.floor(Math.random() * data.workflowIDs.length)];

  // Trigger a run
  const triggerStart = Date.now();
  const runRes = http.post(`${BASE_URL}/workflows/${wfID}/runs`, null, {
    headers: { 'Content-Type': 'application/json' },
  });

  if (!check(runRes, { 'run created': (r) => r.status === 201 })) {
    errorRate.add(1);
    return;
  }
  errorRate.add(0);
  workflowRunsCreated.add(1);

  const run = JSON.parse(runRes.body);
  const runID = run.id;

  // Poll until first task transitions out of PENDING (scheduling latency)
  let schedulingMeasured = false;
  let completed = false;
  const startTime = Date.now();

  for (let i = 0; i < 120; i++) {
    sleep(0.5);

    const statusRes = http.get(`${BASE_URL}/runs/${runID}`);
    if (statusRes.status !== 200) continue;

    const status = JSON.parse(statusRes.body);

    if (!schedulingMeasured) {
      const tasksRes = http.get(`${BASE_URL}/runs/${runID}/tasks`);
      if (tasksRes.status === 200) {
        const tasks = JSON.parse(tasksRes.body);
        const hasNonPending = tasks.some(t => t.status !== 'PENDING');
        if (hasNonPending) {
          schedulingLatency.add(Date.now() - triggerStart);
          schedulingMeasured = true;
        }
      }
    }

    if (status.status === 'SUCCEEDED' || status.status === 'FAILED') {
      runCompletionTime.add(Date.now() - startTime);
      workflowRunsCompleted.add(1);
      check(status, { 'run succeeded': (s) => s.status === 'SUCCEEDED' });
      completed = true;
      break;
    }
  }

  if (!completed) {
    errorRate.add(1);
  }
}
