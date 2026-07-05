package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	WorkflowRunsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "workflow_runs_total",
			Help: "Total number of workflow runs triggered",
		},
		[]string{"workflow_id"},
	)

	WorkflowRunsSucceeded = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "workflow_runs_succeeded_total",
			Help: "Total number of workflow runs that succeeded",
		},
		[]string{"workflow_id"},
	)

	WorkflowRunsFailed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "workflow_runs_failed_total",
			Help: "Total number of workflow runs that failed",
		},
		[]string{"workflow_id"},
	)

	TaskRunsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "task_runs_total",
			Help: "Total number of task run attempts",
		},
		[]string{"task_type", "status"},
	)

	TaskDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "task_duration_seconds",
			Help:    "Time taken to execute a task",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"task_type", "status"},
	)

	TaskRetriesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "task_retries_total",
			Help: "Total number of task retries scheduled",
		},
		[]string{"task_type"},
	)

	TaskLeaseExpiredTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "task_lease_expired_total",
			Help: "Total number of task leases that expired (worker crash recovery)",
		},
	)

	DeadLetterTasksTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dead_letter_tasks_total",
			Help: "Total number of tasks moved to dead letter queue",
		},
		[]string{"task_type"},
	)

	SchedulerLoopDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "scheduler_loop_duration_seconds",
			Help:    "Time taken for one scheduler tick",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0},
		},
	)

	QueuedTasksGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "queued_tasks_count",
			Help: "Current number of tasks in QUEUED status",
		},
	)

	RunningTasksGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "running_tasks_count",
			Help: "Current number of tasks in RUNNING status",
		},
	)
)

func Register() {
	prometheus.MustRegister(
		WorkflowRunsTotal,
		WorkflowRunsSucceeded,
		WorkflowRunsFailed,
		TaskRunsTotal,
		TaskDurationSeconds,
		TaskRetriesTotal,
		TaskLeaseExpiredTotal,
		DeadLetterTasksTotal,
		SchedulerLoopDuration,
		QueuedTasksGauge,
		RunningTasksGauge,
	)
}
