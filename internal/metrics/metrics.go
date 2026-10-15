package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// JobsTotal counts completed/failed/preempted jobs by status + priority
	JobsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gpu_jobs_total",
		Help: "Total GPU jobs by terminal status and priority",
	}, []string{"status", "priority"})

	// JobsRunning is a live gauge of currently executing jobs
	JobsRunning = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gpu_jobs_running",
		Help: "Number of currently running GPU jobs",
	})

	// JobsQueuedByPriority breaks down the live queue by priority tier
	JobsQueuedByPriority = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gpu_jobs_queued_by_priority",
		Help: "Queued jobs per priority tier",
	}, []string{"priority"})

	// JobWaitSeconds histogram — p95 tracks queue backpressure
	JobWaitSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "gpu_job_wait_seconds",
		Help:    "Seconds a job waited in queue before execution started",
		Buckets: []float64{1, 5, 10, 30, 60, 120, 300},
	})

	// PreemptionsTotal counts how many times the PRIORITY algorithm kicked out a lower-priority job
	PreemptionsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gpu_preemptions_total",
		Help: "Total job preemptions triggered by PRIORITY scheduler",
	})

	// NodeUtilization tracks slot usage per node (0.0 – 1.0)
	NodeUtilization = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gpu_node_utilization_ratio",
		Help: "GPU slot utilization ratio per node (0.0 = idle, 1.0 = full)",
	}, []string{"node"})

	// SchedulerAlgorithm shows which algorithm is active (1 = active, 0 = inactive)
	SchedulerAlgorithm = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gpu_scheduler_algorithm_active",
		Help: "Currently active scheduling algorithm flag",
	}, []string{"algorithm"})
)

func Init() {
	JobsQueuedByPriority.WithLabelValues("HIGH").Set(0)
	JobsQueuedByPriority.WithLabelValues("MEDIUM").Set(0)
	JobsQueuedByPriority.WithLabelValues("LOW").Set(0)

	NodeUtilization.WithLabelValues("node-01").Set(0)
	NodeUtilization.WithLabelValues("node-02").Set(0)
	NodeUtilization.WithLabelValues("node-03").Set(0)

	SchedulerAlgorithm.WithLabelValues("FIFO").Set(1)
	SchedulerAlgorithm.WithLabelValues("SJF").Set(0)
	SchedulerAlgorithm.WithLabelValues("PRIORITY").Set(0)
}
