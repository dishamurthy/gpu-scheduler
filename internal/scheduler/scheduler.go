package scheduler

import (
	"context"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"gpu-scheduler/internal/cache"
	"gpu-scheduler/internal/db"
	"gpu-scheduler/internal/metrics"
	"gpu-scheduler/internal/models"
)

type Scheduler struct {
	pool      *pgxpool.Pool
	cache     *cache.Client
	mu        sync.RWMutex
	algorithm models.Algorithm
}

func New(pool *pgxpool.Pool, c *cache.Client) *Scheduler {
	return &Scheduler{pool: pool, cache: c, algorithm: models.AlgoFIFO}
}

func (s *Scheduler) GetAlgorithm() models.Algorithm {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.algorithm
}

func (s *Scheduler) SetAlgorithm(ctx context.Context, algo models.Algorithm) {
	s.mu.Lock()
	s.algorithm = algo
	s.mu.Unlock()

	_ = s.cache.SetAlgorithm(ctx, string(algo))

	metrics.SchedulerAlgorithm.WithLabelValues("FIFO").Set(0)
	metrics.SchedulerAlgorithm.WithLabelValues("SJF").Set(0)
	metrics.SchedulerAlgorithm.WithLabelValues("PRIORITY").Set(0)
	metrics.SchedulerAlgorithm.WithLabelValues(string(algo)).Set(1)

	log.Printf("[scheduler] algorithm switched to %s", algo)
}

func (s *Scheduler) Run(ctx context.Context) {
	// Restore persisted algorithm from Redis on restart
	if algo, err := s.cache.GetAlgorithm(ctx); err == nil && algo != "" {
		s.SetAlgorithm(ctx, models.Algorithm(algo))
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	log.Println("[scheduler] started — tick every 2s")
	for {
		select {
		case <-ctx.Done():
			log.Println("[scheduler] stopped")
			return
		case <-ticker.C:
			s.tick(ctx)
		}
	}
}

func (s *Scheduler) tick(ctx context.Context) {
	// 1. Complete any jobs whose estimated_duration has elapsed
	s.completeFinishedJobs(ctx)

	// 2. Pull queued jobs in submission order (algo sort happens below)
	queued, err := db.GetQueuedJobs(ctx, s.pool)
	if err != nil {
		log.Printf("[scheduler] get queued: %v", err)
		return
	}
	if len(queued) == 0 {
		s.refreshNodeMetrics(ctx)
		s.refreshRunningMetric(ctx)
		return
	}

	// 3. Get node state (authoritative from DB after completions above)
	nodes, err := db.GetNodes(ctx, s.pool)
	if err != nil {
		log.Printf("[scheduler] get nodes: %v", err)
		return
	}

	// Build mutable node map so we track in-flight slot changes within one tick
	nodeMap := map[int]*models.Node{}
	for i := range nodes {
		nodeMap[nodes[i].ID] = &nodes[i]
	}

	// 4. Sort queue by active algorithm
	algo := s.GetAlgorithm()
	sorted := sortJobs(queued, algo)

	// 5. Assign as many jobs as possible this tick
	for _, job := range sorted {
		assigned := s.tryAssign(ctx, job, nodeMap)
		if !assigned && algo == models.AlgoPriority && job.Priority == models.PriorityHigh {
			s.tryPreempt(ctx, job, nodeMap)
		}
	}

	// 6. Sync queue-depth gauges + node utilization + running count to Prometheus.
	// These are recomputed from the DB (the source of truth) every tick rather than
	// trusted from in-process Inc()/Dec() calls alone, so the gauges self-correct
	// after a process restart instead of silently reporting 0 while jobs are
	// actually still running.
	s.refreshQueueMetrics(ctx)
	s.refreshNodeMetrics(ctx)
	s.refreshRunningMetric(ctx)
}

// tryAssign looks for any node with enough free slots and assigns the job
func (s *Scheduler) tryAssign(ctx context.Context, job models.Job, nodeMap map[int]*models.Node) bool {
	for _, node := range nodeMap {
		if node.AvailableSlots >= job.GPUSlotsRequired {
			wait := time.Since(job.SubmittedAt).Seconds()
			if err := db.AssignJobToNode(ctx, s.pool, job.ID, node.ID, wait); err != nil {
				log.Printf("[scheduler] assign %s → %s: %v", job.ID[:8], node.Name, err)
				continue
			}
			node.AvailableSlots -= job.GPUSlotsRequired

			metrics.JobWaitSeconds.Observe(wait)
			metrics.JobsQueuedByPriority.WithLabelValues(string(job.Priority)).Dec()

			log.Printf("[scheduler] assigned %s (priority=%s slots=%d wait=%.1fs) → %s",
				job.ID[:8], job.Priority, job.GPUSlotsRequired, wait, node.Name)
			return true
		}
	}
	return false
}

// tryPreempt finds a lower-priority running job to boot so the HIGH job can run
func (s *Scheduler) tryPreempt(ctx context.Context, highJob models.Job, nodeMap map[int]*models.Node) {
	running, err := db.GetRunningJobs(ctx, s.pool)
	if err != nil {
		return
	}

	for _, victim := range running {
		if victim.Priority == models.PriorityHigh || victim.NodeID == nil {
			continue
		}
		node, ok := nodeMap[*victim.NodeID]
		if !ok {
			continue
		}
		// Only preempt if doing so would actually free enough slots on that node
		if node.AvailableSlots+victim.GPUSlotsRequired < highJob.GPUSlotsRequired {
			continue
		}

		slots, nodeID, err := db.PreemptJob(ctx, s.pool, victim.ID)
		if err != nil {
			log.Printf("[scheduler] preempt %s: %v", victim.ID[:8], err)
			continue
		}

		node = nodeMap[nodeID]
		node.AvailableSlots += slots
		metrics.PreemptionsTotal.Inc()
		s.cache.IncrPreemptions(ctx)

		log.Printf("[scheduler] preempted %s (priority=%s) for HIGH job %s",
			victim.ID[:8], victim.Priority, highJob.ID[:8])

		// Immediately assign the HIGH job to the freed slots
		s.tryAssign(ctx, highJob, nodeMap)
		return
	}
}

func (s *Scheduler) completeFinishedJobs(ctx context.Context) {
	running, err := db.GetRunningJobs(ctx, s.pool)
	if err != nil {
		log.Printf("[scheduler] get running: %v", err)
		return
	}
	for _, job := range running {
		if job.StartedAt == nil {
			continue
		}
		if time.Since(*job.StartedAt).Seconds() < float64(job.EstimatedDuration) {
			continue
		}
		priority, err := db.CompleteJob(ctx, s.pool, job.ID)
		if err != nil {
			log.Printf("[scheduler] complete %s: %v", job.ID[:8], err)
			continue
		}
		metrics.JobsTotal.WithLabelValues("COMPLETED", string(priority)).Inc()
		log.Printf("[scheduler] completed %s (priority=%s)", job.ID[:8], priority)
	}
}

func (s *Scheduler) refreshQueueMetrics(ctx context.Context) {
	jobs, err := db.GetQueuedJobs(ctx, s.pool)
	if err != nil {
		return
	}
	counts := map[string]float64{"HIGH": 0, "MEDIUM": 0, "LOW": 0}
	for _, j := range jobs {
		counts[string(j.Priority)]++
	}
	for p, c := range counts {
		metrics.JobsQueuedByPriority.WithLabelValues(p).Set(c)
	}
}

// refreshRunningMetric recomputes gpu_jobs_running from the DB instead of relying
// solely on the Inc()/Dec() calls in tryAssign/tryPreempt/completeFinishedJobs, which
// reset to 0 on process restart even though jobs already RUNNING in Postgres (and the
// Redis-persisted algorithm) survive it. This keeps the metric accurate after a restart.
func (s *Scheduler) refreshRunningMetric(ctx context.Context) {
	running, err := db.GetRunningJobs(ctx, s.pool)
	if err != nil {
		return
	}
	metrics.JobsRunning.Set(float64(len(running)))
}

func (s *Scheduler) refreshNodeMetrics(ctx context.Context) {
	nodes, err := db.GetNodes(ctx, s.pool)
	if err != nil {
		return
	}
	for _, n := range nodes {
		used := float64(n.TotalSlots-n.AvailableSlots) / float64(n.TotalSlots)
		metrics.NodeUtilization.WithLabelValues(n.Name).Set(used)
	}
}

func sortJobs(jobs []models.Job, algo models.Algorithm) []models.Job {
	out := make([]models.Job, len(jobs))
	copy(out, jobs)

	switch algo {
	case models.AlgoSJF:
		// Shortest estimated duration first — minimises average completion time
		sort.Slice(out, func(i, j int) bool {
			return out[i].EstimatedDuration < out[j].EstimatedDuration
		})
	case models.AlgoPriority:
		// HIGH → MEDIUM → LOW, then FIFO within each tier
		rank := map[models.JobPriority]int{
			models.PriorityHigh:   0,
			models.PriorityMedium: 1,
			models.PriorityLow:    2,
		}
		sort.Slice(out, func(i, j int) bool {
			if rank[out[i].Priority] != rank[out[j].Priority] {
				return rank[out[i].Priority] < rank[out[j].Priority]
			}
			return out[i].SubmittedAt.Before(out[j].SubmittedAt)
		})
	default: // FIFO
		sort.Slice(out, func(i, j int) bool {
			return out[i].SubmittedAt.Before(out[j].SubmittedAt)
		})
	}
	return out
}
