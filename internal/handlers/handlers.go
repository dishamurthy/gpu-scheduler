package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gpu-scheduler/internal/cache"
	"gpu-scheduler/internal/db"
	"gpu-scheduler/internal/metrics"
	"gpu-scheduler/internal/models"
	"gpu-scheduler/internal/scheduler"
)

type Handler struct {
	pool  *pgxpool.Pool
	cache *cache.Client
	sched *scheduler.Scheduler
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func readJSON(r *http.Request, v any) error {
	return json.NewDecoder(r.Body).Decode(v)
}

func NewRouter(pool *pgxpool.Pool, c *cache.Client, s *scheduler.Scheduler) *chi.Mux {
	h := &Handler{pool: pool, cache: c, sched: s}

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(cors)

	r.Get("/health", h.health)
	r.Handle("/metrics", promhttp.Handler())

	r.Post("/jobs", h.submitJob)
	r.Get("/jobs", h.listJobs)
	r.Get("/jobs/{id}", h.getJob)
	r.Delete("/jobs/{id}", h.cancelJob)

	r.Get("/scheduler", h.getScheduler)
	r.Put("/scheduler/algorithm", h.setAlgorithm)

	r.Get("/nodes", h.getNodes)
	r.Get("/stats", h.getStats)

	return r
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{"status": "ok", "algorithm": h.sched.GetAlgorithm()})
}

func (h *Handler) submitJob(w http.ResponseWriter, r *http.Request) {
	var req models.SubmitJobRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if req.Name == "" || req.GPUSlotsRequired < 1 || req.GPUSlotsRequired > 4 ||
		req.EstimatedDuration < 5 || req.EstimatedDuration > 300 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request: name required, slots 1-4, duration 5-300s"})
		return
	}
	job, err := db.CreateJob(r.Context(), h.pool, req, uuid.New().String())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	metrics.JobsQueuedByPriority.WithLabelValues(string(job.Priority)).Inc()
	writeJSON(w, http.StatusCreated, job)
}

func (h *Handler) listJobs(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	jobs, err := db.ListJobs(r.Context(), h.pool,
		r.URL.Query().Get("status"),
		r.URL.Query().Get("priority"),
		limit,
	)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if jobs == nil {
		jobs = []models.Job{}
	}
	writeJSON(w, http.StatusOK, jobs)
}

func (h *Handler) getJob(w http.ResponseWriter, r *http.Request) {
	job, err := db.GetJob(r.Context(), h.pool, chi.URLParam(r, "id"))
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (h *Handler) cancelJob(w http.ResponseWriter, r *http.Request) {
	if err := db.CancelJob(r.Context(), h.pool, chi.URLParam(r, "id")); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"message": "cancelled"})
}

func (h *Handler) setAlgorithm(w http.ResponseWriter, r *http.Request) {
	var req models.SetAlgorithmRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	switch req.Algorithm {
	case models.AlgoFIFO, models.AlgoSJF, models.AlgoPriority:
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "algorithm must be FIFO, SJF, or PRIORITY"})
		return
	}
	h.sched.SetAlgorithm(r.Context(), req.Algorithm)
	writeJSON(w, http.StatusOK, map[string]any{"algorithm": req.Algorithm})
}

func (h *Handler) getScheduler(w http.ResponseWriter, r *http.Request) {
	preemptions, _ := h.cache.GetPreemptions(r.Context())
	writeJSON(w, http.StatusOK, map[string]any{
		"algorithm":   h.sched.GetAlgorithm(),
		"preemptions": preemptions,
	})
}

func (h *Handler) getNodes(w http.ResponseWriter, r *http.Request) {
	nodes, err := db.GetNodes(r.Context(), h.pool)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, nodes)
}

func (h *Handler) getStats(w http.ResponseWriter, r *http.Request) {
	byStatus, byPriority, err := db.GetStats(r.Context(), h.pool)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	nodes, _ := db.GetNodes(r.Context(), h.pool)
	preemptions, _ := h.cache.GetPreemptions(r.Context())
	total := 0
	for _, v := range byStatus {
		total += v
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"total_jobs":        total,
		"by_status":         byStatus,
		"by_priority":       byPriority,
		"total_preemptions": preemptions,
		"current_algorithm": h.sched.GetAlgorithm(),
		"nodes":             nodes,
	})
}
