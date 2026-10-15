package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"gpu-scheduler/internal/models"
)

func NewPool(ctx context.Context, url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse db config: %w", err)
	}
	cfg.MaxConns = 10
	cfg.MinConns = 2

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping db: %w", err)
	}
	return pool, nil
}

func CreateJob(ctx context.Context, pool *pgxpool.Pool, req models.SubmitJobRequest, id string) (*models.Job, error) {
	priority := req.Priority
	if priority == "" {
		priority = models.PriorityMedium
	}
	var job models.Job
	err := pool.QueryRow(ctx, `
		INSERT INTO jobs (id, name, priority, gpu_slots_required, estimated_duration, status)
		VALUES ($1, $2, $3, $4, $5, 'QUEUED')
		RETURNING id, name, priority, gpu_slots_required, estimated_duration,
		          status, submitted_at, preempted_count
	`, id, req.Name, priority, req.GPUSlotsRequired, req.EstimatedDuration).Scan(
		&job.ID, &job.Name, &job.Priority, &job.GPUSlotsRequired,
		&job.EstimatedDuration, &job.Status, &job.SubmittedAt, &job.PreemptedCount,
	)
	return &job, err
}

func GetJob(ctx context.Context, pool *pgxpool.Pool, id string) (*models.Job, error) {
	var job models.Job
	err := pool.QueryRow(ctx, `
		SELECT j.id, j.name, j.priority, j.gpu_slots_required, j.estimated_duration,
		       j.status, j.node_id, n.name, j.submitted_at, j.started_at,
		       j.completed_at, j.wait_time_seconds, j.preempted_count
		FROM jobs j LEFT JOIN nodes n ON j.node_id = n.id
		WHERE j.id = $1
	`, id).Scan(
		&job.ID, &job.Name, &job.Priority, &job.GPUSlotsRequired, &job.EstimatedDuration,
		&job.Status, &job.NodeID, &job.NodeName, &job.SubmittedAt,
		&job.StartedAt, &job.CompletedAt, &job.WaitTimeSeconds, &job.PreemptedCount,
	)
	if err != nil {
		return nil, err
	}
	return &job, nil
}

func ListJobs(ctx context.Context, pool *pgxpool.Pool, status, priority string, limit int) ([]models.Job, error) {
	query := `
		SELECT j.id, j.name, j.priority, j.gpu_slots_required, j.estimated_duration,
		       j.status, j.node_id, n.name, j.submitted_at, j.started_at,
		       j.completed_at, j.wait_time_seconds, j.preempted_count
		FROM jobs j LEFT JOIN nodes n ON j.node_id = n.id
		WHERE 1=1`
	args := []any{}
	i := 1
	if status != "" {
		query += fmt.Sprintf(" AND j.status = $%d", i)
		args = append(args, status)
		i++
	}
	if priority != "" {
		query += fmt.Sprintf(" AND j.priority = $%d", i)
		args = append(args, priority)
		i++
	}
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	query += fmt.Sprintf(" ORDER BY j.submitted_at DESC LIMIT $%d", i)
	args = append(args, limit)

	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []models.Job
	for rows.Next() {
		var job models.Job
		if err := rows.Scan(
			&job.ID, &job.Name, &job.Priority, &job.GPUSlotsRequired, &job.EstimatedDuration,
			&job.Status, &job.NodeID, &job.NodeName, &job.SubmittedAt,
			&job.StartedAt, &job.CompletedAt, &job.WaitTimeSeconds, &job.PreemptedCount,
		); err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func GetQueuedJobs(ctx context.Context, pool *pgxpool.Pool) ([]models.Job, error) {
	rows, err := pool.Query(ctx, `
		SELECT id, name, priority, gpu_slots_required, estimated_duration,
		       status, submitted_at, preempted_count
		FROM jobs WHERE status = 'QUEUED'
		ORDER BY submitted_at ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []models.Job
	for rows.Next() {
		var job models.Job
		if err := rows.Scan(
			&job.ID, &job.Name, &job.Priority, &job.GPUSlotsRequired,
			&job.EstimatedDuration, &job.Status, &job.SubmittedAt, &job.PreemptedCount,
		); err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func GetRunningJobs(ctx context.Context, pool *pgxpool.Pool) ([]models.Job, error) {
	rows, err := pool.Query(ctx, `
		SELECT id, name, priority, gpu_slots_required, estimated_duration,
		       status, node_id, started_at, preempted_count
		FROM jobs WHERE status = 'RUNNING'
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []models.Job
	for rows.Next() {
		var job models.Job
		if err := rows.Scan(
			&job.ID, &job.Name, &job.Priority, &job.GPUSlotsRequired,
			&job.EstimatedDuration, &job.Status, &job.NodeID,
			&job.StartedAt, &job.PreemptedCount,
		); err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func GetNodes(ctx context.Context, pool *pgxpool.Pool) ([]models.Node, error) {
	rows, err := pool.Query(ctx, `
		SELECT id, name, total_slots, available_slots FROM nodes ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var nodes []models.Node
	for rows.Next() {
		var n models.Node
		if err := rows.Scan(&n.ID, &n.Name, &n.TotalSlots, &n.AvailableSlots); err != nil {
			return nil, err
		}
		nodes = append(nodes, n)
	}
	return nodes, nil
}

// AssignJobToNode atomically transitions job to RUNNING and deducts slots from the node
func AssignJobToNode(ctx context.Context, pool *pgxpool.Pool, jobID string, nodeID int, waitSecs float64) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var slots int
	if err := tx.QueryRow(ctx,
		`SELECT gpu_slots_required FROM jobs WHERE id = $1`, jobID,
	).Scan(&slots); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE jobs SET status='RUNNING', node_id=$1, started_at=NOW(), wait_time_seconds=$2
		WHERE id = $3
	`, nodeID, waitSecs, jobID); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE nodes SET available_slots = available_slots - $1 WHERE id = $2
	`, slots, nodeID); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// CompleteJob atomically marks job COMPLETED and restores node slots
func CompleteJob(ctx context.Context, pool *pgxpool.Pool, jobID string) (models.JobPriority, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	var slots, nodeID int
	var priority models.JobPriority
	if err := tx.QueryRow(ctx, `
		SELECT gpu_slots_required, node_id, priority FROM jobs WHERE id = $1
	`, jobID).Scan(&slots, &nodeID, &priority); err != nil {
		return "", err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE jobs SET status='COMPLETED', completed_at=NOW() WHERE id=$1
	`, jobID); err != nil {
		return "", err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE nodes SET available_slots = available_slots + $1 WHERE id = $2
	`, slots, nodeID); err != nil {
		return "", err
	}

	return priority, tx.Commit(ctx)
}

// PreemptJob requeues a running job and restores its node slots
func PreemptJob(ctx context.Context, pool *pgxpool.Pool, jobID string) (int, int, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback(ctx)

	var slots, nodeID int
	if err := tx.QueryRow(ctx, `
		SELECT gpu_slots_required, node_id FROM jobs WHERE id = $1
	`, jobID).Scan(&slots, &nodeID); err != nil {
		return 0, 0, err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE jobs
		SET status='QUEUED', node_id=NULL, started_at=NULL,
		    preempted_count = preempted_count + 1
		WHERE id = $1
	`, jobID); err != nil {
		return 0, 0, err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE nodes SET available_slots = available_slots + $1 WHERE id = $2
	`, slots, nodeID); err != nil {
		return 0, 0, err
	}

	return slots, nodeID, tx.Commit(ctx)
}

func CancelJob(ctx context.Context, pool *pgxpool.Pool, id string) error {
	res, err := pool.Exec(ctx, `
		UPDATE jobs SET status='FAILED', completed_at=NOW()
		WHERE id=$1 AND status='QUEUED'
	`, id)
	if err != nil {
		return err
	}
	if res.RowsAffected() == 0 {
		return fmt.Errorf("job not found or not cancellable")
	}
	return nil
}

func GetStats(ctx context.Context, pool *pgxpool.Pool) (map[string]int, map[string]int, error) {
	byStatus := map[string]int{}
	byPriority := map[string]int{}

	rows, err := pool.Query(ctx, `
		SELECT status, priority, COUNT(*) FROM jobs GROUP BY status, priority
	`)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var status, priority string
		var count int
		if err := rows.Scan(&status, &priority, &count); err != nil {
			return nil, nil, err
		}
		byStatus[status] += count
		byPriority[priority] += count
	}
	return byStatus, byPriority, nil
}
