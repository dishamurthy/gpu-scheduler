package models

import "time"

type JobStatus string
type JobPriority string
type Algorithm string

const (
	StatusQueued    JobStatus = "QUEUED"
	StatusRunning   JobStatus = "RUNNING"
	StatusCompleted JobStatus = "COMPLETED"
	StatusFailed    JobStatus = "FAILED"
	StatusPreempted JobStatus = "PREEMPTED"

	PriorityHigh   JobPriority = "HIGH"
	PriorityMedium JobPriority = "MEDIUM"
	PriorityLow    JobPriority = "LOW"

	AlgoFIFO     Algorithm = "FIFO"
	AlgoSJF      Algorithm = "SJF"
	AlgoPriority Algorithm = "PRIORITY"
)

type Job struct {
	ID                string      `json:"id"`
	Name              string      `json:"name"`
	Priority          JobPriority `json:"priority"`
	GPUSlotsRequired  int         `json:"gpu_slots_required"`
	EstimatedDuration int         `json:"estimated_duration_seconds"`
	Status            JobStatus   `json:"status"`
	NodeID            *int        `json:"node_id,omitempty"`
	NodeName          *string     `json:"node_name,omitempty"`
	SubmittedAt       time.Time   `json:"submitted_at"`
	StartedAt         *time.Time  `json:"started_at,omitempty"`
	CompletedAt       *time.Time  `json:"completed_at,omitempty"`
	WaitTimeSeconds   *float64    `json:"wait_time_seconds,omitempty"`
	PreemptedCount    int         `json:"preempted_count"`
}

type Node struct {
	ID             int    `json:"id"`
	Name           string `json:"name"`
	TotalSlots     int    `json:"total_slots"`
	AvailableSlots int    `json:"available_slots"`
}

type SubmitJobRequest struct {
	Name              string      `json:"name" binding:"required"`
	Priority          JobPriority `json:"priority"`
	GPUSlotsRequired  int         `json:"gpu_slots_required" binding:"required,min=1,max=4"`
	EstimatedDuration int         `json:"estimated_duration_seconds" binding:"required,min=5,max=300"`
}

type SetAlgorithmRequest struct {
	Algorithm Algorithm `json:"algorithm" binding:"required"`
}
