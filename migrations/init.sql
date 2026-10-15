CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TYPE job_status AS ENUM ('QUEUED', 'RUNNING', 'COMPLETED', 'FAILED', 'PREEMPTED');
CREATE TYPE job_priority AS ENUM ('HIGH', 'MEDIUM', 'LOW');

CREATE TABLE IF NOT EXISTS nodes (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    total_slots INTEGER NOT NULL,
    available_slots INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(100) NOT NULL,
    priority job_priority NOT NULL DEFAULT 'MEDIUM',
    gpu_slots_required INTEGER NOT NULL DEFAULT 1,
    estimated_duration INTEGER NOT NULL,
    status job_status NOT NULL DEFAULT 'QUEUED',
    node_id INTEGER REFERENCES nodes(id) ON DELETE SET NULL,
    submitted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    wait_time_seconds DOUBLE PRECISION,
    preempted_count INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_priority ON jobs(priority);
CREATE INDEX IF NOT EXISTS idx_jobs_submitted_at ON jobs(submitted_at);
CREATE INDEX IF NOT EXISTS idx_jobs_node_id ON jobs(node_id);

INSERT INTO nodes (name, total_slots, available_slots) VALUES
    ('node-01', 4, 4),
    ('node-02', 4, 4),
    ('node-03', 2, 2)
ON CONFLICT (name) DO NOTHING;
