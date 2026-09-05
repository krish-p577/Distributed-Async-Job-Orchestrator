-- Requires pgcrypto for gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS pgcrypto;
 
CREATE TYPE task_status AS ENUM (
    'PENDING',    -- exists, waiting on upstream dependencies
    'QUEUED',     -- dependencies satisfied, eligible to be claimed
    'RUNNING',    -- claimed by a worker, in progress
    'COMPLETED',  -- terminal success
    'FAILED',     -- terminal failure (retry budget exhausted)
    'RETRYING',   -- transient state on the way back to QUEUED
    'CANCELLED'   -- terminal, manual cancellation
);
 
CREATE TABLE dags (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    definition JSONB NOT NULL,          -- parsed YAML/JSON DAG spec
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
 
CREATE TABLE dag_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dag_id UUID NOT NULL REFERENCES dags(id),
    status VARCHAR(20) NOT NULL DEFAULT 'RUNNING',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);
 
CREATE TABLE tasks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dag_run_id UUID NOT NULL REFERENCES dag_runs(id),
    task_key VARCHAR(255) NOT NULL,       -- e.g. "fetch_data", unique within a run
    task_type VARCHAR(100) NOT NULL,      -- e.g. "SQL_TRANSFORM", "LLM_SUMMARY", "S3_UPLOAD"
    config JSONB,                         -- task-specific parameters
 
    status task_status NOT NULL DEFAULT 'PENDING',
    retry_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,
 
    worker_id VARCHAR(255),
    -- Stable for the lifetime of this task row (NOT regenerated on retry).
    -- Workers use this to detect "did I already do this side effect?" when
    -- a retry follows a false-positive failure (e.g. lost heartbeat but the
    -- underlying work actually finished).
    idempotency_key UUID NOT NULL DEFAULT gen_random_uuid(),
 
    claimed_at TIMESTAMPTZ,
    last_heartbeat_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error_message TEXT,
 
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 
    UNIQUE (dag_run_id, task_key)
);
 
-- Edge list: (task_id) depends on (depends_on_task_id).
-- A task is eligible to run once every row here for it points at a
-- COMPLETED task.
CREATE TABLE task_dependencies (
    task_id UUID NOT NULL REFERENCES tasks(id),
    depends_on_task_id UUID NOT NULL REFERENCES tasks(id),
    PRIMARY KEY (task_id, depends_on_task_id)
);
 
-- Partial index: the claim query only ever looks at QUEUED rows, so this
-- keeps that scan cheap regardless of how many completed/failed tasks
-- accumulate in the table over time.
CREATE INDEX idx_tasks_queued ON tasks (created_at) WHERE status = 'QUEUED';
 
CREATE INDEX idx_tasks_dag_run ON tasks (dag_run_id);
 
-- Used by the reaper's stale-heartbeat scan.
CREATE INDEX idx_tasks_running_heartbeat ON tasks (last_heartbeat_at) WHERE status = 'RUNNING';
