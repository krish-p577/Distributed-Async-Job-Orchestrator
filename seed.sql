-- Run once against the running Postgres to get a testable DAG in place
-- before the YAML parser exists:
--   docker exec -i orchestrator-postgres-1 psql -U orchestrator -d orchestrator < seed.sql
--
-- Models a 3-step chain: fetch_data -> transform -> upload_s3
-- Only fetch_data starts QUEUED (eligible to claim); the rest are PENDING
-- until their upstream dependency completes.

INSERT INTO dags (id, name, definition) VALUES
    ('11111111-1111-1111-1111-111111111111', 'nightly-pipeline', '{}');

INSERT INTO dag_runs (id, dag_id, status) VALUES
    ('22222222-2222-2222-2222-222222222222', '11111111-1111-1111-1111-111111111111', 'RUNNING');

INSERT INTO tasks (id, dag_run_id, task_key, task_type, status) VALUES
    ('33333333-3333-3333-3333-333333333333', '22222222-2222-2222-2222-222222222222', 'fetch_data', 'FETCH', 'QUEUED'),
    ('44444444-4444-4444-4444-444444444444', '22222222-2222-2222-2222-222222222222', 'transform', 'SQL_TRANSFORM', 'PENDING'),
    ('55555555-5555-5555-5555-555555555555', '22222222-2222-2222-2222-222222222222', 'upload_s3', 'S3_UPLOAD', 'PENDING');

INSERT INTO task_dependencies (task_id, depends_on_task_id) VALUES
    ('44444444-4444-4444-4444-444444444444', '33333333-3333-3333-3333-333333333333'),
    ('55555555-5555-5555-5555-555555555555', '44444444-4444-4444-4444-444444444444');