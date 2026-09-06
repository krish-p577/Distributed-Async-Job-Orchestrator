package com.orchestrator.repository;

import com.orchestrator.model.Task;
import com.orchestrator.model.TaskStatus;
import com.orchestrator.model.WorkerStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class TaskRepository {

    private final JdbcTemplate jdbc;

    public TaskRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static final RowMapper<Task> TASK_MAPPER = (rs, rowNum) -> new Task(
            UUID.fromString(rs.getString("id")),
            UUID.fromString(rs.getString("dag_run_id")),
            rs.getString("task_key"),
            rs.getString("task_type"),
            TaskStatus.valueOf(rs.getString("status")),
            rs.getInt("retry_count"),
            rs.getInt("max_retries"),
            rs.getString("worker_id"),
            UUID.fromString(rs.getString("idempotency_key")),
            toInstant(rs.getTimestamp("claimed_at")),
            toInstant(rs.getTimestamp("last_heartbeat_at")),
            toInstant(rs.getTimestamp("started_at")),
            toInstant(rs.getTimestamp("completed_at")),
            rs.getString("error_message")
    );

    private static Instant toInstant(Timestamp ts) {
        return ts == null ? null : ts.toInstant();
    }

    public Optional<Task> findById(UUID taskId) {
        return jdbc.query("SELECT * FROM tasks WHERE id = ?", TASK_MAPPER, taskId)
                .stream().findFirst();
    }

    /**
     * Atomically claims the oldest QUEUED task using SELECT ... FOR UPDATE
     * SKIP LOCKED. Any number of worker threads/processes can call this
     * concurrently: Postgres guarantees each row is handed to exactly one
     * caller, and callers that would otherwise block on a locked row skip
     * it and move on instead of queueing up behind each other.
     */
    public Optional<Task> claimNextTask(String workerId) {
        String sql = """
                UPDATE tasks
                SET status = 'RUNNING',
                    worker_id = ?,
                    claimed_at = now(),
                    started_at = COALESCE(started_at, now()),
                    last_heartbeat_at = now(),
                    updated_at = now()
                WHERE id = (
                    SELECT id FROM tasks
                    WHERE status = 'QUEUED'
                    ORDER BY created_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                RETURNING *
                """;
        return jdbc.query(sql, TASK_MAPPER, workerId).stream().findFirst();
    }

    public void recordHeartbeat(UUID taskId, String workerId) {
        jdbc.update("""
                UPDATE tasks
                SET last_heartbeat_at = now(), updated_at = now()
                WHERE id = ? AND worker_id = ? AND status = 'RUNNING'
                """, taskId, workerId);
    }

    public void markCompleted(UUID taskId) {
        jdbc.update("""
                UPDATE tasks
                SET status = 'COMPLETED', completed_at = now(), updated_at = now()
                WHERE id = ?
                """, taskId);
    }

    /**
     * Either re-queues the task for another attempt (-> QUEUED, worker_id
     * cleared) or marks it permanently FAILED if the retry budget is
     * exhausted. idempotency_key is deliberately left untouched - it must
     * stay stable across retries so a worker can tell "I already produced
     * this side effect" if the previous attempt actually succeeded but its
     * heartbeat was merely lost to a network blip.
     */
    public void failOrRetry(UUID taskId, String errorMessage) {
        jdbc.update("""
                UPDATE tasks
                SET status = CAST(
                CASE WHEN retry_count < max_retries THEN 'QUEUED' ELSE 'FAILED' END AS task_status),
                    retry_count = retry_count + 1,
                    worker_id = NULL,
                    claimed_at = NULL,
                    error_message = ?,
                    updated_at = now()
                WHERE id = ?
                """, errorMessage, taskId);
    }

    public List<Task> findStaleRunningTasks(Duration heartbeatTimeout) {
        Timestamp cutoff = Timestamp.from(Instant.now().minus(heartbeatTimeout));
        return jdbc.query("""
                SELECT * FROM tasks
                WHERE status = 'RUNNING' AND last_heartbeat_at < ?
                """, TASK_MAPPER, cutoff);
    }

    public List<Task> findDownstreamTasks(UUID completedTaskId) {
        return jdbc.query("""
                SELECT t.* FROM tasks t
                JOIN task_dependencies td ON td.task_id = t.id
                WHERE td.depends_on_task_id = ? AND t.status = 'PENDING'
                """, TASK_MAPPER, completedTaskId);
    }

    public boolean allDependenciesCompleted(UUID taskId) {
        Integer incomplete = jdbc.queryForObject("""
                SELECT count(*) FROM task_dependencies td
                JOIN tasks dep ON dep.id = td.depends_on_task_id
                WHERE td.task_id = ? AND dep.status <> 'COMPLETED'
                """, Integer.class, taskId);
        return incomplete != null && incomplete == 0;
    }

    public void markQueued(UUID taskId) {
        jdbc.update("""
                UPDATE tasks SET status = 'QUEUED', updated_at = now()
                WHERE id = ? AND status = 'PENDING'
                """, taskId);
    }

    /**
     * A live snapshot of what every worker is currently doing - derived
     * directly from the tasks table (no separate registry to keep in
     * sync). This is what makes multiple workers' activity visible from
     * outside their own console output.
     */
    public List<WorkerStatus> findActiveWorkerStatus() {
        return jdbc.query("""
                SELECT worker_id, task_key, task_type, dag_run_id, last_heartbeat_at
                FROM tasks
                WHERE status = 'RUNNING'
                ORDER BY worker_id
                """, (rs, rowNum) -> new WorkerStatus(
                rs.getString("worker_id"),
                rs.getString("task_key"),
                rs.getString("task_type"),
                UUID.fromString(rs.getString("dag_run_id")),
                toInstant(rs.getTimestamp("last_heartbeat_at"))
        ));
    }
}