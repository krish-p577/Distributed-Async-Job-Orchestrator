package com.orchestrator.dag;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Turns a DagDefinition into rows in dags / dag_runs / tasks /
 * task_dependencies. Tasks are inserted in topological order so that by
 * the time any given task's row is created, every task it depends on
 * already has a real database id for task_dependencies to reference.
 */
@Service
public class DagPersistenceService {

    private final JdbcTemplate jdbc;
    private final ObjectMapper jsonMapper = new ObjectMapper();

    public DagPersistenceService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Transactional
    public UUID registerAndStart(DagDefinition definition) {
        // Validation happens here rather than in the controller, so every
        // caller of this service - REST, a future CLI, tests - gets the
        // same guarantee for free instead of having to remember to call it.
        List<String> order = DagValidator.topologicalOrder(definition);

        Map<String, TaskDefinition> byKey = new HashMap<>();
        for (TaskDefinition task : definition.tasks()) {
            byKey.put(task.key(), task);
        }

        UUID dagId = insertDag(definition);
        UUID dagRunId = insertDagRun(dagId);

        Map<String, UUID> taskIds = new HashMap<>();
        for (String key : order) {
            TaskDefinition task = byKey.get(key);
            boolean isRoot = task.dependsOn().isEmpty();
            taskIds.put(key, insertTask(dagRunId, task, isRoot));
        }

        for (TaskDefinition task : definition.tasks()) {
            UUID taskId = taskIds.get(task.key());
            for (String dep : task.dependsOn()) {
                insertDependency(taskId, taskIds.get(dep));
            }
        }

        return dagRunId;
    }

    private UUID insertDag(DagDefinition definition) {
        return jdbc.queryForObject("""
                INSERT INTO dags (name, definition) VALUES (?, ?::jsonb)
                RETURNING id
                """, UUID.class, definition.name(), writeJson(definition));
    }

    private UUID insertDagRun(UUID dagId) {
        return jdbc.queryForObject("""
                INSERT INTO dag_runs (dag_id, status) VALUES (?, 'RUNNING')
                RETURNING id
                """, UUID.class, dagId);
    }

    private UUID insertTask(UUID dagRunId, TaskDefinition task, boolean isRoot) {
        String status = isRoot ? "QUEUED" : "PENDING";
        return jdbc.queryForObject("""
                INSERT INTO tasks (dag_run_id, task_key, task_type, config, status)
                VALUES (?, ?, ?, ?::jsonb, ?::task_status)
                RETURNING id
                """, UUID.class, dagRunId, task.key(), task.type(), writeJson(task.config()), status);
    }

    private void insertDependency(UUID taskId, UUID dependsOnTaskId) {
        jdbc.update("""
                INSERT INTO task_dependencies (task_id, depends_on_task_id) VALUES (?, ?)
                """, taskId, dependsOnTaskId);
    }

    private String writeJson(Object value) {
        try {
            return jsonMapper.writeValueAsString(value);
        } catch (Exception e) {
            throw new DagValidationException("Failed to serialize DAG JSON: " + e.getMessage());
        }
    }
}