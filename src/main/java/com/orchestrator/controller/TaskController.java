package com.orchestrator.controller;

import com.orchestrator.model.Task;
import com.orchestrator.repository.TaskRepository;
import com.orchestrator.service.DagRunService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

/**
 * The only surface workers ever talk to. Everything here is deliberately
 * thin - it validates the request shape and delegates straight into
 * TaskRepository / DagRunService, which own all the actual state-machine
 * logic. No business rules live in this class.
 */
@RestController
@RequestMapping("/api/tasks")
public class TaskController {

    private final TaskRepository taskRepository;
    private final DagRunService dagRunService;

    public TaskController(TaskRepository taskRepository, DagRunService dagRunService) {
        this.taskRepository = taskRepository;
        this.dagRunService = dagRunService;
    }

    /**
     * Workers call this in a loop to pick up work. 200 + a task body means
     * "here's your assignment"; 204 with no body means "queue's empty,
     * back off and poll again shortly" - an empty queue is a normal state,
     * not an error, so it deliberately isn't a 404.
     */
    @PostMapping("/claim")
    public ResponseEntity<Task> claim(@RequestParam String workerId) {
        return taskRepository.claimNextTask(workerId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.noContent().build());
    }

    /**
     * Called on a fixed interval (2-5s) by whichever worker currently
     * holds the task, for as long as it's executing. If this stops
     * arriving, DagRunService.reapStaleTasks() eventually reclaims the
     * task on its own - the worker doesn't need to do anything special
     * to "release" it on crash, since there's nothing to release from.
     */
    @PostMapping("/{taskId}/heartbeat")
    public ResponseEntity<Void> heartbeat(@PathVariable UUID taskId, @RequestParam String workerId) {
        if (taskRepository.findById(taskId).isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        taskRepository.recordHeartbeat(taskId, workerId);
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/{taskId}/complete")
    public ResponseEntity<Void> complete(@PathVariable UUID taskId) {
        if (taskRepository.findById(taskId).isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        dagRunService.completeTask(taskId);
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/{taskId}/fail")
    public ResponseEntity<Void> fail(@PathVariable UUID taskId, @RequestBody FailRequest request) {
        if (taskRepository.findById(taskId).isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        dagRunService.failTask(taskId, request.errorMessage());
        return ResponseEntity.noContent().build();
    }

    public record FailRequest(String errorMessage) {
    }
}