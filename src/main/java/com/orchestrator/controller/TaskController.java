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


@RestController
@RequestMapping("/api/tasks")
public class TaskController {

    private final TaskRepository taskRepository;
    private final DagRunService dagRunService;

    public TaskController(TaskRepository taskRepository, DagRunService dagRunService) {
        this.taskRepository = taskRepository;
        this.dagRunService = dagRunService;
    }


    // pick up tasks if you are a worker
    // you become assinged to that task
    @PostMapping("/claim")
    public ResponseEntity<Task> claim(@RequestParam String workerId) {
        return taskRepository.claimNextTask(workerId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.noContent().build());
    }

    // heartbeat a worker has to send every 5 seconds
    // missing heartbeats cause the worker to be assumed dead and the task to be 
    //sent to someone else
    @PostMapping("/{taskId}/heartbeat")
    public ResponseEntity<Void> heartbeat(@PathVariable UUID taskId, @RequestParam String workerId) {
        if (taskRepository.findById(taskId).isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        taskRepository.recordHeartbeat(taskId, workerId);
        return ResponseEntity.noContent().build();
    }

    // worker sends this to mark a task as complete
    @PostMapping("/{taskId}/complete")
    public ResponseEntity<Void> complete(@PathVariable UUID taskId) {
        if (taskRepository.findById(taskId).isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        dagRunService.completeTask(taskId);
        return ResponseEntity.noContent().build();
    }

    // worker sends this to mark a task as failed
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