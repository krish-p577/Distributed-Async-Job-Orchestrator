package com.orchestrator.controller;

import com.orchestrator.model.WorkerStatus;
import com.orchestrator.repository.TaskRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * A live cross-worker view, read straight from Postgres rather than any
 * separate registry the workers push into - so it can never drift out of
 * sync with what claimNextTask/heartbeat/complete actually did. This is
 * also the natural data source for the future dashboard's "current
 * state" panel.
 */
@RestController
@RequestMapping("/api/workers")
public class WorkerStatusController {

    private final TaskRepository taskRepository;

    public WorkerStatusController(TaskRepository taskRepository) {
        this.taskRepository = taskRepository;
    }

    @GetMapping("/status")
    public List<WorkerStatus> status() {
        return taskRepository.findActiveWorkerStatus();
    }
}