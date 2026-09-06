package com.orchestrator.controller;

import com.orchestrator.model.WorkerStatus;
import com.orchestrator.repository.TaskRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/workers")
public class WorkerStatusController {

    private final TaskRepository taskRepository;

    public WorkerStatusController(TaskRepository taskRepository) {
        this.taskRepository = taskRepository;
    }

    // get a view of all the current workers and their status
    // from the posgres database
    @GetMapping("/status")
    public List<WorkerStatus> status() {
        return taskRepository.findActiveWorkerStatus();
    }
}