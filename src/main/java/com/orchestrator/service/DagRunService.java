package com.orchestrator.service;

import com.orchestrator.model.Task;
import com.orchestrator.repository.TaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

@Service
public class DagRunService {

    private static final Logger log = LoggerFactory.getLogger(DagRunService.class);

    private static final Duration HEARTBEAT_TIMEOUT = Duration.ofSeconds(15);

    private final TaskRepository taskRepository;

    public DagRunService(TaskRepository taskRepository) {
        this.taskRepository = taskRepository;
    }

    
    // marks a task as complete, then checks all downstream tasks to see if they can be
    //  queued
    @Transactional
    public void completeTask(UUID taskId) {
        taskRepository.markCompleted(taskId);
        log.info("Task {} COMPLETED", taskId);

        for (Task downstream : taskRepository.findDownstreamTasks(taskId)) {
            if (taskRepository.allDependenciesCompleted(downstream.id())) {
                taskRepository.markQueued(downstream.id());
                log.info("Task {} ({}) unlocked -> QUEUED", downstream.id(), downstream.taskKey());
            }
        }
    }

    // sets the task as queued, or failed if max retries is passed
    @Transactional
    public void failTask(UUID taskId, String errorMessage) {
        taskRepository.failOrRetry(taskId, errorMessage);
        log.warn("Task {} failed/retried: {}", taskId, errorMessage);
    }


    // scheduled task that runs every 5 seconds to reap any tasks which have lost 
    // their heartbeat
    // logs the ticks for more visibility, even if no stale tasks are found
    @Scheduled(fixedDelay = 5000)
    @Transactional
    public void reapStaleTasks() {
        List<Task> stale = taskRepository.findStaleRunningTasks(HEARTBEAT_TIMEOUT);
        log.debug("Reaper scan: {} stale task(s) found", stale.size());
        for (Task task : stale) {
            log.info("Reaping task {} ({}) - no heartbeat since {}",
                    task.id(), task.taskKey(), task.lastHeartbeatAt());
            failTask(task.id(), "Heartbeat timeout - worker presumed dead");
        }
    }
}