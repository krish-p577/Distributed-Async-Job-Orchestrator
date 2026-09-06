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

    // A worker is presumed dead after missing this many heartbeat windows.
    // With a 5s heartbeat interval this gives ~15s before a task is
    // reassigned - tune based on expected task duration and network jitter.
    private static final Duration HEARTBEAT_TIMEOUT = Duration.ofSeconds(15);

    private final TaskRepository taskRepository;

    public DagRunService(TaskRepository taskRepository) {
        this.taskRepository = taskRepository;
    }

    /**
     * Call when a worker reports a task finished successfully. Marks it
     * COMPLETED, then walks its outgoing edges and unlocks (PENDING ->
     * QUEUED) any downstream task whose dependencies are now all
     * satisfied. Wrapped in a transaction so a crash mid-cascade can't
     * leave part of the DAG unlocked and part not.
     */
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

    /**
     * Call when a worker explicitly reports a task failed. Whether this
     * lands the task back in QUEUED or in terminal FAILED is decided by
     * TaskRepository.failOrRetry based on the task's remaining retry
     * budget.
     */
    @Transactional
    public void failTask(UUID taskId, String errorMessage) {
        taskRepository.failOrRetry(taskId, errorMessage);
        log.warn("Task {} failed/retried: {}", taskId, errorMessage);
    }

    /**
     * The failure-recovery daemon. Runs every 5s, finds RUNNING tasks
     * whose last heartbeat is older than HEARTBEAT_TIMEOUT, and treats
     * each one exactly like an explicit failure report: the worker is
     * presumed dead (crashed, lost power, network-partitioned - the
     * orchestrator can't tell which, and doesn't need to) and the task
     * goes back into the queue for another worker, up to the retry limit.
     *
     * Logs on every tick, even when nothing is found - if this line ever
     * stops appearing in the console, the scheduler itself has stopped
     * running (check @EnableScheduling), which is a different problem
     * than the reaper running but finding nothing stale.
     */
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