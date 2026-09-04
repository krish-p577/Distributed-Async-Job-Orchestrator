package com.orchestrator.service;
 
import com.orchestrator.model.Task;
import com.orchestrator.repository.TaskRepository;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
 
import java.time.Duration;
import java.util.UUID;
 
@Service
public class DagRunService {
 
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
 
        for (Task downstream : taskRepository.findDownstreamTasks(taskId)) {
            if (taskRepository.allDependenciesCompleted(downstream.id())) {
                taskRepository.markQueued(downstream.id());
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
    }
 
    /**
     * The failure-recovery daemon. Runs every 5s, finds RUNNING tasks
     * whose last heartbeat is older than HEARTBEAT_TIMEOUT, and treats
     * each one exactly like an explicit failure report: the worker is
     * presumed dead (crashed, lost power, network-partitioned - the
     * orchestrator can't tell which, and doesn't need to) and the task
     * goes back into the queue for another worker, up to the retry limit.
     */
    @Scheduled(fixedDelay = 5000)
    @Transactional
    public void reapStaleTasks() {
        for (Task stale : taskRepository.findStaleRunningTasks(HEARTBEAT_TIMEOUT)) {
            failTask(stale.id(), "Heartbeat timeout - worker presumed dead");
        }
    }
}
