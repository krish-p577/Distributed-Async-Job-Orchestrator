package com.orchestrator.worker;

import com.orchestrator.model.Task;

import java.util.function.Consumer;

/**
 * Stands in for the real business logic a task_type would trigger - an
 * actual SQL transform, an actual LLM call, an actual S3 upload. Each
 * type just sleeps for a plausible duration and reports progress; this
 * is the one place you'd plug in real integrations later without
 * touching anything in WorkerProcess.
 */
public class TaskExecutor {

    public void execute(Task task, Consumer<String> onProgress) throws InterruptedException {
        int steps = switch (task.taskType()) {
            case "FETCH" -> 2;
            case "SQL_TRANSFORM" -> 3;
            case "LLM_SUMMARY" -> 4;
            case "S3_UPLOAD" -> 2;
            case "SLACK_NOTIFY" -> 1;
            default -> 2;
        };

        for (int i = 1; i <= steps; i++) {
            Thread.sleep(1000);
            onProgress.accept("working (" + i + "/" + steps + ")");
        }
    }
}