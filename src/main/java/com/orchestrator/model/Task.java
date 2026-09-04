package com.orchestrator.model;
 
import java.time.Instant;
import java.util.UUID;
 
public record Task(
        UUID id,
        UUID dagRunId,
        String taskKey,
        String taskType,
        TaskStatus status,
        int retryCount,
        int maxRetries,
        String workerId,
        UUID idempotencyKey,
        Instant claimedAt,
        Instant lastHeartbeatAt,
        Instant startedAt,
        Instant completedAt,
        String errorMessage
) {
    public boolean canRetry() {
        return retryCount < maxRetries;
    }
}
