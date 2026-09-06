package com.orchestrator.model;

import java.time.Instant;
import java.util.UUID;

public record WorkerStatus(
        String workerId,
        String taskKey,
        String taskType,
        UUID dagRunId,
        Instant lastHeartbeatAt
) {
}