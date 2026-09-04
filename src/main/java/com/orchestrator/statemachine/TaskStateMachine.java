package com.orchestrator.statemachine;
 
import com.orchestrator.model.TaskStatus;
 
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
 
/**
 * Encodes the legal state transitions for a task:
 *
 *   PENDING  --> QUEUED, CANCELLED
 *   QUEUED   --> RUNNING, CANCELLED
 *   RUNNING  --> COMPLETED, RETRYING, FAILED
 *   RETRYING --> QUEUED, FAILED
 *   COMPLETED, FAILED, CANCELLED are terminal.
 *
 * Every state change in the system should be validated against this table
 * (or go through a repository method that already encodes one of these
 * transitions) so an illegal jump - e.g. COMPLETED -> RUNNING after a
 * duplicate/racing worker response - fails loudly instead of silently
 * corrupting the DAG run.
 */
public final class TaskStateMachine {
 
    private static final Map<TaskStatus, Set<TaskStatus>> ALLOWED_TRANSITIONS = new EnumMap<>(TaskStatus.class);
 
    static {
        ALLOWED_TRANSITIONS.put(TaskStatus.PENDING, EnumSet.of(TaskStatus.QUEUED, TaskStatus.CANCELLED));
        ALLOWED_TRANSITIONS.put(TaskStatus.QUEUED, EnumSet.of(TaskStatus.RUNNING, TaskStatus.CANCELLED));
        ALLOWED_TRANSITIONS.put(TaskStatus.RUNNING, EnumSet.of(TaskStatus.COMPLETED, TaskStatus.RETRYING, TaskStatus.FAILED));
        ALLOWED_TRANSITIONS.put(TaskStatus.RETRYING, EnumSet.of(TaskStatus.QUEUED, TaskStatus.FAILED));
        ALLOWED_TRANSITIONS.put(TaskStatus.COMPLETED, EnumSet.noneOf(TaskStatus.class));
        ALLOWED_TRANSITIONS.put(TaskStatus.FAILED, EnumSet.noneOf(TaskStatus.class));
        ALLOWED_TRANSITIONS.put(TaskStatus.CANCELLED, EnumSet.noneOf(TaskStatus.class));
    }
 
    private TaskStateMachine() {
    }
 
    public static boolean isValidTransition(TaskStatus from, TaskStatus to) {
        return ALLOWED_TRANSITIONS.getOrDefault(from, Set.of()).contains(to);
    }
 
    public static void assertValidTransition(TaskStatus from, TaskStatus to) {
        if (!isValidTransition(from, to)) {
            throw new IllegalStateException("Illegal task transition: " + from + " -> " + to);
        }
    }
 
    public static boolean isTerminal(TaskStatus status) {
        return ALLOWED_TRANSITIONS.get(status).isEmpty();
    }
}
