package com.orchestrator.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.orchestrator.model.Task;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A standalone worker process. Deliberately NOT a Spring bean and NOT
 * part of the orchestrator's JVM - this is its own program that talks to
 * the orchestrator purely over HTTP, exactly like a worker on a
 * different machine would. Run several of these under different worker
 * ids to watch them compete for QUEUED tasks and pick up each other's
 * retries.
 *
 * Usage:
 *   java -cp &lt;classpath&gt; com.orchestrator.worker.WorkerProcess worker-1
 *   java -cp &lt;classpath&gt; com.orchestrator.worker.WorkerProcess worker-2 --crash-after-first-claim
 *
 * The --crash-after-first-claim flag claims one task, sends a couple of
 * heartbeats, then halts the JVM without ever calling /complete or
 * /fail - simulating a hard crash so you can watch the orchestrator's
 * reaper reclaim the task on its own.
 */
public class WorkerProcess {

    private static final String BASE_URL = System.getenv().getOrDefault("ORCHESTRATOR_URL", "http://localhost:8080");
    private static final Duration HEARTBEAT_INTERVAL = Duration.ofSeconds(3);
    private static final Duration POLL_INTERVAL = Duration.ofSeconds(2);

    private final String workerId;
    private final boolean crashAfterFirstClaim;
    private final HttpClient http = HttpClient.newHttpClient();
    private final ObjectMapper json = buildObjectMapper();
    private final TaskExecutor executor = new TaskExecutor();

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Usage: WorkerProcess <workerId> [--crash-after-first-claim]");
            System.exit(1);
        }
        boolean crash = Arrays.asList(args).contains("--crash-after-first-claim");
        new WorkerProcess(args[0], crash).run();
    }

    public WorkerProcess(String workerId, boolean crashAfterFirstClaim) {
        this.workerId = workerId;
        this.crashAfterFirstClaim = crashAfterFirstClaim;
    }

    private static ObjectMapper buildObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        return mapper;
    }

    private void run() {
        log("STARTED", "polling " + BASE_URL);
        boolean firstClaim = true;

        while (true) {
            try {
                Optional<Task> claimed = claim();
                if (claimed.isEmpty()) {
                    log("IDLE", "no queued tasks");
                    sleep(POLL_INTERVAL);
                    continue;
                }

                Task task = claimed.get();
                log("CLAIMED", task.taskKey() + " (" + task.taskType() + ")");

                if (firstClaim && crashAfterFirstClaim) {
                    simulateCrash(task);
                    return; // unreachable after halt(), kept for clarity
                }
                firstClaim = false;

                ScheduledExecutorService heartbeats = startHeartbeats(task);
                try {
                    executor.execute(task, step -> log("STEP", task.taskKey() + ": " + step));
                    complete(task);
                    log("COMPLETED", task.taskKey());
                } catch (Exception e) {
                    fail(task, String.valueOf(e.getMessage()));
                    log("FAILED", task.taskKey() + ": " + e.getMessage());
                } finally {
                    heartbeats.shutdownNow();
                }
            } catch (Exception e) {
                // A transient HTTP/parse error shouldn't kill the whole
                // worker process - log it and keep polling.
                log("ERROR", String.valueOf(e.getMessage()));
                sleep(POLL_INTERVAL);
            }
        }
    }

    private void simulateCrash(Task task) {
        heartbeat(task);
        sleep(Duration.ofSeconds(3));
        heartbeat(task);
        log("SIMULATING CRASH", task.taskKey() + " - halting without completing");
        Runtime.getRuntime().halt(1);
    }

    private ScheduledExecutorService startHeartbeats(Task task) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> heartbeat(task),
                HEARTBEAT_INTERVAL.toSeconds(), HEARTBEAT_INTERVAL.toSeconds(), TimeUnit.SECONDS);
        return scheduler;
    }

    private Optional<Task> claim() {
        HttpRequest request = HttpRequest.newBuilder(URI.create(BASE_URL + "/api/tasks/claim?workerId=" + workerId))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        HttpResponse<String> response = send(request);
        if (response.statusCode() == 204) {
            return Optional.empty();
        }
        return Optional.of(readTask(response.body()));
    }

    private void heartbeat(Task task) {
        HttpRequest request = HttpRequest.newBuilder(
                        URI.create(BASE_URL + "/api/tasks/" + task.id() + "/heartbeat?workerId=" + workerId))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        send(request);
        log("HEARTBEAT", task.taskKey());
    }

    private void complete(Task task) {
        HttpRequest request = HttpRequest.newBuilder(URI.create(BASE_URL + "/api/tasks/" + task.id() + "/complete"))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        send(request);
    }

    private void fail(Task task, String errorMessage) {
        String safeMessage = errorMessage == null ? "unknown error" : errorMessage.replace("\"", "'");
        String body = "{\"errorMessage\":\"" + safeMessage + "\"}";
        HttpRequest request = HttpRequest.newBuilder(URI.create(BASE_URL + "/api/tasks/" + task.id() + "/fail"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        send(request);
    }

    private HttpResponse<String> send(HttpRequest request) {
        try {
            return http.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("HTTP call failed: " + e.getMessage(), e);
        }
    }

    private Task readTask(String body) {
        try {
            return json.readValue(body, Task.class);
        } catch (IOException e) {
            throw new RuntimeException("Could not parse task response: " + e.getMessage(), e);
        }
    }

    private void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void log(String event, String detail) {
        System.out.printf("%s [%s] %-18s %s%n", Instant.now(), workerId, event, detail);
    }
}