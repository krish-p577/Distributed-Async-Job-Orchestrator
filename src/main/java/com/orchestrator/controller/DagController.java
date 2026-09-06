package com.orchestrator.controller;

import com.orchestrator.dag.DagDefinition;
import com.orchestrator.dag.DagDefinitionParser;
import com.orchestrator.dag.DagPersistenceService;
import com.orchestrator.dag.DagValidationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/dag-runs")
public class DagController {

    private final DagDefinitionParser parser;
    private final DagPersistenceService persistenceService;

    public DagController(DagDefinitionParser parser, DagPersistenceService persistenceService) {
        this.parser = parser;
        this.persistenceService = persistenceService;
    }

    /**
     * Accepts a raw YAML DAG definition, validates it (unique keys, known
     * dependency references, no cycles), persists it, and starts a run.
     * Root tasks (no dependencies) are inserted already QUEUED, so a
     * worker can start claiming work immediately after this returns.
     */
    @PostMapping(consumes = "text/yaml")
    public ResponseEntity<Map<String, UUID>> createDagRun(@RequestBody String yaml) {
        DagDefinition definition = parser.parse(yaml);
        UUID dagRunId = persistenceService.registerAndStart(definition);
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("dagRunId", dagRunId));
    }

    @ExceptionHandler(DagValidationException.class)
    public ResponseEntity<Map<String, String>> handleInvalidDag(DagValidationException ex) {
        return ResponseEntity.badRequest().body(Map.of("error", ex.getMessage()));
    }
}