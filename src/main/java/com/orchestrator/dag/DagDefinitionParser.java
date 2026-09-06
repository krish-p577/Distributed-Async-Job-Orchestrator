package com.orchestrator.dag;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Parses a DAG definition from YAML. This step only builds the Java
 * object graph - it does NOT check that the graph is acyclic or that
 * dependency references resolve to real tasks. That's DagValidator's
 * job; callers should always run the parsed result through
 * DagValidator.topologicalOrder(...) before persisting or scheduling
 * anything from it.
 */
@Component
public class DagDefinitionParser {

    private final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());

    public DagDefinition parse(String yaml) {
        try {
            return yamlMapper.readValue(yaml, DagDefinition.class);
        } catch (IOException e) {
            throw new DagValidationException("Malformed DAG YAML: " + e.getMessage());
        }
    }
}