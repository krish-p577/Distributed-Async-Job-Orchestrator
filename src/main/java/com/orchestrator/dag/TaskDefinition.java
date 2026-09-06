package com.orchestrator.dag;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public record TaskDefinition(
        String key,
        String type,
        Map<String, Object> config,
        @JsonProperty("depends_on") List<String> dependsOn
) {
   
    public TaskDefinition {
        dependsOn = dependsOn == null ? List.of() : dependsOn;
        config = config == null ? Map.of() : config;
    }
}