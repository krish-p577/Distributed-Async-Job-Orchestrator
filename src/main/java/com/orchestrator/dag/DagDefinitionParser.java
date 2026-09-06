package com.orchestrator.dag;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;



// parse DAG definition from YAML
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