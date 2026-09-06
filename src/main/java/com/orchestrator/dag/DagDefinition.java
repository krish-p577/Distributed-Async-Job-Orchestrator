package com.orchestrator.dag;

import java.util.List;

public record DagDefinition(String name, List<TaskDefinition> tasks) {
}