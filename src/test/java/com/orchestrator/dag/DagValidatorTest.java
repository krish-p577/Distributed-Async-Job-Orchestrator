package com.orchestrator.dag;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DagValidatorTest {

    @Test
    void ordersTasksByDependency() {
        DagDefinition dag = new DagDefinition("test", List.of(
                new TaskDefinition("c", "TYPE", Map.of(), List.of("b")),
                new TaskDefinition("a", "TYPE", Map.of(), List.of()),
                new TaskDefinition("b", "TYPE", Map.of(), List.of("a"))
        ));

        assertThat(DagValidator.topologicalOrder(dag)).containsExactly("a", "b", "c");
    }

    @Test
    void rejectsCycles() {
        DagDefinition dag = new DagDefinition("cyclic", List.of(
                new TaskDefinition("a", "TYPE", Map.of(), List.of("c")),
                new TaskDefinition("b", "TYPE", Map.of(), List.of("a")),
                new TaskDefinition("c", "TYPE", Map.of(), List.of("b"))
        ));

        assertThatThrownBy(() -> DagValidator.topologicalOrder(dag))
                .isInstanceOf(DagValidationException.class)
                .hasMessageContaining("cycle");
    }

    @Test
    void rejectsUnknownDependency() {
        DagDefinition dag = new DagDefinition("bad-ref", List.of(
                new TaskDefinition("a", "TYPE", Map.of(), List.of("missing"))
        ));

        assertThatThrownBy(() -> DagValidator.topologicalOrder(dag))
                .isInstanceOf(DagValidationException.class)
                .hasMessageContaining("unknown task");
    }

    @Test
    void rejectsDuplicateKeys() {
        DagDefinition dag = new DagDefinition("dupe", List.of(
                new TaskDefinition("a", "TYPE", Map.of(), List.of()),
                new TaskDefinition("a", "TYPE", Map.of(), List.of())
        ));

        assertThatThrownBy(() -> DagValidator.topologicalOrder(dag))
                .isInstanceOf(DagValidationException.class)
                .hasMessageContaining("Duplicate");
    }
}