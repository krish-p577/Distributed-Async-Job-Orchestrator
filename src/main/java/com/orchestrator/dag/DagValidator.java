package com.orchestrator.dag;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;



// creates topological order of tasks using Kahn's algorithm
// creates acyclic graph to insure no cycles
public final class DagValidator {

    private DagValidator() {
    }

    public static List<String> topologicalOrder(DagDefinition dag) {
        Map<String, TaskDefinition> byKey = new HashMap<>();
        for (TaskDefinition task : dag.tasks()) {
            if (byKey.put(task.key(), task) != null) {
                throw new DagValidationException("Duplicate task key: " + task.key());
            }
        }

        for (TaskDefinition task : dag.tasks()) {
            for (String dep : task.dependsOn()) {
                if (!byKey.containsKey(dep)) {
                    throw new DagValidationException(
                            "Task '" + task.key() + "' depends on unknown task '" + dep + "'");
                }
            }
        }

        // in-degree = number of not-yet-satisfied dependencies per task.
        Map<String, Integer> inDegree = new HashMap<>();
        // children = reverse edges: for a given task, who depends on it.
        Map<String, List<String>> children = new HashMap<>();
        for (String key : byKey.keySet()) {
            inDegree.put(key, 0);
            children.put(key, new ArrayList<>());
        }
        for (TaskDefinition task : dag.tasks()) {
            inDegree.put(task.key(), task.dependsOn().size());
            for (String dep : task.dependsOn()) {
                children.get(dep).add(task.key());
            }
        }

        // Kahn's algorithm: repeatedly peel off nodes with no remaining
        // unsatisfied dependencies. Order of removal is a valid execution
        // order for the whole graph.
        Deque<String> ready = new ArrayDeque<>();
        for (Map.Entry<String, Integer> entry : inDegree.entrySet()) {
            if (entry.getValue() == 0) {
                ready.add(entry.getKey());
            }
        }

        List<String> order = new ArrayList<>();
        while (!ready.isEmpty()) {
            String current = ready.poll();
            order.add(current);
            for (String child : children.get(current)) {
                int updated = inDegree.merge(child, -1, Integer::sum);
                if (updated == 0) {
                    ready.add(child);
                }
            }
        }

        // Anything never reduced to in-degree 0 is part of (or downstream
        // of) a cycle - report exactly those nodes rather than a generic
        // "invalid DAG" message.
        if (order.size() != byKey.size()) {
            Set<String> unresolved = new HashSet<>(byKey.keySet());
            unresolved.removeAll(order);
            throw new DagValidationException("DAG contains a cycle involving: " + unresolved);
        }

        return order;
    }
}