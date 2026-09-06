package com.orchestrator.dag;

public class DagValidationException extends RuntimeException {
    public DagValidationException(String message) {
        super(message);
    }
}