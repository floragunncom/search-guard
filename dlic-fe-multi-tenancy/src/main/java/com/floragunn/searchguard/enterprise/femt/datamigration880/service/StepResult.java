package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

import java.util.Objects;

public record StepResult(ExecutionStatus status, String message) {
    public StepResult {
        Objects.requireNonNull(status, "Status is required");
        Objects.requireNonNull(message, "Message is required");
    }

    public boolean isSuccess() {
        return ExecutionStatus.SUCCESS.equals(status);
    }
}
