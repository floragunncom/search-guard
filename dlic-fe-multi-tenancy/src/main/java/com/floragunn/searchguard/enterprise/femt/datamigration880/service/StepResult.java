package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

import javax.annotation.Nullable;
import java.util.Objects;

public record StepResult(StepExecutionStatus status, String message, @Nullable String details) {
    public StepResult {
        Objects.requireNonNull(status, "Status is required");
        Objects.requireNonNull(message, "Message is required");
    }

    public StepResult(StepExecutionStatus status, String message) {
        this(status, message, null);
    }

    public boolean isSuccess() {
        return status.isSuccess();
    }

    public boolean isFailure() {
        return ! status.isSuccess();
    }
}