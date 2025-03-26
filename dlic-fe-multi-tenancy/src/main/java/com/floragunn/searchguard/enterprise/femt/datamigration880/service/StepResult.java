/*
 * Copyright 2023 by floragunn GmbH - All rights reserved
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */
package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

import jakarta.annotation.Nullable;

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
