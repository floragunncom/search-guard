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
package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus;

import java.io.Serial;
import java.util.Objects;

public class StepException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 2984219352347067232L;

    private final String details;
    private final StepExecutionStatus status;

    StepException(String message, StepExecutionStatus status, String details) {
        super(message);
        this.status = Objects.requireNonNull(status, "Step execution status is required.");
        this.details = details;
    }

    public StepExecutionStatus getStatus() {
        return status;
    }

    public String getDetails() {
        return details;
    }
}
