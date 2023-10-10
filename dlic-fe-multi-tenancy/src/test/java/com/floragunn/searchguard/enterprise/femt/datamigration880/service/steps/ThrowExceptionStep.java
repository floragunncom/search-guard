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

import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationStep;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;

public class ThrowExceptionStep implements MigrationStep {
    private final String message;
    private final StepExecutionStatus status;
    private final String details;

    public ThrowExceptionStep(String message, StepExecutionStatus status, String details) {
        this.message = message;
        this.status = status;
        this.details = details;
    }

    @Override
    public StepResult execute(DataMigrationContext dataMigrationContext) throws StepException {
        throw new StepException(message, status, details);
    }

    @Override
    public String name() {
        return "I always throw exception";
    }
}
