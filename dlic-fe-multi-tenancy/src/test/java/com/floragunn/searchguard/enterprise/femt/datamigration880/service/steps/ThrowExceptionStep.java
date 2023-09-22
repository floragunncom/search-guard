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
