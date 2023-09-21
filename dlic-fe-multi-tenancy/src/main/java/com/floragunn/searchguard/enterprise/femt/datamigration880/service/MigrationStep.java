package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

import com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.StepException;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.ROLLBACK;

public interface MigrationStep {

    StepResult execute(DataMigrationContext dataMigrationContext) throws StepException;

    String name();

    default StepResult rollback(DataMigrationContext dataMigrationContext) throws StepException {
        return new StepResult(ROLLBACK, "nothing to be rollback");
    }

}
