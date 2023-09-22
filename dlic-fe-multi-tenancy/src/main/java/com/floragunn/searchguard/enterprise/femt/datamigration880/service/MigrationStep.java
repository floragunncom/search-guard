package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

import com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.StepException;

public interface MigrationStep {

    StepResult execute(DataMigrationContext dataMigrationContext) throws StepException;

    String name();

}
