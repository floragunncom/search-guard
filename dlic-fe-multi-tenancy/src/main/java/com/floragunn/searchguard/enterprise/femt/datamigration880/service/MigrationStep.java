package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

public interface MigrationStep {

    StepResult execute(DataMigrationContext dataMigrationContext);

    String name();

}
