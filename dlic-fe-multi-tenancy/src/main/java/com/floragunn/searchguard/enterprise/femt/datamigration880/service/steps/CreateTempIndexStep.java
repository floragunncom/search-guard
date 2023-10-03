package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationStep;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;

import java.util.Objects;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.OK;

class CreateTempIndexStep implements MigrationStep {

    private final IndexSettingsManager indexSettingsManager;

    public CreateTempIndexStep(IndexSettingsManager indexSettingsManager) {
        this.indexSettingsManager = Objects.requireNonNull(indexSettingsManager, "Index settings duplicator is required");
    }

    @Override
    public StepResult execute(DataMigrationContext context) throws StepException {
        String globalTenantIndexName = context.getGlobalTenantIndexName();
        String tempIndexName = context.getTempIndexName();
        IndexSettingsManager.BasicIndexSettings settings = indexSettingsManager //
            .createIndexWithClonedSettings(globalTenantIndexName, tempIndexName, true);
        String message = "Temporary index '" + tempIndexName + "' created with " + settings.numberOfShards() + " shards, replicas "
            + settings.numberOfReplicas() + " and total mapping fields limit " + settings.mappingsTotalFieldsLimit();
        String details = "Temp index mappings " + settings.mappings();
        return new StepResult(OK, message, details);
    }

    @Override
    public String name() {
        return "create temp index";
    }
}
