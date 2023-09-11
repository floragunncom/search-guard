package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.ExecutionStatus;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationStep;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;

import java.util.Objects;

class PopulateIndexNamesStep implements MigrationStep {

    private final ConfigurationRepository configurationRepository;

    public PopulateIndexNamesStep(ConfigurationRepository configurationRepository) {
        this.configurationRepository = Objects.requireNonNull(configurationRepository, "Configuration repository is required");
    }

    @Override
    public StepResult execute(DataMigrationContext dataMigrationContext) {
        return new StepResult(ExecutionStatus.SUCCESS, "Not implemented yet.");
    }

    @Override
    public String name() {
        return "Populate index names";
    }
}
