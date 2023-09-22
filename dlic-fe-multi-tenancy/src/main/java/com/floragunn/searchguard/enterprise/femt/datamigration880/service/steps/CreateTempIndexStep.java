package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationStep;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.common.settings.Settings;

import java.util.Objects;
import java.util.Optional;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.NO_GLOBAL_TENANT_SETTINGS;

class CreateTempIndexStep implements MigrationStep {

    private final StepRepository stepRepository;

    public CreateTempIndexStep(StepRepository stepRepository) {
        this.stepRepository = Objects.requireNonNull(stepRepository, "Step repository is required");
    }

    @Override
    public StepResult execute(DataMigrationContext context) throws StepException {
        String globalTenantIndexName = context.getGlobalTenantIndexName();
        GetSettingsResponse response = stepRepository.getIndexSettings(globalTenantIndexName);
        Settings settings = Optional.ofNullable(response) //
            .map(GetSettingsResponse::getIndexToSettings) //
            .map(settingsMap -> settingsMap.get(globalTenantIndexName)) //
            .orElseThrow(() -> new StepException(
                "Cannot find global tenant index settings",
                NO_GLOBAL_TENANT_SETTINGS,
                "Cannot load index '" + globalTenantIndexName + "' settings"));
        return new StepResult(StepExecutionStatus.OK, "Step is not implemented.");
    }

    @Override
    public String name() {
        return "create temp index";
    }
}
