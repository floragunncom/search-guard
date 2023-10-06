package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.enterprise.femt.FeMultiTenancyConfigurationProvider;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationStep;
import com.floragunn.searchguard.support.PrivilegedConfigClient;

import java.util.Objects;

public class StepsFactory {
    private final PrivilegedConfigClient client;

    private final FeMultiTenancyConfigurationProvider configurationProvider;

    public StepsFactory(PrivilegedConfigClient privilegedConfigClient, FeMultiTenancyConfigurationProvider provider) {
        this.client = Objects.requireNonNull(privilegedConfigClient, "Privileged config client is required");
        this.configurationProvider = Objects.requireNonNull(provider, "Multi-tenancy configuration provider is required");
    }

    public ImmutableList<MigrationStep> createSteps() {
        StepRepository repository = new StepRepository(client);
        IndexSettingsManager settingsManager = new IndexSettingsManager(repository);
        return ImmutableList.of(new PopulateTenantsStep(configurationProvider, repository),
            new PopulateBackupIndicesStep(repository),
            new CheckIndicesStateStep(repository),
            new CheckIfIndicesAreBlockedStep(repository),
            new WriteBlockStep(repository),
            new CreateTempIndexStep(settingsManager),
            new CopyDataToTempIndexStep(repository, configurationProvider, settingsManager),
            new CreateBackupStep(repository, settingsManager),
            new VerifyPreviousBackupStep(repository, settingsManager),
            new AddMigrationMarkerToGlobalTenantIndexStep(settingsManager),
            new DeleteGlobalIndexContentStep(repository),
            new CopyDataToGlobalIndexStep(repository),
            new UnblockDataIndicesStep(repository),
            new DeleteTempIndexStep(repository));
    }
}