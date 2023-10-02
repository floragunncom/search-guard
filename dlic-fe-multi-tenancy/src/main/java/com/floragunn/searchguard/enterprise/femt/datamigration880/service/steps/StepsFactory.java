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
        IndexSettingsDuplicator duplicator = new IndexSettingsDuplicator(repository);
        return ImmutableList.of(new PopulateTenantsStep(configurationProvider, repository),
            new PopulateBackupIndicesStep(repository),
            new CheckIndicesStateStep(repository),
            new CheckIfIndicesAreBlockedStep(repository),
            new WriteBlockStep(repository),
            new CreateTempIndexStep(duplicator),
            new CopyDataToTempIndexStep(repository, configurationProvider),
            new CreateBackupStep(repository, duplicator),
            new VerifyPreviousBackupStep(repository, duplicator),
            new AddMigrationMarkerToGlobalTenantIndexStep(duplicator),
            new UnblockDataIndicesStep(repository),
            new DeleteTempIndexStep(repository));
    }
}
