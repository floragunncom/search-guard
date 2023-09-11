package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.enterprise.femt.MultiTenancyConfigurationProvider;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationStep;
import com.floragunn.searchguard.support.PrivilegedConfigClient;

import java.util.Objects;

public class StepsFactory {
    private final PrivilegedConfigClient client;

    private final MultiTenancyConfigurationProvider configurationProvider;

    public StepsFactory(PrivilegedConfigClient privilegedConfigClient, MultiTenancyConfigurationProvider provider) {
        this.client = Objects.requireNonNull(privilegedConfigClient, "Privileged config client is required");
        this.configurationProvider = Objects.requireNonNull(provider, "Multi-tenancy configuration provider is required");
    }

    public ImmutableList<MigrationStep> createSteps() {
        return ImmutableList.of(new PopulateTenantsStep(configurationProvider, client));
    }
}
