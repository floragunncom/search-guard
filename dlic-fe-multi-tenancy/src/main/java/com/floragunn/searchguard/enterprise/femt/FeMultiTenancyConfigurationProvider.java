package com.floragunn.searchguard.enterprise.femt;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.config.MultiTenancyConfigurationProvider;

import java.util.Objects;
import java.util.Optional;

public class FeMultiTenancyConfigurationProvider implements MultiTenancyConfigurationProvider {
    private final FeMultiTenancyModule module;

    public FeMultiTenancyConfigurationProvider(FeMultiTenancyModule module) {
        this.module = Objects.requireNonNull(module, "Front-end multi-tenancy module is required");
    }

    public Optional<FeMultiTenancyConfig> getConfig() {
        return Optional.ofNullable(module.getConfig());
    }

    public ImmutableSet<String> getTenantNames() {
        return module.getTenantNames();
    }

    @Override
    public boolean isMultiTenancyEnabled() {
        return getConfig().map(FeMultiTenancyConfig::isEnabled)
                .orElse(module.isEnabled());
    }

    @Override
    public String getKibanaServerUser() {
        return getConfig().map(FeMultiTenancyConfig::getServerUsername)
                .orElse("kibanaserver");
    }

    @Override
    public String getKibanaIndex() {
        return getConfig().map(FeMultiTenancyConfig::getServerUsername)
                .orElse(".kibana");
    }
}

