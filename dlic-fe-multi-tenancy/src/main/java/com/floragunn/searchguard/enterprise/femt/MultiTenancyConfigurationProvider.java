package com.floragunn.searchguard.enterprise.femt;

import com.floragunn.fluent.collections.ImmutableSet;

import java.util.Objects;
import java.util.Optional;

public class MultiTenancyConfigurationProvider {
    private final FeMultiTenancyModule module;

    public MultiTenancyConfigurationProvider(FeMultiTenancyModule module) {
        this.module = Objects.requireNonNull(module, "Front-end multi-tenancy module is required");
    }

    public Optional<FeMultiTenancyConfig> getConfig() {
        return Optional.ofNullable(module.getConfig());
    }

    public ImmutableSet<String> getTenantNames() {
        return module.getTenantNames();
    }
}
