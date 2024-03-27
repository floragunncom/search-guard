package com.floragunn.searchguard.enterprise.femt;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.TenantAccessMapper;
import com.floragunn.searchguard.authz.config.MultiTenancyConfigurationProvider;

import java.util.Collections;
import java.util.List;
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
        return getConfig().map(FeMultiTenancyConfig::getIndex)
                .orElse(".kibana");
    }

    @Override
    public TenantAccessMapper getTenantAccessMapper() {
        return module.getTenantAccessMapper();
    }

    @Override
    public boolean isGlobalTenantEnabled() {
        return getConfig().map(FeMultiTenancyConfig::isGlobalTenantEnabled)
                .orElse(true);
    }

    @Override
    public boolean isPrivateTenantEnabled() {
        return getConfig().map(FeMultiTenancyConfig::isPrivateTenantEnabled)
                .orElse(false);
    }

    @Override
    public List<String> getPreferredTenants() {
        return getConfig().map(FeMultiTenancyConfig::getPreferredTenants)
                .orElse(Collections.emptyList());
    }
}

