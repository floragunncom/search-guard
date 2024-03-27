/*
 * Copyright 2024 by floragunn GmbH - All rights reserved
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */
package com.floragunn.searchguard.authz;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.config.MultiTenancyConfigurationProvider;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.user.User;
import org.elasticsearch.ElasticsearchException;

import java.util.Objects;
import java.util.Set;

public class TenantManager {

    private static final String USER_TENANT = "__user__";

    private final ImmutableSet<String> configuredTenants;
    private final MultiTenancyConfigurationProvider multiTenancyConfigurationProvider;

    public TenantManager(Set<String> tenantNames, MultiTenancyConfigurationProvider multiTenancyConfigurationProvider) {
        this.configuredTenants = ImmutableSet.of(tenantNames);
        this.multiTenancyConfigurationProvider = Objects.requireNonNull(
                multiTenancyConfigurationProvider, "Multi Tenancy Configuration Provider is required"
        );
    }

    public boolean isTenantHeaderValid(String tenant) {
        if (multiTenancyConfigurationProvider.isMultiTenancyEnabled()) {
            return (Tenant.GLOBAL_TENANT_ID.equals(tenant) && multiTenancyConfigurationProvider.isGlobalTenantEnabled()) //
                || (USER_TENANT.equals(tenant) && multiTenancyConfigurationProvider.isPrivateTenantEnabled()) //
                || ((!ImmutableSet.of(Tenant.GLOBAL_TENANT_ID, USER_TENANT).contains(tenant)) && configuredTenants.contains(tenant));
        } else {
            return Tenant.GLOBAL_TENANT_ID.equals(tenant);
        }
    }

    public boolean isUserTenantHeader(String tenant) {
        return USER_TENANT.equals(tenant);
    }

    public boolean isGlobalTenantHeader(String tenant) {
        return Tenant.GLOBAL_TENANT_ID.equals(tenant);
    }

    public boolean isTenantHeaderEmpty(String tenant) {
        return tenant == null || tenant.isEmpty();
    }

    public String toInternalTenantName(User user) {
        final String requestedTenant = user.getRequestedTenant();
        if (isUserTenantHeader(requestedTenant)) {
            return toInternalTenantName(user.getName());
        } else {
            return toInternalTenantName(requestedTenant);
        }
    }

    /**
     * @return set of all tenants names as defined in configuration
     */
    public ImmutableSet<String> getConfiguredTenantNames() {
        return configuredTenants;
    }

    /**
     * @return set of all known tenant names - global tenant and tenants defined in configuration
     */
    public ImmutableSet<String> getAllKnownTenantNames() {
        return configuredTenants.with(Tenant.GLOBAL_TENANT_ID);
    }

    public static String toInternalTenantName(String tenant) {
        if (tenant == null) {
            throw new ElasticsearchException("tenant must not be null here");
        }
        String tenantWithoutUnwantedChars = tenant.toLowerCase().replaceAll("[^a-z0-9]+", "");
        return String.format("%d_%s", tenant.hashCode(), tenantWithoutUnwantedChars);
    }
}
