package com.floragunn.searchguard.authz;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.user.User;
import org.elasticsearch.ElasticsearchException;

import java.util.Set;

public class TenantManager {

    private static final String USER_TENANT = "__user__";

    private final ImmutableSet<String> configuredTenants;

    public TenantManager(Set<String> tenantNames) {
        this.configuredTenants = ImmutableSet.of(tenantNames);
    }

    public boolean isTenantHeaderValid(String tenant) {
        return Tenant.GLOBAL_TENANT_ID.equals(tenant) || USER_TENANT.equals(tenant) || configuredTenants.contains(tenant);
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
