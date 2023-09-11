package com.floragunn.searchguard.enterprise.femt;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.user.User;
import org.elasticsearch.ElasticsearchException;

import java.util.Set;

public class TenantManager {

    private static final String USER_TENANT = "__user__";

    private final ImmutableSet<String> tenantNames;

    public TenantManager(Set<String> tenantNames) {
        this.tenantNames = ImmutableSet.of(tenantNames).with(Tenant.GLOBAL_TENANT_ID);
    }

    public boolean isTenantValid(String tenant) {
        return USER_TENANT.equals(tenant) || tenantNames.contains(tenant);
    }

    public boolean isUserTenant(String tenant) {
        return USER_TENANT.equals(tenant);
    }

    public boolean isGlobalTenant(String tenant) {
        return tenant == null || tenant.isEmpty();
    }

    public String toInternalTenantName(User user) {
        final String requestedTenant = user.getRequestedTenant();
        if (isUserTenant(requestedTenant)) {
            return toInternalTenantName(user.getName());
        } else {
            return toInternalTenantName(requestedTenant);
        }
    }

    public ImmutableSet<String> getTenantNames() {
        return tenantNames;
    }

    private String toInternalTenantName(String tenant) {
        if (tenant == null) {
            throw new ElasticsearchException("tenant must not be null here");
        }
        String tenantWithoutUnwantedChars = tenant.toLowerCase().replaceAll("[^a-z0-9]+", "");
        return String.format("%d_%s", tenant.hashCode(), tenantWithoutUnwantedChars);
    }
}
