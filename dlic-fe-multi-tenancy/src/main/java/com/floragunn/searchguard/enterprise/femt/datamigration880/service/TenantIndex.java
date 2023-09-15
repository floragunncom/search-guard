package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

import com.floragunn.searchguard.authz.config.Tenant;
import org.elasticsearch.common.Strings;

import javax.annotation.Nullable;

import static org.elasticsearch.common.Strings.requireNonEmpty;

public record TenantIndex(String indexName, @Nullable String tenantName) {

    public TenantIndex {
        requireNonEmpty(indexName, "Tenant index name is required");
    }
    public boolean belongsToGlobalTenant() {
        return Tenant.GLOBAL_TENANT_ID.equals(tenantName);
    }

    public boolean belongsToUserPrivateTenant() {
        return Strings.isNullOrEmpty(tenantName);
    }
}
