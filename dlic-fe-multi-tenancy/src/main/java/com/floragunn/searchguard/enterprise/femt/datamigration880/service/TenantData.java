package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

import com.floragunn.searchguard.authz.config.Tenant;
import com.google.common.base.Strings;

import javax.annotation.Nullable;

public record TenantData(String indexName, @Nullable String tenantName) {

    public TenantData {
        org.elasticsearch.common.Strings.requireNonEmpty(indexName, "Tenant index name is required");
    }
    public boolean isGlobal() {
        return Tenant.GLOBAL_TENANT_ID.equals(tenantName);
    }

    public boolean isUserTenant() {
        return Strings.isNullOrEmpty(tenantName);
    }
}
