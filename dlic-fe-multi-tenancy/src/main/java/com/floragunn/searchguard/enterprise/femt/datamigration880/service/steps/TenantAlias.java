package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import javax.annotation.Nullable;

import static org.elasticsearch.common.Strings.requireNonEmpty;

record TenantAlias(String aliasName, @Nullable String tenantName) {
    public TenantAlias {
        requireNonEmpty(aliasName, "Tenant index alias name is required");
    }
}
