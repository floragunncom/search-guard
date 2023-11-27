/*
 * Copyright 2023 by floragunn GmbH - All rights reserved
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
