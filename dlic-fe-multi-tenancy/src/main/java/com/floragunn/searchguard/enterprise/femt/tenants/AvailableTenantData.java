/*
 * Copyright 2023-2024 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.femt.tenants;

import com.floragunn.codova.documents.Document;
import com.floragunn.fluent.collections.OrderedImmutableMap;

import java.util.Map;

record AvailableTenantData(boolean multiTenancyEnabled,  Map<String, TenantAccessData> tenants, String username,
                           String userRequestedTenant, String defaultTenant) implements Document<TenantAccessData> {

    private final static String FIELD_ENABLED = "multi_tenancy_enabled";
    private final static String FIELD_TENANTS = "tenants";
    private final static String FIELD_USERNAME = "username";
    private final static String USER_REQUESTED_TENANT = "user_requested_tenant";
    private final static String FIELD_DEFAULT_TENANT = "default_tenant";

    @Override
    public Object toBasicObject() {
        return OrderedImmutableMap.of(
                FIELD_ENABLED, multiTenancyEnabled,  FIELD_TENANTS, tenants, FIELD_USERNAME, username,
                USER_REQUESTED_TENANT, userRequestedTenant, FIELD_DEFAULT_TENANT, defaultTenant
        );
    }
}
