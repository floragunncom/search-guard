package com.floragunn.searchguard.enterprise.femt.tenants;

import com.floragunn.codova.documents.Document;
import com.floragunn.fluent.collections.OrderedImmutableMap;

import java.util.Map;

record AvailableTenantData(boolean multiTenancyEnabled,  Map<String, TenantAccessData> tenants)
    implements Document<TenantAccessData> {

    private final static String FIELD_ENABLED = "multi_tenancy_enabled";
    private final static String FIELD_TENANTS = "tenants";

    @Override
    public Object toBasicObject() {
        return OrderedImmutableMap.of(FIELD_ENABLED, multiTenancyEnabled,  FIELD_TENANTS, tenants);
    }
}
