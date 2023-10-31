package com.floragunn.searchguard.enterprise.femt.tenants;

import com.floragunn.codova.documents.Document;
import com.floragunn.fluent.collections.OrderedImmutableMap;

record TenantAccessData(boolean readAccess, boolean writeAccess, boolean exists) implements Document<TenantAccessData> {
    public static final String FIELD_READ_ACCESS = "read_access";
    public static final String FIELD_WRITE_ACCESS = "write_access";
    public static final String FIELD_EXIST = "exists";


    @Override
    public OrderedImmutableMap<String, Object> toBasicObject() {
        return OrderedImmutableMap.of(FIELD_READ_ACCESS, readAccess,
            FIELD_WRITE_ACCESS, writeAccess,
            FIELD_EXIST, exists);
    }
}
