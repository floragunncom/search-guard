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
