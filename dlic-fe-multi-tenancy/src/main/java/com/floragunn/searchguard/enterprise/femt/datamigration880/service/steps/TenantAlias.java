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
package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;


import jakarta.annotation.Nullable;

import static org.elasticsearch.common.Strings.requireNonEmpty;

record TenantAlias(String aliasName, @Nullable String tenantName) {
    public TenantAlias {
        requireNonEmpty(aliasName, "Tenant index alias name is required");
    }
}
