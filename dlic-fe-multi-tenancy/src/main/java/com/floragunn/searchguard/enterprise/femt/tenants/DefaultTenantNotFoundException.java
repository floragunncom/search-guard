/*
 * Copyright 2024 by floragunn GmbH - All rights reserved
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

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;

class DefaultTenantNotFoundException extends ElasticsearchStatusException {

    public DefaultTenantNotFoundException(String username) {
        super(String.format("Default tenant for user: '%s' not found", username), RestStatus.UNAUTHORIZED);
    }
}
