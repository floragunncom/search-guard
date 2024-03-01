package com.floragunn.searchguard.enterprise.femt.tenants;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;

class DefaultTenantNotFoundException extends ElasticsearchStatusException {

    public DefaultTenantNotFoundException(String username) {
        super(String.format("Default tenant for user: '%s' not found", username), RestStatus.UNAUTHORIZED);
    }
}
