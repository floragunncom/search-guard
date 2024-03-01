package com.floragunn.searchguard.enterprise.femt.tenants;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;

public class DefaultTenantNotFoundException extends ElasticsearchStatusException {

    public DefaultTenantNotFoundException() {
        super("Default tenant for current user not found", RestStatus.UNAUTHORIZED);
    }
}
