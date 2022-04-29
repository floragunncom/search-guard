package com.floragunn.searchguard.authc.rest;

import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;

public interface TenantAwareRestHandler extends RestHandler {
    default String getTenantParamName() {
        return "tenant";
    }

    default String getTenantName(RestRequest request) {
        String result = request.param(getTenantParamName());
        
        if ("_main".equals(result)) {
            return null;
        } else {
            return result;
        }
    }
}
