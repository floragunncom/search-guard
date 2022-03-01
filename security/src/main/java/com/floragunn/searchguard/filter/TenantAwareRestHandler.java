package com.floragunn.searchguard.filter;

import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;

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
